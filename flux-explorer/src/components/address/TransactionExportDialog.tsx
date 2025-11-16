"use client";

import { useState } from "react";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Download, FileJson } from "lucide-react";
import { AddressTransactionSummary } from "@/types/flux-api";
import { FluxAPI } from "@/lib/api/client";
import { batchGetFluxPrices } from "@/lib/api/price-history-client";
import { DateRange } from "react-day-picker";
import { DateRangePicker } from "@/components/ui/date-range-picker";
import { addDays, startOfYear, startOfDay, endOfDay } from "date-fns";

interface TransactionExportDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  address: string;
  totalTransactions: number;
}

type ExportFormat = "csv" | "json";

// Flux blockchain genesis block timestamp (approximate)
const FLUX_GENESIS_DATE = new Date("2018-06-01");

const PRESET_RANGES = [
  { label: "Last 30 days", days: 30 },
  { label: "Last 90 days", days: 90 },
  { label: "Last 6 months", days: 180 },
  { label: "Last year", days: 365 },
  { label: "This year", value: "year" as const },
  { label: "All time", value: "all" as const },
];

export function TransactionExportDialog({
  open,
  onOpenChange,
  address,
  totalTransactions,
}: TransactionExportDialogProps) {
  const [dateRange, setDateRange] = useState<DateRange | undefined>();
  const [isExporting, setIsExporting] = useState(false);
  const [progress, setProgress] = useState(0);
  const [fetchedCount, setFetchedCount] = useState(0);
  const [targetCount, setTargetCount] = useState(0);
  const [currentStatus, setCurrentStatus] = useState("");

  const handlePresetClick = (preset: typeof PRESET_RANGES[number]) => {
    const today = new Date();
    const endDate = endOfDay(today);
    let startDate: Date;

    if (preset.value === "all") {
      startDate = FLUX_GENESIS_DATE;
    } else if (preset.value === "year") {
      startDate = startOfYear(today);
    } else {
      startDate = startOfDay(addDays(today, -preset.days!));
    }

    setDateRange({ from: startDate, to: endDate });
  };

  const handleDateRangeChange = (range: DateRange | undefined) => {
    if (!range) {
      setDateRange(undefined);
      return;
    }

    // Normalize dates to UTC midnight/end of day
    // Blockchain timestamps are in UTC, so we need to work in UTC
    const normalizedRange: DateRange = {
      from: range.from ? getUTCStartOfDay(range.from) : undefined,
      to: range.to ? getUTCEndOfDay(range.to) : range.from ? getUTCEndOfDay(range.from) : undefined,
    };

    setDateRange(normalizedRange);
  };

  // Helper: Get UTC start of day (00:00:00 UTC)
  const getUTCStartOfDay = (date: Date): Date => {
    return new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0));
  };

  // Helper: Get UTC end of day (23:59:59 UTC)
  const getUTCEndOfDay = (date: Date): Date => {
    return new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999));
  };

  const handleExport = async (format: ExportFormat) => {
    if (!dateRange?.from || !dateRange?.to) {
      alert("Please select a date range");
      return;
    }

    setIsExporting(true);
    setProgress(0);
    setFetchedCount(0);
    setTargetCount(0);
    setCurrentStatus("Fetching transactions...");

    try {
      // Convert dates to Unix timestamps (seconds)
      const fromTimestamp = Math.floor(dateRange.from.getTime() / 1000);
      const toTimestamp = Math.floor(dateRange.to.getTime() / 1000);

      const batchSize = 10000; // Fetch up to 10000 transactions per batch for faster exports
      let allTransactions: AddressTransactionSummary[] = [];
      let offset = 0;
      let hasMore = true;

      // Step 1: Fetch all transactions in the date range
      while (hasMore) {
        // Fetch batch from API using FluxAPI client
        const data = await FluxAPI.getAddressTransactions([address], {
          from: offset,
          to: offset + batchSize,
          fromTimestamp,
          toTimestamp,
        });

        const items = data.items || [];
        allTransactions = allTransactions.concat(items);

        offset += items.length; // Increment by actual items received
        setFetchedCount(allTransactions.length);

        // Update target count based on filteredTotal from first response
        if (targetCount === 0 && data.filteredTotal) {
          setTargetCount(data.filteredTotal);
        }

        // Calculate progress (first 50% is fetching transactions)
        const estimatedTotal = data.filteredTotal || allTransactions.length;
        setProgress((allTransactions.length / Math.max(estimatedTotal, 1)) * 50);

        // Break if we've received fewer transactions than requested (end of data)
        if (items.length === 0 || items.length < batchSize || allTransactions.length >= (data.filteredTotal || 0)) {
          hasMore = false;
        }
      }

      if (allTransactions.length === 0) {
        alert("No transactions found in the selected date range");
        setIsExporting(false);
        return;
      }

      // Step 2: Fetch price data for all transactions (second 50% of progress)
      setCurrentStatus("Fetching price data...");
      const timestamps = allTransactions.map(tx => tx.timestamp).filter(ts => ts > 0);

      // Batch price fetching with progress updates (chunks of 1000 timestamps)
      const priceMap = new Map<number, number | null>();
      const priceChunkSize = 1000;

      for (let i = 0; i < timestamps.length; i += priceChunkSize) {
        const chunk = timestamps.slice(i, i + priceChunkSize);
        const chunkResults = await batchGetFluxPrices(chunk);

        // Merge results into main map
        chunkResults.forEach((price, timestamp) => priceMap.set(timestamp, price));

        // Update progress (50% to 75% range)
        const chunkProgress = Math.min(i + priceChunkSize, timestamps.length);
        setProgress(50 + (chunkProgress / timestamps.length) * 25);
      }

      // Step 3: Split into multiple files if needed (100K transactions per file)
      const MAX_TRANSACTIONS_PER_FILE = 100000;
      const totalFiles = Math.ceil(allTransactions.length / MAX_TRANSACTIONS_PER_FILE);
      const dateStr = new Date().toISOString().split('T')[0];

      if (totalFiles > 1) {
        setCurrentStatus(`Generating ${totalFiles} files (100K transactions per file)...`);
      } else {
        setCurrentStatus("Generating file...");
      }

      for (let fileIndex = 0; fileIndex < totalFiles; fileIndex++) {
        const start = fileIndex * MAX_TRANSACTIONS_PER_FILE;
        const end = Math.min(start + MAX_TRANSACTIONS_PER_FILE, allTransactions.length);
        const chunk = allTransactions.slice(start, end);

        // Generate file content based on format
        let content: string;
        let filename: string;
        let mimeType: string;

        if (format === "csv") {
          content = generateCSV(chunk, address, priceMap);
          filename = totalFiles > 1
            ? `${address}_transactions_${dateStr}_part${fileIndex + 1}of${totalFiles}.csv`
            : `${address}_transactions_${dateStr}.csv`;
          mimeType = "text/csv";
        } else {
          content = JSON.stringify(chunk, null, 2);
          filename = totalFiles > 1
            ? `${address}_transactions_${dateStr}_part${fileIndex + 1}of${totalFiles}.json`
            : `${address}_transactions_${dateStr}.json`;
          mimeType = "application/json";
        }

        setProgress(75 + ((fileIndex + 1) / totalFiles) * 25);

        // Trigger download
        const blob = new Blob([content], { type: mimeType });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);

        // Small delay between file downloads to avoid browser blocking
        if (fileIndex < totalFiles - 1) {
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      }

      // Close dialog after successful export
      setTimeout(() => {
        onOpenChange(false);
        setIsExporting(false);
        setProgress(0);
      }, 500);

    } catch (error) {
      console.error("Export failed:", error);
      alert(`Export failed: ${error instanceof Error ? error.message : "Unknown error"}`);
      setIsExporting(false);
    }
  };

  const generateCSV = (
    transactions: AddressTransactionSummary[],
    address: string,
    priceMap: Map<number, number | null>
  ): string => {
    // CSV header compatible with Koinly and other tax software
    // Date format: UTC timezone
    const header = "Date (UTC),Type,Amount,Currency,Price USD,Value USD,TxHash,Block Height,Confirmations,From Address,To Address,Notes\n";

    const rows = transactions.map((tx) => {
      const date = tx.timestamp
        ? new Date(tx.timestamp * 1000).toISOString().replace('T', ' ').replace('Z', '')
        : "";

      const type = tx.isCoinbase
        ? "Block Reward"
        : tx.direction === "received"
          ? "Receive"
          : "Send";

      const amount = Math.abs(tx.value).toFixed(8);
      const currency = "FLUX";

      // Get price at transaction time
      const price = tx.timestamp ? (priceMap.get(tx.timestamp) ?? null) : null;
      const priceStr = price !== null ? price.toFixed(6) : "";
      const valueUsd = price !== null ? (Math.abs(tx.value) * price).toFixed(2) : "";

      const txHash = tx.txid;
      const blockHeight = tx.blockHeight || "";
      const confirmations = tx.confirmations || 0;

      // For received transactions, show sender(s); for sent, show recipient(s)
      const fromAddress = tx.direction === "received"
        ? (tx.fromAddresses && tx.fromAddresses.length > 0 ? tx.fromAddresses.join("; ") : "")
        : address;

      const toAddress = tx.direction === "sent"
        ? (tx.toAddresses && tx.toAddresses.length > 0 ? tx.toAddresses.join("; ") : "")
        : address;

      const notes = tx.isCoinbase ? "Coinbase" : "";

      // Escape fields that might contain commas
      return [
        date,
        type,
        amount,
        currency,
        priceStr,
        valueUsd,
        txHash,
        blockHeight,
        confirmations,
        `"${fromAddress}"`,
        `"${toAddress}"`,
        notes,
      ].join(",");
    });

    return header + rows.join("\n");
  };

  const isValidRange = dateRange?.from && dateRange?.to;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[600px]">
        <DialogHeader>
          <DialogTitle>Export Transaction History</DialogTitle>
          <DialogDescription>
            Select a date range to export transactions. {totalTransactions.toLocaleString()} total transactions available.
          </DialogDescription>
        </DialogHeader>

        {isExporting ? (
          <div className="space-y-4 py-4">
            <div className="text-sm font-medium text-center">
              {currentStatus}
            </div>
            {progress < 50 && targetCount > 0 && (
              <div className="text-sm text-muted-foreground text-center">
                {fetchedCount.toLocaleString()} / {targetCount.toLocaleString()} transactions
              </div>
            )}
            <Progress value={progress} className="h-2" />
            <div className="text-center">
              <div className="text-2xl font-bold">{Math.round(progress)}%</div>
            </div>
            {targetCount > 100000 && (
              <div className="text-xs text-muted-foreground text-center">
                Large exports are split into multiple files (100K transactions each)
              </div>
            )}
          </div>
        ) : (
          <div className="space-y-6 py-4">
            {/* Date range selection */}
            <div className="space-y-3">
              <label className="text-sm font-medium">
                Select date range
              </label>

              {/* Preset range buttons */}
              <div className="flex flex-wrap gap-2">
                {PRESET_RANGES.map((preset) => (
                  <Button
                    key={preset.label}
                    variant="outline"
                    size="sm"
                    onClick={() => handlePresetClick(preset)}
                  >
                    {preset.label}
                  </Button>
                ))}
              </div>

              {/* Date range picker */}
              <div className="flex gap-2">
                <DateRangePicker
                  value={dateRange}
                  onChange={handleDateRangeChange}
                  placeholder="Select custom date range"
                  minDate={FLUX_GENESIS_DATE}
                  maxDate={new Date()}
                  className="flex-1"
                />
                {dateRange && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setDateRange(undefined)}
                    className="px-3"
                    title="Clear date range"
                  >
                    Clear
                  </Button>
                )}
              </div>

              {isValidRange && dateRange.from && dateRange.to && (
                <div className="text-xs text-muted-foreground">
                  Transactions from {dateRange.from.toLocaleDateString()} to {dateRange.to.toLocaleDateString()}
                </div>
              )}
            </div>

            {/* Export buttons */}
            <div className="flex gap-2">
              <Button
                onClick={() => handleExport("csv")}
                disabled={!isValidRange}
                className="flex-1"
                variant="default"
              >
                <Download className="h-4 w-4 mr-2" />
                Export CSV
              </Button>
              <Button
                onClick={() => handleExport("json")}
                disabled={!isValidRange}
                className="flex-1"
                variant="outline"
              >
                <FileJson className="h-4 w-4 mr-2" />
                Export JSON
              </Button>
            </div>

            <Button
              onClick={() => onOpenChange(false)}
              variant="ghost"
              className="w-full"
            >
              Cancel
            </Button>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
