"use client";

import { Fragment, useEffect, useState } from "react";
import Link from "next/link";
import { keepPreviousData } from "@tanstack/react-query";
import { AddressInfo } from "@/types/flux-api";
import { useAddressTransactions } from "@/lib/api/hooks/useAddress";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Download, FileJson } from "lucide-react";
import { format } from "date-fns";
import { TransactionExportDialog } from "./TransactionExportDialog";

interface AddressTransactionsProps {
  addressInfo: AddressInfo;
  pollingToken?: number;
  pollingInterval?: number;
  pollingActive?: boolean;
}

const ITEMS_PER_PAGE = 25;
const MAX_INLINE_COUNTERPARTIES = 3;
const MAX_EXPANDED_COUNTERPARTIES = 12;

const formatFlux = (value: number): string =>
  value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 8 });

const formatTimestamp = (timestamp?: number | null): string => {
  if (!timestamp) return "—";
  try {
    return format(new Date(timestamp * 1000), "yyyy-MM-dd HH:mm:ss");
  } catch {
    return "—";
  }
};

export function AddressTransactions({
  addressInfo,
  pollingToken,
  pollingInterval = 30000,
  pollingActive = false,
}: AddressTransactionsProps) {
  const [currentPage, setCurrentPage] = useState(1);
  const [pageInput, setPageInput] = useState("");
  const [expandedRows, setExpandedRows] = useState<Set<string>>(() => new Set());
  const [showExportDialog, setShowExportDialog] = useState(false);

  // Cursor-based pagination state: track cursor for each page
  const [cursorStack, setCursorStack] = useState<Array<{ height: number; txid: string } | null>>([null]);

  // Get cursor for current page
  const currentCursor = cursorStack[currentPage - 1] || null;

  const from = (currentPage - 1) * ITEMS_PER_PAGE;
  const to = from + ITEMS_PER_PAGE;

  const handlePageJump = (e: React.FormEvent) => {
    e.preventDefault();
    const pageNum = parseInt(pageInput, 10);
    if (!isNaN(pageNum) && pageNum >= 1 && pageNum <= totalPages) {
      setCurrentPage(pageNum);
      setPageInput("");
    }
  };

  const { data: txPage, isLoading, refetch } = useAddressTransactions(
    [addressInfo.addrStr],
    currentCursor ? { cursorHeight: currentCursor.height, cursorTxid: currentCursor.txid, to: ITEMS_PER_PAGE } : { from, to },
    {
      staleTime: 0,
      placeholderData: keepPreviousData,
      refetchOnWindowFocus: false,
    }
  );

  useEffect(() => {
    if (pollingToken === undefined) return;
    refetch({ cancelRefetch: true });
  }, [pollingToken, refetch]);

  useEffect(() => {
    if (pollingToken !== undefined) return;
    if (!pollingActive) return;

    const intervalId = setInterval(() => {
      refetch({ cancelRefetch: true });
    }, pollingInterval);

    return () => clearInterval(intervalId);
  }, [pollingToken, pollingActive, pollingInterval, refetch]);

  // Update cursor stack when we receive new data with nextCursor
  useEffect(() => {
    if (txPage?.nextCursor) {
      setCursorStack((prev) => {
        const newStack = [...prev];
        // Store the cursor for the next page
        if (newStack.length === currentPage) {
          newStack.push(txPage.nextCursor || null);
        }
        return newStack;
      });
    }
  }, [txPage, currentPage]);

  const transactions = txPage?.items ?? [];
  const totalItems = txPage?.filteredTotal ?? txPage?.totalItems ?? addressInfo.txApperances ?? 0;
  const totalPages = totalItems > 0 ? Math.ceil(totalItems / ITEMS_PER_PAGE) : 1;

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Transactions</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {[...Array(ITEMS_PER_PAGE)].map((_, i) => (
            <Skeleton key={i} className="h-12 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }

  if (!transactions.length) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Transactions</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          No transactions found for this address.
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div className="flex-1">
          <CardTitle>Transactions</CardTitle>
          <div className="text-xs text-muted-foreground mt-1">
            Showing {from + 1} - {Math.min(to, totalItems)} of {totalItems.toLocaleString()} transactions
          </div>
        </div>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowExportDialog(true)}
            className="gap-2"
          >
            <Download className="h-4 w-4" />
            Export CSV
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowExportDialog(true)}
            className="gap-2"
          >
            <FileJson className="h-4 w-4" />
            Export JSON
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="w-full overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[180px]">Date & Time</TableHead>
                <TableHead>Transaction</TableHead>
                <TableHead className="w-[140px]">Type</TableHead>
                <TableHead className="text-right">Net Amount</TableHead>
                <TableHead className="w-[140px] text-right">Confirmations</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {transactions.map((tx) => {
                const isReceived = tx.direction === "received";
                const counterparties = isReceived ? tx.fromAddresses : tx.toAddresses;
                const counterpartLabel = isReceived ? "From" : "To";
                const totalCounterparties = isReceived
                  ? tx.fromAddressCount ?? counterparties.length
                  : tx.toAddressCount ?? counterparties.length;
                const displayedCounterparties = counterparties.slice(0, MAX_INLINE_COUNTERPARTIES);
                const remainingCounterparties = Math.max(0, totalCounterparties - displayedCounterparties.length);
                const renderAddressLink = (addr: string) => (
                  <Link
                    key={addr}
                    href={`/address/${addr}`}
                    className="hover:text-primary"
                  >
                    {addr.slice(0, 8)}...{addr.slice(-6)}
                  </Link>
                );
                const typeLabel = tx.isCoinbase
                  ? "Block Reward"
                  : isReceived
                    ? "Received"
                    : "Sent";
                const isExpanded = expandedRows.has(tx.txid);
                // Use direction to determine if positive (received) or negative (sent)
                const netPositive = isReceived;
                const netFlux = formatFlux(Math.abs(tx.value));
                const toggleExpanded = () => {
                  setExpandedRows((prev) => {
                    const next = new Set(prev);
                    if (next.has(tx.txid)) {
                      next.delete(tx.txid);
                    } else {
                      next.add(tx.txid);
                    }
                    return next;
                  });
                };
                return (
                  <Fragment key={tx.txid}>
                    <TableRow>
                      <TableCell className="text-sm">
                        {formatTimestamp(tx.timestamp)}
                      </TableCell>
                      <TableCell className="text-xs">
                        <div className="font-mono">
                          <Link href={`/tx/${tx.txid}`} className="text-primary hover:underline">
                            {tx.txid.slice(0, 16)}...{tx.txid.slice(-8)}
                          </Link>
                        </div>
                        {displayedCounterparties.length > 0 && (
                          <div className="mt-1 space-x-1 text-xs text-muted-foreground">
                            <span className="uppercase tracking-wide">{counterpartLabel}:</span>
                            {displayedCounterparties.map((addr, index) => (
                              <span key={addr} className="inline-flex items-center gap-1">
                                {renderAddressLink(addr)}
                                {index < displayedCounterparties.length - 1 ? <span>•</span> : null}
                              </span>
                            ))}
                            {remainingCounterparties > 0 && (
                              <span>+{remainingCounterparties}</span>
                            )}
                          </div>
                        )}
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant="outline"
                          className={`w-full justify-center ${
                            tx.isCoinbase
                              ? "text-yellow-500 border-yellow-500/20 bg-yellow-500/10"
                              : isReceived
                                ? "text-blue-500 border-blue-500/20 bg-blue-500/10"
                                : "text-rose-500 border-rose-500/20 bg-rose-500/10"
                          }`}
                        >
                          {typeLabel}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-right align-middle">
                        <div
                          className={`font-semibold ${netPositive ? "text-emerald-400" : "text-rose-400"}`}
                          title={netPositive ? "Net received" : "Net sent"}
                        >
                          {netPositive ? "+" : "-"}
                          {netFlux} FLUX
                        </div>
                        <div className="mt-1 flex justify-end">
                          <Button variant="link" size="sm" className="h-6 px-0 text-xs" onClick={toggleExpanded}>
                            {isExpanded ? "Hide breakdown" : "Show breakdown"}
                          </Button>
                        </div>
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="font-medium">{tx.confirmations.toLocaleString()}</div>
                        <div className="text-[11px] text-muted-foreground">
                          Block{" "}
                          <Link href={`/block/${tx.blockHeight}`} className="hover:text-primary">
                            #{tx.blockHeight.toLocaleString()}
                          </Link>
                        </div>
                      </TableCell>
                    </TableRow>
                    {isExpanded && (
                      <TableRow className="bg-muted/40">
                        <TableCell colSpan={5} className="py-4">
                          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 text-sm">
                            <div>
                              <div className="text-xs uppercase tracking-wide text-muted-foreground">
                                Net change
                              </div>
                              <div className="font-mono">
                                {netPositive ? "+" : "-"}
                                {netFlux} FLUX
                              </div>
                            </div>
                            <div>
                              <div className="text-xs uppercase tracking-wide text-muted-foreground">
                                Outputs to this address
                              </div>
                              <div className="font-mono">{formatFlux(tx.receivedValue)} FLUX</div>
                            </div>
                            {!tx.isCoinbase && (
                              <div>
                                <div className="text-xs uppercase tracking-wide text-muted-foreground">
                                  Sent to others
                                </div>
                                <div className="font-mono">{formatFlux(tx.toOthersValue)} FLUX</div>
                              </div>
                            )}
                            {!tx.isCoinbase && tx.changeValue > 0 && (
                              <div>
                                <div className="text-xs uppercase tracking-wide text-muted-foreground">
                                  Change returned
                                </div>
                                <div className="font-mono">{formatFlux(tx.changeValue)} FLUX</div>
                              </div>
                            )}
                            {!tx.isCoinbase && tx.feeValue > 0 && (
                              <div>
                                <div className="text-xs uppercase tracking-wide text-muted-foreground">
                                  Miner fee
                                </div>
                                <div className="font-mono">{formatFlux(tx.feeValue)} FLUX</div>
                              </div>
                            )}
                            {tx.isCoinbase && (
                              <div>
                                <div className="text-xs uppercase tracking-wide text-muted-foreground">
                                  Reward total
                                </div>
                                <div className="font-mono">{formatFlux(tx.receivedValue)} FLUX</div>
                              </div>
                            )}
                            {tx.selfTransfer && !tx.isCoinbase && (
                              <div>
                                <div className="text-xs uppercase tracking-wide text-muted-foreground">
                                  Note
                                </div>
                                <div className="font-mono text-amber-500">Self transfer</div>
                              </div>
                            )}
                          </div>
                          {(tx.fromAddresses.length > 0 || tx.toAddresses.length > 0) && (
                            <div className="mt-4 space-y-2 text-xs">
                              {tx.fromAddresses.length > 0 && (
                                <div className="flex flex-wrap gap-2">
                                  <span className="uppercase tracking-wide text-muted-foreground">Inputs:</span>
                                  {tx.fromAddresses.slice(0, MAX_EXPANDED_COUNTERPARTIES).map((addr) => (
                                    <Link key={addr} href={`/address/${addr}`} className="font-mono hover:text-primary">
                                      {addr}
                                    </Link>
                                  ))}
                                  {(tx.fromAddressCount ?? tx.fromAddresses.length) > MAX_EXPANDED_COUNTERPARTIES && (
                                    <span className="text-muted-foreground">
                                      +{(tx.fromAddressCount ?? tx.fromAddresses.length) - MAX_EXPANDED_COUNTERPARTIES} more
                                    </span>
                                  )}
                                </div>
                              )}
                              {tx.toAddresses.length > 0 && (
                                <div className="flex flex-wrap gap-2">
                                  <span className="uppercase tracking-wide text-muted-foreground">Outputs:</span>
                                  {tx.toAddresses.slice(0, MAX_EXPANDED_COUNTERPARTIES).map((addr) => (
                                    <Link key={addr} href={`/address/${addr}`} className="font-mono hover:text-primary">
                                      {addr}
                                    </Link>
                                  ))}
                                  {(tx.toAddressCount ?? tx.toAddresses.length) > MAX_EXPANDED_COUNTERPARTIES && (
                                    <span className="text-muted-foreground">
                                      +{(tx.toAddressCount ?? tx.toAddresses.length) - MAX_EXPANDED_COUNTERPARTIES} more
                                    </span>
                                  )}
                                </div>
                              )}
                            </div>
                          )}
                        </TableCell>
                      </TableRow>
                    )}
                  </Fragment>
                );
              })}
            </TableBody>
          </Table>
        </div>

        <div className="flex flex-col sm:flex-row items-center justify-between gap-4 pt-4 border-t">
          {/* Page info and navigation */}
          <div className="flex items-center gap-3">
            <span className="text-sm text-muted-foreground">
              Page {currentPage} of {totalPages}
            </span>
            <div className="flex items-center gap-1">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCurrentPage(1)}
                disabled={currentPage === 1}
                title="First page"
              >
                <ChevronsLeft className="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCurrentPage((page) => Math.max(1, page - 1))}
                disabled={currentPage === 1}
                title="Previous page"
              >
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCurrentPage((page) => Math.min(totalPages, page + 1))}
                disabled={currentPage === totalPages}
                title="Next page"
              >
                <ChevronRight className="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCurrentPage(totalPages)}
                disabled={currentPage === totalPages}
                title="Last page"
              >
                <ChevronsRight className="h-4 w-4" />
              </Button>
            </div>
          </div>

          {/* Jump to page */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Jump to:</span>
            <form onSubmit={handlePageJump} className="flex items-center gap-2">
              <Input
                type="number"
                min="1"
                max={totalPages}
                value={pageInput}
                onChange={(e) => setPageInput(e.target.value)}
                placeholder="Page"
                className="w-20 h-8"
              />
              <Button type="submit" size="sm" variant="outline">
                Go
              </Button>
            </form>
          </div>
        </div>
      </CardContent>

      {/* Export Dialog */}
      <TransactionExportDialog
        open={showExportDialog}
        onOpenChange={setShowExportDialog}
        address={addressInfo.addrStr}
        totalTransactions={totalItems}
      />
    </Card>
  );
}
