"use client";

import { Transaction } from "@/types/flux-api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { format } from "date-fns";
import {
  CheckCircle2,
  Clock,
  Calendar,
  Database,
  Coins,
  ArrowDownUp,
  ArrowDown,
  ArrowUp
} from "lucide-react";
import { getExpectedBlockReward } from "@/lib/block-rewards";

interface TransactionOverviewProps {
  transaction: Transaction;
}

export function TransactionOverview({ transaction }: TransactionOverviewProps) {
  // Coinbase transactions have a 'coinbase' property on the first input
  const isCoinbase = transaction.vin.length > 0 && !!transaction.vin[0]?.coinbase;

  const timestamp = transaction.time || transaction.blocktime;
  const sizeBytes = transaction.size > 0 ? transaction.size : (transaction.vsize ?? 0);
  const vsizeBytes = transaction.vsize ?? sizeBytes;

  // Calculate fees and block reward
  // The API now correctly calculates fees for coinbase transactions
  const feeDisplay = transaction.fees || 0;
  let blockReward = transaction.valueOut;

  if (isCoinbase && transaction.blockheight !== undefined) {
    // For coinbase: block reward = expected reward (for display purposes)
    // Actual output may be less (burned) or more (fees included)
    const expectedReward = getExpectedBlockReward(transaction.blockheight);
    blockReward = expectedReward;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Transaction Overview</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 sm:gap-4 lg:grid-cols-3">
          {/* Confirmations */}
          <StatCard
            icon={<CheckCircle2 className="h-5 w-5" />}
            label="Confirmations"
            value={transaction.confirmations.toLocaleString()}
            description={transaction.confirmations === 0 ? "Unconfirmed" : "Confirmed"}
          />

          {/* Timestamp */}
          {timestamp && (
            <StatCard
              icon={<Calendar className="h-5 w-5" />}
              label="Timestamp"
              value={format(new Date(timestamp * 1000), "PPp")}
              description={format(new Date(timestamp * 1000), "yyyy-MM-dd HH:mm:ss")}
            />
          )}

          {/* Size */}
          <StatCard
            icon={<Database className="h-5 w-5" />}
            label="Size"
            value={`${sizeBytes.toLocaleString()} bytes`}
            description={`${(sizeBytes / 1024).toFixed(2)} KB`}
          />

          {/* Fee */}
          <StatCard
            icon={<Coins className="h-5 w-5" />}
            label="Fee"
            value={`${feeDisplay.toFixed(8)} FLUX`}
            description={vsizeBytes > 0 && feeDisplay > 0 && !isCoinbase
              ? `Fee rate: ${(feeDisplay / vsizeBytes * 100000000).toFixed(2)} sat/vByte`
              : isCoinbase
                ? "Coinbase transaction (fees added to reward)"
                : "No fee"
            }
          />

          {/* Total Input */}
          <StatCard
            icon={<ArrowDown className="h-5 w-5 text-red-500" />}
            label={isCoinbase ? "Block Reward" : "Total Input"}
            value={`${isCoinbase ? blockReward.toFixed(8) : (transaction.valueIn !== null && transaction.valueIn !== undefined ? transaction.valueIn.toFixed(8) : '0.00000000')} FLUX`}
            description={
              isCoinbase
                ? transaction.blockheight !== undefined
                  ? `Block height ${transaction.blockheight.toLocaleString()}`
                  : "Block height unavailable"
                : `${transaction.vin.length} input${transaction.vin.length !== 1 ? 's' : ''}`
            }
          />

          {/* Total Output */}
          <StatCard
            icon={<ArrowUp className="h-5 w-5 text-green-500" />}
            label="Total Output"
            value={`${transaction.valueOut !== null && transaction.valueOut !== undefined ? transaction.valueOut.toFixed(8) : '0.00000000'} FLUX`}
            description={`${transaction.vout.length} output${transaction.vout.length !== 1 ? 's' : ''}`}
          />

          {/* Lock Time */}
          {transaction.locktime > 0 && (
            <StatCard
              icon={<Clock className="h-5 w-5" />}
              label="Lock Time"
              value={transaction.locktime.toLocaleString()}
              description={
                transaction.locktime < 500000000
                  ? "Block height"
                  : format(new Date(transaction.locktime * 1000), "PPp")
              }
            />
          )}

          {/* Version */}
          <StatCard
            icon={<ArrowDownUp className="h-5 w-5" />}
            label="Version"
            value={transaction.version.toString()}
            description="Transaction version"
          />
        </div>
      </CardContent>
    </Card>
  );
}

interface StatCardProps {
  icon: React.ReactNode;
  label: string;
  value: string;
  description: string;
}

function StatCard({ icon, label, value, description }: StatCardProps) {
  return (
    <div className="flex gap-3 p-4 rounded-lg border bg-card">
      <div className="flex-shrink-0 mt-1 text-muted-foreground">{icon}</div>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-muted-foreground">{label}</p>
        <p className="text-lg font-semibold truncate mt-1">{value}</p>
        <p className="text-xs text-muted-foreground mt-1 truncate">{description}</p>
      </div>
    </div>
  );
}
