"use client";

import { Transaction, TransactionInput, TransactionOutput } from "@/types/flux-api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { CopyButton } from "@/components/ui/copy-button";
import { ArrowRight, Coins, Lock, CheckCircle } from "lucide-react";
import { getRewardLabel } from "@/lib/block-rewards";

interface TransactionInputsOutputsProps {
  transaction: Transaction;
}

export function TransactionInputsOutputs({ transaction }: TransactionInputsOutputsProps) {
  // Coinbase transactions have a 'coinbase' property on the first input
  const isCoinbase = transaction.vin.length > 0 && !!transaction.vin[0]?.coinbase;

  const hasNoInputs = transaction.vin.length === 0 || (!transaction.vin[0]?.txid && !isCoinbase);

  // Check if this is a FluxNode transaction (version 5 or 6, or has nType field)
  const isFluxNode = transaction.version === 5 || transaction.version === 6 || transaction.nType !== undefined;

  // FluxNode transactions are NOT shielded - they have no inputs but are a special on-chain message
  // Shielded transactions have no transparent inputs and are not coinbase or FluxNode
  const isShielded = hasNoInputs && !isCoinbase && !isFluxNode;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Inputs & Outputs</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid gap-6 md:grid-cols-[1fr_auto_1fr] items-start">
          {/* INPUTS */}
          <div className="space-y-3">
            {(() => {
              // Calculate if funds are coming from shielded pool
              // For v2/v4 transactions: shieldedAmount = valueOut + fees - valueIn (transparent)
              const isShieldedCapable = transaction.version === 2 || transaction.version === 4;
              const shieldedInputAmount = isShieldedCapable && !isCoinbase && isShielded
                ? transaction.valueOut + (transaction.fees || 0) - transaction.valueIn
                : 0;
              const hasShieldedInput = shieldedInputAmount > 0.00000001;
              const inputCount = isCoinbase ? 1 : (hasShieldedInput ? 1 : transaction.vin.length);
              const totalInputValue = hasShieldedInput
                ? transaction.valueIn + shieldedInputAmount
                : transaction.valueIn;

              return (
                <>
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-semibold">
                      Inputs ({inputCount})
                    </h3>
                    {!isCoinbase && (
                      <Badge variant="outline">{totalInputValue.toFixed(8)} FLUX</Badge>
                    )}
                  </div>

                  {isCoinbase ? (
                    <CoinbaseInput />
                  ) : isFluxNode ? (
                    <FluxNodeInput transaction={transaction} />
                  ) : hasShieldedInput ? (
                    <ShieldedInput amount={shieldedInputAmount} />
                  ) : (
                    transaction.vin.map((input, index) => (
                      <InputCard key={`${input.txid}-${input.vout}`} input={input} index={index} />
                    ))
                  )}
                </>
              );
            })()}
          </div>

          {/* ARROW - Hidden on mobile, shown on md+ */}
          <div className="hidden md:flex items-center justify-center py-8">
            <div className="flex items-center justify-center w-12 h-12 rounded-full bg-primary/10">
              <ArrowRight className="h-6 w-6 text-primary" />
            </div>
          </div>

          {/* OUTPUTS */}
          <div className="space-y-3">
            {(() => {
              // Calculate if funds are going to shielded pool
              // For v2/v4 transactions: shieldedAmount = valueIn - valueOut - fees
              const isShieldedCapable = transaction.version === 2 || transaction.version === 4;
              const shieldedOutputAmount = isShieldedCapable && !isCoinbase
                ? transaction.valueIn - transaction.valueOut - (transaction.fees || 0)
                : 0;
              const hasShieldedOutput = shieldedOutputAmount > 0.00000001; // Account for floating point
              const outputCount = transaction.vout.length + (hasShieldedOutput ? 1 : 0);

              return (
                <>
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-semibold">
                      Outputs ({outputCount})
                    </h3>
                    <Badge variant="outline">
                      {(transaction.valueOut + (hasShieldedOutput ? shieldedOutputAmount : 0)).toFixed(8)} FLUX
                    </Badge>
                  </div>

                  {transaction.vout.map((output, index) => (
                    <OutputCard
                      key={`${output.n}`}
                      output={output}
                      index={index}
                      isCoinbase={isCoinbase}
                      blockHeight={transaction.blockheight}
                    />
                  ))}

                  {hasShieldedOutput && (
                    <ShieldedOutput amount={shieldedOutputAmount} index={transaction.vout.length} />
                  )}
                </>
              );
            })()}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function CoinbaseInput() {
  return (
    <div className="p-4 rounded-lg border bg-card hover:bg-muted/50 transition-colors border-primary/20">
      <div className="flex items-center gap-3 mb-3">
        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-primary/10">
          <Coins className="h-5 w-5 text-primary" />
        </div>
        <div className="flex-1">
          <div className="font-semibold text-base">Coinbase</div>
          <p className="text-xs text-muted-foreground">Newly Generated Coins</p>
        </div>
      </div>
      <div className="pt-3 border-t border-border/50">
        <p className="text-sm text-muted-foreground leading-relaxed">
          This is a coinbase transaction that generates new coins as a block reward for mining.
        </p>
      </div>
    </div>
  );
}

interface ShieldedInputProps {
  amount: number;
}

function ShieldedInput({ amount }: ShieldedInputProps) {
  return (
    <div className="p-4 rounded-lg border bg-card hover:bg-muted/50 transition-colors border-purple-500/20">
      <div className="space-y-2">
        {/* Index and Value */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Badge variant="secondary">#0</Badge>
            <Badge variant="outline" className="gap-1 text-xs border-purple-500 text-purple-500">
              <Lock className="h-3 w-3" />
              Shielded
            </Badge>
          </div>
          <span className="font-mono text-sm font-semibold">
            {amount.toFixed(8)} FLUX
          </span>
        </div>

        {/* Shielded Pool Info */}
        <div className="flex items-center gap-3 pt-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-full bg-purple-500/10">
            <Lock className="h-4 w-4 text-purple-500" />
          </div>
          <div className="flex-1">
            <div className="font-medium text-sm">From Shielded Pool</div>
            <p className="text-xs text-muted-foreground">
              Funds received from the privacy pool. The source address is hidden.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

interface ShieldedOutputProps {
  amount: number;
  index: number;
}

function ShieldedOutput({ amount, index }: ShieldedOutputProps) {
  return (
    <div className="p-4 rounded-lg border bg-card hover:bg-muted/50 transition-colors border-purple-500/20">
      <div className="space-y-2">
        {/* Index and Value */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Badge variant="secondary">#{index}</Badge>
            <Badge variant="outline" className="gap-1 text-xs border-purple-500 text-purple-500">
              <Lock className="h-3 w-3" />
              Shielded
            </Badge>
          </div>
          <span className="font-mono text-sm font-semibold">
            {amount.toFixed(8)} FLUX
          </span>
        </div>

        {/* Shielded Pool Info */}
        <div className="flex items-center gap-3 pt-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-full bg-purple-500/10">
            <Lock className="h-4 w-4 text-purple-500" />
          </div>
          <div className="flex-1">
            <div className="font-medium text-sm">To Shielded Pool</div>
            <p className="text-xs text-muted-foreground">
              Funds sent to the privacy pool. The destination address is hidden.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

interface FluxNodeInputProps {
  transaction: Transaction;
}

function FluxNodeInput({ transaction }: FluxNodeInputProps) {
  const tierColorMap: Record<string, string> = {
    CUMULUS: "text-pink-500 border-pink-500/20 bg-pink-500/10",
    NIMBUS: "text-purple-500 border-purple-500/20 bg-purple-500/10",
    STRATUS: "text-blue-500 border-blue-500/20 bg-blue-500/10",
  };

  const typeLabel = transaction.nType === 2 ? "Starting" : transaction.nType === 4 ? "Confirming" : "FluxNode";
  const tier = transaction.benchmarkTier?.toUpperCase();
  const tierColor = tier && tierColorMap[tier] ? tierColorMap[tier] : "text-blue-500 border-blue-500/20 bg-blue-500/10";

  return (
    <div className={`p-4 rounded-lg border bg-card hover:bg-muted/50 transition-colors ${tierColor.replace('text-', 'border-').split(' ')[1]}`}>
      <div className="flex items-center gap-3 mb-3">
        <div className={`flex h-10 w-10 items-center justify-center rounded-full ${tierColor.split(' ')[2]}`}>
          <CheckCircle className={`h-5 w-5 ${tierColor.split(' ')[0]}`} />
        </div>
        <div className="flex-1">
          <div className="font-semibold text-base">FluxNode {typeLabel}</div>
          <p className="text-xs text-muted-foreground">On-Chain FluxNode Message</p>
        </div>
        {tier && (
          <Badge variant="outline" className={tierColor}>
            {tier}
          </Badge>
        )}
      </div>
      <div className="pt-3 border-t border-border/50 space-y-2">
        <p className="text-sm text-muted-foreground leading-relaxed">
          This is a FluxNode {typeLabel.toLowerCase()} transaction. FluxNodes are special transactions that register or confirm node status on the blockchain.
        </p>
        {transaction.ip && (
          <div className="flex items-center gap-2 text-xs">
            <span className="text-muted-foreground">IP:</span>
            <span className="font-mono">{transaction.ip}</span>
          </div>
        )}
        {transaction.collateralOutputHash && (
          <div>
            <p className="text-xs text-muted-foreground mb-1">Collateral</p>
            <div className="flex items-center gap-1">
              <a
                href={`/tx/${transaction.collateralOutputHash}`}
                className="font-mono text-xs text-primary hover:underline truncate"
              >
                {transaction.collateralOutputHash}
              </a>
              <span className="text-xs text-muted-foreground">:{transaction.collateralOutputIndex}</span>
              <CopyButton text={transaction.collateralOutputHash} />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

interface InputCardProps {
  input: TransactionInput;
  index: number;
}

function InputCard({ input, index }: InputCardProps) {
  return (
    <div className="p-4 rounded-lg border bg-card hover:bg-muted/50 transition-colors">
      <div className="space-y-2">
        {/* Index and Value */}
        <div className="flex items-center justify-between">
          <Badge variant="secondary">#{index}</Badge>
          <span className="font-mono text-sm font-semibold">
            {input.value.toFixed(8)} FLUX
          </span>
        </div>

        {/* Address */}
        {input.addr && (
          <div>
            <p className="text-xs text-muted-foreground mb-1">From Address</p>
            <div className="flex items-center gap-1">
              <a
                href={`/address/${input.addr}`}
                className="font-mono text-sm text-primary hover:underline truncate"
              >
                {input.addr}
              </a>
              <CopyButton text={input.addr} />
            </div>
          </div>
        )}

        {/* Previous Transaction */}
        <div>
          <p className="text-xs text-muted-foreground mb-1">Previous Output</p>
          <div className="flex items-center gap-1">
            <a
              href={`/tx/${input.txid}`}
              className="font-mono text-xs text-primary hover:underline truncate"
            >
              {input.txid}
            </a>
            <span className="text-xs text-muted-foreground">:{input.vout}</span>
            <CopyButton text={input.txid} />
          </div>
        </div>

        {/* Sequence */}
        {input.sequence !== 4294967295 && (
          <div className="flex items-center gap-2 text-xs">
            <span className="text-muted-foreground">Sequence:</span>
            <span className="font-mono">{input.sequence}</span>
          </div>
        )}
      </div>
    </div>
  );
}

interface OutputCardProps {
  output: TransactionOutput;
  index: number;
  isCoinbase?: boolean;
  blockHeight?: number;
}

function OutputCard({ output, index, isCoinbase = false, blockHeight }: OutputCardProps) {
  const isSpent = output.spentTxId !== undefined;
  const addresses = (output.scriptPubKey.addresses || []).filter((addr) => addr && addr !== 'SHIELDED_OR_NONSTANDARD');
  const isOpReturn = output.scriptPubKey.type === 'nulldata';
  const opReturnText = output.scriptPubKey.opReturnText || null;
  const opReturnHex = output.scriptPubKey.opReturnHex || null;

  // Tier detection for coinbase transactions using unified reward labeling
  const getTierInfo = (amount: number): { tier: string; colorClass: string } | null => {
    if (!isCoinbase || blockHeight === undefined) return null;

    const label = getRewardLabel(amount, blockHeight);

    // Map the reward label to display format with color classes
    const colorMap = {
      'MINING': 'text-yellow-500 border-yellow-500/20 bg-yellow-500/10',
      'FOUNDATION': 'text-green-500 border-green-500/20 bg-green-500/10',
      'CUMULUS': 'text-pink-500 border-pink-500/20 bg-pink-500/10',
      'NIMBUS': 'text-purple-500 border-purple-500/20 bg-purple-500/10',
      'STRATUS': 'text-blue-500 border-blue-500/20 bg-blue-500/10',
    };

    return {
      tier: label.type,
      colorClass: colorMap[label.type],
    };
  };

  const amount = parseFloat(output.value);
  const tierInfo = getTierInfo(amount);

  return (
    <div className="p-4 rounded-lg border bg-card hover:bg-muted/50 transition-colors">
      <div className="space-y-2">
        {/* Index, Value, and Status */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Badge variant="secondary">#{index}</Badge>
            {isSpent ? (
              <Badge variant="outline" className="gap-1 text-xs">
                <CheckCircle className="h-3 w-3" />
                Spent
              </Badge>
            ) : (
              <Badge variant="outline" className="gap-1 text-xs border-green-500 text-green-600">
                <Lock className="h-3 w-3" />
                UTXO
              </Badge>
            )}
            {/* Tier Badge for Coinbase Transactions */}
            {tierInfo && (
              <Badge variant="outline" className={`text-xs ${tierInfo.colorClass}`}>
                {tierInfo.tier}
              </Badge>
            )}
          </div>
          <span className="font-mono text-sm font-semibold">
            {amount.toFixed(8)} FLUX
          </span>
        </div>

        {/* Address or OP_RETURN */}
        {isOpReturn ? (
          <div>
            <p className="text-xs text-muted-foreground mb-1">OP_RETURN Data</p>
            {opReturnText && (
              <div className="flex items-center gap-1 mb-1">
                <span className="font-mono text-sm text-muted-foreground truncate">{opReturnText}</span>
                <CopyButton text={opReturnText} />
              </div>
            )}
            {opReturnHex && (
              <div className="flex items-center gap-1">
                <span className="font-mono text-xs text-muted-foreground truncate">{opReturnHex}</span>
                <CopyButton text={opReturnHex} />
              </div>
            )}
            {!opReturnText && !opReturnHex && (
              <p className="text-xs text-muted-foreground">(no data)</p>
            )}
          </div>
        ) : addresses.length > 0 ? (
          <div>
            <p className="text-xs text-muted-foreground mb-1">To Address</p>
            {addresses.map((addr) => (
              <div key={addr} className="flex items-center gap-1 mb-1">
                <a
                  href={`/address/${addr}`}
                  className="font-mono text-sm text-primary hover:underline truncate"
                >
                  {addr}
                </a>
                <CopyButton text={addr} />
              </div>
            ))}
          </div>
        ) : (
          <div>
            <p className="text-xs text-muted-foreground mb-1">To Address</p>
            <p className="text-xs text-muted-foreground">Non-standard output</p>
          </div>
        )}

        {/* Script Type */}
        {output.scriptPubKey.type && output.scriptPubKey.type !== 'unknown' && (
          <div className="flex items-center gap-2 text-xs">
            <span className="text-muted-foreground">Type:</span>
            <Badge variant="outline" className="text-xs">
              {output.scriptPubKey.type}
            </Badge>
          </div>
        )}

        {/* Spent Information */}
        {isSpent && output.spentTxId && (
          <div>
            <p className="text-xs text-muted-foreground mb-1">Spent In</p>
            <div className="flex items-center gap-1">
              <a
                href={`/tx/${output.spentTxId}`}
                className="font-mono text-xs text-primary hover:underline truncate"
              >
                {output.spentTxId}
              </a>
              <CopyButton text={output.spentTxId} />
            </div>
            {output.spentHeight && (
              <p className="text-xs text-muted-foreground mt-1">
                Block: {output.spentHeight.toLocaleString()}
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
