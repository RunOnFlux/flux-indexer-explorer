"use client";

import { useState, useMemo, useEffect } from "react";
import Link from "next/link";
import { Block, BlockTransactionDetail } from "@/types/flux-api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { CopyButton } from "@/components/ui/copy-button";
import { Badge } from "@/components/ui/badge";
import { ArrowRight, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, FileText, Server } from "lucide-react";

interface BlockTransactionsProps {
  block: Block;
}

const TRANSACTIONS_PER_PAGE = 10;

const tierBadgeStyles: Record<string, string> = {
  CUMULUS: "text-pink-500 border-pink-500/20 bg-pink-500/10",
  NIMBUS: "text-purple-500 border-purple-500/20 bg-purple-500/10",
  STRATUS: "text-blue-500 border-blue-500/20 bg-blue-500/10",
  STARTING: "text-yellow-500 border-yellow-500/20 bg-yellow-500/10",
};

function formatFlux(value: number | undefined): string {
  if (value === undefined) return "—";
  return value.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 8,
  });
}

function getFluxNodeBadge(detail: BlockTransactionDetail) {
  let tier = detail.fluxnodeTier?.toString().toUpperCase();

  // Convert numeric tier to name if needed (1=CUMULUS, 2=NIMBUS, 3=STRATUS)
  if (tier === "1") tier = "CUMULUS";
  else if (tier === "2") tier = "NIMBUS";
  else if (tier === "3") tier = "STRATUS";

  // Always prefer showing the tier if available
  if (tier && tier !== "UNKNOWN" && tierBadgeStyles[tier]) {
    return { label: tier, className: tierBadgeStyles[tier] };
  }
  // Check if it's a starting transaction (kind or fluxnodeType 2)
  if (detail.kind === "fluxnode_start" || detail.fluxnodeType === 2) {
    return {
      label: "STARTING",
      className: tierBadgeStyles.STARTING || "text-yellow-500 border-yellow-500/20 bg-yellow-500/10",
    };
  }
  return {
    label: "FLUXNODE",
    className: "text-blue-500 border-blue-500/20 bg-blue-500/10",
  };
}

function summarizeCounts(block: Block) {
  const summary = block.txSummary;
  if (summary) {
    return {
      regular: summary.regular,
      nodeConfirmations: summary.fluxnodeConfirm,
      tierCounts: summary.tierCounts,
    };
  }

  const details: BlockTransactionDetail[] = block.txDetails || [];
  let regular = 0;
  let confirmations = 0;
  const tierCounts = { cumulus: 0, nimbus: 0, stratus: 0, starting: 0, unknown: 0 };

  details.forEach((detail) => {
    if (detail.kind === "coinbase" || detail.kind === "transfer") {
      regular += 1;
    } else {
      confirmations += detail.kind === "fluxnode_confirm" ? 1 : 0;
      const tier = detail.fluxnodeTier?.toUpperCase();
      if (tier && tierCounts[tier.toLowerCase() as keyof typeof tierCounts] !== undefined) {
        tierCounts[tier.toLowerCase() as keyof typeof tierCounts] += 1;
      } else if (detail.kind === "fluxnode_start") {
        tierCounts.starting += 1;
      } else {
        tierCounts.unknown += 1;
      }
    }
  });

  return { regular, nodeConfirmations: confirmations, tierCounts };
}

export function BlockTransactions({ block }: BlockTransactionsProps) {
  const [currentPage, setCurrentPage] = useState(1);
  const details = block.txDetails || [];
  const totalPages = Math.max(1, Math.ceil(details.length / TRANSACTIONS_PER_PAGE));
  const startIndex = (currentPage - 1) * TRANSACTIONS_PER_PAGE;
  const currentDetails = details.slice(startIndex, startIndex + TRANSACTIONS_PER_PAGE);

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages);
    }
  }, [totalPages, currentPage]);

  // No batch API call needed - txDetails now includes fromAddr/toAddr directly
  const counts = useMemo(() => summarizeCounts(block), [block]);

  const goToPage = (page: number) => {
    setCurrentPage(Math.min(Math.max(1, page), totalPages));
  };

  return (
    <Card className="w-full max-w-full">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <FileText className="h-5 w-5" />
            Transactions
            <Badge variant="secondary">{counts.regular.toLocaleString()}</Badge>
            {counts.nodeConfirmations > 0 && (
              <div className="relative group">
                <Badge variant="outline" className="gap-1 cursor-help">
                  <Server className="h-3 w-3" />
                  {counts.nodeConfirmations.toLocaleString()}
                </Badge>
                <div className="absolute left-0 bottom-full mb-2 p-3 bg-card border rounded-md shadow-xl opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200 z-50 min-w-[220px]">
                  <div className="space-y-2 text-sm">
                    <p className="font-semibold mb-2">Node Confirmations</p>
                    {Object.entries(counts.tierCounts).map(([tier, value]) => (
                      value > 0 && (
                        <div key={tier} className="flex items-center justify-between">
                          <span className="uppercase text-muted-foreground">{tier}</span>
                          <span className="font-bold">{value}</span>
                        </div>
                      )
                    ))}
                  </div>
                  <div className="absolute left-4 bottom-[-6px] w-3 h-3 bg-card border-r border-b rotate-45"></div>
                </div>
              </div>
            )}
          </CardTitle>
          {totalPages > 1 && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              Page {currentPage} of {totalPages}
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent className="space-y-3 w-full max-w-full overflow-x-hidden">
        {currentDetails.map((detail, index) => {
            const globalIndex = startIndex + index;
            const sizeBytes = detail.size && detail.size > 0 ? detail.size : null;

            const badge = () => {
              if (detail.kind === "coinbase") {
                return <Badge variant="outline" className="bg-green-500/10 border-green-500/20 text-green-500">Coinbase</Badge>;
              }
              if (detail.kind === "transfer") {
                return <Badge variant="outline" className="bg-orange-500/10 border-orange-500/20 text-orange-500">Transfer</Badge>;
              }
              // FluxNode transaction - API now returns correct tier name
              const fluxBadge = getFluxNodeBadge(detail);
              return <Badge variant="outline" className={fluxBadge.className}>{fluxBadge.label}</Badge>;
            };

            const renderSize = () => {
              if (sizeBytes === null || sizeBytes === undefined) return null;
              return (
                <div className="text-[11px] uppercase tracking-wide text-muted-foreground">
                  {sizeBytes.toLocaleString()} bytes
                </div>
              );
            };

            const description = () => {
              if (detail.kind === "coinbase") {
                // Show coinbase reward (simplified - no breakdown for faster loading)
                return (
                  <div className="space-y-1">
                    <div className="font-medium">Block reward: {formatFlux(detail.value)} FLUX</div>
                    {renderSize()}
                  </div>
                );
              }
              if (detail.kind === "transfer") {
                // Use fromAddr/toAddr directly from txDetails (no batch API needed)
                const fromAddr = detail.fromAddr;
                const toAddr = detail.toAddr;

                // Calculate display amount - for shielded txs, show the appropriate value
                // Shielded → Transparent: value (output) is the deshielded amount
                // Transparent → Shielded: calculate shieldedAmount = valueIn - value - fee
                const isShieldedCapable = detail.version === 2 || detail.version === 4;
                const valueIn = detail.valueIn || 0;
                const valueOut = detail.value || 0;
                const fee = detail.fee || 0;

                // Determine if this is a shielding tx (transparent → shielded)
                const isShieldingTx = isShieldedCapable && !toAddr && fromAddr && valueIn > valueOut;
                const shieldedAmount = isShieldingTx ? valueIn - valueOut - fee : 0;

                // Choose the display amount
                const displayAmount = isShieldingTx && shieldedAmount > 0
                  ? shieldedAmount  // Show shielded amount for transparent→shielded
                  : valueOut;       // Show output for normal/deshielding txs

                return (
                  <div className="space-y-1">
                    <div className="flex items-center gap-1 text-xs text-muted-foreground">
                      {fromAddr ? (
                        <Link href={`/address/${fromAddr}`} className="truncate max-w-[140px] hover:underline" title={fromAddr}>
                          {fromAddr.slice(0, 8)}...{fromAddr.slice(-6)}
                        </Link>
                      ) : (
                        <span>Shielded pool</span>
                      )}
                      <ArrowRight className="h-3 w-3" />
                      {toAddr ? (
                        <Link href={`/address/${toAddr}`} className="truncate max-w-[140px] hover:underline" title={toAddr}>
                          {toAddr.slice(0, 8)}...{toAddr.slice(-6)}
                        </Link>
                      ) : (
                        <span>Shielded pool</span>
                      )}
                      <span className="ml-2 font-medium text-foreground">{formatFlux(displayAmount)} FLUX</span>
                    </div>
                    {renderSize()}
                  </div>
                );
              }
              if (detail.kind === "fluxnode_confirm") {
                return (
                  <div className="space-y-1">
                    <div>{detail.fluxnodeIp ? `Confirming node at ${detail.fluxnodeIp}` : "FluxNode confirmation"}</div>
                    {renderSize()}
                  </div>
                );
              }
              if (detail.kind === "fluxnode_start") {
                return (
                  <div className="space-y-1">
                    <div>{detail.fluxnodeIp ? `Starting node at ${detail.fluxnodeIp}` : "FluxNode starting"}</div>
                    {renderSize()}
                  </div>
                );
              }
              return renderSize() ?? "FluxNode message";
            };

            return (
              <div key={detail.txid} className="flex items-center gap-3 rounded-lg border bg-card p-3">
                <div className="flex h-8 w-8 items-center justify-center rounded-full bg-muted text-sm font-medium">
                  {globalIndex + 1}
                </div>
                <div className="flex-1 min-w-0 space-y-1">
                  <div className="flex items-center gap-2 flex-wrap">
                    <Link href={`/tx/${detail.txid}`} className="font-mono text-sm hover:text-primary truncate">
                      {detail.txid.slice(0, 16)}...{detail.txid.slice(-8)}
                    </Link>
                    {badge()}
                  </div>
                  <div className="text-xs text-muted-foreground" aria-live="polite">
                    {description()}
                  </div>
                </div>
                <div className="flex items-center gap-1 flex-shrink-0">
                  <CopyButton text={detail.txid} />
                  <Button variant="ghost" size="icon" asChild>
                    <Link href={`/tx/${detail.txid}`}>
                      <ArrowRight className="h-4 w-4" />
                    </Link>
                  </Button>
                </div>
              </div>
            );
          })}

        {totalPages > 1 && (
          <div className="w-full">
            <div className="flex flex-col sm:flex-row items-center justify-center sm:justify-between gap-2 pt-4 border-t">
              {/* Left navigation group */}
              <div className="flex items-center gap-1 min-w-0">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => goToPage(1)}
                  disabled={currentPage === 1}
                  title="First page"
                  className="h-8 w-8 sm:h-9 sm:w-9 p-0 flex-shrink-0"
                >
                  <ChevronsLeft className="h-4 w-4" />
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => goToPage(currentPage - 1)}
                  disabled={currentPage === 1}
                  className="h-8 sm:h-9 px-2 sm:px-3 flex-shrink-0"
                >
                  <ChevronLeft className="h-4 w-4 sm:mr-1" />
                  <span className="hidden sm:inline">Previous</span>
                </Button>
              </div>

              {/* Page numbers - adaptive layout based on count */}
              <div className="flex items-center justify-center gap-1 min-w-0 flex-wrap">
                {Array.from({ length: Math.min(5, totalPages) }).map((_, i) => {
                  let pageNum: number;
                  if (totalPages <= 5) {
                    pageNum = i + 1;
                  } else if (currentPage <= 3) {
                    pageNum = i + 1;
                  } else if (currentPage >= totalPages - 2) {
                    pageNum = totalPages - 4 + i;
                  } else {
                    pageNum = currentPage - 2 + i;
                  }

                  return (
                    <Button
                      key={pageNum}
                      variant={currentPage === pageNum ? "default" : "outline"}
                      size="sm"
                      onClick={() => goToPage(pageNum)}
                      className="h-8 w-8 sm:h-9 sm:w-10 p-0 text-xs sm:text-sm flex-shrink-0 min-w-0"
                    >
                      {pageNum}
                    </Button>
                  );
                })}
              </div>

              {/* Right navigation group */}
              <div className="flex items-center gap-1 min-w-0">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => goToPage(currentPage + 1)}
                  disabled={currentPage === totalPages}
                  className="h-8 sm:h-9 px-2 sm:px-3 flex-shrink-0"
                >
                  <span className="hidden sm:inline">Next</span>
                  <ChevronRight className="h-4 w-4 sm:ml-1" />
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => goToPage(totalPages)}
                  disabled={currentPage === totalPages}
                  title="Last page"
                  className="h-8 w-8 sm:h-9 sm:w-9 p-0 flex-shrink-0"
                >
                  <ChevronsRight className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
