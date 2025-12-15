"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { useLatestBlocks } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Blocks, ArrowRight, Server, Clock } from "lucide-react";

// Format time ago with minutes and seconds
function formatTimeAgo(timestamp: number): string {
  const now = Date.now();
  const diff = Math.floor((now - timestamp * 1000) / 1000); // seconds

  if (diff < 60) {
    return `${diff}s ago`;
  }

  const minutes = Math.floor(diff / 60);
  const seconds = diff % 60;
  return `${minutes}m ${seconds}s ago`;
}

export function LatestBlocks() {
  const { data: blocks, isLoading, error } = useLatestBlocks(6);
  const [, setTick] = useState(0);

  // Force re-render every second to update the time display
  useEffect(() => {
    const interval = setInterval(() => {
      setTick(t => t + 1);
    }, 1000);

    return () => clearInterval(interval);
  }, []);

  if (error) {
    return (
      <div className="rounded-xl flux-glass-card p-6">
        <div className="flex items-center gap-2 mb-4">
          <Blocks className="h-5 w-5 text-[var(--flux-cyan)]" />
          <h3 className="font-semibold text-[var(--flux-text-primary)]">Latest Blocks</h3>
        </div>
        <p className="text-sm text-destructive">Failed to load latest blocks</p>
      </div>
    );
  }

  return (
    <div className="rounded-xl flux-glass-card overflow-hidden">
      {/* Header */}
      <div className="p-5 border-b border-[var(--flux-border)]">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="p-2 rounded-lg bg-[var(--flux-cyan)]/10">
              <Blocks className="h-5 w-5 text-[var(--flux-cyan)]" />
            </div>
            <h3 className="font-semibold text-[var(--flux-text-primary)]">Latest Blocks</h3>
          </div>
          <Link
            href="/blocks"
            className="flex items-center gap-1.5 text-sm text-[var(--flux-cyan)] hover:text-[#7df3ff] transition-colors"
          >
            View all
            <ArrowRight className="h-4 w-4" />
          </Link>
        </div>
      </div>

      {/* Content */}
      <div className="divide-y divide-[var(--flux-border)]">
        {isLoading ? (
          [...Array(6)].map((_, i) => (
            <div key={i} className="p-4 space-y-3">
              <div className="flex items-center justify-between">
                <Skeleton className="h-5 w-24 bg-white/5" />
                <Skeleton className="h-5 w-16 bg-white/5" />
              </div>
              <div className="flex items-center justify-between">
                <Skeleton className="h-4 w-32 bg-white/5" />
                <Skeleton className="h-4 w-20 bg-white/5" />
              </div>
            </div>
          ))
        ) : (
          blocks?.map((block, index) => {
            const nodeCount = block.nodeConfirmationCount ?? 0;
            const regularTxCount = block.regularTxCount ?? block.txlength ?? 0;
            return (
              <Link
                key={block.hash}
                href={`/block/${block.height}`}
                className="block p-4 hover:bg-white/[0.02] transition-all duration-200 group"
                style={{ animationDelay: `${index * 50}ms` }}
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="space-y-2 flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="font-mono font-bold text-[var(--flux-cyan)] group-hover:flux-text-glow transition-all">
                        #{block.height.toLocaleString()}
                      </span>
                      <Badge variant="secondary" className="text-xs">
                        {regularTxCount} txs
                      </Badge>
                      {nodeCount > 0 && (
                        <Badge variant="outline" className="text-xs gap-1">
                          <Server className="h-3 w-3" />
                          {nodeCount}
                        </Badge>
                      )}
                    </div>
                    <div className="flex items-center gap-2 text-xs text-[var(--flux-text-muted)]">
                      <span className="font-mono truncate">{block.hash.substring(0, 20)}...</span>
                    </div>
                  </div>
                  <div className="text-right space-y-1.5 shrink-0">
                    <div className="flex items-center gap-1 text-xs text-[var(--flux-text-muted)] font-mono">
                      <Clock className="h-3 w-3" />
                      {formatTimeAgo(block.time)}
                    </div>
                    <div className="text-xs text-[var(--flux-text-dim)]">
                      {(block.size / 1024).toFixed(1)} KB
                    </div>
                  </div>
                </div>
              </Link>
            );
          })
        )}
      </div>
    </div>
  );
}
