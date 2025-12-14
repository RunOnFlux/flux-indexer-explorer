"use client";

import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Pickaxe, Coins, ArrowRight } from "lucide-react";
import { useDashboardStats } from "@/lib/api/hooks/useDashboardStats";
import { getRewardLabel } from "@/lib/block-rewards";

export function RecentBlockRewards() {
  const { data: dashboardStats, isLoading } = useDashboardStats();
  const latestReward = dashboardStats?.latestRewards?.[0];

  const rewards = latestReward
    ? latestReward.outputs
        .filter((output) => output.value > 0)
        .map((output) => {
          const label = getRewardLabel(output.value, latestReward.height);
          return {
            address: output.address || "Unknown",
            amount: output.value,
            tier: label.type,
            color: label.color,
          };
        })
    : [];

  const totalReward = rewards.reduce((sum, reward) => sum + reward.amount, 0);

  return (
    <div className="rounded-xl flux-glass-card overflow-hidden">
      {/* Header */}
      <div className="p-5 border-b border-[var(--flux-border)]">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="p-2 rounded-lg bg-[var(--flux-gold)]/10">
              <Pickaxe className="h-5 w-5 text-[var(--flux-gold)] animate-flux-float" />
            </div>
            <h3 className="font-semibold text-[var(--flux-text-primary)]">Block Rewards</h3>
          </div>
          <Link
            href={latestReward ? `/block/${latestReward.hash}` : '#'}
            className="flex items-center gap-1.5 text-sm text-[var(--flux-cyan)] hover:text-[#7df3ff] transition-colors"
          >
            View block
            <ArrowRight className="h-4 w-4" />
          </Link>
        </div>
      </div>

      {/* Content */}
      <div className="p-5">
        {isLoading ? (
          <div className="space-y-4">
            <Skeleton className="h-8 w-48" />
            <Skeleton className="h-6 w-32" />
            <div className="space-y-2 mt-4">
              {[...Array(3)].map((_, i) => (
                <Skeleton key={i} className="h-16 w-full" />
              ))}
            </div>
          </div>
        ) : latestReward ? (
          <div className="space-y-5">
            {/* Block Info */}
            <div>
              <Link
                href={`/block/${latestReward.hash}`}
                className="text-2xl font-bold text-[var(--flux-text-primary)] hover:text-[var(--flux-cyan)] transition-colors"
              >
                Block #{latestReward.height.toLocaleString()}
              </Link>
              <p className="text-sm text-[var(--flux-text-muted)] mt-1 flex items-center gap-2">
                <Coins className="h-4 w-4 text-[var(--flux-gold)]" />
                Total Reward: <span className="text-[var(--flux-gold)] font-semibold">{totalReward.toFixed(2)} FLUX</span>
              </p>
            </div>

            {/* Reward Recipients */}
            <div className="space-y-2">
              <h4 className="text-xs font-medium text-[var(--flux-text-muted)] uppercase tracking-wider mb-3">
                Distribution
              </h4>
              {rewards.map((reward, i) => {
                const linkTarget = reward.address !== "Unknown" ? `/address/${reward.address}` : "#";
                return (
                  <Link
                    key={`${latestReward.height}-${reward.address}-${i}`}
                    href={linkTarget}
                    className="flex items-center justify-between p-3 rounded-lg hover:bg-white/[0.03] transition-all duration-200 border border-[var(--flux-border)] group"
                  >
                    <div className="flex items-center gap-3">
                      <div className={`w-1 h-10 rounded-full ${reward.color}`} />
                      <div className="min-w-0">
                        <Badge
                          variant="outline"
                          className="mb-1 text-[10px]"
                          style={{
                            color: reward.color.includes('pink') ? 'var(--tier-cumulus)' :
                                   reward.color.includes('purple') ? 'var(--tier-nimbus)' :
                                   reward.color.includes('blue') ? 'var(--tier-stratus)' :
                                   reward.color.includes('yellow') ? 'var(--flux-gold)' :
                                   reward.color.includes('green') ? 'var(--flux-green)' :
                                   'var(--flux-text-secondary)',
                            borderColor: 'currentColor',
                            backgroundColor: 'transparent',
                          }}
                        >
                          {reward.tier}
                        </Badge>
                        <p className="text-xs font-mono text-[var(--flux-text-muted)] truncate max-w-[200px] group-hover:text-[var(--flux-text-secondary)] transition-colors">
                          {reward.address.substring(0, 12)}...{reward.address.substring(Math.max(reward.address.length - 8, 0))}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-1.5 text-sm font-mono font-bold text-[var(--flux-text-primary)]">
                      <Coins className="h-3.5 w-3.5 text-[var(--flux-gold)]" />
                      {reward.amount.toFixed(8)}
                    </div>
                  </Link>
                );
              })}
            </div>
          </div>
        ) : (
          <div className="text-center text-sm text-[var(--flux-text-muted)] py-8">
            No block data available
          </div>
        )}
      </div>
    </div>
  );
}
