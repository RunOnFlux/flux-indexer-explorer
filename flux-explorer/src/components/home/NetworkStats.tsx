"use client";

import { useFluxNodeCount, useFluxInstancesCount, useArcaneAdoption } from "@/lib/api/hooks/useFluxStats";
import { useFluxSupply } from "@/lib/api/hooks/useFluxSupply";
import { useDashboardStats } from "@/lib/api/hooks/useDashboardStats";
import { useLatestBlocks } from "@/lib/api";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Activity,
  Coins,
  TrendingUp,
  Database,
  Clock,
  Server,
  Layers,
  Boxes,
} from "lucide-react";

interface StatCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: React.ReactNode;
  accentColor: string;
  glowColor: string;
  isLoading?: boolean;
  delay?: number;
}

function StatCard({ title, value, subtitle, icon, accentColor, glowColor, isLoading, delay = 0 }: StatCardProps) {
  return (
    <div
      className="group relative overflow-hidden rounded-xl flux-glass-card p-5 transition-all duration-300 hover:scale-[1.02] animate-flux-fade-in"
      style={{ animationDelay: `${delay}ms` }}
    >
      {/* Top accent line */}
      <div
        className="absolute top-0 left-0 right-0 h-[2px] opacity-60 group-hover:opacity-100 transition-opacity"
        style={{ background: `linear-gradient(90deg, transparent, ${accentColor}, transparent)` }}
      />

      {/* Icon glow effect on hover */}
      <div
        className="absolute -top-10 -right-10 w-32 h-32 rounded-full blur-3xl opacity-0 group-hover:opacity-20 transition-opacity duration-500"
        style={{ background: glowColor }}
      />

      <div className="relative">
        {/* Header */}
        <div className="flex items-center gap-2 mb-3">
          <div
            className="p-2 rounded-lg transition-all duration-300 group-hover:scale-110"
            style={{ background: `${accentColor}20`, color: accentColor }}
          >
            {icon}
          </div>
          <span className="text-sm font-medium text-[var(--flux-text-muted)]">{title}</span>
        </div>

        {/* Value */}
        {isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-8 w-28 bg-white/5" />
            {subtitle && <Skeleton className="h-4 w-20 bg-white/5" />}
          </div>
        ) : (
          <div>
            <div
              className="text-2xl font-bold tracking-tight transition-colors"
              style={{ color: 'var(--flux-text-primary)' }}
            >
              {value}
            </div>
            {subtitle && (
              <p className="text-xs text-[var(--flux-text-muted)] mt-1">{subtitle}</p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

interface TooltipCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: React.ReactNode;
  accentColor: string;
  glowColor: string;
  isLoading?: boolean;
  delay?: number;
  children?: React.ReactNode;
  tooltipContent?: React.ReactNode;
}

function TooltipCard({
  title,
  value,
  subtitle,
  icon,
  accentColor,
  glowColor,
  isLoading,
  delay = 0,
  tooltipContent,
}: TooltipCardProps) {
  return (
    <div
      className="group relative overflow-visible rounded-xl flux-glass-card p-5 transition-all duration-300 hover:scale-[1.02] animate-flux-fade-in"
      style={{ animationDelay: `${delay}ms` }}
    >
      {/* Top accent line */}
      <div
        className="absolute top-0 left-0 right-0 h-[2px] opacity-60 group-hover:opacity-100 transition-opacity rounded-t-xl"
        style={{ background: `linear-gradient(90deg, transparent, ${accentColor}, transparent)` }}
      />

      {/* Icon glow effect on hover */}
      <div
        className="absolute -top-10 -right-10 w-32 h-32 rounded-full blur-3xl opacity-0 group-hover:opacity-20 transition-opacity duration-500"
        style={{ background: glowColor }}
      />

      <div className="relative">
        {/* Header */}
        <div className="flex items-center gap-2 mb-3">
          <div
            className="p-2 rounded-lg transition-all duration-300 group-hover:scale-110"
            style={{ background: `${accentColor}20`, color: accentColor }}
          >
            {icon}
          </div>
          <span className="text-sm font-medium text-[var(--flux-text-muted)]">{title}</span>
        </div>

        {/* Value */}
        {isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-8 w-28 bg-white/5" />
            {subtitle && <Skeleton className="h-4 w-20 bg-white/5" />}
          </div>
        ) : (
          <div>
            <div className="text-2xl font-bold tracking-tight text-[var(--flux-text-primary)]">
              {value}
            </div>
            {subtitle && (
              <p className="text-xs text-[var(--flux-text-muted)] mt-1">{subtitle}</p>
            )}
          </div>
        )}

        {/* Tooltip */}
        {tooltipContent && (
          <div className="absolute left-0 bottom-full mb-3 p-4 rounded-xl flux-glass-strong opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200 z-50 min-w-[220px] shadow-2xl">
            {tooltipContent}
            {/* Arrow */}
            <div className="absolute left-6 bottom-[-6px] w-3 h-3 bg-[var(--flux-bg-surface)] border-r border-b border-[var(--flux-border)] rotate-45" />
          </div>
        )}
      </div>
    </div>
  );
}

export function NetworkStats() {
  const { data: supplyStats, isLoading: supplyLoading } = useFluxSupply();
  const { data: dashboardStats, isLoading: dashboardLoading } = useDashboardStats();
  const { data: latestBlocks, isLoading: latestBlocksLoading } = useLatestBlocks(1);
  const { data: nodeCount, isLoading: nodeCountLoading } = useFluxNodeCount();
  const { data: instancesCount, isLoading: instancesCountLoading } = useFluxInstancesCount();
  const { data: arcaneAdoption, isLoading: arcaneLoading } = useArcaneAdoption();
  const avgBlockTime = dashboardStats?.averages.blockTimeSeconds ?? null;
  const tx24h = dashboardStats?.transactions24h ?? null;
  const latestHeight = latestBlocks?.[0]?.height ?? null;
  const blockHeightValue = latestHeight !== null && latestHeight !== undefined
    ? latestHeight.toLocaleString()
    : "—";

  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
      <StatCard
        title="Block Height"
        value={blockHeightValue}
        subtitle="Current height"
        icon={<Database className="h-4 w-4" />}
        accentColor="#38e8ff"
        glowColor="#38e8ff"
        isLoading={latestBlocksLoading && latestHeight === null}
        delay={0}
      />

      <StatCard
        title="App Instances"
        value={instancesCount?.toLocaleString() ?? "—"}
        subtitle="Running on network"
        icon={<Boxes className="h-4 w-4" />}
        accentColor="#a855f7"
        glowColor="#a855f7"
        isLoading={instancesCountLoading}
        delay={50}
      />

      <TooltipCard
        title="PoUW Nodes"
        value={nodeCount?.total.toLocaleString() ?? "—"}
        subtitle="Active FluxNodes"
        icon={<Server className="h-4 w-4" />}
        accentColor="#38e8ff"
        glowColor="#38e8ff"
        isLoading={nodeCountLoading}
        delay={100}
        tooltipContent={
          nodeCount && (
            <div className="space-y-2.5 text-sm">
              <div className="flex items-center justify-between gap-4">
                <span className="flex items-center gap-2 text-[var(--tier-cumulus)]">
                  <span className="w-2.5 h-2.5 rounded-full bg-[var(--tier-cumulus)]" />
                  CUMULUS
                </span>
                <span className="font-bold text-[var(--tier-cumulus)]">
                  {nodeCount["cumulus-enabled"].toLocaleString()}
                </span>
              </div>
              <div className="flex items-center justify-between gap-4">
                <span className="flex items-center gap-2 text-[var(--tier-nimbus)]">
                  <span className="w-2.5 h-2.5 rounded-full bg-[var(--tier-nimbus)]" />
                  NIMBUS
                </span>
                <span className="font-bold text-[var(--tier-nimbus)]">
                  {nodeCount["nimbus-enabled"].toLocaleString()}
                </span>
              </div>
              <div className="flex items-center justify-between gap-4">
                <span className="flex items-center gap-2 text-[var(--tier-stratus)]">
                  <span className="w-2.5 h-2.5 rounded-full bg-[var(--tier-stratus)]" />
                  STRATUS
                </span>
                <span className="font-bold text-[var(--tier-stratus)]">
                  {nodeCount["stratus-enabled"].toLocaleString()}
                </span>
              </div>
            </div>
          )
        }
      />

      <TooltipCard
        title="ArcaneOS"
        value={arcaneAdoption ? `${arcaneAdoption.percentage.toFixed(1)}%` : "—"}
        subtitle={arcaneAdoption ? `${arcaneAdoption.arcane.toLocaleString()} nodes` : undefined}
        icon={<Activity className="h-4 w-4" />}
        accentColor="#22c55e"
        glowColor="#22c55e"
        isLoading={arcaneLoading}
        delay={150}
        tooltipContent={
          arcaneAdoption && (
            <div className="space-y-2.5 text-sm">
              <div className="flex items-center justify-between gap-4">
                <span className="flex items-center gap-2 text-[var(--flux-green)]">
                  <span className="w-2.5 h-2.5 rounded-full bg-[var(--flux-green)]" />
                  Arcane
                </span>
                <span className="font-bold text-[var(--flux-green)]">
                  {arcaneAdoption.arcane.toLocaleString()}
                </span>
              </div>
              <div className="flex items-center justify-between gap-4">
                <span className="flex items-center gap-2 text-[var(--flux-gold)]">
                  <span className="w-2.5 h-2.5 rounded-full bg-[var(--flux-gold)]" />
                  Legacy
                </span>
                <span className="font-bold text-[var(--flux-gold)]">
                  {arcaneAdoption.legacy.toLocaleString()}
                </span>
              </div>
            </div>
          )
        }
      />

      <StatCard
        title="Circulating Supply"
        value={supplyStats ? `${(supplyStats.circulatingSupply / 1e6).toFixed(2)}M` : "—"}
        subtitle={supplyStats ? `${supplyStats.circulatingSupply.toLocaleString()} FLUX` : undefined}
        icon={<Layers className="h-4 w-4" />}
        accentColor="#38e8ff"
        glowColor="#38e8ff"
        isLoading={supplyLoading}
        delay={200}
      />

      <StatCard
        title="Max Supply"
        value={supplyStats ? `${(supplyStats.maxSupply / 1e6).toFixed(2)}M` : "—"}
        subtitle={supplyStats ? `${supplyStats.maxSupply.toLocaleString()} FLUX` : undefined}
        icon={<Coins className="h-4 w-4" />}
        accentColor="#a855f7"
        glowColor="#a855f7"
        isLoading={supplyLoading}
        delay={250}
      />

      <StatCard
        title="Avg Block Time"
        value={avgBlockTime !== null ? `${avgBlockTime.toFixed(1)}s` : "—"}
        subtitle="Target: 30 seconds"
        icon={<Clock className="h-4 w-4" />}
        accentColor="#fbbf24"
        glowColor="#fbbf24"
        isLoading={dashboardLoading && avgBlockTime === null}
        delay={300}
      />

      <StatCard
        title="Transactions (24h)"
        value={tx24h !== null ? tx24h.toLocaleString() : "—"}
        subtitle="Last 24 hours"
        icon={<TrendingUp className="h-4 w-4" />}
        accentColor="#ec4899"
        glowColor="#ec4899"
        isLoading={dashboardLoading && tx24h === null}
        delay={350}
      />
    </div>
  );
}
