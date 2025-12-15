import { SearchBar } from "@/components/SearchBar";
import { NetworkStats } from "@/components/home/NetworkStats";
import { LatestBlocks } from "@/components/home/LatestBlocks";
import { RecentBlockRewards } from "@/components/home/RecentBlockRewards";

export default function Home() {
  return (
    <div className="min-h-screen">
      {/* Hero Section */}
      <div className="relative overflow-hidden">
        {/* Animated gradient orbs */}
        <div className="absolute top-0 left-1/4 w-[500px] h-[500px] rounded-full bg-[var(--flux-cyan)]/10 blur-[120px] animate-flux-float" />
        <div className="absolute top-20 right-1/4 w-[400px] h-[400px] rounded-full bg-[var(--flux-purple)]/10 blur-[100px] animate-flux-float animation-delay-1000" />
        <div className="absolute -bottom-20 left-1/2 w-[600px] h-[300px] rounded-full bg-[var(--flux-blue)]/5 blur-[80px]" />

        <div className="relative container py-16 sm:py-20 md:py-28 max-w-[1600px] mx-auto px-4 sm:px-6">
          <div className="flex flex-col items-center text-center space-y-8 sm:space-y-10">
            {/* Title with chrome effect */}
            <div className="space-y-4 sm:space-y-5 max-w-4xl">
              <h1 className="text-4xl font-black tracking-tight sm:text-6xl lg:text-7xl flux-text-chrome animate-flux-fade-in">
                Flux Explorer
              </h1>
              <p className="text-base sm:text-lg md:text-xl text-[var(--flux-text-secondary)] max-w-2xl mx-auto px-4 animate-flux-fade-in animation-delay-100">
                Explore the decentralized cloud in real-time.{" "}
                <span className="text-[var(--flux-cyan)]">Powered by PoUW consensus.</span>
              </p>
            </div>

            {/* Search Bar */}
            <div className="w-full flex justify-center px-4 animate-flux-fade-in animation-delay-200">
              <SearchBar />
            </div>

            {/* Quick stats teaser */}
            <div className="flex flex-wrap justify-center gap-6 sm:gap-10 text-sm animate-flux-fade-in animation-delay-300">
              <div className="flex items-center gap-2 text-[var(--flux-text-muted)]">
                <div className="w-2 h-2 rounded-full bg-[var(--flux-green)] animate-flux-pulse" />
                <span>Network Active</span>
              </div>
              <div className="flex items-center gap-2 text-[var(--flux-text-muted)]">
                <div className="w-2 h-2 rounded-full bg-[var(--flux-cyan)]" />
                <span>~30s Block Time</span>
              </div>
              <div className="flex items-center gap-2 text-[var(--flux-text-muted)]">
                <div className="w-2 h-2 rounded-full bg-[var(--flux-purple)]" />
                <span>Decentralized</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Network Stats Section */}
      <div className="container py-10 max-w-[1600px] mx-auto px-4 sm:px-6">
        <div className="space-y-8">
          <div className="flex items-center gap-3">
            <div className="h-8 w-1 rounded-full bg-gradient-to-b from-[var(--flux-cyan)] to-[var(--flux-purple)]" />
            <div>
              <h2 className="text-xl sm:text-2xl font-bold text-[var(--flux-text-primary)]">
                Network Statistics
              </h2>
              <p className="text-sm text-[var(--flux-text-muted)]">
                Real-time metrics from the Flux network
              </p>
            </div>
          </div>
          <NetworkStats />
        </div>
      </div>

      {/* Latest Data Section */}
      <div className="container py-10 pb-20 max-w-[1600px] mx-auto px-4 sm:px-6">
        <div className="grid gap-6 sm:gap-8 lg:grid-cols-2">
          <LatestBlocks />
          <RecentBlockRewards />
        </div>
      </div>
    </div>
  );
}
