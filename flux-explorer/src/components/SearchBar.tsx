"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Search } from "lucide-react";

export function SearchBar() {
  const [query, setQuery] = useState("");
  const router = useRouter();

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;

    const trimmedQuery = query.trim();

    // Use smart search route that handles ambiguous queries
    router.push(`/search/${encodeURIComponent(trimmedQuery)}`);
    setQuery("");
  };

  return (
    <form onSubmit={handleSearch} className="w-full max-w-2xl">
      <div className="relative">
        {/* Glow effect behind the search bar */}
        <div className="absolute -inset-1 bg-gradient-to-r from-[var(--flux-cyan)]/20 via-[var(--flux-purple)]/20 to-[var(--flux-cyan)]/20 rounded-2xl blur-lg opacity-60" />

        <div className="relative flex items-center gap-3 p-1.5 rounded-xl flux-glass-strong">
          <div className="relative flex-1">
            <Search className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-[var(--flux-text-muted)]" />
            <Input
              type="text"
              placeholder="Search by block, transaction, or address..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="pl-12 pr-4 h-12 text-base bg-transparent border-0 focus-visible:ring-0 focus-visible:ring-offset-0 placeholder:text-[var(--flux-text-muted)]"
            />
          </div>
          <Button
            type="submit"
            size="lg"
            className="h-11 px-6 rounded-lg shrink-0"
          >
            <Search className="h-4 w-4 mr-2" />
            Search
          </Button>
        </div>
      </div>
      <p className="text-xs text-[var(--flux-text-muted)] mt-3 text-center">
        Search for blocks, transactions, or addresses on the Flux network
      </p>
    </form>
  );
}
