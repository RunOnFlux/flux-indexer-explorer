"use client";

import Link from "next/link";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Search, TrendingUp, Menu, X, Blocks } from "lucide-react";
import { Button } from "@/components/ui/button";

export function Header() {
  const pathname = usePathname();
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const isHomePage = pathname === "/";

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;
    router.push(`/search/${encodeURIComponent(query.trim())}`);
    setQuery("");
    setMobileMenuOpen(false);
  };

  return (
    <header className="sticky top-0 z-50 w-full flux-electric-border">
      {/* Glass background */}
      <div className="absolute inset-0 flux-glass-strong" />

      {/* Content */}
      <div className="relative container flex h-16 items-center max-w-[1600px] mx-auto px-4 sm:px-6 gap-3 sm:gap-6">
        {/* Logo */}
        <Link href="/" className="flex items-center gap-2 sm:gap-3 group shrink-0">
          <div className="relative">
            <Image
              src="/flux-logo.svg"
              alt="Flux Logo"
              width={32}
              height={32}
              className="relative z-10 group-hover:scale-110 transition-transform duration-300 ease-flux sm:w-9 sm:h-9"
            />
            {/* Logo glow effect */}
            <div className="absolute inset-0 blur-lg bg-[#38e8ff]/30 group-hover:bg-[#38e8ff]/50 transition-all duration-300 scale-150" />
          </div>
          <span className="font-bold text-lg sm:text-2xl flux-text-gradient group-hover:flux-text-glow transition-all whitespace-nowrap tracking-tight">
            Flux Explorer
          </span>
        </Link>

        {/* Navigation Links - Desktop */}
        <nav className="hidden md:flex items-center gap-1">
          <Link
            href="/blocks"
            className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-[var(--flux-text-secondary)] hover:text-[var(--flux-text-primary)] hover:bg-white/5 transition-all duration-200"
          >
            <Blocks className="h-4 w-4" />
            <span>Blocks</span>
          </Link>
          <Link
            href="/rich-list"
            className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-[var(--flux-text-secondary)] hover:text-[var(--flux-text-primary)] hover:bg-white/5 transition-all duration-200"
          >
            <TrendingUp className="h-4 w-4" />
            <span>Rich List</span>
          </Link>
        </nav>

        {/* Spacer */}
        <div className="flex-1" />

        {/* Search bar - only show on non-homepage */}
        {!isHomePage && (
          <form onSubmit={handleSearch} className="hidden sm:block max-w-md w-full">
            <div className="relative group">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-[var(--flux-text-muted)] group-focus-within:text-[var(--flux-cyan)] transition-colors" />
              <Input
                type="text"
                placeholder="Search address, tx, block..."
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="pl-10 pr-4 h-10 text-sm bg-white/5 border-[var(--flux-border)] hover:border-[var(--flux-border-hover)] focus:border-[var(--flux-cyan)] focus:ring-1 focus:ring-[var(--flux-cyan)]/20 placeholder:text-[var(--flux-text-muted)] transition-all"
              />
            </div>
          </form>
        )}

        {/* Mobile Menu Button */}
        <Button
          variant="ghost"
          size="icon"
          className="md:hidden shrink-0 text-[var(--flux-text-secondary)] hover:text-[var(--flux-text-primary)] hover:bg-white/5"
          onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
        >
          {mobileMenuOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
        </Button>
      </div>

      {/* Mobile Navigation Menu */}
      {mobileMenuOpen && (
        <div className="md:hidden relative border-t border-[var(--flux-border)]">
          <div className="absolute inset-0 flux-glass-strong" />
          <nav className="relative container max-w-[1600px] mx-auto px-4 py-4 space-y-2">
            {/* Mobile Search */}
            {!isHomePage && (
              <form onSubmit={handleSearch} className="mb-4">
                <div className="relative group">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-[var(--flux-text-muted)]" />
                  <Input
                    type="text"
                    placeholder="Search address, tx, block..."
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    className="pl-10 pr-4 h-10 text-sm bg-white/5 border-[var(--flux-border)] placeholder:text-[var(--flux-text-muted)]"
                  />
                </div>
              </form>
            )}

            <Link
              href="/blocks"
              onClick={() => setMobileMenuOpen(false)}
              className="flex items-center gap-3 px-4 py-3 rounded-lg text-[var(--flux-text-secondary)] hover:text-[var(--flux-text-primary)] hover:bg-white/5 transition-all"
            >
              <Blocks className="h-5 w-5" />
              <span className="font-medium">Blocks</span>
            </Link>

            <Link
              href="/rich-list"
              onClick={() => setMobileMenuOpen(false)}
              className="flex items-center gap-3 px-4 py-3 rounded-lg text-[var(--flux-text-secondary)] hover:text-[var(--flux-text-primary)] hover:bg-white/5 transition-all"
            >
              <TrendingUp className="h-5 w-5" />
              <span className="font-medium">Rich List</span>
            </Link>
          </nav>
        </div>
      )}
    </header>
  );
}
