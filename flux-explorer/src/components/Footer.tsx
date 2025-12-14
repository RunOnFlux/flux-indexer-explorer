"use client";

import { Heart, Github, ExternalLink, AlertCircle } from "lucide-react";
import { CopyButton } from "@/components/ui/copy-button";

export function Footer() {
  const donationAddress = "t3aYE1U7yncYeCoAGmfpbEXo3dbQSegZCSP";

  return (
    <footer className="relative border-t border-[var(--flux-border)]">
      {/* Glass background */}
      <div className="absolute inset-0 flux-glass" />

      <div className="relative container py-10 max-w-[1600px] mx-auto px-4 sm:px-6">
        <div className="flex flex-col items-center gap-6 text-center">
          {/* Built with love message */}
          <p className="text-sm text-[var(--flux-text-secondary)] flex items-center gap-2">
            Built with{" "}
            <Heart className="h-4 w-4 text-pink-500 fill-pink-500 animate-flux-pulse" />{" "}
            for the Flux community
          </p>

          {/* Donation section */}
          <div className="flex flex-col items-center gap-3">
            <p className="text-xs text-[var(--flux-text-muted)]">
              Donations help cover development and hosting costs
            </p>
            <div className="flex items-center gap-3 px-4 py-3 rounded-xl flux-glass-card max-w-full">
              <code className="text-xs font-mono text-[var(--flux-cyan)] break-all max-w-[250px] sm:max-w-none">
                {donationAddress}
              </code>
              <CopyButton text={donationAddress} className="shrink-0" />
            </div>
          </div>

          {/* Links */}
          <div className="flex flex-wrap items-center justify-center gap-3 sm:gap-6 pt-2">
            <a
              href="https://runonflux.io"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 text-xs text-[var(--flux-text-muted)] hover:text-[var(--flux-cyan)] transition-colors"
            >
              <ExternalLink className="h-3 w-3" />
              Flux Network
            </a>
            <a
              href="https://github.com/Sikbik/flux-blockchain-explorer"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 text-xs text-[var(--flux-text-muted)] hover:text-[var(--flux-cyan)] transition-colors"
            >
              <Github className="h-3 w-3" />
              GitHub
            </a>
            <a
              href="https://github.com/Sikbik/flux-blockchain-explorer/issues"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 text-xs text-[var(--flux-text-muted)] hover:text-[var(--flux-cyan)] transition-colors"
            >
              <AlertCircle className="h-3 w-3" />
              Report Issue
            </a>
          </div>

          {/* Copyright */}
          <p className="text-[10px] text-[var(--flux-text-dim)] pt-2">
            {new Date().getFullYear()} Flux Explorer. Not affiliated with Flux Foundation.
          </p>
        </div>
      </div>
    </footer>
  );
}
