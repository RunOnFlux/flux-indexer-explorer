"use client";

import { Github, ExternalLink, AlertCircle } from "lucide-react";

export function Footer() {
  return (
    <footer className="relative border-t border-[var(--flux-border)]">
      {/* Glass background */}
      <div className="absolute inset-0 flux-glass" />

      <div className="relative container py-8 max-w-[1600px] mx-auto px-4 sm:px-6">
        <div className="flex flex-col items-center gap-4 text-center">
          {/* Links */}
          <div className="flex flex-wrap items-center justify-center gap-3 sm:gap-6">
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
        </div>
      </div>
    </footer>
  );
}
