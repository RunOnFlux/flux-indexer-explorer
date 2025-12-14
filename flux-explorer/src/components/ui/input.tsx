import * as React from "react"

import { cn } from "@/lib/utils"

const Input = React.forwardRef<HTMLInputElement, React.ComponentProps<"input">>(
  ({ className, type, ...props }, ref) => {
    return (
      <input
        type={type}
        className={cn(
          "flex h-10 w-full rounded-lg border border-[var(--flux-border)] bg-[var(--flux-bg-surface)]/50 px-4 py-2 text-sm text-[var(--flux-text-primary)] shadow-sm transition-all duration-200",
          "file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-[var(--flux-text-primary)]",
          "placeholder:text-[var(--flux-text-muted)]",
          "hover:border-[var(--flux-border-hover)]",
          "focus-visible:outline-none focus-visible:border-[var(--flux-cyan)] focus-visible:ring-2 focus-visible:ring-[var(--flux-cyan)]/20",
          "disabled:cursor-not-allowed disabled:opacity-50",
          className
        )}
        ref={ref}
        {...props}
      />
    )
  }
)
Input.displayName = "Input"

export { Input }
