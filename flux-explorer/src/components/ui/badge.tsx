import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-[var(--flux-cyan)] focus:ring-offset-2",
  {
    variants: {
      variant: {
        default:
          "border-transparent bg-[var(--flux-cyan)]/15 text-[var(--flux-cyan)] shadow-sm",
        secondary:
          "border-transparent bg-[var(--flux-bg-elevated)] text-[var(--flux-text-secondary)]",
        destructive:
          "border-transparent bg-destructive/15 text-destructive",
        outline:
          "border-[var(--flux-border)] text-[var(--flux-text-secondary)]",
        success:
          "border-transparent bg-[var(--flux-green)]/15 text-[var(--flux-green)]",
        warning:
          "border-transparent bg-[var(--flux-orange)]/15 text-[var(--flux-orange)]",
        purple:
          "border-transparent bg-[var(--flux-purple)]/15 text-[var(--flux-purple)]",
        // Tier variants
        cumulus:
          "border-[var(--tier-cumulus)]/30 bg-[var(--tier-cumulus)]/15 text-[var(--tier-cumulus)]",
        nimbus:
          "border-[var(--tier-nimbus)]/30 bg-[var(--tier-nimbus)]/15 text-[var(--tier-nimbus)]",
        stratus:
          "border-[var(--tier-stratus)]/30 bg-[var(--tier-stratus)]/15 text-[var(--tier-stratus)]",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
)

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return (
    <div className={cn(badgeVariants({ variant }), className)} {...props} />
  )
}

export { Badge, badgeVariants }
