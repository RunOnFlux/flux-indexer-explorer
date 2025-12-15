import * as React from "react"
import { Slot } from "@radix-ui/react-slot"
import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-lg text-sm font-medium transition-all duration-200 ease-flux focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--flux-cyan)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--flux-bg-deep)] disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        default:
          "bg-gradient-to-r from-[var(--flux-cyan)] to-[var(--flux-blue)] text-[var(--flux-bg-deep)] font-semibold shadow-flux-glow hover:shadow-[0_0_30px_rgba(56,232,255,0.5)] hover:scale-[1.02] active:scale-[0.98]",
        destructive:
          "bg-destructive text-destructive-foreground shadow-sm hover:bg-destructive/90",
        outline:
          "border border-[var(--flux-border)] bg-transparent text-[var(--flux-text-primary)] hover:border-[var(--flux-border-hover)] hover:bg-white/5",
        secondary:
          "bg-[var(--flux-bg-elevated)] text-[var(--flux-text-secondary)] border border-[var(--flux-border)] hover:text-[var(--flux-text-primary)] hover:border-[var(--flux-border-hover)]",
        ghost:
          "text-[var(--flux-text-secondary)] hover:text-[var(--flux-text-primary)] hover:bg-white/5",
        link:
          "text-[var(--flux-cyan)] underline-offset-4 hover:underline hover:text-[#7df3ff]",
        electric:
          "relative overflow-hidden bg-transparent border border-[var(--flux-cyan)] text-[var(--flux-cyan)] hover:text-[var(--flux-bg-deep)] before:absolute before:inset-0 before:bg-[var(--flux-cyan)] before:translate-y-full hover:before:translate-y-0 before:transition-transform before:duration-300 [&>*]:relative",
      },
      size: {
        default: "h-10 px-5 py-2",
        sm: "h-8 rounded-md px-3 text-xs",
        lg: "h-12 rounded-xl px-8 text-base",
        icon: "h-9 w-9",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
)

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild = false, ...props }, ref) => {
    const Comp = asChild ? Slot : "button"
    return (
      <Comp
        className={cn(buttonVariants({ variant, size, className }))}
        ref={ref}
        {...props}
      />
    )
  }
)
Button.displayName = "Button"

export { Button, buttonVariants }
