import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: ["class"],
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Outfit', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'ui-monospace', 'monospace'],
        display: ['Outfit', 'system-ui', 'sans-serif'],
      },
      colors: {
        background: 'hsl(var(--background))',
        foreground: 'hsl(var(--foreground))',
        card: {
          DEFAULT: 'hsl(var(--card))',
          foreground: 'hsl(var(--card-foreground))'
        },
        popover: {
          DEFAULT: 'hsl(var(--popover))',
          foreground: 'hsl(var(--popover-foreground))'
        },
        primary: {
          DEFAULT: 'hsl(var(--primary))',
          foreground: 'hsl(var(--primary-foreground))'
        },
        secondary: {
          DEFAULT: 'hsl(var(--secondary))',
          foreground: 'hsl(var(--secondary-foreground))'
        },
        muted: {
          DEFAULT: 'hsl(var(--muted))',
          foreground: 'hsl(var(--muted-foreground))'
        },
        accent: {
          DEFAULT: 'hsl(var(--accent))',
          foreground: 'hsl(var(--accent-foreground))'
        },
        destructive: {
          DEFAULT: 'hsl(var(--destructive))',
          foreground: 'hsl(var(--destructive-foreground))'
        },
        border: 'hsl(var(--border))',
        input: 'hsl(var(--input))',
        ring: 'hsl(var(--ring))',
        chart: {
          '1': 'hsl(var(--chart-1))',
          '2': 'hsl(var(--chart-2))',
          '3': 'hsl(var(--chart-3))',
          '4': 'hsl(var(--chart-4))',
          '5': 'hsl(var(--chart-5))'
        },
        // Flux brand colors
        flux: {
          cyan: '#38e8ff',
          purple: '#a855f7',
          blue: '#3b82f6',
          orange: '#fb923c',
          green: '#22c55e',
          gold: '#fbbf24',
          deep: '#020617',
          surface: '#0f172a',
          elevated: '#1e293b',
        },
        tier: {
          cumulus: '#4da6ff',
          nimbus: '#b388ff',
          stratus: '#ff8f4d',
        }
      },
      borderRadius: {
        lg: 'var(--radius)',
        md: 'calc(var(--radius) - 2px)',
        sm: 'calc(var(--radius) - 4px)',
        xl: 'calc(var(--radius) + 4px)',
        '2xl': 'calc(var(--radius) + 8px)',
      },
      boxShadow: {
        'flux-glow': '0 0 20px rgba(56, 232, 255, 0.3), 0 0 40px rgba(56, 232, 255, 0.15)',
        'flux-glow-purple': '0 0 20px rgba(168, 85, 247, 0.3), 0 0 40px rgba(168, 85, 247, 0.15)',
        'flux-glow-soft': '0 0 40px rgba(56, 232, 255, 0.1)',
        'flux-card': '0 4px 24px rgba(0, 0, 0, 0.3), inset 0 1px 0 rgba(255, 255, 255, 0.04)',
        'flux-card-hover': '0 8px 32px rgba(0, 0, 0, 0.4), 0 0 40px rgba(56, 232, 255, 0.08)',
      },
      backdropBlur: {
        'flux': '20px',
        'flux-strong': '24px',
      },
      animation: {
        'flux-pulse': 'flux-pulse 2s ease-in-out infinite',
        'flux-float': 'flux-float 4s ease-in-out infinite',
        'flux-glow': 'flux-glow-pulse 2s ease-in-out infinite',
        'flux-shimmer': 'flux-shimmer 2s infinite',
        'flux-fade-in': 'flux-fade-in 0.5s cubic-bezier(0.16, 1, 0.3, 1) forwards',
        'flux-slide-in': 'flux-slide-in 0.5s cubic-bezier(0.16, 1, 0.3, 1) forwards',
        'flux-scale-in': 'flux-scale-in 0.3s cubic-bezier(0.16, 1, 0.3, 1) forwards',
        'flux-spin-slow': 'flux-spin-slow 8s linear infinite',
        'flux-electric': 'flux-electric-flow 3s linear infinite',
      },
      keyframes: {
        'flux-pulse': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.6' },
        },
        'flux-float': {
          '0%, 100%': { transform: 'translateY(0)' },
          '50%': { transform: 'translateY(-8px)' },
        },
        'flux-glow-pulse': {
          '0%, 100%': {
            boxShadow: '0 0 20px rgba(56, 232, 255, 0.3), 0 0 40px rgba(56, 232, 255, 0.15)',
          },
          '50%': {
            boxShadow: '0 0 30px rgba(56, 232, 255, 0.5), 0 0 60px rgba(56, 232, 255, 0.25)',
          },
        },
        'flux-shimmer': {
          '0%': { backgroundPosition: '-200% 0' },
          '100%': { backgroundPosition: '200% 0' },
        },
        'flux-fade-in': {
          from: { opacity: '0', transform: 'translateY(10px)' },
          to: { opacity: '1', transform: 'translateY(0)' },
        },
        'flux-slide-in': {
          from: { opacity: '0', transform: 'translateX(-20px)' },
          to: { opacity: '1', transform: 'translateX(0)' },
        },
        'flux-scale-in': {
          from: { opacity: '0', transform: 'scale(0.95)' },
          to: { opacity: '1', transform: 'scale(1)' },
        },
        'flux-spin-slow': {
          from: { transform: 'rotate(0deg)' },
          to: { transform: 'rotate(360deg)' },
        },
        'flux-electric-flow': {
          '0%': { backgroundPosition: '200% 0' },
          '100%': { backgroundPosition: '-200% 0' },
        },
      },
      transitionTimingFunction: {
        'flux': 'cubic-bezier(0.16, 1, 0.3, 1)',
      },
    }
  },
  plugins: [require("tailwindcss-animate")],
};

export default config;
