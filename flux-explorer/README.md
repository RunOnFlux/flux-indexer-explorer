# Flux Blockchain Explorer

A modern, high-performance blockchain explorer for the Flux network. Built with Next.js 14, TypeScript, and powered by FluxIndexer with ClickHouse for blazing-fast queries.

[![TypeScript](https://img.shields.io/badge/TypeScript-100%25-blue)](https://www.typescriptlang.org/)
[![Next.js](https://img.shields.io/badge/Next.js-14-black)](https://nextjs.org/)
[![ClickHouse](https://img.shields.io/badge/Backend-ClickHouse-yellow)](https://clickhouse.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Features

### Core Functionality
- **Universal Search** - Intelligent search for blocks, transactions, and addresses
- **Live Network Statistics** - Real-time blockchain metrics and analytics
- **Block Explorer** - Browse blocks with detailed transaction breakdowns and FluxNode summaries
- **Transaction Viewer** - Comprehensive transaction details with UTXO tracking
- **Address Tracker** - Monitor balances, transaction history with cursor-based pagination
- **Mining Rewards** - Live visualization of block rewards and FluxNode payouts
- **Rich List** - Top holders with FluxNode counts and collateral breakdown
- **CSV Export** - Tax-compliant transaction history export with progress tracking

### FluxNode Features
- **Tier Detection** - Automatic identification of CUMULUS, NIMBUS, STRATUS nodes
- **Node Confirmations** - Real-time FluxNode confirmation tracking
- **Tier Statistics** - Breakdown of node confirmations by tier
- **Color-Coded Badges** - Visual distinction of different node tiers
- **FluxNode Counts** - Per-address FluxNode counts synced from daemon

### CSV Export Features
- **Tax Software Compatible** - Koinly, CoinTracker, CryptoTaxCalculator, TokenTax
- **CSV Injection Protection** - RFC 4180 compliant, formula injection prevention
- **Progress Tracking** - Real-time progress bar with status and cancellation
- **Multi-File Export** - Automatic splitting for large datasets (50k transactions per file)
- **Smart Categorization** - Automatic detection of Receive/Send with FluxNode tier labels
- **Unlimited History** - Export complete transaction history without memory issues

### Technical Features
- **Optimized Performance** - Aggressive caching, minimal API calls, rate limiting
- **Real-time Updates** - Auto-refreshing data with smart polling intervals
- **Responsive Design** - Seamless experience on desktop, tablet, and mobile
- **Security Hardened** - Zero vulnerabilities, input validation, XSS protection
- **Production Ready** - Docker-optimized, health checks, monitoring
- **ClickHouse Backend** - Sub-second query performance on 120M+ transactions

## Tech Stack

- **Framework:** [Next.js 14](https://nextjs.org/) (App Router, React Server Components)
- **Language:** [TypeScript](https://www.typescriptlang.org/) (Strict mode)
- **Styling:** [Tailwind CSS](https://tailwindcss.com/) v3
- **UI Components:** [Radix UI](https://www.radix-ui.com/) + [shadcn/ui](https://ui.shadcn.com/)
- **Data Fetching:** [TanStack React Query](https://tanstack.com/query) v5
- **HTTP Client:** [ky](https://github.com/sindresorhus/ky) (Modern fetch wrapper)
- **Charts:** [Recharts](https://recharts.org/) (Data visualization)
- **API:** FluxIndexer with ClickHouse (this repository's `/flux-indexer` service)

## Quick Start

### Prerequisites

- Node.js 18.x or higher
- npm, yarn, pnpm, or bun
- Docker (optional, for containerized deployment)

### Docker Quick Start (Recommended)

The easiest way to run the complete stack:

```bash
# From repository root
cd flux-blockchain-explorer

# Start the complete ClickHouse stack (Indexer + Explorer + ClickHouse)
docker compose -f docker-compose.production.yml up -d

# Access the explorer
open http://127.0.0.1:42069
```

### Local Development

```bash
# Navigate to explorer
cd flux-explorer

# Install dependencies
npm install

# Configure environment (optional)
cp .env.example .env.local

# Start development server
npm run dev
```

Open [http://127.0.0.1:42069](http://127.0.0.1:42069) to view the explorer.

**Note:** Requires FluxIndexer running on `http://127.0.0.1:42067`

## Scripts

```bash
npm run dev      # Start development server (port 42069)
npm run build    # Build for production
npm run start    # Start production server
npm run lint     # Run ESLint checks
```

## Project Structure

```
flux-explorer/
├── src/
│   ├── app/                    # Next.js App Router
│   │   ├── page.tsx            # Home page
│   │   ├── blocks/             # Blocks list page
│   │   ├── block/[hash]/       # Block detail pages
│   │   ├── tx/[txid]/          # Transaction detail pages
│   │   ├── address/[address]/  # Address detail pages
│   │   ├── rich-list/          # Rich list page
│   │   ├── search/[query]/     # Smart search results
│   │   └── api/                # Server-side API routes
│   │       ├── blocks/         # Cached block lookups
│   │       ├── cache/stats/    # Cache diagnostics
│   │       ├── health/         # Runtime health checks
│   │       ├── prices/batch/   # Price cache lookups
│   │       ├── rich-list/      # Rich list loader
│   │       └── supply/         # CoinMarketCap proxy
│   ├── components/             # React components
│   │   ├── blocks/             # Block list components
│   │   ├── block/              # Block detail widgets
│   │   ├── transaction/        # Transaction UI
│   │   ├── address/            # Address widgets
│   │   ├── home/               # Homepage sections
│   │   ├── ui/                 # shadcn/ui primitives
│   │   ├── Header.tsx          # Navigation header
│   │   ├── Footer.tsx          # Site footer
│   │   └── SearchBar.tsx       # Universal search
│   ├── lib/                    # Core utilities
│   │   ├── api/                # API client layer + React Query hooks
│   │   ├── db/                 # SQLite price cache helpers
│   │   ├── flux-tx-parser.ts   # FluxNode transaction parser
│   │   └── utils.ts            # Shared helpers
│   ├── hooks/                  # Custom React hooks
│   └── types/                  # TypeScript definitions
├── data/                       # SQLite price cache (runtime)
├── public/                     # Static assets
├── next.config.mjs             # Next.js configuration
├── tailwind.config.ts          # Tailwind CSS config
├── tsconfig.json               # TypeScript config
├── Dockerfile                  # Docker production build
├── docker-compose.yml          # Local Docker Compose
├── DEPLOYMENT.md               # Deployment guide
└── README.md                   # This file
```

## Features in Detail

### FluxNode Tier Detection

The explorer automatically identifies FluxNode tiers based on collateral amounts:

| Tier | Collateral | Badge Color | Description |
|------|-----------|-------------|-------------|
| **STRATUS** | 40,000 FLUX | Blue | Highest tier node |
| **NIMBUS** | 12,500 FLUX | Purple | Mid tier node |
| **CUMULUS** | 1,000 FLUX | Pink | Entry tier node |
| **STARTING** | N/A | Yellow | Node initialization |
| **MINER** | N/A | Amber | Block mining reward |

Tiers are determined by parsing collateral transaction outputs from FluxNode confirmations.

### Search Intelligence

The search bar automatically detects input type:

- **Block Height**: Pure numbers (e.g., `123456`)
- **Block Hash**: 64-character hex string (e.g., `00000000...`)
- **Transaction ID**: 64-character hex string
- **Address**: Strings starting with `t1` or `t3`

When a 64-char hex is ambiguous, it tries transaction first, then block hash.

### Performance Optimizations

1. **Per-Block Caching** - Explorer pages reuse cached block responses via React Query
2. **Server-Side Caching** - `/api/blocks/latest` and `/api/prices/batch` consolidate FluxIndexer requests
3. **React Query** - Automatic request deduplication and background refetching
4. **Optimized Hooks** - Shared caches for status, supply, and dashboard stats prevent duplicate calls
5. **Static Generation** - Non-dynamic pages pre-rendered at build time
6. **ClickHouse Backend** - Sub-10ms query response for most lookups

### Real-time Updates

Components poll at optimized intervals:

- **Latest Blocks**: Adaptive polling (2 seconds)
- **Block Rewards & Dashboard Stats**: 2 seconds
- **Network Status**: 2 seconds
- **Sync Status**: 30 seconds while catching up
- **Supply Data**: 5 minutes (cached)

## Docker Deployment

### Build & Run Locally

```bash
# Using Docker Compose (recommended)
docker-compose up --build

# Or manually
docker build -t flux-explorer:latest .
docker run -p 42069:42069 flux-explorer:latest
```

### Full Stack Deployment

For the complete stack including FluxIndexer and ClickHouse:

```bash
# From repository root
docker compose -f docker-compose.production.yml up -d

# Services:
# - Explorer: http://127.0.0.1:42069
# - FluxIndexer API: http://127.0.0.1:42067
# - ClickHouse: http://127.0.0.1:8123
```

See [DEPLOYMENT.md](DEPLOYMENT.md) for comprehensive deployment instructions.

## Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `NEXT_PUBLIC_API_URL` | Client-side FluxIndexer API endpoint (build-time) | `http://127.0.0.1:42067` | No |
| `SERVER_API_URL` | Server-side FluxIndexer API endpoint (runtime) | Falls back to `NEXT_PUBLIC_API_URL` | No |
| `NODE_ENV` | Environment mode | `development` | No |
| `PORT` | Server port | `42069` | No |

### API Configuration

The explorer supports flexible API configuration:

**Docker Compose (Default)**
```env
# Automatically configured in docker-compose.production.yml
SERVER_API_URL=http://indexer:42067
```

**Local Development**
```env
NEXT_PUBLIC_API_URL=http://127.0.0.1:42067
```

**Custom FluxIndexer**
```env
SERVER_API_URL=https://your-custom-fluxindexer.com
NEXT_PUBLIC_API_URL=https://your-custom-fluxindexer.com
```

All blockchain data is public - no authentication required.

## Security

This project has undergone comprehensive security auditing with all critical vulnerabilities addressed:

### Core Security
- **Zero CVEs** - All dependencies up-to-date and secure
- **Input Validation** - All user inputs sanitized with regex validation, numeric bounds checking
- **XSS Protection** - React's built-in sanitization, no `dangerouslySetInnerHTML`
- **No Injection Risks** - No eval(), no shell execution, no dynamic code
- **SSRF Protected** - Environment-controlled API URLs, validated user input
- **Error Handling** - Generic error messages, no stack trace leakage
- **Type Safety** - 100% TypeScript with strict mode

### CSV Export Security
- **CSV Injection Prevention** - RFC 4180 compliant escaping
- **Formula Injection Protection** - Dangerous characters prefixed with single quote
- **DoS Protection** - Multi-file segmentation (50k transactions per file), API rate limiting (100ms delays)
- **Memory Safety** - Chunked processing, export cancellation support
- **Input Validation** - parseFloat() results validated with isFinite(), negative value rejection

**Security Status: PRODUCTION READY** - All critical and medium-risk vulnerabilities resolved.

## API Endpoints

### FluxIndexer API (Consumed by the explorer)

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/status` | Indexer and daemon status |
| `GET /api/v1/blocks/latest` | Latest block summaries with FluxNode data |
| `GET /api/v1/blocks/:heightOrHash` | Block details |
| `GET /api/v1/transactions/:txid` | Transaction details |
| `POST /api/v1/transactions/batch` | Batch transaction lookup |
| `GET /api/v1/addresses/:address` | Address info with FluxNode counts |
| `GET /api/v1/addresses/:address/transactions` | Cursor-based paginated history |
| `GET /api/v1/addresses/:address/utxos` | Address UTXOs |
| `GET /api/v1/producers` | FluxNode block producer stats |
| `GET /api/v1/richlist` | Top holders with FluxNode counts |
| `GET /api/v1/supply` | Circulating and total supply |
| `GET /api/v1/stats/dashboard` | Dashboard statistics |

### Explorer Server Routes

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Runtime health check |
| `GET /api/supply` | Circulating/max supply data |
| `GET /api/prices/batch` | Batched price lookups (SQLite cache) |
| `GET /api/cache/stats` | Cache hit/miss diagnostics |
| `GET /api/blocks/[hashOrHeight]` | Server-side block cache |
| `GET /api/blocks/latest` | Server-side latest block cache |
| `GET /api/rich-list` | Rich list data loader |

## Contributing

Contributions are welcome! Please follow these guidelines:

1. **Fork the repository**
2. **Create a feature branch** (`git checkout -b feature/amazing-feature`)
3. **Commit your changes** (`git commit -m 'Add amazing feature'`)
4. **Push to the branch** (`git push origin feature/amazing-feature`)
5. **Open a Pull Request**

### Development Guidelines

- Use TypeScript strict mode
- Follow existing code style (ESLint)
- Add tests for new features
- Update documentation as needed
- Keep commits atomic and well-described

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- **Flux Team** - For building the amazing Flux blockchain network
- **ClickHouse** - High-performance columnar database powering the backend
- **shadcn** - Beautiful, accessible UI components
- **Vercel** - Next.js framework and inspiration
- **Community** - All contributors and users

## Links

- **GitHub Repository**: [https://github.com/RunOnFlux/flux-indexer-explorer](https://github.com/RunOnFlux/flux-indexer-explorer)
- **Flux Website**: [https://runonflux.com/](https://runonflux.com/)
- **Flux GitHub**: [https://github.com/RunOnFlux](https://github.com/RunOnFlux)
- **FluxIndexer**: Located in [`../flux-indexer`](../flux-indexer)
- **ClickHouse**: [https://clickhouse.com/](https://clickhouse.com/)

## Support

- **Issues**: [GitHub Issues](https://github.com/RunOnFlux/flux-indexer-explorer/issues)
- **Discussions**: [GitHub Discussions](https://github.com/RunOnFlux/flux-indexer-explorer/discussions)
- **Flux Discord**: [https://discord.com/invite/runonflux](https://discord.com/invite/runonflux)

---
