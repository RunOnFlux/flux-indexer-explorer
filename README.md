# Flux Blockchain Explorer

A modern, real-time blockchain explorer for the Flux network. Built with Next.js 14 and powered by FluxIndexer API v1.

![Flux Explorer](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)
![Next.js](https://img.shields.io/badge/Next.js-14-black)
![TypeScript](https://img.shields.io/badge/TypeScript-5-blue)
![Docker](https://img.shields.io/badge/Docker-Ready-blue)

## ️ Architecture

FluxExplorer is a **3-component system** providing complete blockchain exploration capabilities:

```
┌─────────────────────────────────────────────────────────────┐
│                     Flux Blockchain                          │
└────────────────────┬────────────────────────────────────────┘
                     │
          ┌──────────┴───────────┐
          │                      │
          v                      v
┌──────────────────┐    ┌──────────────────┐
│   FluxIndexer    │◄───┤   PostgreSQL     │
│   (API v1)       │───►│   (Database)     │
└────────┬─────────┘    └──────────────────┘
         │
         │ RESTful API
         │ /api/v1/*
         │
         v
┌──────────────────┐
│  Flux Explorer   │
│  (Next.js 14)    │
│  Web Interface   │
└──────────────────┘
```

### Components

1. **PostgreSQL** - Database for blockchain data
2. **FluxIndexer** - Custom indexer with RESTful API v1
3. **Flux Explorer** - Web Interface

##  Features

### Core Features
-  **Smart Search** - Automatically detects blocks, transactions, and addresses
-  **Network Statistics** - Real-time metrics including circulating supply, block time, and transaction count
-  **Block Explorer** - Browse blocks with full transaction details
-  **Transaction Viewer** - Detailed transaction info with FluxNode tier detection (STRATUS/NIMBUS/CUMULUS)
-  **Address Tracker** - Monitor balances and transaction history with CSV export
- ️ **Block Rewards** - Live visualization of mining rewards and FluxNode payouts
-  **Rich List** - Top Flux holders with balance distribution analytics
-  **Responsive Design** - Seamless experience on all devices
-  **Modern UI** - Clean, professional interface with dark theme

### Advanced Features
-  **FluxIndexer API v1** - RESTful API with Flux-specific endpoints
-  **Docker Compose Deployment** - Complete stack deployment with one command
-  **Real-time Updates** - Live blockchain data synchronization
-  **SQLite Price Cache** - Persistent caching of historical cryptocurrency prices
-  **Flux PoN Support** - Full support for Flux Proof-of-Node consensus

##  Quick Start

### Prerequisites

- Docker & Docker Compose (recommended)
- Or: Node.js 18.x+ (for local development)

### Fresh Production Deployment (One-Shot)

For deploying on a fresh VPS with clean blockchain sync:

### Production Deployment (Docker Compose)

```bash
# Clone the repository
git clone https://github.com/Sikbik/flux-blockchain-explorer.git
cd flux-blockchain-explorer

# Configure environment
cp .env.production.example .env.production
# Edit .env.production with your settings

# Start the complete stack (PostgreSQL + FluxIndexer + Explorer)
# Zcash parameters (~900MB) will be automatically downloaded on first run
docker compose -f docker-compose.production.yml up -d

# View logs
docker compose -f docker-compose.production.yml logs -f

# Access the explorer
open http://127.0.0.1:42067  # FluxIndexer API
open http://127.0.0.1:42069  # Explorer UI
```

**Note**: Zcash parameters are now stored in a Docker volume and automatically downloaded. No manual host setup required!

### Local Development

```bash
# Clone the repository
git clone https://github.com/Sikbik/flux-blockchain-explorer.git
cd flux-blockchain-explorer/flux-explorer

# Install dependencies
npm install

# Configure environment
cp .env.example .env.local
# Edit .env.local to point to your FluxIndexer instance

# Start development server
npm run dev
```

Open [http://127.0.0.1:42069](http://127.0.0.1:42069) to view the explorer.

## ️ Tech Stack

### Blockchain Infrastructure
- **FluxIndexer** - Custom blockchain indexer with RESTful API v1
- **PostgreSQL** - High-performance blockchain data storage
- **Fluxd** - Flux daemon (bundled with FluxIndexer)

### Frontend Stack
- **Framework:** [Next.js 14](https://nextjs.org/) (App Router)
- **Language:** [TypeScript](https://www.typescriptlang.org/)
- **Styling:** [Tailwind CSS](https://tailwindcss.com/) + [shadcn/ui](https://ui.shadcn.com/)
- **Data Fetching:** [TanStack React Query](https://tanstack.com/query)
- **HTTP Client:** [ky](https://github.com/sindresorhus/ky)
- **Charts:** [Recharts](https://recharts.org/)
- **Database:** [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) (for price caching)

##  Project Structure

```
flux-blockchain-explorer/
├── flux-indexer/                 # FluxIndexer service (daemon + API)
│   ├── src/
│   │   ├── api/                 # Express handlers for /api/v1/*
│   │   ├── indexer/             # Sync engine and block processing
│   │   ├── database/            # PostgreSQL connection + optimizer
│   │   ├── rpc/                 # Flux RPC client wrappers
│   │   ├── scripts/             # Debug/inspection utilities
│   │   ├── types/               # Shared TypeScript types
│   │   └── index.ts             # Service entry point
│   ├── frontend/                # Bundled status dashboard (static export)
│   ├── Dockerfile               # Multi-stage build (fluxd + indexer)
│   ├── docker-compose.yml       # Local development stack
│   └── README.md                # FluxIndexer documentation
├── flux-explorer/               # Next.js application
│   ├── src/
│   │   ├── app/
│   │   │   ├── page.tsx         # Landing page
│   │   │   ├── block/[hash]/    # Block detail route
│   │   │   ├── blocks/          # Latest blocks listing
│   │   │   ├── tx/[txid]/       # Transaction pages
│   │   │   ├── address/[address]/ # Address detail pages
│   │   │   ├── rich-list/       # Rich list UI
│   │   │   └── api/             # Server-side API routes
│   │   │       ├── blocks/      # Cached block lookups
│   │   │       ├── cache/stats  # Cache diagnostics
│   │   │       ├── health/      # Health probe
│   │   │       ├── prices/batch # Price cache lookups
│   │   │       ├── rich-list/   # Rich list loader
│   │   │       └── supply/      # CoinMarketCap proxy
│   │   ├── components/          # UI building blocks
│   │   ├── lib/                 # API clients, helpers, price cache
│   │   └── types/               # Explorer-specific types
│   ├── data/                    # SQLite price cache (runtime)
│   ├── Dockerfile               # Production build
│   └── README.md                # Explorer documentation
├── docker-compose.production.yml # Full stack deployment
├── flux-spec.json               # FluxOS multi-component spec template
└── README.md                    # This file
```

##  FluxIndexer API v1

FluxIndexer provides a RESTful API for blockchain data access:

### Core Endpoints

- `GET /api/v1/status` - Indexer and daemon status
- `GET /api/v1/sync` - Synchronization progress
- `GET /api/v1/blocks` - Recent block summaries
- `GET /api/v1/blocks/latest` - Latest block snapshots (batched counts)
- `GET /api/v1/blocks/:heightOrHash` - Block details
- `GET /api/v1/transactions/:txid` - Transaction details
- `GET /api/v1/addresses/:address` - Address information
- `GET /api/v1/addresses/:address/utxos` - Address UTXOs
- `GET /api/v1/addresses/:address/transactions` - Address transaction history

### Flux-Specific Endpoints

- `GET /api/v1/richlist` - Top Flux holders
- `GET /api/v1/supply` - Circulating/max supply snapshot
- `GET /api/v1/producers` - FluxNode producer leaderboard
- `GET /api/v1/producers/:identifier` - Block producer information
- `GET /api/v1/nodes` - FluxNode list
- `GET /api/v1/nodes/:ip` - Detailed FluxNode status
- `GET /api/v1/network` - Network statistics
- `GET /api/v1/mempool` - Mempool information
- `GET /api/v1/stats/dashboard` - Aggregated explorer statistics

See [flux-indexer/README.md](flux-indexer/README.md) for complete API documentation.

##  Key Features

### FluxNode Tier Detection
Automatically identifies FluxNode types based on block reward amounts:
- **FOUNDATION** - Green badge
- **STRATUS** - Blue badge
- **NIMBUS** - Purple badge
- **CUMULUS** - Pink badge
- **MINER** - Yellow badge

### Real-time Updates
Components refresh automatically with configurable polling intervals based on deployment mode (local vs. production).

### Smart Search
Automatically detects search type:
- Block heights (numbers like `2009100`)
- Block hashes (64-char hex)
- Transaction IDs (64-char hex)
- Addresses (t1/t3 prefixes)

### Network Statistics
Real-time metrics:
- Current block height
- Circulating & max supply
- Average block time (calculated from last 100 blocks)
- 24-hour transaction count (cached for performance)
- FluxNode count & network statistics

### Rich List Analytics
- Top Flux holders
- Balance distribution charts
- Address rankings with percentages
- Paginated view with search
- Real-time updates from FluxIndexer

##  Environment Variables

### Explorer Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `NEXT_PUBLIC_API_URL` | FluxIndexer API endpoint | `http://127.0.0.1:42067` |
| `NEXT_PUBLIC_API_MODE` | API mode (`local`, `public`, `auto`) | `local` |

### Production Stack (.env.production)

```bash
# PostgreSQL Configuration
POSTGRES_USER=fluxindexer
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=flux_blockchain

# FluxIndexer Configuration
DB_HOST=postgres
DB_PORT=5432
DB_USER=fluxindexer
DB_PASSWORD=your_secure_password
DB_NAME=flux_blockchain

# Explorer Configuration
NEXT_PUBLIC_API_URL=http://indexer:42067
```

##  Documentation

### Getting Started
- **[README.md](README.md)** - This file (overview and quick start)
- **[.env.production.example](.env.production.example)** - Environment variable reference

### FluxIndexer
- **[flux-indexer/README.md](flux-indexer/README.md)** - FluxIndexer documentation
- **[flux-indexer/DEPLOYMENT.md](flux-indexer/DEPLOYMENT.md)** - Production deployment guide
- **[flux-indexer/PERFORMANCE_OPTIMIZATION.md](flux-indexer/PERFORMANCE_OPTIMIZATION.md)** - Performance tuning guide

### Explorer
- **[flux-explorer/README.md](flux-explorer/README.md)** - Explorer-specific documentation
- **[flux-explorer/DEPLOYMENT.md](flux-explorer/DEPLOYMENT.md)** - Explorer deployment guide
- **[flux-explorer/PRICE_DATA_SETUP.md](flux-explorer/PRICE_DATA_SETUP.md)** - Price data configuration

##  Docker Deployment

### Complete Stack Deployment

```bash
# Start all components (PostgreSQL + FluxIndexer + Explorer)
docker compose -f docker-compose.production.yml up -d

# View status
docker compose -f docker-compose.production.yml ps

# View logs
docker compose -f docker-compose.production.yml logs -f indexer
docker compose -f docker-compose.production.yml logs -f explorer

# Stop all components
docker compose -f docker-compose.production.yml down
```

**Important**: The production docker-compose file includes a `price-cache-data` volume for the explorer service. This persists the SQLite price cache database (~50MB with 35,000+ historical price points) across container rebuilds. Without this volume, the price data would need to be re-downloaded on every deployment (5-10 minute process).

### Individual Component Management

```bash
# Rebuild and restart just the indexer
docker compose -f docker-compose.production.yml up -d --build indexer

# Rebuild and restart just the explorer
docker compose -f docker-compose.production.yml up -d --build explorer

# Restart PostgreSQL
docker compose -f docker-compose.production.yml restart postgres
```

##  Security

-  **Zero vulnerabilities** in dependencies (scanned regularly)
-  **No hardcoded secrets** - all config via environment variables
-  **Docker security** - containers run as non-root users
-  **API validation** - all inputs validated and sanitized
-  **Rate limiting** - intelligent caching prevents API abuse

##  Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Development Guidelines

- Follow TypeScript best practices
- Write clean, documented code
- Test with local FluxIndexer instance
- Update documentation for new features
- Run `npm run lint` before committing

##  Performance

### Resource Requirements

**PostgreSQL Database:**
- CPU: 2 cores
- RAM: 16 GB (actual usage: ~12GB)
- Storage: 400 GB (~260GB current, grows over time)

**FluxIndexer (with Fluxd):**
- CPU: 4 cores
- RAM: 6 GB (actual usage: ~2GB, includes bootstrap overhead)
- Storage: 150 GB (~45GB blockchain + params, ~115GB peak during bootstrap)

**Explorer Frontend:**
- CPU: 1 core
- RAM: 1 GB (actual usage: ~60MB)
- Storage: 5 GB (price cache ~50MB)

### System Performance
- **Indexer Performance**: Optimized blockchain synchronization
- **Docker Images**:
  - FluxIndexer: ~1.2GB (includes Fluxd + dependencies)
  - Explorer: ~400MB (Next.js production build)
- **Database**: PostgreSQL with optimized indexes
- **Caching**: Intelligent server-side caching for expensive queries
- **Request Coalescing**: DDoS protection with 90-95% load reduction

##  License

MIT License - see LICENSE file for details

##  Acknowledgments

- Built for the [Flux](https://runonflux.io/) blockchain network
- UI components from [shadcn/ui](https://ui.shadcn.com/)
- Icons from [Lucide](https://lucide.dev/)

##  Links

- **Flux Website:** https://runonflux.io
- **Flux Documentation:** https://docs.runonflux.io
- **Official Flux Explorer:** https://explorer.runonflux.io
- **Flux GitHub:** https://github.com/RunOnFlux
