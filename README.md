# Flux Blockchain Explorer

Blockchain explorer for the Flux network, supporting Flux v9.0.0+ with Proof-of-Node (PoN) consensus. Built with Next.js 14 and backed by ClickHouse.

![Next.js](https://img.shields.io/badge/Next.js-14-black)
![TypeScript](https://img.shields.io/badge/TypeScript-5-blue)
![ClickHouse](https://img.shields.io/badge/ClickHouse-24.x-yellow)
![Docker](https://img.shields.io/badge/Docker-Ready-blue)

## Architecture

Three-component stack:

```
┌─────────────────────────────────────────────────────────────┐
│                     Flux Blockchain                          │
│                      (v9.0.0+ PoN)                          │
└────────────────────────┬────────────────────────────────────┘
                         │ RPC
           ┌─────────────┴─────────────┐
           │                           │
           v                           v
┌──────────────────┐         ┌──────────────────┐
│   FluxIndexer    │◄───────►│   ClickHouse     │
│   (TypeScript)   │         │   (Database)     │
│   Port: 42067    │         │   Port: 8123     │
└────────┬─────────┘         └──────────────────┘
         │
         │ REST API
         v
┌──────────────────┐
│  Flux Explorer   │
│  (Next.js 14)    │
│  Port: 42069     │
└──────────────────┘
```

| Component | Description |
|-----------|-------------|
| **ClickHouse** | Columnar database for blockchain data |
| **FluxIndexer** | TypeScript indexer with REST API |
| **Flux Explorer** | Next.js web interface |

## Features

### Explorer
- Block browsing with transaction details
- Transaction viewer with input/output visualization
- Address tracking with balance and history
- Rich list with FluxNode counts
- Network statistics and supply data

### FluxNode Support
- Tier detection (CUMULUS, NIMBUS, STRATUS)
- START/CONFIRM transaction parsing
- Block producer statistics
- Live FluxNode count per address

### Technical
- ClickHouse backend (~61GB vs ~260GB with PostgreSQL)
- Full sync in ~9 hours from genesis
- 120M+ transactions indexed
- Request coalescing for load reduction
- Cursor-based pagination

## Quick Start

### Requirements

- Docker & Docker Compose v2
- 16GB RAM minimum
- 120GB storage

### Deploy

```bash
git clone https://github.com/RunOnFlux/flux-indexer-explorer.git
cd flux-blockchain-explorer

docker compose -f docker-compose.production.yml up -d

# View logs
docker compose -f docker-compose.production.yml logs -f indexer

# Access services
# API: http://127.0.0.1:42067
# Explorer: http://127.0.0.1:42069
```

### Management

```bash
# Stop
docker compose -f docker-compose.production.yml stop

# Rebuild service
docker compose -f docker-compose.production.yml build indexer
docker compose -f docker-compose.production.yml up -d indexer

# Status
docker compose -f docker-compose.production.yml ps

# Database size
docker exec fluxindexer-clickhouse clickhouse-client --query "
  SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE active
"
```

## Project Structure

```
flux-blockchain-explorer/
├── flux-indexer/                 # Indexer service
│   ├── src/
│   │   ├── api/server.ts         # REST API
│   │   ├── indexer/              # Sync and block processing
│   │   ├── database/             # ClickHouse connection
│   │   └── rpc/                  # Flux daemon client
│   ├── docker/                   # Docker configuration
│   └── Dockerfile
├── flux-explorer/                # Next.js frontend
│   ├── src/
│   │   ├── app/                  # Pages
│   │   ├── components/           # UI components
│   │   └── lib/api/              # API clients
│   └── Dockerfile
└── docker-compose.production.yml
```

## API Reference

### Status
- `GET /health` - Health check
- `GET /api/v1/status` - Sync status

### Blocks
- `GET /api/v1/blocks/latest` - Recent blocks
- `GET /api/v1/blocks/:heightOrHash` - Block details
- `GET /api/v1/stats/dashboard` - Dashboard stats

### Transactions
- `GET /api/v1/transactions/:txid` - Transaction details
- `POST /api/v1/transactions/batch` - Batch lookup

### Addresses
- `GET /api/v1/addresses/:address` - Balance and stats
- `GET /api/v1/addresses/:address/transactions` - History
- `GET /api/v1/addresses/:address/utxos` - UTXOs

### Analytics
- `GET /api/v1/richlist` - Top holders
- `GET /api/v1/supply` - Supply data
- `GET /api/v1/producers` - Block producers

## Performance

### Storage
| Component | RAM | Storage |
|-----------|-----|---------|
| ClickHouse | 9-16GB | 61GB |
| FluxIndexer | 3-4GB | 50GB |
| Explorer | 100MB | 1GB |
| **Total** | ~16GB | ~112GB |

### Sync
- Full sync: ~9 hours
- 2.1M+ blocks, 120M+ transactions
- ~65 blocks/second (bulk)

### Query Times
- Block lookup: <10ms
- Address transactions: <50ms
- Rich list: <100ms

## Configuration

### FluxIndexer
```bash
CH_HOST=clickhouse
CH_HTTP_PORT=8123
CH_DATABASE=flux
FLUX_RPC_URL=http://127.0.0.1:16124
FLUX_RPC_USER=fluxrpc
FLUX_RPC_PASSWORD=your_password
INDEXER_BATCH_SIZE=1000
API_PORT=42067
```

### Explorer
```bash
NEXT_PUBLIC_API_URL=http://127.0.0.1:42067
SERVER_API_URL=http://indexer:42067
```

## Flux Blockchain

### Transaction Versions
| Version | Type |
|---------|------|
| v1 | Legacy transparent |
| v2 | Sprout shielded |
| v4 | Sapling shielded |
| v5/v6 | FluxNode operations |

### FluxNode Tiers
| Tier | Collateral |
|------|------------|
| CUMULUS | 1,000 FLUX |
| NIMBUS | 12,500 FLUX |
| STRATUS | 40,000 FLUX |

### Block Types
- PoW (version < 100): Equihash mining
- PoN (version >= 100): FluxNode signatures, ~30 second blocks

## Development

```bash
# Indexer
cd flux-indexer
npm install
npm run dev

# Explorer
cd flux-explorer
npm install
npm run dev
```

## License

MIT License

## Links

- [Flux Network](https://runonflux.com/)
- [Flux Documentation](https://docs.runonflux.io)
- [ClickHouse](https://clickhouse.com/)
