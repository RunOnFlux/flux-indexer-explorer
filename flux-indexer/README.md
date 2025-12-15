# FluxIndexer

Blockchain indexer for Flux v9.0.0+ with Proof of Node (PoN) consensus, backed by ClickHouse.

[![TypeScript](https://img.shields.io/badge/TypeScript-5.3-blue)](https://www.typescriptlang.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.x-yellow)](https://clickhouse.com/)
[![Node.js](https://img.shields.io/badge/Node.js-20-green)](https://nodejs.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue)](https://www.docker.com/)

## Overview

FluxIndexer tracks Flux blockchain data optimized for the PoN hard fork introduced in v9.0.0:

- 30-second block times (previously 2 minutes)
- FluxNodes produce blocks instead of miners
- FluxNode START/CONFIRM transaction parsing
- Block producer tracking and statistics

The ClickHouse backend provides significant storage savings (~61GB vs ~260GB with PostgreSQL) and fast query performance.

---

## Architecture

```
┌──────────────┐      ┌──────────────┐      ┌────────────┐
│  Flux Daemon │──────│ FluxIndexer  │──────│  REST API  │
│   v9.0.0+    │  RPC │              │      │  :42067    │
└──────────────┘      └──────┬───────┘      └────────────┘
                             │
                             ▼
                      ┌──────────────┐
                      │  ClickHouse  │
                      │    :8123     │
                      └──────────────┘
```

### Components

1. **Flux RPC Client** - Communicates with Flux daemon
2. **Sync Engine** - Continuous block indexing with reorg handling
3. **Block Indexer** - Parses blocks, transactions, and UTXOs
4. **Bulk Loader** - High-throughput inserts with UTXO caching
5. **API Server** - REST API endpoints

---

## Quick Start

### Prerequisites

- Node.js 20+
- Docker & Docker Compose v2
- Flux daemon v9.0.0+ with RPC enabled

### Docker (Recommended)

```bash
# From repository root
docker compose -f docker-compose.production.yml up -d

# View logs
docker compose -f docker-compose.production.yml logs -f indexer

# Health check
curl http://127.0.0.1:42067/health
```

### Local Development

```bash
cd flux-indexer
npm install
cp .env.example .env
# Edit .env with your settings
npm run build
npm start
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| **Flux RPC** |
| `FLUX_RPC_URL` | `http://127.0.0.1:16124` | Flux daemon RPC endpoint |
| `FLUX_RPC_USER` | - | RPC username |
| `FLUX_RPC_PASSWORD` | - | RPC password |
| **ClickHouse** |
| `CH_HOST` | `127.0.0.1` | ClickHouse host |
| `CH_HTTP_PORT` | `8123` | ClickHouse HTTP port |
| `CH_DATABASE` | `flux` | Database name |
| `CH_USER` | `default` | ClickHouse username |
| `CH_PASSWORD` | - | ClickHouse password |
| **Indexer** |
| `INDEXER_BATCH_SIZE` | `1000` | Blocks per batch |
| `INDEXER_POLLING_INTERVAL` | `5000` | Polling interval (ms) |
| `INDEXER_START_HEIGHT` | - | Start height (-1 for genesis) |
| **API** |
| `API_PORT` | `42067` | API server port |
| `API_HOST` | `0.0.0.0` | API server bind address |

---

## API Endpoints

### Status
```
GET /health              # Health check
GET /api/v1/status       # Sync status and daemon info
```

### Blocks
```
GET /api/v1/blocks/latest              # Recent blocks
GET /api/v1/blocks/:heightOrHash       # Block by height or hash
GET /api/v1/stats/dashboard            # Dashboard statistics
```

### Transactions
```
GET /api/v1/transactions/:txid         # Transaction details
POST /api/v1/transactions/batch        # Batch lookup
```

### Addresses
```
GET /api/v1/addresses/:address                    # Balance and stats
GET /api/v1/addresses/:address/transactions       # Transaction history (cursor-based)
GET /api/v1/addresses/:address/utxos              # Unspent outputs
```

### Analytics
```
GET /api/v1/richlist           # Top holders
GET /api/v1/supply             # Circulating supply
GET /api/v1/producers          # Block producer stats
```

---

## Database Schema

ClickHouse tables use MergeTree family engines:

| Table | Engine | Purpose |
|-------|--------|---------|
| `blocks` | ReplacingMergeTree | Block headers |
| `transactions` | ReplacingMergeTree | Transaction data |
| `utxos` | ReplacingMergeTree | UTXO tracking |
| `address_transactions` | ReplacingMergeTree | Address-tx mapping |
| `address_summary` | SummingMergeTree | Address balances |
| `fluxnode_transactions` | ReplacingMergeTree | FluxNode operations |
| `live_fluxnodes` | ReplacingMergeTree | FluxNode counts |
| `supply_stats` | ReplacingMergeTree | Supply tracking |
| `producers` | SummingMergeTree | Producer statistics |
| `sync_state` | ReplacingMergeTree | Sync status |

Schema is created automatically by the indexer on startup.

---

## Project Structure

```
flux-indexer/
├── src/
│   ├── api/server.ts              # REST API
│   ├── database/
│   │   ├── connection.ts          # ClickHouse client
│   │   ├── bulk-loader.ts         # Bulk inserts
│   │   └── schema.sql             # Table definitions
│   ├── indexer/
│   │   ├── sync-engine.ts         # Sync with reorg handling
│   │   ├── block-indexer.ts       # Block parsing
│   │   └── parallel-fetcher.ts    # Parallel RPC fetching
│   ├── rpc/flux-rpc-client.ts     # Daemon RPC client
│   ├── services/fluxnode-sync.ts  # FluxNode sync
│   ├── types/index.ts             # TypeScript types
│   ├── config.ts                  # Configuration
│   └── index.ts                   # Entry point
├── docker/
│   ├── clickhouse-memory.xml      # ClickHouse config
│   ├── entrypoint.sh              # Container entrypoint
│   ├── flux.conf.template         # Daemon config
│   └── supervisord.conf           # Process manager
├── Dockerfile
└── package.json
```

---

## Performance

### Sync Speed
- Full sync: ~9 hours (2.1M+ blocks)
- Bulk sync: ~65 blocks/second
- Tip-following: ~18 blocks/second

### Storage
| Component | Size |
|-----------|------|
| ClickHouse | ~61 GB |
| Flux daemon | ~50 GB |
| **Total** | ~112 GB |

### Resources
| Component | RAM | CPU |
|-----------|-----|-----|
| ClickHouse | 9-16 GB | 4 cores |
| FluxIndexer | 3-4 GB | 2 cores |

### Query Times
- Block lookup: <10ms
- Transaction lookup: <10ms
- Address transactions: <50ms
- Rich list: <100ms

---

## Deployment

### Docker Compose

```bash
docker compose -f docker-compose.production.yml up -d
docker compose -f docker-compose.production.yml logs -f indexer
```

### Check Database Size

```bash
docker exec fluxindexer-clickhouse clickhouse-client --query "
  SELECT
    formatReadableSize(sum(bytes_on_disk)) as size,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed
  FROM system.parts WHERE active
"
```

---

## Troubleshooting

### RPC Connection Failed
```bash
curl -u user:pass http://127.0.0.1:16124 \
  -d '{"method":"getblockcount","params":[],"id":1}'
```

### ClickHouse Connection Failed
```bash
curl "http://127.0.0.1:8123/?query=SELECT%201"
```

### Slow Sync
- Increase `INDEXER_BATCH_SIZE` (2000-5000 for initial sync)
- Ensure ClickHouse has sufficient RAM (9GB+ recommended)

### Reorg Handling
The indexer handles reorgs up to 100 blocks deep automatically. Check logs:
```bash
docker logs fluxindexer-prod | grep -i reorg
```

---

## License

MIT License

---

## Links

- [Flux Network](https://runonflux.com/)
- [ClickHouse](https://clickhouse.com/)
- [Issues](https://github.com/RunOnFlux/flux-indexer-explorer/issues)
