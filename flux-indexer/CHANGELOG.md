# FluxIndexer Changelog

## [2.0.0] - 2025-12-14

### Database Migration

Replaced PostgreSQL/TimescaleDB with ClickHouse for improved storage efficiency and query performance.

**Storage:**
- ClickHouse: ~61GB (vs ~260GB PostgreSQL)
- Compression ratio: 2.86x with Delta + ZSTD codecs

**Sync:**
- Full sync: ~9 hours from genesis
- Bulk sync: ~65 blocks/second
- Tip-following: ~18 blocks/second

### Schema Changes

**Tables (ClickHouse MergeTree family):**

| Table | Engine | Purpose |
|-------|--------|---------|
| `blocks` | ReplacingMergeTree | Block headers |
| `transactions` | ReplacingMergeTree | Transaction data |
| `utxos` | ReplacingMergeTree | UTXO tracking |
| `address_transactions` | ReplacingMergeTree | Address-tx mapping |
| `address_summary` | SummingMergeTree | Address balances |
| `address_summary_agg` | AggregatingMergeTree | Pre-aggregated data |
| `fluxnode_transactions` | ReplacingMergeTree | FluxNode operations |
| `live_fluxnodes` | ReplacingMergeTree | FluxNode counts |
| `supply_stats` | ReplacingMergeTree | Supply tracking |
| `producers` | SummingMergeTree | Producer statistics |
| `sync_state` | ReplacingMergeTree | Sync status |
| `reorgs` | MergeTree | Reorg audit log |

### Changes

**Added:**
- ClickHouse connection and bulk loader
- Async inserts for high-throughput sync
- Sync inserts for FluxNode data (immediate visibility)
- Cross-batch UTXO cache for async insert visibility
- Parallel block fetching from RPC
- Sequential bootstrap (ClickHouse first, then daemon)
- Schema auto-creation by indexer on startup
- Projections for optimized query patterns

**Changed:**
- Environment variables: `DB_*` → `CH_*`
- Docker compose uses ClickHouse instead of PostgreSQL

**Removed:**
- PostgreSQL/TimescaleDB support
- Database migration scripts
- Bootstrap importer

### Breaking Changes

- PostgreSQL not supported; requires full re-index
- Environment variable names changed

---

## [1.0.0] - 2025-11-15

Initial release with PostgreSQL backend.

### Features

- Full block and transaction indexing
- UTXO tracking
- Address balance calculations
- FluxNode producer tracking
- REST API compatible with FluxIndexer

### Components

- Bundled Flux daemon v9.0.0+
- PostgreSQL 15 database
- Express REST API
- Supervisord process management

### Requirements

- Node.js 20+
- PostgreSQL 15
- 8GB RAM, 100GB storage
