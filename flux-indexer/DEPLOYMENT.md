# FluxIndexer Deployment Guide

Complete guide for deploying FluxIndexer with ClickHouse to production.

## Prerequisites

- Docker & Docker Compose v2
- Flux daemon v9.0.0+ running with RPC enabled (bundled in indexer image)
- **Minimum 20GB RAM recommended** (or 16GB with 4GB swap - see Memory Requirements below)
- **225GB+ storage** for bootstrap (120GB ClickHouse + 100GB daemon + 5GB explorer)
  - After bootstrap completes: ~112GB (can reclaim ~113GB)

## Quick Start with Docker Compose

### 1. Clone and Navigate

```bash
git clone https://github.com/RunOnFlux/flux-indexer-explorer.git
cd flux-blockchain-explorer
```

### 2. Start the Stack

```bash
# Start ClickHouse + Indexer + Explorer
docker compose -f docker-compose.production.yml up -d

# View logs
docker compose -f docker-compose.production.yml logs -f indexer

# Check health
curl http://127.0.0.1:42067/health
```

### 3. Monitor Sync Progress

```bash
# Via API
curl http://127.0.0.1:42067/api/v1/status

# Check ClickHouse table counts
docker exec fluxindexer-clickhouse clickhouse-client --query "
SELECT
  (SELECT count() FROM blocks) as blocks,
  (SELECT count() FROM transactions) as transactions
"
```

---

## Deployment Options

### Option 1: Docker Compose (Recommended)

The `docker-compose.production.yml` file contains the complete production stack:

```yaml
services:
  # Official ClickHouse image with custom memory config
  clickhouse:
    image: clickhouse/clickhouse-server:24.3-alpine
    container_name: fluxindexer-clickhouse
    ports:
      - "127.0.0.1:8123:8123"   # HTTP interface (local only)
      - "127.0.0.1:9000:9000"   # Native protocol (local only)
    volumes:
      - clickhouse-data:/var/lib/clickhouse
      - clickhouse-backup:/var/lib/clickhouse/backup  # Shared for bootstrap
      - ./flux-indexer/docker/clickhouse-memory.xml:/etc/clickhouse-server/config.d/memory.xml:ro
    environment:
      CLICKHOUSE_USER: fluxindexer
      CLICKHOUSE_PASSWORD: your_secure_password
      CLICKHOUSE_DB: fluxindexer
    # Note: Schema is created automatically by the indexer application on startup

  # Indexer handles both ClickHouse and daemon bootstraps sequentially
  indexer:
    build: ./flux-indexer
    container_name: fluxindexer-prod
    ports:
      - "42067:42067"
    environment:
      CH_HOST: clickhouse
      CH_PORT: 9000           # Native protocol for clickhouse-client
      CH_HTTP_PORT: 8123      # HTTP protocol for indexer app
      CH_DATABASE: fluxindexer
      CH_USER: fluxindexer
      CH_PASSWORD: your_secure_password
      # CH_BOOTSTRAP_URL: https://your-cdn.com/clickhouse-backup.tar.gz
      # BOOTSTRAP_URL: https://your-cdn.com/flux-daemon-bootstrap.tar.gz
    volumes:
      - flux-data:/home/flux/.flux
      - clickhouse-backup:/clickhouse-backup  # Shared for bootstrap
    depends_on:
      clickhouse:
        condition: service_healthy

  explorer:
    build: ./flux-explorer
    container_name: flux-explorer-prod
    ports:
      - "42069:42069"
    environment:
      SERVER_API_URL: http://indexer:42067
    depends_on:
      - indexer

volumes:
  clickhouse-data:
  clickhouse-backup:  # Shared for bootstrap file transfer
  flux-data:
```

### Bootstrap Configuration

The indexer handles both bootstraps **sequentially** to minimize peak storage:

1. **CH_BOOTSTRAP_URL** - Downloads ClickHouse backup, restores it, then cleans up
2. **BOOTSTRAP_URL** - Downloads daemon blockchain data, extracts it, then cleans up

This reduces peak storage from ~225GB to ~150GB by not having both archives on disk simultaneously.

> **Note:** `CH_BOOTSTRAP_URL` requires shared volume support:
> - **Docker Compose**: Uses `clickhouse-backup` shared volume
> - **Flux Network**: Requires Flux v7.1.0+ with the new mount syntax (`c:<component_index>:<subdirectory>:<container_path>`)

### Option 2: Standalone Docker Container

```bash
# Build indexer image
docker build -t fluxindexer:latest ./flux-indexer

# Create shared network and volumes
docker network create fluxindexer
docker volume create clickhouse-data
docker volume create clickhouse-backup
docker volume create flux-data

# Start ClickHouse (official image)
# Note: Schema is created automatically by the indexer on first startup
docker run -d \
  --name clickhouse \
  --network fluxindexer \
  -p 127.0.0.1:8123:8123 \
  -p 127.0.0.1:9000:9000 \
  -v clickhouse-data:/var/lib/clickhouse \
  -v clickhouse-backup:/var/lib/clickhouse/backup \
  -v $(pwd)/flux-indexer/docker/clickhouse-memory.xml:/etc/clickhouse-server/config.d/memory.xml:ro \
  -e CLICKHOUSE_USER=fluxindexer \
  -e CLICKHOUSE_PASSWORD=your_password \
  -e CLICKHOUSE_DB=fluxindexer \
  clickhouse/clickhouse-server:24.3-alpine

# Start Indexer (bundled daemon + indexer)
docker run -d \
  --name fluxindexer \
  --network fluxindexer \
  -p 42067:42067 \
  -v flux-data:/home/flux/.flux \
  -v clickhouse-backup:/clickhouse-backup \
  -e CH_HOST=clickhouse \
  -e CH_PORT=9000 \
  -e CH_HTTP_PORT=8123 \
  -e CH_DATABASE=fluxindexer \
  -e CH_USER=fluxindexer \
  -e CH_PASSWORD=your_password \
  fluxindexer:latest
```

### Option 3: VPS Deployment

```bash
# Copy files to server
scp -r flux-blockchain-explorer user@your-server:/opt/

# SSH to server
ssh user@your-server
cd /opt/flux-blockchain-explorer

# Deploy
docker compose -f docker-compose.production.yml up -d

# Enable auto-restart
docker update --restart=unless-stopped \
  fluxindexer-clickhouse \
  fluxindexer-indexer \
  fluxindexer-explorer
```

---

## Configuration

### Required Flux Daemon Settings

Your Flux daemon must have these settings in `flux.conf`:

```conf
server=1
rpcuser=your_rpc_username
rpcpassword=your_rpc_password
rpcport=16124
rpcallowip=127.0.0.1
rpcallowip=172.0.0.0/8

# Required for address indexing
addressindex=1
timestampindex=1
spentindex=1
txindex=1
```

**Important:** Restart Flux daemon after changing config:
```bash
flux-cli stop
fluxd -reindex
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| **ClickHouse** |
| `CH_HOST` | Yes | `clickhouse` | ClickHouse hostname |
| `CH_PORT` | No | `9000` | ClickHouse native protocol port (for clickhouse-client) |
| `CH_HTTP_PORT` | No | `8123` | ClickHouse HTTP port (for indexer app) |
| `CH_DATABASE` | Yes | `fluxindexer` | Database name |
| `CH_USER` | No | `default` | ClickHouse username |
| `CH_PASSWORD` | No | - | ClickHouse password |
| **Bootstrap** |
| `CH_BOOTSTRAP_URL` | No | - | ClickHouse backup URL (requires shared volumes) |
| `BOOTSTRAP_URL` | No | - | Flux daemon blockchain bootstrap URL |
| `FORCE_CH_BOOTSTRAP` | No | `false` | Force re-download of ClickHouse bootstrap |
| `FORCE_BOOTSTRAP` | No | `false` | Force re-download of daemon bootstrap |
| **Flux RPC** |
| `FLUX_RPC_URL` | Yes | - | Flux daemon RPC endpoint |
| `FLUX_RPC_USER` | Yes | - | RPC username from flux.conf |
| `FLUX_RPC_PASSWORD` | Yes | - | RPC password from flux.conf |
| **Indexer** |
| `INDEXER_BATCH_SIZE` | No | `5000` | Blocks per batch (increase for faster sync) |
| `INDEXER_START_HEIGHT` | No | `-1` | Block height to start (-1 = genesis) |
| `BACKFILL_GAPS` | No | `true` | Backfill detected block gaps |
| `API_PORT` | No | `42067` | API server port |
| `LOG_LEVEL` | No | `info` | Logging level (debug, info, warn, error) |

---

## API Endpoints

### Health & Status

- `GET /health` - Health check
- `GET /api/v1/status` - Indexer + sync status

### Blocks & Transactions

- `GET /api/v1/blocks/latest` - Latest blocks with FluxNode summaries
- `GET /api/v1/blocks/:heightOrHash` - Block details
- `GET /api/v1/transactions/:txid` - Transaction details
- `POST /api/v1/transactions/batch` - Batch transaction lookup
- `GET /api/v1/stats/dashboard` - Dashboard statistics

### Addresses

- `GET /api/v1/addresses/:address` - Address summary with FluxNode counts
- `GET /api/v1/addresses/:address/transactions` - Paginated history (cursor-based)
- `GET /api/v1/addresses/:address/utxos` - Address UTXOs

### Analytics

- `GET /api/v1/richlist` - Rich list with FluxNode counts
- `GET /api/v1/supply` - Circulating and total supply
- `GET /api/v1/producers` - Block producer leaderboard

---

## Monitoring

### Check Sync Progress

```bash
# Via API
curl http://localhost:42067/api/v1/status

# Example response:
{
  "fluxindexer": {
    "coin": "Flux",
    "bestHeight": 2159733,
    "inSync": true,
    "syncProgress": 100.0,
    "blocksPerSecond": 65.2
  },
  "backend": {
    "chain": "main",
    "blocks": 2159733,
    "bestBlockHash": "000000000..."
  }
}
```

### ClickHouse Statistics

```bash
# Database size
docker exec fluxindexer-clickhouse clickhouse-client --query "
SELECT
  formatReadableSize(sum(bytes_on_disk)) as compressed,
  formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed,
  round(sum(data_uncompressed_bytes) / sum(bytes_on_disk), 2) as compression_ratio
FROM system.parts WHERE active
"

# Table row counts
docker exec fluxindexer-clickhouse clickhouse-client --query "
SELECT
  (SELECT count() FROM blocks) as blocks,
  (SELECT count() FROM transactions) as transactions,
  (SELECT count() FROM utxos WHERE spent = 0) as unspent_utxos,
  (SELECT count() FROM address_summary_agg) as addresses,
  (SELECT count() FROM fluxnode_transactions) as fluxnode_txs
"

# Check async insert queue
docker exec fluxindexer-clickhouse clickhouse-client --query "
SELECT * FROM system.asynchronous_insert_queue FORMAT Vertical
"
```

### Docker Logs

```bash
# Real-time logs
docker compose -f docker-compose.production.yml logs -f indexer

# Last 100 lines
docker compose -f docker-compose.production.yml logs --tail=100 indexer

# Filter by level
docker compose -f docker-compose.production.yml logs indexer 2>&1 | grep ERROR
```

---

## Troubleshooting

### RPC Connection Failed

```bash
# Test RPC connectivity
curl --user your_rpc_username:your_rpc_password \
  --data-binary '{"jsonrpc":"2.0","id":"test","method":"getblockcount","params":[]}' \
  -H 'content-type: text/plain;' \
  http://localhost:16124/

# Check Flux daemon is running
flux-cli getinfo

# Verify RPC settings
cat ~/.flux/flux.conf | grep rpc
```

### ClickHouse Connection Issues

```bash
# Check ClickHouse is running
docker compose -f docker-compose.production.yml ps clickhouse

# Test ClickHouse connection
curl "http://localhost:8123/?query=SELECT%201"

# Check ClickHouse logs
docker compose -f docker-compose.production.yml logs clickhouse
```

### Sync Stuck or Slow

```bash
# Check for errors
docker compose -f docker-compose.production.yml logs --tail=50 indexer | grep ERROR

# Verify Flux daemon is synced
flux-cli getblockchaininfo

# Restart indexer
docker compose -f docker-compose.production.yml restart indexer

# Increase batch size for faster sync
# Edit docker-compose.production.yml:
# environment:
#   - INDEXER_BATCH_SIZE=2000
docker compose -f docker-compose.production.yml up -d indexer
```

### Reorg Detected

The indexer automatically handles reorgs up to 100 blocks deep. Check logs:

```bash
docker compose -f docker-compose.production.yml logs indexer | grep -i reorg
```

If issues persist:

```bash
# Check reorg table
docker exec fluxindexer-clickhouse clickhouse-client --query "
SELECT * FROM reorgs ORDER BY occurred_at DESC LIMIT 5
"

# Force resync from safe height (edit env and restart)
# INDEXER_START_HEIGHT=<height_before_reorg>
```

### ClickHouse Keeps Restarting (OOM)

If ClickHouse container keeps restarting, check for OOM kills:

```bash
# Check kernel logs for OOM kills
dmesg | grep -i 'oom\|out of memory' | tail -10

# Check container restart count
docker inspect fluxindexer-clickhouse --format 'RestartCount: {{.RestartCount}}'

# Check memory limit
docker inspect fluxindexer-clickhouse --format 'Memory Limit: {{.HostConfig.Memory}}'
```

**Solutions:**

1. **Increase RAM** - Recommended: 24GB+ system RAM
2. **Add swap** (for 16GB systems):
   ```bash
   fallocate -l 4G /swapfile
   chmod 600 /swapfile
   mkswap /swapfile
   swapon /swapfile
   echo '/swapfile none swap sw 0 0' >> /etc/fstab
   sysctl vm.swappiness=60
   ```
3. **Remove Docker memory limits** (use system memory management):
   - Remove `deploy.resources.limits.memory` from ClickHouse service in docker-compose

The root cause is ClickHouse's background merge operations (`UniqExactMerger`, `QueryPipelineEx`) which temporarily need 12-15GB RAM on a 120M+ transaction dataset.

### FluxNode Data Not Appearing

FluxNode transactions use synchronous inserts when near chain tip. If FluxNode badges don't appear immediately:

```bash
# Check if indexer is in tip-following mode
docker compose -f docker-compose.production.yml logs --tail=20 indexer | grep -i "tip\|sync"

# Check fluxnode_transactions table
docker exec fluxindexer-clickhouse clickhouse-client --query "
SELECT * FROM fluxnode_transactions FINAL ORDER BY block_height DESC LIMIT 5
"
```

---

## Maintenance

### Backup ClickHouse

```bash
# Create backup (snapshot)
docker exec fluxindexer-clickhouse clickhouse-client --query "
BACKUP DATABASE flux TO Disk('backups', 'flux_backup_$(date +%Y%m%d)')
"

# For simpler backup, use data directory
docker cp fluxindexer-clickhouse:/var/lib/clickhouse ./clickhouse-backup/
```

### Update Indexer

```bash
# Pull latest changes
git pull origin main

# Rebuild and restart
docker compose -f docker-compose.production.yml build indexer
docker compose -f docker-compose.production.yml up -d indexer
```

### Disk Space Management

```bash
# Check disk usage
df -h

# Clean old Docker images
docker system prune -a

# Check ClickHouse disk usage by table
docker exec fluxindexer-clickhouse clickhouse-client --query "
SELECT
  table,
  formatReadableSize(sum(bytes_on_disk)) as size
FROM system.parts
WHERE active AND database = 'flux'
GROUP BY table
ORDER BY sum(bytes_on_disk) DESC
"

# Force merge (reclaim space from deleted data)
docker exec fluxindexer-clickhouse clickhouse-client --query "
OPTIMIZE TABLE flux.transactions FINAL
"
```

### Optimize Tables

```bash
# After initial sync, optimize all tables
docker exec fluxindexer-clickhouse clickhouse-client --query "
OPTIMIZE TABLE flux.blocks FINAL;
OPTIMIZE TABLE flux.transactions FINAL;
OPTIMIZE TABLE flux.utxos FINAL;
OPTIMIZE TABLE flux.address_transactions FINAL;
"
```

---

## Performance Tuning

### ClickHouse Optimization

The default configuration is optimized for 16GB RAM. For larger servers:

```bash
# Edit ClickHouse settings
docker exec -it fluxindexer-clickhouse bash
cat >> /etc/clickhouse-server/config.d/custom.xml << 'EOF'
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>0.7</max_server_memory_usage_to_ram_ratio>
    <max_concurrent_queries>100</max_concurrent_queries>
</clickhouse>
EOF

# Restart ClickHouse
docker restart fluxindexer-clickhouse
```

### Indexer Optimization

For initial sync, increase batch size:

```env
INDEXER_BATCH_SIZE=2000
```

After sync complete, reduce for real-time accuracy:

```env
INDEXER_BATCH_SIZE=100
INDEXER_POLLING_INTERVAL=5000
```

---

## Security

### Production Checklist

- [ ] Use environment variables, never commit credentials
- [ ] Restrict RPC access to localhost or internal network
- [ ] Enable firewall on API port 42067
- [ ] Use HTTPS reverse proxy (nginx/caddy) for public access
- [ ] Regular ClickHouse backups
- [ ] Monitor disk space (ClickHouse ~61GB, Fluxd ~50GB)
- [ ] Set up log rotation
- [ ] Restrict ClickHouse ports (8123, 9000) to internal network only

### Recommended nginx Config

```nginx
server {
    listen 80;
    server_name explorer-api.yourdomain.com;

    location / {
        proxy_pass http://localhost:42067;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

### ClickHouse Security

By default, ClickHouse is configured without authentication. For production with external access:

```xml
<!-- /etc/clickhouse-server/users.d/custom.xml -->
<clickhouse>
    <users>
        <default>
            <password_sha256_hex>YOUR_PASSWORD_HASH</password_sha256_hex>
        </default>
    </users>
</clickhouse>
```

---

## Resource Requirements

### Memory Requirements (IMPORTANT)

ClickHouse requires significant RAM during merge/compression operations:

| Component | Normal Usage | Peak Usage | Notes |
|-----------|-------------|------------|-------|
| **ClickHouse** | ~2 GB | **12-15 GB** | Spikes during background merges |
| **Indexer** (daemon + Node.js) | ~2.5 GB | ~3 GB | Daemon ~1.5GB, indexer ~1GB |
| **Explorer** (Next.js) | ~70 MB | ~200 MB | Very lightweight |
| **System overhead** | ~1-2 GB | ~2 GB | OS, Docker, buffers |

### Production Specifications

| Component | RAM Limit | Storage (Final) | Storage (w/ Bootstrap) | CPU |
|-----------|-----------|-----------------|------------------------|-----|
| ClickHouse | 14 GB | 61 GB | **120 GB** | 4 cores |
| Indexer (daemon + indexer) | 4 GB | 50 GB | **100 GB** | 4 cores |
| Explorer | 512 MB | 1 GB | 5 GB | 1 core |
| **Total** | **~19 GB** | **~112 GB** | **~225 GB** | **4+ cores** |

> **Note:** Bootstrap requires temporary space for download + extraction. After bootstrap completes, storage usage drops to "Final" values.

### Minimum System Requirements

- **Recommended:** 24GB+ RAM, 250GB storage, 4+ CPU cores
- **Minimum (with swap):** 16GB RAM + 4GB swap, 225GB storage, 4 cores
  - Add swap: `fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile`
  - Set swappiness: `sysctl vm.swappiness=60`

### Why So Much RAM?

ClickHouse performs background merge operations to optimize storage and query performance. On a dataset with 120M+ transactions, these merges can temporarily consume 12-15GB RAM. After initial stabilization (first few minutes after startup), memory usage drops to ~2GB for normal operation.

### Sync Performance

- **Full sync time:** ~9 hours from genesis (with bootstrap)
- **Bulk sync speed:** ~65 blocks/second
- **Tip-following speed:** ~15-18 blocks/second
- **Blocks indexed:** 2.1M+
- **Transactions indexed:** 120M+

---

## Support

- **GitHub Issues:** https://github.com/RunOnFlux/flux-indexer-explorer/issues
- **Flux Discord:** https://discord.com/invite/runonflux
- **Documentation:** See README.md
