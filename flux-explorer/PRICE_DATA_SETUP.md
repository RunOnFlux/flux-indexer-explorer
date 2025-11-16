# Price Data Setup Guide

This guide explains how the automatic price data system works in Flux Explorer.

## Overview

The explorer uses a local SQLite database to store hourly Flux/USD price data. This ensures:
-  **FMV Compliance**: Hourly precision meets Fair Market Value requirements for tax reporting
-  **No Rate Limits**: All exports use cached data (no API calls during export)
-  **Fast Exports**: Instant price lookups from local database
-  **Multi-User Safe**: Unlimited concurrent users
-  **Auto-Initialize**: Automatically populates on first startup in production

## Automatic Initialization

**The price database populates automatically when you deploy the app** - no manual intervention required!

### Initial Population (First Startup)

On first startup (or if data is outdated), the app:
1. Checks if price data exists and is recent
2. Automatically spawns a background process to populate 4 years of historical data
3. Continues running normally while data populates (non-blocking)
4. Takes ~5-10 minutes to complete initial population

**App startup logs:**
```
 Initializing price data: Database is empty
   This will run in the background and may take 30-45 minutes
   The app will remain fully functional during this time
   Price data will be available once complete

 Price data population started (PID: 12345)
   Monitor container logs (docker logs -f flux-explorer) for progress
```

### Continuous Hourly Updates

After initial population, the app automatically:
1. Fetches the latest hourly price every 60 minutes
2. Stores it in the database for immediate use
3. Ensures brand new transactions always have price data available

**Hourly update logs:**
```
⏰ Starting hourly price updates (runs every 60 minutes)
 Updated hourly price: $0.5234 at 2025-10-28T14:00:00.000Z
 Updated hourly price: $0.5241 at 2025-10-28T15:00:00.000Z
```

This means **no manual maintenance required** - the system stays up-to-date automatically.

### Development Mode

Auto-initialization is disabled in development to avoid unnecessary API calls. To enable:

```bash
export AUTO_INIT_PRICES=true
npm run dev
```

Or manually populate:

```bash
npm run populate-prices
```

**What it does:**
- Fetches hourly Flux/USD prices from CryptoCompare (last 4 years)
- Stores ~35,000 hourly data points in SQLite
- Takes ~5-10 minutes (1 second delays between API calls for rate limit safety)
- Resumable if interrupted
- Skips already-populated date ranges

**Output example:**
```
 Starting price history population...

Current database state:
  Total entries: 0

 Empty database - will populate last 5 years of hourly data

 Will fetch ~18 chunks (~83 days each)

Chunk 1/18:
  Fetching from 2021-01-01T... to 2021-03-25T...
   Stored 2,000 price points
  ⏳ Waiting 1 second (rate limit protection)...

...

 Population complete!
   Added 43,800 new price points

Final database stats:
  Total entries: 43,800
  Date range: 2021-01-01 to 2025-01-01
```

## Manual Maintenance (Optional)

The app maintains price data automatically with hourly updates, but you can manually trigger updates if needed:

### Force Update Latest Prices

```bash
npm run update-prices
```

This fetches the last 48 hours of data (takes ~10 seconds). Useful if:
- The app was stopped for an extended period
- You want to backfill any gaps
- Testing the update mechanism

### Cron Jobs (Not Required)

If you prefer external scheduling instead of the built-in hourly updates, you can disable auto-updates and use cron:

```bash
# In your .env file:
AUTO_INIT_PRICES=false

# Add cron job to run hourly:
0 * * * * cd /path/to/flux-explorer && npm run update-prices >> /var/log/price-updates.log 2>&1
```

**Note**: This is not recommended for Flux deployments - the built-in auto-updates are more reliable.

## How It Works

### Data Storage

- **Location**: `flux-explorer/data/price-cache.db` (SQLite)
- **Schema**: `price_history (timestamp INTEGER, price_usd REAL, fetched_at INTEGER)`
- **Size**: ~2-3 MB for 5 years of hourly data

### Price Lookups

When exporting transactions:
1. Export gathers all transaction timestamps
2. Batch API endpoint (`/api/prices/batch`) looks up prices
3. Database finds closest price within 2 hours of each timestamp
4. Prices are added to CSV: `Price USD` and `Value USD` columns

### Rate Limiting Safety

- **Initial population**: 1 second delays between ~83-day chunks (≈60 calls/hour)
- **CryptoCompare free tier**: Generous limits with no API key required
- **Our rate**: Well under the limit, safe for any deployment

## Troubleshooting

### Missing Prices in Exports

If CSV exports show empty price columns:

```bash
# Check database status
npm run populate-prices
```

If it shows "Database is up to date" but prices are still missing, the database may be corrupted:

```bash
# Delete and repopulate
rm -rf data/price-cache.db
npm run populate-prices
```

### Population Failed Midway

The script is resumable. Just run it again:

```bash
npm run populate-prices
```

It will automatically skip already-populated ranges and continue from where it left off.

### Rate Limit Errors

If you see `429 Too Many Requests` errors:
- The script already has 1-second delays (should never hit this)
- If it happens, just wait 1 minute and run again
- Script will resume from where it stopped

## Database Maintenance

### Check Database Stats

```bash
npm run populate-prices
# Shows current state without fetching if up-to-date
```

### Clean Old Data (Optional)

Price data older than 7 years is rarely needed:

```sql
-- Connect to database
sqlite3 data/price-cache.db

-- Remove entries older than 7 years
DELETE FROM price_history WHERE timestamp < strftime('%s', 'now', '-7 years');
```

## Production Deployment

### Docker

**CRITICAL**: Configure a Docker volume for `/app/data` to persist the price cache database:

```yaml
# docker-compose.yml
services:
  explorer:
    image: yourusername/flux-explorer:latest
    volumes:
      - price-cache-data:/app/data  # Required: Persist SQLite price cache
    # ... other configuration

volumes:
  price-cache-data:
    driver: local
```

**Why this is critical**:
- Without the volume, the price database is lost on every rebuild
- Forces a 5-10 minute re-download of ~35,000 historical price points
- Can hit rate limits if rebuilding frequently
- Price data (~50MB SQLite database) will persist with the volume

### Flux Deployment

**CRITICAL**: Configure `containerData` in your Flux spec to persist the price cache:

```json
{
  "name": "explorer",
  "containerData": "/app/data",  // Required for price cache persistence
  // ... other configuration
}
```

See [flux-spec.json](../flux-spec.json) for a complete example.

**Fully automatic** - the app detects empty/outdated price data and populates it automatically. The database file (`data/price-cache.db`) persists across restarts when the volume is properly configured.

## API Endpoints Used

- **Historical data**: `https://api.coingecko.com/api/v3/coins/zelcash/market_chart/range`
  - Returns hourly prices for date range
  - Free tier: No API key required
  - Rate limit: 10-50 calls/minute

## Cost

**Free** - Uses CryptoCompare's free API tier. No API keys or subscriptions required.
