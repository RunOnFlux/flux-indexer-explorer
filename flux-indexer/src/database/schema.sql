-- ============================================================================
-- FluxIndexer ClickHouse Schema - ULTIMATE EDITION v2
-- ============================================================================
-- OPTIMIZATIONS:
--   1. Compression codecs (Delta for sequential, ZSTD(3) for balanced compression)
--   2. Projections for alternative query patterns (10-100x faster queries)
--      - Projections use ONLY needed columns (no SELECT * waste)
--   3. LowCardinality for repeated strings (dictionary encoding)
--   4. Optimized ORDER BY for each table's primary access pattern
--   5. PREWHERE-friendly column ordering (filter columns first in ORDER BY)
--   6. Tuned index_granularity per table based on query patterns
--   7. Lightweight delete support for efficient reorg handling
--   8. TTL RECOMPRESS: New data uses fast ZSTD(3), old data auto-compresses to ZSTD(9)
-- ============================================================================

SET allow_experimental_lightweight_delete = 1;

-- ============================================================================
-- BLOCKS TABLE
-- Primary access: by height (range scans), by hash (point lookup), by producer
-- Using ReplacingMergeTree to deduplicate on re-indexing (version = insert timestamp)
-- ============================================================================
CREATE TABLE IF NOT EXISTS blocks (
    height UInt32 CODEC(Delta(4), ZSTD(3)),
    hash FixedString(64) CODEC(ZSTD(3)),
    prev_hash FixedString(64) CODEC(ZSTD(3)),
    merkle_root FixedString(64) CODEC(ZSTD(3)),
    timestamp UInt32 CODEC(Delta(4), ZSTD(3)),
    bits String CODEC(LZ4),
    nonce String CODEC(LZ4),
    version UInt32 CODEC(LZ4),
    size UInt32 CODEC(LZ4),
    tx_count UInt16 CODEC(LZ4),
    producer LowCardinality(String) DEFAULT '' CODEC(LZ4),
    producer_reward UInt64 CODEC(LZ4),
    difficulty Float64 CODEC(Gorilla),
    chainwork String CODEC(ZSTD(3)),
    is_valid UInt8 DEFAULT 1,                         -- For reorg handling (soft delete)
    _version UInt64 DEFAULT toUnixTimestamp64Milli(now64()) CODEC(Delta(8), ZSTD(3))  -- For deduplication
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (height)
PARTITION BY intDiv(height, 100000)
SETTINGS index_granularity = 8192,
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;

-- Projection for hash lookups (O(1) instead of O(log n))
ALTER TABLE blocks ADD PROJECTION IF NOT EXISTS proj_by_hash (
    SELECT * ORDER BY hash
);

-- Projection for producer queries (rich list of producers)
-- Note: DESC not supported in projections, use ASC and reverse in query
ALTER TABLE blocks ADD PROJECTION IF NOT EXISTS proj_by_producer (
    SELECT producer, height, hash, timestamp, producer_reward
    ORDER BY producer, height
);

-- Skip indexes
ALTER TABLE blocks ADD INDEX IF NOT EXISTS idx_hash (hash) TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE blocks ADD INDEX IF NOT EXISTS idx_timestamp (timestamp) TYPE minmax GRANULARITY 1;
ALTER TABLE blocks ADD INDEX IF NOT EXISTS idx_is_valid (is_valid) TYPE set(2) GRANULARITY 1;


-- ============================================================================
-- TRANSACTIONS TABLE
-- Primary access: by block_height (batch loading), by txid (point lookup)
-- Using ReplacingMergeTree to deduplicate on re-indexing
-- TTL: Data older than 7 days auto-recompresses to ZSTD(9) for better storage
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions (
    block_height UInt32 CODEC(Delta(4), ZSTD(3)),
    txid FixedString(64) CODEC(ZSTD(3)),
    tx_index UInt16 DEFAULT 0 CODEC(LZ4),
    timestamp UInt32 CODEC(Delta(4), ZSTD(3)),
    version UInt32 CODEC(LZ4),
    locktime UInt64 CODEC(LZ4),
    size UInt32 CODEC(LZ4),
    vsize UInt32 CODEC(LZ4),
    input_count UInt16 CODEC(LZ4),
    output_count UInt16 CODEC(LZ4),
    input_total UInt64 CODEC(LZ4),
    output_total UInt64 CODEC(LZ4),
    fee Int64 CODEC(LZ4),
    is_coinbase UInt8 DEFAULT 0,
    is_fluxnode_tx UInt8 DEFAULT 0,
    fluxnode_type Nullable(UInt8) DEFAULT NULL,
    is_shielded UInt8 DEFAULT 0,
    is_valid UInt8 DEFAULT 1,                         -- For reorg handling (soft delete)
    _version UInt64 DEFAULT toUnixTimestamp64Milli(now64()) CODEC(Delta(8), ZSTD(3))  -- For deduplication
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (txid)
PARTITION BY intDiv(block_height, 100000)
TTL toDateTime(timestamp) + INTERVAL 7 DAY RECOMPRESS CODEC(ZSTD(9))
SETTINGS index_granularity = 8192;

-- NOTE: proj_by_txid is NOT needed - table is already ORDER BY txid
-- Redundant projection would waste ~10GB of storage

-- Projection for coinbase transactions (block rewards analysis)
-- Note: WHERE clause in projections not supported, filter at query time
ALTER TABLE transactions ADD PROJECTION IF NOT EXISTS proj_coinbase (
    SELECT block_height, txid, output_total, timestamp, is_coinbase
    ORDER BY is_coinbase, block_height
);

-- Bloom filter for txid (backup for projection misses)
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_txid (txid) TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_timestamp (timestamp) TYPE minmax GRANULARITY 8192;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_is_valid (is_valid) TYPE set(2) GRANULARITY 1;


-- ============================================================================
-- UTXOS TABLE (ReplacingMergeTree for spent updates)
-- Primary access: by (txid, vout) for spending, by address for balance
-- ============================================================================
CREATE TABLE IF NOT EXISTS utxos (
    txid FixedString(64) CODEC(ZSTD(3)),
    vout UInt16 CODEC(LZ4),
    address LowCardinality(String) CODEC(ZSTD(3)),
    value UInt64 CODEC(LZ4),
    script_pubkey String DEFAULT '' CODEC(ZSTD(3)),
    script_type LowCardinality(String) CODEC(LZ4),
    block_height UInt32 CODEC(Delta(4), ZSTD(3)),
    spent UInt8 DEFAULT 0,
    spent_txid FixedString(64) DEFAULT '0000000000000000000000000000000000000000000000000000000000000000' CODEC(ZSTD(3)),
    spent_block_height UInt32 DEFAULT 0 CODEC(Delta(4), ZSTD(3)),
    version UInt64 CODEC(LZ4)
) ENGINE = ReplacingMergeTree(version)
ORDER BY (txid, vout)
PARTITION BY intDiv(block_height, 100000)
SETTINGS index_granularity = 4096;

-- Projection for address UTXO queries - CRITICAL for wallet balance
ALTER TABLE utxos ADD PROJECTION IF NOT EXISTS proj_by_address (
    SELECT address, txid, vout, value, script_type, block_height, spent, spent_txid
    ORDER BY address, spent, block_height
);

-- Projection for unspent UTXOs by value (for coin selection algorithms)
-- Note: WHERE clause in projections not supported, filter at query time
ALTER TABLE utxos ADD PROJECTION IF NOT EXISTS proj_unspent_by_value (
    SELECT address, txid, vout, value, block_height, spent
    ORDER BY spent, value
);

-- Skip indexes
ALTER TABLE utxos ADD INDEX IF NOT EXISTS idx_address (address) TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE utxos ADD INDEX IF NOT EXISTS idx_spent (spent) TYPE set(2) GRANULARITY 1;
ALTER TABLE utxos ADD INDEX IF NOT EXISTS idx_block_height (block_height) TYPE minmax GRANULARITY 1;


-- ============================================================================
-- ADDRESS_TRANSACTIONS TABLE
-- Primary access: paginated by address (most recent first)
-- Denormalized for fast queries: includes block_hash to avoid JOINs
-- Using ReplacingMergeTree to deduplicate on re-indexing
-- TTL: Data older than 7 days auto-recompresses to ZSTD(9) for better storage
-- ============================================================================
CREATE TABLE IF NOT EXISTS address_transactions (
    address LowCardinality(String) CODEC(ZSTD(3)),
    block_height UInt32 CODEC(Delta(4), ZSTD(3)),
    block_hash FixedString(64) CODEC(ZSTD(3)),        -- Denormalized for fast access
    txid FixedString(64) CODEC(ZSTD(3)),
    tx_index UInt16 CODEC(LZ4),                       -- Position in block (for ordering)
    timestamp UInt32 CODEC(Delta(4), LZ4),
    direction LowCardinality(String) DEFAULT 'received' CODEC(LZ4),
    received_value UInt64 CODEC(LZ4),
    sent_value UInt64 CODEC(LZ4),
    is_coinbase UInt8 DEFAULT 0,                      -- Fast coinbase identification
    is_valid UInt8 DEFAULT 1,                         -- For reorg handling (soft delete)
    _version UInt64 DEFAULT toUnixTimestamp64Milli(now64()) CODEC(Delta(8), LZ4)  -- For deduplication
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (address, txid)
PARTITION BY intDiv(block_height, 100000)
TTL toDateTime(timestamp) + INTERVAL 7 DAY RECOMPRESS CODEC(ZSTD(9))
SETTINGS index_granularity = 8192;

-- Projection for txid lookup within address context
ALTER TABLE address_transactions ADD PROJECTION IF NOT EXISTS proj_by_txid (
    SELECT * ORDER BY txid, address
);

ALTER TABLE address_transactions ADD INDEX IF NOT EXISTS idx_txid (txid) TYPE bloom_filter(0.01) GRANULARITY 4;


-- ============================================================================
-- ADDRESS_SUMMARY TABLE (SummingMergeTree for incremental balance updates)
-- Primary access: by address (point lookup), by balance (rich list)
-- Using SummingMergeTree: insert deltas, ClickHouse automatically sums on merge
-- This works perfectly with async inserts since no pre-read is needed
--
-- ARCHITECTURE:
-- 1. Raw table (address_summary): Stores delta values, uses SummingMergeTree
-- 2. Materialized view (address_summary_agg): Pre-aggregates for fast queries
-- 3. Query the _agg table for accurate, fast results
-- ============================================================================
CREATE TABLE IF NOT EXISTS address_summary (
    address LowCardinality(String) CODEC(LZ4),
    -- Summing columns: these accumulate across inserts
    balance Int64 CODEC(LZ4),
    tx_count UInt32 CODEC(LZ4),
    received_total UInt64 CODEC(LZ4),
    sent_total UInt64 CODEC(LZ4),
    unspent_count UInt32 CODEC(LZ4),
    unconfirmed_balance Int64 DEFAULT 0 CODEC(LZ4),
    -- Non-summing columns: aggregated via min/max in the materialized view
    first_seen UInt32 CODEC(Delta, LZ4),
    last_activity UInt32 CODEC(Delta, LZ4),
    -- FluxNode counts: aggregated via max in the materialized view
    cumulus_count UInt16 DEFAULT 0,
    nimbus_count UInt16 DEFAULT 0,
    stratus_count UInt16 DEFAULT 0,
    fluxnode_last_sync Nullable(DateTime) DEFAULT NULL
) ENGINE = SummingMergeTree((balance, tx_count, received_total, sent_total, unspent_count, unconfirmed_balance))
ORDER BY (address)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- ADDRESS_SUMMARY_AGG - Materialized View for pre-aggregated address data
-- This is the PRIMARY table to query for address information
-- It automatically aggregates data from address_summary using AggregatingMergeTree
-- ============================================================================
CREATE TABLE IF NOT EXISTS address_summary_agg (
    address LowCardinality(String) CODEC(LZ4),
    -- AggregateFunction states for accurate aggregation
    balance AggregateFunction(sum, Int64),
    tx_count AggregateFunction(sum, UInt32),
    received_total AggregateFunction(sum, UInt64),
    sent_total AggregateFunction(sum, UInt64),
    unspent_count AggregateFunction(sum, UInt32),
    first_seen AggregateFunction(min, UInt32),
    last_activity AggregateFunction(max, UInt32),
    cumulus_count AggregateFunction(max, UInt16),
    nimbus_count AggregateFunction(max, UInt16),
    stratus_count AggregateFunction(max, UInt16),
    fluxnode_last_sync AggregateFunction(max, Nullable(DateTime))
) ENGINE = AggregatingMergeTree()
ORDER BY (address)
SETTINGS index_granularity = 8192;

-- Materialized view that populates address_summary_agg from address_summary inserts
CREATE MATERIALIZED VIEW IF NOT EXISTS address_summary_mv TO address_summary_agg AS
SELECT
    address,
    sumState(balance) AS balance,
    sumState(tx_count) AS tx_count,
    sumState(received_total) AS received_total,
    sumState(sent_total) AS sent_total,
    sumState(unspent_count) AS unspent_count,
    minState(first_seen) AS first_seen,
    maxState(last_activity) AS last_activity,
    maxState(cumulus_count) AS cumulus_count,
    maxState(nimbus_count) AS nimbus_count,
    maxState(stratus_count) AS stratus_count,
    maxState(fluxnode_last_sync) AS fluxnode_last_sync
FROM address_summary
GROUP BY address;

-- Note: Projections with aggregation functions cannot have ORDER BY in ClickHouse.
-- Rich list queries will use ORDER BY in the query itself, which is efficient
-- since the _agg table is already grouped by address.


-- ============================================================================
-- SUPPLY_STATS TABLE
-- Primary access: latest record, historical range queries
-- Using ReplacingMergeTree to deduplicate on re-indexing
-- ============================================================================
CREATE TABLE IF NOT EXISTS supply_stats (
    block_height UInt32 CODEC(Delta(4), ZSTD(3)),
    timestamp UInt32 CODEC(Delta(4), ZSTD(3)),         -- Block timestamp for accurate date grouping
    transparent_supply Int64 CODEC(Delta(8), ZSTD(3)),
    shielded_pool Int64 CODEC(Delta(8), ZSTD(3)),
    total_supply Int64 CODEC(Delta(8), ZSTD(3)),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(now64()) CODEC(Delta(8), ZSTD(3))  -- For deduplication
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (block_height)
PARTITION BY intDiv(block_height, 100000)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- FLUXNODE_TRANSACTIONS TABLE
-- Primary access: by block_height, by IP, by tier
-- Using ReplacingMergeTree to deduplicate on re-indexing
-- TTL: Data older than 7 days auto-recompresses to ZSTD(9) for better storage
-- ============================================================================
CREATE TABLE IF NOT EXISTS fluxnode_transactions (
    block_height UInt32 CODEC(Delta(4), ZSTD(3)),
    txid FixedString(64) CODEC(ZSTD(3)),
    block_time DateTime CODEC(Delta(4), ZSTD(3)),
    version UInt32 CODEC(ZSTD(3)),
    type UInt8,
    collateral_hash String DEFAULT '' CODEC(ZSTD(3)),
    collateral_index UInt16 DEFAULT 0 CODEC(ZSTD(3)),
    ip_address String DEFAULT '' CODEC(ZSTD(3)),
    public_key String DEFAULT '' CODEC(ZSTD(3)),
    signature String DEFAULT '' CODEC(ZSTD(3)),
    p2sh_address LowCardinality(String) DEFAULT '' CODEC(ZSTD(3)),
    benchmark_tier LowCardinality(String) DEFAULT '' CODEC(ZSTD(3)),
    extra_data String DEFAULT '' CODEC(ZSTD(3)),
    is_valid UInt8 DEFAULT 1,                         -- For reorg handling (soft delete)
    _version UInt64 DEFAULT toUnixTimestamp64Milli(now64()) CODEC(Delta(8), ZSTD(3))  -- For deduplication
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (txid)
PARTITION BY intDiv(block_height, 100000)
TTL block_time + INTERVAL 7 DAY RECOMPRESS CODEC(ZSTD(9))
SETTINGS index_granularity = 8192;

-- Projection for IP lookups - ONLY includes columns needed by getFluxNodeStatus API
-- Excludes signature (~200 bytes) and extra_data to save ~14GB vs SELECT *
ALTER TABLE fluxnode_transactions ADD PROJECTION IF NOT EXISTS proj_by_ip (
    SELECT
        txid, block_height, block_time, type, ip_address,
        public_key, benchmark_tier, p2sh_address,
        collateral_hash, collateral_index, is_valid, _version
    ORDER BY ip_address, block_height
);

-- Projection for tier analysis (already optimized - only needed columns)
ALTER TABLE fluxnode_transactions ADD PROJECTION IF NOT EXISTS proj_by_tier (
    SELECT benchmark_tier, block_height, txid, ip_address, type
    ORDER BY benchmark_tier, block_height
);

ALTER TABLE fluxnode_transactions ADD INDEX IF NOT EXISTS idx_type (type) TYPE set(8) GRANULARITY 1;
ALTER TABLE fluxnode_transactions ADD INDEX IF NOT EXISTS idx_tier (benchmark_tier) TYPE set(8) GRANULARITY 1;
ALTER TABLE fluxnode_transactions ADD INDEX IF NOT EXISTS idx_ip (ip_address) TYPE bloom_filter(0.01) GRANULARITY 4;


-- ============================================================================
-- LIVE_FLUXNODES TABLE
-- Current FluxNode counts per payment address (synced from daemon)
-- Using ReplacingMergeTree: each sync replaces all data for an address
-- This is SEPARATE from address_summary to avoid SummingMergeTree zero-row deletion
-- ============================================================================
CREATE TABLE IF NOT EXISTS live_fluxnodes (
    address LowCardinality(String) CODEC(LZ4),
    cumulus_count UInt16 DEFAULT 0,
    nimbus_count UInt16 DEFAULT 0,
    stratus_count UInt16 DEFAULT 0,
    total_collateral UInt64 DEFAULT 0 CODEC(LZ4),  -- Total FLUX locked (1000*cumulus + 12500*nimbus + 40000*stratus)
    last_sync DateTime DEFAULT now() CODEC(Delta, LZ4),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(now64()) CODEC(Delta, LZ4)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (address)
SETTINGS index_granularity = 8192;

-- Projection for rich list queries (by total collateral)
ALTER TABLE live_fluxnodes ADD PROJECTION IF NOT EXISTS proj_by_collateral (
    SELECT address, cumulus_count, nimbus_count, stratus_count, total_collateral
    ORDER BY total_collateral
);


-- ============================================================================
-- PRODUCERS TABLE (PoN Block Producers)
-- Primary access: by blocks_produced (leaderboard), by fluxnode (lookup)
-- ============================================================================
CREATE TABLE IF NOT EXISTS producers (
    fluxnode LowCardinality(String) CODEC(LZ4),
    blocks_produced UInt32 CODEC(LZ4),
    first_block UInt32 CODEC(Delta, LZ4),
    last_block UInt32 CODEC(Delta, LZ4),
    total_rewards UInt64 CODEC(LZ4),
    avg_block_time Float32 CODEC(Gorilla),
    updated_at DateTime DEFAULT now() CODEC(Delta, LZ4)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (fluxnode)
SETTINGS index_granularity = 8192;

-- Projection for producer leaderboard
ALTER TABLE producers ADD PROJECTION IF NOT EXISTS proj_leaderboard (
    SELECT fluxnode, blocks_produced, total_rewards, last_block
    ORDER BY blocks_produced
);


-- ============================================================================
-- SYNC_STATE TABLE (Single row tracking sync progress)
-- ============================================================================
CREATE TABLE IF NOT EXISTS sync_state (
    id UInt8 DEFAULT 1,
    current_height Int32,
    chain_height UInt32,
    sync_percentage Float32,
    last_block_hash FixedString(64),
    last_sync_time DateTime,
    is_syncing UInt8 DEFAULT 0,
    blocks_per_second Float32 CODEC(Gorilla),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id)
SETTINGS index_granularity = 1;

-- Initialize sync state
INSERT INTO sync_state (id, current_height, chain_height, sync_percentage, last_block_hash, is_syncing)
VALUES (1, -1, 0, 0, '0000000000000000000000000000000000000000000000000000000000000000', 0);


-- ============================================================================
-- REORGS TABLE (Audit log for chain reorganizations)
-- ============================================================================
CREATE TABLE IF NOT EXISTS reorgs (
    id UInt64 CODEC(Delta, LZ4),
    from_height UInt32,
    to_height UInt32,
    common_ancestor UInt32,
    old_hash FixedString(64),
    new_hash FixedString(64),
    blocks_affected UInt16,
    occurred_at DateTime DEFAULT now() CODEC(Delta, LZ4)
) ENGINE = MergeTree()
ORDER BY (occurred_at, id)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- MATERIALIZED VIEW: Real-time transaction count per hour (for charts)
-- ============================================================================
CREATE TABLE IF NOT EXISTS mv_hourly_tx_count (
    hour DateTime CODEC(Delta, LZ4),
    tx_count UInt64,
    block_count UInt64
) ENGINE = SummingMergeTree()
ORDER BY (hour)
SETTINGS index_granularity = 256;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_tx_count_view TO mv_hourly_tx_count AS
SELECT
    toStartOfHour(toDateTime(timestamp)) AS hour,
    count() AS tx_count,
    1 AS block_count
FROM transactions
GROUP BY hour;


-- ============================================================================
-- MATERIALIZED VIEW: Daily supply history (for supply charts)
-- Uses actual block timestamps for accurate date grouping
-- ============================================================================
CREATE TABLE IF NOT EXISTS mv_daily_supply (
    day Date CODEC(Delta, LZ4),
    max_height UInt32,
    transparent_supply Int64,
    shielded_pool Int64,
    total_supply Int64
) ENGINE = ReplacingMergeTree(max_height)
ORDER BY (day)
SETTINGS index_granularity = 256;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_supply_view TO mv_daily_supply AS
SELECT
    toDate(toDateTime(timestamp)) AS day,  -- Use actual block timestamp
    block_height AS max_height,
    transparent_supply,
    shielded_pool,
    total_supply
FROM supply_stats;
