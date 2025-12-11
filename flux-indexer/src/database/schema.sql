-- FluxIndexer Database Schema
-- PostgreSQL 15+ with TimescaleDB
-- Optimized for Flux blockchain with time-series compression
--
-- PHASE 3 OPTIMIZATIONS:
-- - Address Normalization: address TEXT → address_id INTEGER (-8GB)
-- - txid → BYTEA: 64-char TEXT → 32-byte BYTEA (-15GB)
-- Combined with previous optimizations: 174GB → ~27GB target

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop existing tables and views (for clean migrations)
DROP VIEW IF EXISTS utxos CASCADE;  -- utxos is now a VIEW, not a table
DROP TABLE IF EXISTS reorgs CASCADE;
DROP TABLE IF EXISTS sync_state CASCADE;
DROP TABLE IF EXISTS producers CASCADE;
DROP TABLE IF EXISTS address_summary CASCADE;
DROP TABLE IF EXISTS fluxnode_transactions CASCADE;
DROP TABLE IF EXISTS address_transactions CASCADE;
DROP TABLE IF EXISTS supply_stats CASCADE;
DROP TABLE IF EXISTS utxos_unspent CASCADE;
DROP TABLE IF EXISTS utxos_spent CASCADE;
DROP TABLE IF EXISTS tx_lookup CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS blocks CASCADE;
DROP TABLE IF EXISTS addresses CASCADE;
DROP TABLE IF EXISTS schema_migrations CASCADE;

-- ============================================================================
-- ADDRESS NORMALIZATION TABLE
-- ============================================================================

-- Addresses lookup table for normalization
-- Reduces address storage from ~40 bytes (TEXT) to 4 bytes (INTEGER) per reference
-- Saves ~8GB across utxos_unspent, utxos_spent, and address_transactions
CREATE TABLE addresses (
  id SERIAL PRIMARY KEY,
  address TEXT UNIQUE NOT NULL,
  first_seen INTEGER,  -- block_height of first occurrence
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_addresses_address ON addresses(address);

-- ============================================================================
-- HYPERTABLES (Time-series optimized tables)
-- ============================================================================

-- Blocks table (hypertable partitioned by height)
CREATE TABLE blocks (
  height INTEGER NOT NULL,
  hash TEXT NOT NULL,
  prev_hash TEXT,
  merkle_root TEXT,
  timestamp INTEGER NOT NULL,
  bits TEXT,
  nonce TEXT,
  version INTEGER,
  size INTEGER,
  tx_count INTEGER DEFAULT 0,
  producer TEXT,
  producer_reward BIGINT,
  difficulty DECIMAL(20, 8),
  chainwork TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (height)
);

-- Convert blocks to hypertable (100k blocks per chunk)
SELECT create_hypertable('blocks', by_range('height', 100000));

CREATE INDEX idx_blocks_hash ON blocks(hash);
CREATE INDEX idx_blocks_timestamp ON blocks(timestamp DESC);
CREATE INDEX idx_blocks_producer ON blocks(producer) WHERE producer IS NOT NULL;

-- Transactions table (hypertable partitioned by block_height)
-- OPTIMIZED: Removed block_hash column (-15GB across all tables)
-- OPTIMIZED: txid stored as BYTEA (32 bytes) instead of TEXT (64 chars) - 50% savings
CREATE TABLE transactions (
  txid BYTEA NOT NULL,  -- PHASE 3: Changed from TEXT to BYTEA
  block_height INTEGER NOT NULL,
  timestamp INTEGER,
  version INTEGER,
  locktime BIGINT,
  size INTEGER,
  vsize INTEGER,
  input_count INTEGER DEFAULT 0,
  output_count INTEGER DEFAULT 0,
  input_total BIGINT DEFAULT 0,
  output_total BIGINT DEFAULT 0,
  fee BIGINT DEFAULT 0,
  is_coinbase BOOLEAN DEFAULT false,
  is_fluxnode_tx BOOLEAN DEFAULT false,
  fluxnode_type INTEGER,
  hex TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (block_height, txid)
);

-- Convert transactions to hypertable (100k blocks per chunk)
SELECT create_hypertable('transactions', by_range('block_height', 100000));

CREATE INDEX idx_tx_timestamp ON transactions(timestamp DESC);
CREATE INDEX idx_tx_coinbase ON transactions(is_coinbase) WHERE is_coinbase = true;
CREATE INDEX idx_tx_fluxnode ON transactions(is_fluxnode_tx) WHERE is_fluxnode_tx = true;

-- Address transactions cache (hypertable partitioned by block_height)
-- OPTIMIZED: address_id (INTEGER) instead of address (TEXT) - 90% savings per row
-- OPTIMIZED: txid stored as BYTEA (32 bytes) instead of TEXT (64 chars)
CREATE TABLE address_transactions (
  address_id INTEGER NOT NULL REFERENCES addresses(id),  -- PHASE 3: Normalized
  txid BYTEA NOT NULL,  -- PHASE 3: Changed from TEXT to BYTEA
  block_height INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  direction TEXT NOT NULL CHECK (direction IN ('received', 'sent')),
  received_value BIGINT NOT NULL DEFAULT 0,
  sent_value BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (block_height, address_id, txid)
);

-- Convert address_transactions to hypertable (100k blocks per chunk)
SELECT create_hypertable('address_transactions', by_range('block_height', 100000));

-- Index for address lookups (via address_id)
CREATE INDEX idx_address_tx_address_id ON address_transactions(address_id, block_height DESC, txid DESC);

-- FluxNode transactions (hypertable partitioned by block_height)
-- OPTIMIZED: txid stored as BYTEA
CREATE TABLE fluxnode_transactions (
  txid BYTEA NOT NULL,  -- PHASE 3: Changed from TEXT to BYTEA
  block_height INTEGER NOT NULL,
  block_time TIMESTAMPTZ NOT NULL,
  version INTEGER NOT NULL,
  type INTEGER NOT NULL,
  collateral_hash TEXT,
  collateral_index INTEGER,
  ip_address TEXT,
  public_key TEXT,
  signature TEXT,
  p2sh_address TEXT,
  benchmark_tier TEXT,
  extra_data JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (block_height, txid)
);

-- Convert fluxnode_transactions to hypertable (100k blocks per chunk)
SELECT create_hypertable('fluxnode_transactions', by_range('block_height', 100000));

CREATE INDEX idx_fluxnode_tx_txid ON fluxnode_transactions(txid);
CREATE INDEX idx_fluxnode_tx_type ON fluxnode_transactions(type);
CREATE INDEX idx_fluxnode_tx_collateral ON fluxnode_transactions(collateral_hash, collateral_index) WHERE collateral_hash IS NOT NULL;
CREATE INDEX idx_fluxnode_tx_pubkey ON fluxnode_transactions(public_key) WHERE public_key IS NOT NULL;
CREATE INDEX idx_fluxnode_tx_ip ON fluxnode_transactions(ip_address) WHERE ip_address IS NOT NULL;
CREATE INDEX idx_fluxnode_tx_p2sh ON fluxnode_transactions(p2sh_address) WHERE p2sh_address IS NOT NULL;

-- Supply statistics (hypertable partitioned by block_height)
CREATE TABLE supply_stats (
  block_height INTEGER NOT NULL,
  transparent_supply BIGINT NOT NULL DEFAULT 0,
  shielded_pool BIGINT NOT NULL DEFAULT 0,
  total_supply BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (block_height)
);

-- Convert supply_stats to hypertable (100k blocks per chunk)
SELECT create_hypertable('supply_stats', by_range('block_height', 100000));

-- ============================================================================
-- SPLIT UTXO TABLES (Optimized for storage and query performance)
-- ============================================================================

-- Unspent UTXOs - Small hot table (~2M rows) for fast balance queries
-- OPTIMIZED: address_id (INTEGER) instead of address (TEXT)
-- OPTIMIZED: txid stored as BYTEA (32 bytes)
CREATE TABLE utxos_unspent (
  txid BYTEA NOT NULL,  -- PHASE 3: Changed from TEXT to BYTEA
  vout INTEGER NOT NULL,
  address_id INTEGER NOT NULL REFERENCES addresses(id),  -- PHASE 3: Normalized
  value BIGINT NOT NULL,
  script_pubkey TEXT,
  script_type TEXT,
  block_height INTEGER NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (txid, vout)
);

CREATE INDEX idx_utxos_unspent_address_id ON utxos_unspent(address_id);
CREATE INDEX idx_utxos_unspent_block_height ON utxos_unspent(block_height);
CREATE INDEX idx_utxos_unspent_value ON utxos_unspent(value DESC);
-- Expression index for TEXT-based txid lookups (used by utxos VIEW queries)
-- Critical for performance: enables index scans instead of seq scans when
-- queries use encode(txid, 'hex') pattern through the utxos VIEW
CREATE INDEX idx_utxos_unspent_txid_hex ON utxos_unspent (encode(txid, 'hex'), vout);

-- Spent UTXOs - Large cold table (~100M rows) with TimescaleDB compression
-- OPTIMIZED: address_id (INTEGER) instead of address (TEXT)
-- OPTIMIZED: txid and spent_txid stored as BYTEA
CREATE TABLE utxos_spent (
  txid BYTEA NOT NULL,  -- PHASE 3: Changed from TEXT to BYTEA
  vout INTEGER NOT NULL,
  address_id INTEGER NOT NULL REFERENCES addresses(id),  -- PHASE 3: Normalized
  value BIGINT NOT NULL,
  script_pubkey TEXT,
  script_type TEXT,
  block_height INTEGER NOT NULL,  -- When UTXO was created
  spent_txid BYTEA NOT NULL,  -- PHASE 3: Changed from TEXT to BYTEA
  spent_block_height INTEGER NOT NULL,  -- When UTXO was spent (partition key)
  created_at TIMESTAMPTZ DEFAULT NOW(),
  spent_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (spent_block_height, txid, vout)  -- Include partition key in PK
);

-- Convert to hypertable partitioned by spent_block_height (100k blocks per chunk)
SELECT create_hypertable('utxos_spent', by_range('spent_block_height', 100000));

CREATE INDEX idx_utxos_spent_address_id ON utxos_spent(address_id, spent_block_height DESC);
CREATE INDEX idx_utxos_spent_txid ON utxos_spent(txid);
CREATE INDEX idx_utxos_spent_spent_txid ON utxos_spent(spent_txid);
CREATE INDEX idx_utxos_spent_block_height ON utxos_spent(block_height);
-- Expression index for TEXT-based txid lookups (used by utxos VIEW queries)
-- Critical for performance: enables index scans instead of seq scans when
-- queries use encode(txid, 'hex') pattern through the utxos VIEW
CREATE INDEX idx_utxos_spent_txid_hex ON utxos_spent (encode(txid, 'hex'), vout);

-- View to unify both tables with address string (for backward compatibility)
-- JOINs to addresses table to return address TEXT
-- Encodes BYTEA txid/spent_txid to hex TEXT for API compatibility
CREATE OR REPLACE VIEW utxos AS
SELECT
  encode(u.txid, 'hex') AS txid,
  u.vout,
  a.address,
  u.value,
  u.script_pubkey,
  u.script_type,
  u.block_height,
  false AS spent,
  NULL::TEXT AS spent_txid,
  NULL::INTEGER AS spent_block_height,
  u.created_at,
  NULL::TIMESTAMPTZ AS spent_at
FROM utxos_unspent u
JOIN addresses a ON u.address_id = a.id
UNION ALL
SELECT
  encode(s.txid, 'hex') AS txid,
  s.vout,
  a.address,
  s.value,
  s.script_pubkey,
  s.script_type,
  s.block_height,
  true AS spent,
  encode(s.spent_txid, 'hex') AS spent_txid,
  s.spent_block_height,
  s.created_at,
  s.spent_at
FROM utxos_spent s
JOIN addresses a ON s.address_id = a.id;

-- Transaction lookup table for fast txid -> block_height resolution
-- Already uses BYTEA from previous optimization
CREATE TABLE tx_lookup (
  txid BYTEA PRIMARY KEY,
  block_height INTEGER NOT NULL
);

CREATE INDEX idx_tx_lookup_block_height ON tx_lookup(block_height);

-- Helper function to convert txid hex string to bytea for lookups
CREATE OR REPLACE FUNCTION txid_to_bytes(txid_hex TEXT) RETURNS BYTEA AS $$
BEGIN
  RETURN decode(txid_hex, 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Helper function to convert bytea txid to hex string
CREATE OR REPLACE FUNCTION txid_to_hex(txid_bytes BYTEA) RETURNS TEXT AS $$
BEGIN
  RETURN encode(txid_bytes, 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Helper function to get address_id, creating if not exists
CREATE OR REPLACE FUNCTION get_or_create_address_id(addr TEXT) RETURNS INTEGER AS $$
DECLARE
  addr_id INTEGER;
BEGIN
  -- Try to get existing
  SELECT id INTO addr_id FROM addresses WHERE address = addr;
  IF addr_id IS NOT NULL THEN
    RETURN addr_id;
  END IF;

  -- Insert new address
  INSERT INTO addresses (address)
  VALUES (addr)
  ON CONFLICT (address) DO UPDATE SET address = EXCLUDED.address
  RETURNING id INTO addr_id;

  RETURN addr_id;
END;
$$ LANGUAGE plpgsql;

-- Address summary (for quick balance lookups) - Regular table, keyed by address
-- KEPT AS TEXT: Small table (~3M rows), direct lookups by address string
CREATE TABLE address_summary (
  address TEXT PRIMARY KEY,
  balance BIGINT NOT NULL DEFAULT 0,
  tx_count INTEGER NOT NULL DEFAULT 0,
  received_total BIGINT NOT NULL DEFAULT 0,
  sent_total BIGINT NOT NULL DEFAULT 0,
  unspent_count INTEGER NOT NULL DEFAULT 0,
  unconfirmed_balance BIGINT NOT NULL DEFAULT 0,
  first_seen INTEGER,
  last_activity INTEGER,
  cumulus_count INTEGER NOT NULL DEFAULT 0,
  nimbus_count INTEGER NOT NULL DEFAULT 0,
  stratus_count INTEGER NOT NULL DEFAULT 0,
  fluxnode_last_sync TIMESTAMPTZ,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_address_balance ON address_summary(balance DESC);
CREATE INDEX idx_address_activity ON address_summary(last_activity DESC);
CREATE INDEX idx_address_tx_count ON address_summary(tx_count DESC);
CREATE INDEX idx_address_fluxnodes ON address_summary((cumulus_count + nimbus_count + stratus_count) DESC)
  WHERE (cumulus_count + nimbus_count + stratus_count) > 0;

-- FluxNode producers (PoN specific) - Regular table
CREATE TABLE producers (
  fluxnode TEXT PRIMARY KEY,
  blocks_produced INTEGER DEFAULT 0,
  first_block INTEGER,
  last_block INTEGER,
  total_rewards BIGINT DEFAULT 0,
  avg_block_time DECIMAL(10, 2),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_producer_blocks ON producers(blocks_produced DESC);
CREATE INDEX idx_producer_last_block ON producers(last_block DESC);

-- Sync state (single row table tracking indexer progress)
CREATE TABLE sync_state (
  id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  current_height INTEGER NOT NULL DEFAULT 0,
  chain_height INTEGER NOT NULL DEFAULT 0,
  sync_percentage DECIMAL(5,2) DEFAULT 0,
  last_block_hash TEXT,
  last_sync_time TIMESTAMPTZ,
  is_syncing BOOLEAN DEFAULT false,
  sync_start_time TIMESTAMPTZ,
  blocks_per_second DECIMAL(10, 2),
  estimated_completion TIMESTAMPTZ
);

-- Initialize sync state (start from -1 so first sync fetches block 0)
INSERT INTO sync_state (id, current_height, chain_height) VALUES (1, -1, 0)
ON CONFLICT (id) DO NOTHING;

-- Reorg history (track chain reorganizations) - Regular table
CREATE TABLE reorgs (
  id SERIAL PRIMARY KEY,
  from_height INTEGER NOT NULL,
  to_height INTEGER NOT NULL,
  common_ancestor INTEGER NOT NULL,
  old_hash TEXT,
  new_hash TEXT,
  blocks_affected INTEGER,
  occurred_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_reorg_occurred_at ON reorgs(occurred_at DESC);

-- Schema migrations tracking (matches migrate.ts format)
CREATE TABLE schema_migrations (
  name TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ DEFAULT NOW()
);

-- Mark all migrations as applied since this schema already includes everything
INSERT INTO schema_migrations (name) VALUES
  ('001_incremental_address_summaries'),
  ('002_statement_level_triggers'),
  ('002_fix_bigint_overflow_in_batch_triggers'),
  ('003_fluxnode_transactions'),
  ('006_block_transaction_indexes'),
  ('007_address_transactions_cache'),
  ('008_rebuild_address_transactions_cache'),
  ('009_utxos_txid_index'),
  ('011_fully_shielded_transactions'),
  ('012_performance_indexes'),
  ('013_cursor_pagination_indexes'),
  ('014_address_transactions_tx_count_trigger'),
  ('015_add_fluxnode_counts'),
  ('016_phase3_address_normalization'),
  ('017_phase3_txid_bytea')
ON CONFLICT (name) DO NOTHING;

-- ============================================================================
-- COMPRESSION POLICIES (Auto-compress old data)
-- ============================================================================

-- Enable compression on hypertables
ALTER TABLE blocks SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'height DESC'
);

ALTER TABLE transactions SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'block_height DESC, txid'
);

-- PHASE 3: Segment by address_id instead of address
ALTER TABLE address_transactions SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'address_id',
  timescaledb.compress_orderby = 'block_height DESC, txid'
);

ALTER TABLE fluxnode_transactions SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'type, benchmark_tier',
  timescaledb.compress_orderby = 'block_height DESC, txid'
);

ALTER TABLE supply_stats SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'block_height DESC'
);

-- PHASE 3: Segment by address_id instead of address
ALTER TABLE utxos_spent SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'address_id',
  timescaledb.compress_orderby = 'spent_block_height DESC, txid'
);

-- Create integer_now function for compression policies
-- TimescaleDB requires this for integer-based partitioning to determine "current" value
-- (equivalent to now() for timestamp-based partitioning)
CREATE OR REPLACE FUNCTION current_block_height() RETURNS INTEGER
LANGUAGE SQL STABLE SET search_path = public
AS $$ SELECT COALESCE(MAX(height), 0)::INTEGER FROM blocks; $$;

-- Register integer_now function for all hypertables
SELECT set_integer_now_func('blocks', 'current_block_height');
SELECT set_integer_now_func('transactions', 'current_block_height');
SELECT set_integer_now_func('address_transactions', 'current_block_height');
SELECT set_integer_now_func('fluxnode_transactions', 'current_block_height');
SELECT set_integer_now_func('supply_stats', 'current_block_height');
SELECT set_integer_now_func('utxos_spent', 'current_block_height');

-- Add compression policies (compress chunks older than 100k blocks from current tip)
SELECT add_compression_policy('blocks', compress_after => 100000::integer);
SELECT add_compression_policy('transactions', compress_after => 100000::integer);
SELECT add_compression_policy('address_transactions', compress_after => 100000::integer);
SELECT add_compression_policy('fluxnode_transactions', compress_after => 100000::integer);
SELECT add_compression_policy('supply_stats', compress_after => 100000::integer);
-- NOTE: utxos_spent compression is DISABLED because:
-- 1. Expression indexes like encode(txid, 'hex') don't work efficiently on compressed chunks
-- 2. During sync, UTXOs are queried by txid through the utxos VIEW which uses encode()
-- 3. Compressed chunks require full decompression + seq scan, taking minutes per query
-- 4. This breaks sync performance completely
-- To enable compression after sync completes (for archival), manually run:
--   SELECT add_compression_policy('utxos_spent', compress_after => 100000::integer);
-- SELECT add_compression_policy('utxos_spent', compress_after => 100000::integer);

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Helper function to calculate address balance from UTXOs
-- PHASE 3: Uses address_id lookup
CREATE OR REPLACE FUNCTION calculate_address_balance(addr TEXT)
RETURNS BIGINT AS $$
DECLARE
  addr_id INTEGER;
BEGIN
  -- Get address_id
  SELECT id INTO addr_id FROM addresses WHERE address = addr;
  IF addr_id IS NULL THEN
    RETURN 0;
  END IF;

  RETURN COALESCE(
    (SELECT SUM(value) FROM utxos_unspent WHERE address_id = addr_id),
    0
  );
END;
$$ LANGUAGE plpgsql;

-- Helper function to update address summary
-- PHASE 3: Uses utxos VIEW which handles JOINs internally
CREATE OR REPLACE FUNCTION update_address_summary(addr TEXT)
RETURNS VOID AS $$
DECLARE
  summary_data RECORD;
  actual_tx_count BIGINT;
  addr_id INTEGER;
BEGIN
  -- Get address_id
  SELECT id INTO addr_id FROM addresses WHERE address = addr;
  IF addr_id IS NULL THEN
    RETURN;
  END IF;

  -- Calculate UTXO-based statistics using the VIEW
  SELECT
    COALESCE(SUM(CASE WHEN spent = false THEN value ELSE 0 END), 0) as balance,
    COALESCE(SUM(value), 0) as received,
    COALESCE(SUM(CASE WHEN spent = true THEN value ELSE 0 END), 0) as sent,
    COUNT(CASE WHEN spent = false THEN 1 END) as unspent_count,
    MIN(block_height) as first_seen,
    MAX(GREATEST(block_height, COALESCE(spent_block_height, 0))) as last_activity
  INTO summary_data
  FROM utxos
  WHERE address = addr;

  -- Get actual transaction count from address_transactions
  SELECT COUNT(*)
  INTO actual_tx_count
  FROM address_transactions
  WHERE address_id = addr_id;

  -- Upsert address summary
  INSERT INTO address_summary (
    address, balance, tx_count, received_total, sent_total,
    unspent_count, first_seen, last_activity, updated_at
  ) VALUES (
    addr,
    summary_data.balance,
    actual_tx_count,
    summary_data.received,
    summary_data.sent,
    summary_data.unspent_count,
    summary_data.first_seen,
    summary_data.last_activity,
    NOW()
  )
  ON CONFLICT (address) DO UPDATE SET
    balance = EXCLUDED.balance,
    tx_count = EXCLUDED.tx_count,
    received_total = EXCLUDED.received_total,
    sent_total = EXCLUDED.sent_total,
    unspent_count = EXCLUDED.unspent_count,
    first_seen = EXCLUDED.first_seen,
    last_activity = EXCLUDED.last_activity,
    updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE addresses IS 'Address normalization table - maps address strings to integer IDs (Phase 3 optimization)';
COMMENT ON TABLE blocks IS 'Indexed blockchain blocks (TimescaleDB hypertable)';
COMMENT ON TABLE transactions IS 'All blockchain transactions with BYTEA txid (TimescaleDB hypertable)';
COMMENT ON TABLE address_transactions IS 'Address transaction history with normalized address_id and BYTEA txid (TimescaleDB hypertable)';
COMMENT ON TABLE fluxnode_transactions IS 'FluxNode-related transactions with BYTEA txid (TimescaleDB hypertable)';
COMMENT ON TABLE supply_stats IS 'Per-block supply statistics (TimescaleDB hypertable)';
COMMENT ON TABLE utxos_unspent IS 'Unspent UTXOs with normalized address_id and BYTEA txid';
COMMENT ON TABLE utxos_spent IS 'Spent UTXOs with normalized address_id and BYTEA txid/spent_txid (TimescaleDB hypertable)';
COMMENT ON VIEW utxos IS 'Unified view of unspent and spent UTXOs with address strings and hex txids';
COMMENT ON TABLE address_summary IS 'Cached address balances and statistics (TEXT address for direct lookups)';
COMMENT ON TABLE producers IS 'FluxNode block producers (PoN consensus)';
COMMENT ON TABLE sync_state IS 'Indexer synchronization state';
