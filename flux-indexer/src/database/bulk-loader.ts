/**
 * ClickHouse Bulk Loader - ULTIMATE EDITION
 *
 * High-performance bulk insert operations optimized for blockchain indexing.
 * Features:
 *   - Parallel inserts for independent tables
 *   - Optimized data transformation
 *   - Type conversion for ClickHouse columnar storage
 *   - PREWHERE-friendly data ordering
 */

import { ClickHouseConnection } from './connection';

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Pad a hash to exactly 64 characters (for FixedString(64))
 */
function padHash(hash: string | null | undefined): string {
  if (!hash) return '0'.repeat(64);
  return hash.padStart(64, '0').slice(0, 64);
}

/**
 * Convert a value to BigInt string safely (ClickHouse can't serialize native BigInt)
 */
function toBigIntString(value: string | number | bigint | null | undefined): string {
  if (value === null || value === undefined) return '0';
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'number') return Math.round(value).toString();
  try {
    return BigInt(value).toString();
  } catch {
    return '0';
  }
}

/**
 * Get current timestamp in milliseconds for versioning
 * Returns as number for UInt64 column type
 */
function getVersion(): number {
  return Date.now();
}

// ============================================================================
// Parallel Insert Helpers
// ============================================================================

/**
 * Execute multiple inserts in parallel for independent tables
 * This significantly speeds up batch processing
 */
export async function parallelInserts(
  operations: Array<() => Promise<any>>
): Promise<void> {
  await Promise.all(operations.map(op => op()));
}

// ============================================================================
// Block Operations
// ============================================================================

export interface BlockInsert {
  height: number;
  hash: string;
  prevHash: string | null;
  merkleRoot: string | null;
  timestamp: number;
  bits: string | null;
  nonce: string | null;
  version: number | null;
  size: number | null;
  txCount: number;
  producer: string | null;
  producerReward: string | bigint | null;
  difficulty: number | null;
  chainwork: string | null;
}

export async function bulkInsertBlocks(
  ch: ClickHouseConnection,
  blocks: BlockInsert[]
): Promise<number> {
  if (blocks.length === 0) return 0;

  const version = getVersion();
  const rows = blocks.map((b) => ({
    height: b.height,
    hash: padHash(b.hash),
    prev_hash: padHash(b.prevHash),
    merkle_root: padHash(b.merkleRoot),
    timestamp: b.timestamp,
    bits: b.bits || '',
    nonce: b.nonce || '',
    version: b.version || 0,
    size: b.size || 0,
    tx_count: b.txCount,
    producer: b.producer || '',
    producer_reward: toBigIntString(b.producerReward),
    difficulty: b.difficulty || 0,
    chainwork: b.chainwork || '',
    is_valid: 1,
    _version: version,
  }));

  await ch.insert('blocks', rows);
  return blocks.length;
}

// ============================================================================
// Transaction Operations
// ============================================================================

export interface TransactionInsert {
  txid: string;
  blockHeight: number;
  txIndex?: number;
  timestamp: number;
  version: number;
  locktime: number;
  size: number;
  vsize: number;
  inputCount: number;
  outputCount: number;
  inputTotal: string | bigint;
  outputTotal: string | bigint;
  fee: string | bigint;
  isCoinbase: boolean;
  isFluxnodeTx?: boolean;
  fluxnodeType?: number | null;
  isShielded?: boolean;
}

export async function bulkInsertTransactions(
  ch: ClickHouseConnection,
  transactions: TransactionInsert[]
): Promise<number> {
  if (transactions.length === 0) return 0;

  const version = getVersion();
  const txRows = transactions.map((tx, idx) => ({
    txid: padHash(tx.txid),
    block_height: tx.blockHeight,
    tx_index: tx.txIndex ?? idx,
    timestamp: tx.timestamp,
    version: tx.version,
    locktime: tx.locktime,
    size: tx.size,
    vsize: tx.vsize,
    input_count: tx.inputCount,
    output_count: tx.outputCount,
    input_total: toBigIntString(tx.inputTotal),
    output_total: toBigIntString(tx.outputTotal),
    fee: toBigIntString(tx.fee),
    is_coinbase: tx.isCoinbase ? 1 : 0,
    is_fluxnode_tx: tx.isFluxnodeTx ? 1 : 0,
    fluxnode_type: tx.isFluxnodeTx ? (tx.fluxnodeType ?? null) : null,  // NULL when not a fluxnode tx
    is_shielded: tx.isShielded ? 1 : 0,
    is_valid: 1,
    _version: version,
  }));

  // Insert transactions
  // NOTE: tx_lookup table removed - bloom filter index on txid is sufficient
  await ch.insert('transactions', txRows);

  return transactions.length;
}

// ============================================================================
// UTXO Operations
// ============================================================================

export interface UtxoInsert {
  txid: string;
  vout: number;
  address: string;
  value: string | bigint;
  scriptPubkey?: string;
  scriptType: string;
  blockHeight: number;
}

export async function bulkInsertUtxos(
  ch: ClickHouseConnection,
  utxos: UtxoInsert[]
): Promise<number> {
  if (utxos.length === 0) return 0;

  const version = getVersion();
  const rows = utxos.map((u) => ({
    txid: padHash(u.txid),
    vout: u.vout,
    address: u.address || 'UNKNOWN',
    value: toBigIntString(u.value),
    script_pubkey: u.scriptPubkey || '',
    script_type: u.scriptType || 'unknown',
    block_height: u.blockHeight,
    spent: 0,
    spent_txid: padHash(null),
    spent_block_height: 0,
    version: version.toString(),
  }));

  await ch.insert('utxos', rows);
  return utxos.length;
}

export interface UtxoSpend {
  txid: string;
  vout: number;
  spentTxid: string;
  spentBlockHeight: number;
}

export interface ExistingUtxo {
  address: string;
  value: bigint;
  scriptPubkey?: string;
  scriptType: string;
  blockHeight: number;
}

/**
 * Mark UTXOs as spent by inserting new rows with spent=1
 * ReplacingMergeTree will merge based on the version column
 */
export async function bulkSpendUtxos(
  ch: ClickHouseConnection,
  existingUtxos: Map<string, ExistingUtxo>,
  spends: UtxoSpend[]
): Promise<number> {
  if (spends.length === 0) return 0;

  const version = getVersion();
  const rows = spends.map((s) => {
    const key = `${s.txid}:${s.vout}`;
    const existing = existingUtxos.get(key);

    return {
      txid: padHash(s.txid),
      vout: s.vout,
      address: existing?.address || 'UNKNOWN',
      value: existing?.value?.toString() || '0',
      script_pubkey: existing?.scriptPubkey || '',
      script_type: existing?.scriptType || 'unknown',
      block_height: existing?.blockHeight || 0,
      spent: 1,
      spent_txid: padHash(s.spentTxid),
      spent_block_height: s.spentBlockHeight,
      version: version.toString(),
    };
  });

  await ch.insert('utxos', rows);
  return spends.length;
}

/**
 * Fetch existing UTXOs for a batch of outpoints
 */
export async function fetchExistingUtxos(
  ch: ClickHouseConnection,
  outpoints: Array<{ txid: string; vout: number }>
): Promise<Map<string, ExistingUtxo>> {
  if (outpoints.length === 0) return new Map();

  const map = new Map<string, ExistingUtxo>();

  // Process in batches to avoid query size limits
  const BATCH_SIZE = 500;

  for (let i = 0; i < outpoints.length; i += BATCH_SIZE) {
    const batch = outpoints.slice(i, i + BATCH_SIZE);

    // Build the IN clause values with proper escaping
    // txid should be a 64-char hex string - validate and sanitize
    const inValues = batch
      .map((o) => {
        // Ensure txid is only hex characters
        const safeTxid = padHash(o.txid).replace(/[^0-9a-fA-F]/g, '0');
        return `('${safeTxid}', ${Math.floor(o.vout)})`;
      })
      .join(', ');

    try {
      const results = await ch.query<{
        txid: string;
        vout: number;
        address: string;
        value: string;
        script_pubkey: string;
        script_type: string;
        block_height: number;
      }>(`
        SELECT txid, vout, address, value, script_pubkey, script_type, block_height
        FROM utxos FINAL
        WHERE (txid, vout) IN (${inValues})
          AND spent = 0
      `);

      for (const row of results) {
        const key = `${row.txid}:${row.vout}`;
        map.set(key, {
          address: row.address,
          value: BigInt(row.value),
          scriptPubkey: row.script_pubkey,
          scriptType: row.script_type,
          blockHeight: row.block_height,
        });
      }
    } catch (error: any) {
      // Log but continue - missing UTXOs will be handled gracefully
      console.error('Failed to fetch UTXO batch:', error.message);
    }
  }

  return map;
}

// ============================================================================
// Address Transaction Operations
// ============================================================================

export interface AddressTransactionInsert {
  address: string;
  txid: string;
  blockHeight: number;
  blockHash: string;           // Denormalized for fast queries - no JOINs needed
  txIndex: number;             // Position in block for proper ordering
  timestamp: number;
  direction?: 'received' | 'sent';  // Computed if not provided
  received: string | bigint;
  sent: string | bigint;
  isCoinbase?: boolean;
}

export async function bulkInsertAddressTransactions(
  ch: ClickHouseConnection,
  records: AddressTransactionInsert[]
): Promise<number> {
  if (records.length === 0) return 0;

  const version = getVersion();
  const rows = records.map((r) => {
    const receivedVal = toBigIntString(r.received);
    const sentVal = toBigIntString(r.sent);
    // Compute direction if not provided
    const direction = r.direction || (BigInt(receivedVal) >= BigInt(sentVal) ? 'received' : 'sent');

    return {
      address: r.address,
      txid: padHash(r.txid),
      block_height: r.blockHeight,
      block_hash: padHash(r.blockHash),
      tx_index: r.txIndex,
      timestamp: r.timestamp,
      direction,
      received_value: receivedVal,
      sent_value: sentVal,
      is_coinbase: r.isCoinbase ? 1 : 0,
      is_valid: 1,
      _version: version,
    };
  });

  await ch.insert('address_transactions', rows);
  return records.length;
}

// ============================================================================
// Address Summary Operations
// ============================================================================

export interface AddressSummaryUpdate {
  address: string;
  balance: bigint;
  txCount: number;
  receivedTotal: bigint;
  sentTotal: bigint;
  unspentCount: number;
  firstSeen: number;
  lastActivity: number;
  cumulusCount?: number;
  nimbusCount?: number;
  stratusCount?: number;
}

export async function bulkUpdateAddressSummary(
  ch: ClickHouseConnection,
  updates: AddressSummaryUpdate[]
): Promise<number> {
  if (updates.length === 0) return 0;

  // With SummingMergeTree, we just insert DELTA values
  // ClickHouse automatically sums the numeric columns on merge
  // No need to fetch existing data - this works perfectly with async inserts!
  const rows = updates.map((u) => ({
    address: u.address,
    // These columns are summed by SummingMergeTree
    balance: u.balance.toString(),
    tx_count: u.txCount,
    received_total: u.receivedTotal.toString(),
    sent_total: u.sentTotal.toString(),
    unspent_count: u.unspentCount,
    // These use SimpleAggregateFunction(min/max) - just insert the value
    first_seen: u.firstSeen,
    last_activity: u.lastActivity,
    // FluxNode counts use SimpleAggregateFunction(max)
    cumulus_count: u.cumulusCount || 0,
    nimbus_count: u.nimbusCount || 0,
    stratus_count: u.stratusCount || 0,
  }));

  await ch.insert('address_summary', rows);
  return updates.length;
}

// ============================================================================
// Supply Stats Operations
// ============================================================================

export interface SupplyStatsInsert {
  blockHeight: number;
  timestamp: number;              // Block timestamp for accurate date grouping
  transparentSupply: bigint;
  shieldedPool: bigint;
  totalSupply: bigint;
}

export async function bulkInsertSupplyStats(
  ch: ClickHouseConnection,
  stats: SupplyStatsInsert[]
): Promise<number> {
  if (stats.length === 0) return 0;

  const version = getVersion();
  const rows = stats.map((s) => ({
    block_height: s.blockHeight,
    timestamp: s.timestamp,
    transparent_supply: s.transparentSupply.toString(),
    shielded_pool: s.shieldedPool.toString(),
    total_supply: s.totalSupply.toString(),
    _version: version,
  }));

  await ch.insert('supply_stats', rows);
  return stats.length;
}

// ============================================================================
// FluxNode Transaction Operations
// ============================================================================

export interface FluxnodeTransactionInsert {
  txid: string;
  blockHeight: number;
  blockTime: Date;
  version: number;
  type: number;
  collateralHash: string | null;
  collateralIndex: number | null;
  ipAddress: string | null;
  publicKey: string | null;
  signature: string | null;
  p2shAddress: string | null;
  benchmarkTier: string | null;
  extraData: string | null;
}

export async function bulkInsertFluxnodeTransactions(
  ch: ClickHouseConnection,
  transactions: FluxnodeTransactionInsert[],
  options?: { sync?: boolean }
): Promise<number> {
  if (transactions.length === 0) return 0;

  const version = getVersion();
  const rows = transactions.map((tx) => ({
    txid: padHash(tx.txid),
    block_height: tx.blockHeight,
    block_time: Math.floor(tx.blockTime.getTime() / 1000),
    version: tx.version,
    type: tx.type,
    collateral_hash: tx.collateralHash || '',
    collateral_index: tx.collateralIndex || 0,
    ip_address: tx.ipAddress || '',
    public_key: tx.publicKey || '',
    signature: tx.signature || '',
    p2sh_address: tx.p2shAddress || '',
    benchmark_tier: tx.benchmarkTier || '',
    extra_data: tx.extraData || '',
    is_valid: 1,
    _version: version,
  }));

  // Use sync insert when near chain tip for immediate visibility
  // Async insert during historical sync for performance
  if (options?.sync) {
    await ch.syncInsert('fluxnode_transactions', rows);
  } else {
    await ch.insert('fluxnode_transactions', rows);
  }
  return transactions.length;
}

// ============================================================================
// Producer Operations
// ============================================================================

export interface ProducerUpdate {
  fluxnode: string;
  blocksProduced: number;
  firstBlock: number;
  lastBlock: number;
  totalRewards: bigint;
  avgBlockTime: number;
}

export async function bulkUpdateProducers(
  ch: ClickHouseConnection,
  producers: ProducerUpdate[]
): Promise<number> {
  if (producers.length === 0) return 0;

  const rows = producers.map((p) => ({
    fluxnode: p.fluxnode,
    blocks_produced: p.blocksProduced,
    first_block: p.firstBlock,
    last_block: p.lastBlock,
    total_rewards: p.totalRewards.toString(),
    avg_block_time: p.avgBlockTime,
  }));

  await ch.insert('producers', rows);
  return producers.length;
}

// ============================================================================
// Sync State Operations
// ============================================================================

export interface SyncStateUpdate {
  currentHeight: number;
  chainHeight: number;
  syncPercentage: number;
  lastBlockHash: string;
  isSyncing: boolean;
  blocksPerSecond: number;
}

export async function updateSyncState(
  ch: ClickHouseConnection,
  state: SyncStateUpdate
): Promise<void> {
  // Use synchronous INSERT command instead of async insert
  // This ensures sync_state is immediately readable for reorg checks
  const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
  const hash = padHash(state.lastBlockHash);
  const isSyncing = state.isSyncing ? 1 : 0;

  await ch.command(`
    INSERT INTO sync_state (id, current_height, chain_height, sync_percentage, last_block_hash, last_sync_time, is_syncing, blocks_per_second)
    VALUES (1, ${state.currentHeight}, ${state.chainHeight}, ${state.syncPercentage}, '${hash}', '${now}', ${isSyncing}, ${state.blocksPerSecond})
  `);
}

export async function getSyncState(
  ch: ClickHouseConnection
): Promise<SyncStateUpdate | null> {
  // Use argMax to get the row with the latest updated_at
  // This is more reliable than FINAL which may return stale data from unmerged parts
  const result = await ch.queryOne<{
    current_height: number;
    chain_height: number;
    sync_percentage: number;
    last_block_hash: string;
    is_syncing: number;
    blocks_per_second: number;
  }>(`
    SELECT
      argMax(current_height, updated_at) as current_height,
      argMax(chain_height, updated_at) as chain_height,
      argMax(sync_percentage, updated_at) as sync_percentage,
      argMax(last_block_hash, updated_at) as last_block_hash,
      argMax(is_syncing, updated_at) as is_syncing,
      argMax(blocks_per_second, updated_at) as blocks_per_second
    FROM sync_state
    WHERE id = 1
  `);

  if (!result) return null;

  return {
    currentHeight: result.current_height,
    chainHeight: result.chain_height,
    syncPercentage: result.sync_percentage,
    lastBlockHash: result.last_block_hash,
    isSyncing: result.is_syncing === 1,
    blocksPerSecond: result.blocks_per_second,
  };
}

// ============================================================================
// Reorg Operations
// ============================================================================

export interface ReorgRecord {
  fromHeight: number;
  toHeight: number;
  commonAncestor: number;
  oldHash: string;
  newHash: string;
  blocksAffected: number;
}

export async function recordReorg(
  ch: ClickHouseConnection,
  reorg: ReorgRecord
): Promise<void> {
  await ch.insert('reorgs', [
    {
      id: Date.now(),
      from_height: reorg.fromHeight,
      to_height: reorg.toHeight,
      common_ancestor: reorg.commonAncestor,
      old_hash: padHash(reorg.oldHash),
      new_hash: padHash(reorg.newHash),
      blocks_affected: reorg.blocksAffected,
    },
  ]);
}

/**
 * Invalidate data for a reorg by setting is_valid = 0
 * Uses ALTER TABLE UPDATE which creates a mutation
 */
export async function invalidateFromHeight(
  ch: ClickHouseConnection,
  fromHeight: number
): Promise<void> {
  // Invalidate blocks
  await ch.command(`
    ALTER TABLE blocks UPDATE is_valid = 0
    WHERE height >= ${fromHeight} AND is_valid = 1
  `);

  // Invalidate transactions
  await ch.command(`
    ALTER TABLE transactions UPDATE is_valid = 0
    WHERE block_height >= ${fromHeight} AND is_valid = 1
  `);

  // Invalidate address_transactions
  await ch.command(`
    ALTER TABLE address_transactions UPDATE is_valid = 0
    WHERE block_height >= ${fromHeight} AND is_valid = 1
  `);

  // Invalidate fluxnode_transactions
  await ch.command(`
    ALTER TABLE fluxnode_transactions UPDATE is_valid = 0
    WHERE block_height >= ${fromHeight} AND is_valid = 1
  `);

  // For UTXOs, we need to handle both created and spent UTXOs
  // Mark UTXOs created in rolled-back blocks as having 0 value
  const version = getVersion();

  // Get UTXOs created in the reorged range
  const createdUtxos = await ch.query<{ txid: string; vout: number }>(`
    SELECT txid, vout FROM utxos FINAL
    WHERE block_height >= ${fromHeight}
  `);

  if (createdUtxos.length > 0) {
    // Insert with value=0 to effectively remove them
    const invalidRows = createdUtxos.map((u) => ({
      txid: u.txid,
      vout: u.vout,
      address: '',
      value: '0',
      script_type: 'invalidated',
      block_height: 0,
      spent: 1,
      spent_txid: padHash(null),
      spent_block_height: 0,
      version: version.toString(),
    }));
    await ch.insert('utxos', invalidRows);
  }

  // "Unspend" UTXOs that were spent in rolled-back blocks
  const spentInReorg = await ch.query<{
    txid: string;
    vout: number;
    address: string;
    value: string;
    script_type: string;
    block_height: number;
  }>(`
    SELECT txid, vout, address, value, script_type, block_height
    FROM utxos FINAL
    WHERE spent_block_height >= ${fromHeight} AND spent = 1
  `);

  if (spentInReorg.length > 0) {
    const unspendRows = spentInReorg.map((u) => ({
      txid: u.txid,
      vout: u.vout,
      address: u.address,
      value: u.value,  // Already a string from query
      script_type: u.script_type,
      block_height: u.block_height,
      spent: 0,
      spent_txid: padHash(null),
      spent_block_height: 0,
      version: version.toString(),
    }));
    await ch.insert('utxos', unspendRows);
  }

  // Wait for mutations to complete
  await ch.waitForMutations();
}
