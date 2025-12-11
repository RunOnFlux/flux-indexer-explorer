/**
 * Bulk Loader using PostgreSQL COPY
 *
 * Uses COPY FROM STDIN for extremely fast bulk inserts.
 * Can be 10-100x faster than individual INSERT statements.
 *
 * PHASE 3 OPTIMIZATIONS:
 * - BYTEA txid support (32 bytes vs 64 char TEXT)
 * - Address ID support (INTEGER vs TEXT)
 */

import { Pool, PoolClient } from 'pg';
import { from as copyFrom } from 'pg-copy-streams';
import { Readable } from 'stream';
import { logger } from '../utils/logger';

/**
 * Column type hint for COPY format
 */
type ColumnType = 'text' | 'bytea' | 'integer' | 'bigint' | 'boolean' | 'timestamp';

/**
 * Sanitize a string to valid UTF-8 by removing invalid byte sequences
 * This prevents "invalid byte sequence for encoding UTF8" errors in PostgreSQL
 * AGGRESSIVE MODE: Only allows ASCII printable characters (0x20-0x7E) plus common whitespace
 * This is safe for blockchain data (hex strings, addresses, type names)
 * EXPORTED for use in block-indexer.ts and other modules
 */
export function sanitizeUtf8(str: string): string {
  if (str === null || str === undefined) {
    return '';
  }

  // Convert to string if not already
  const input = String(str);

  // AGGRESSIVE: Only allow ASCII printable characters (space through tilde)
  // This removes ALL bytes outside 0x20-0x7E range
  // Safe for blockchain data: hex strings, addresses, type names are all ASCII
  let sanitized = '';
  for (let i = 0; i < input.length; i++) {
    const code = input.charCodeAt(i);
    // Only keep ASCII printable (0x20-0x7E = 32-126)
    if (code >= 0x20 && code <= 0x7E) {
      sanitized += input[i];
    }
  }

  return sanitized;
}

/**
 * Sanitize a hex string to contain only valid hex characters
 * EXPORTED for use in block-indexer.ts
 */
export function sanitizeHex(str: string): string {
  return str.replace(/[^0-9a-fA-F]/g, '');
}

/**
 * Escape a value for COPY format (tab-separated)
 * NULL values become \N, tabs and newlines are escaped
 * BYTEA values use \x prefix (hex format)
 */
function escapeCopyValue(value: any, type?: ColumnType): string {
  if (value === null || value === undefined) {
    return '\\N';
  }

  // BYTEA: Use escape format for COPY TEXT mode
  // In COPY TEXT format, \xNN is interpreted as a 2-digit hex escape!
  // So \x94c7... is parsed as byte 0x94 + text "c7..."
  // To send the full hex-encoded BYTEA, use double backslash: \\x
  // This sends literal \x followed by hex digits, which PostgreSQL interprets as BYTEA hex format
  if (type === 'bytea') {
    // Value should be a hex string - sanitize to remove any non-hex characters
    const hexStr = sanitizeHex(String(value));
    // Double backslash produces literal \x in COPY stream, which is the BYTEA hex format marker
    return `\\\\x${hexStr}`;
  }

  let str = String(value);

  // Sanitize to valid UTF-8 (removes control chars and invalid sequences)
  str = sanitizeUtf8(str);

  // Escape backslashes, tabs, newlines, and carriage returns for COPY format
  return str
    .replace(/\\/g, '\\\\')
    .replace(/\t/g, '\\t')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}

/**
 * Create a tab-separated row from values with optional type hints
 */
function createCopyRow(values: any[], types?: ColumnType[]): string {
  const formatted = values.map((value, i) => escapeCopyValue(value, types?.[i]));
  return formatted.join('\t') + '\n';
}

/**
 * Bulk insert using COPY FROM STDIN
 * Returns the number of rows inserted
 */
export async function bulkCopy(
  client: PoolClient,
  tableName: string,
  columns: string[],
  rows: any[][],
  types?: ColumnType[]
): Promise<number> {
  if (rows.length === 0) {
    return 0;
  }

  const rowCount = rows.length;

  return new Promise((resolve, reject) => {
    const copyQuery = `COPY ${tableName} (${columns.join(', ')}) FROM STDIN`;
    const stream = client.query(copyFrom(copyQuery));

    // Use once() instead of on() to auto-remove listeners and avoid memory leaks
    const onError = (err: Error) => {
      stream.removeListener('finish', onFinish);
      logger.error('COPY stream error', { table: tableName, error: err.message });
      reject(err);
    };

    const onFinish = () => {
      stream.removeListener('error', onError);
      resolve(rowCount);
    };

    stream.once('error', onError);
    stream.once('finish', onFinish);

    // Write all rows - avoid closure capture by iterating with index
    // Use Buffer.from() to explicitly encode as UTF-8 to avoid encoding issues
    for (let i = 0; i < rows.length; i++) {
      const row = createCopyRow(rows[i], types);
      // Convert to UTF-8 buffer explicitly - this ensures consistent encoding
      const buf = Buffer.from(row, 'utf8');
      stream.write(buf);
    }

    stream.end();
  });
}

/**
 * Bulk insert with conflict handling using temp table + INSERT ... ON CONFLICT
 * Use this when you need upsert behavior
 */
export async function bulkUpsert(
  client: PoolClient,
  tableName: string,
  columns: string[],
  rows: any[][],
  conflictColumns: string[],
  updateColumns?: string[],
  types?: ColumnType[]
): Promise<number> {
  if (rows.length === 0) {
    return 0;
  }

  const tempTable = `temp_bulk_${tableName.replace(/\./g, '_')}_${Date.now()}`;

  try {
    // Create temp table with same structure
    await client.query(`
      CREATE TEMP TABLE ${tempTable} (LIKE ${tableName} INCLUDING DEFAULTS)
      ON COMMIT DROP
    `);

    // COPY data to temp table
    await bulkCopy(client, tempTable, columns, rows, types);

    // Upsert from temp table to real table
    const conflictClause = conflictColumns.join(', ');
    const updateClause = updateColumns && updateColumns.length > 0
      ? `DO UPDATE SET ${updateColumns.map(col => `${col} = EXCLUDED.${col}`).join(', ')}`
      : 'DO NOTHING';

    const result = await client.query(`
      INSERT INTO ${tableName} (${columns.join(', ')})
      SELECT ${columns.join(', ')} FROM ${tempTable}
      ON CONFLICT (${conflictClause}) ${updateClause}
    `);

    return result.rowCount || rows.length;
  } finally {
    // Temp table is dropped on commit, but clean up just in case
    await client.query(`DROP TABLE IF EXISTS ${tempTable}`).catch(() => {});
  }
}

/**
 * Bulk create addresses and return a map of address -> id
 * Uses INSERT ... ON CONFLICT to handle existing addresses efficiently
 */
export async function bulkCreateAddresses(
  client: PoolClient,
  addresses: string[],
  firstSeenHeight?: number
): Promise<Map<string, number>> {
  if (addresses.length === 0) {
    return new Map();
  }

  // Use unnest for efficient bulk insert
  const result = await client.query(`
    INSERT INTO addresses (address, first_seen)
    SELECT addr, $2
    FROM unnest($1::text[]) AS addr
    ON CONFLICT (address) DO UPDATE SET address = EXCLUDED.address
    RETURNING id, address
  `, [addresses, firstSeenHeight ?? null]);

  const idMap = new Map<string, number>();
  for (const row of result.rows) {
    idMap.set(row.address, row.id);
  }
  return idMap;
}

/**
 * Get address IDs for a list of addresses (existing only, no creation)
 */
export async function getAddressIds(
  client: PoolClient,
  addresses: string[]
): Promise<Map<string, number>> {
  if (addresses.length === 0) {
    return new Map();
  }

  const result = await client.query(`
    SELECT id, address FROM addresses WHERE address = ANY($1)
  `, [addresses]);

  const idMap = new Map<string, number>();
  for (const row of result.rows) {
    idMap.set(row.address, row.id);
  }
  return idMap;
}

/**
 * Bulk insert blocks using COPY
 */
export async function bulkInsertBlocks(
  client: PoolClient,
  blocks: Array<{
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
    producerReward: string | null;
    difficulty: number | null;
    chainwork: string | null;
  }>
): Promise<number> {
  const columns = [
    'height', 'hash', 'prev_hash', 'merkle_root', 'timestamp', 'bits', 'nonce',
    'version', 'size', 'tx_count', 'producer', 'producer_reward', 'difficulty', 'chainwork'
  ];
  const rows = blocks.map(b => [
    b.height, b.hash, b.prevHash, b.merkleRoot, b.timestamp, b.bits, b.nonce,
    b.version, b.size, b.txCount, b.producer, b.producerReward, b.difficulty, b.chainwork
  ]);

  return bulkUpsert(client, 'blocks', columns, rows, ['height'], [
    'hash', 'prev_hash', 'merkle_root', 'timestamp', 'bits', 'nonce',
    'version', 'size', 'tx_count', 'producer', 'producer_reward', 'difficulty', 'chainwork'
  ]);
}

/**
 * Bulk insert transactions using COPY
 * PHASE 3: txid is now BYTEA (pass 64-char hex string, stored as 32 bytes)
 */
export async function bulkInsertTransactions(
  client: PoolClient,
  transactions: Array<{
    txid: string;  // 64-char hex string, will be stored as BYTEA
    blockHeight: number;
    timestamp: number;
    version: number;
    locktime: number;
    size: number;
    vsize: number;
    inputCount: number;
    outputCount: number;
    inputTotal: string;
    outputTotal: string;
    fee: string;
    isCoinbase: boolean;
    hex: string | null;
    isFluxnodeTx?: boolean;
    fluxnodeType?: number | null;
  }>
): Promise<number> {
  const columns = [
    'txid', 'block_height', 'timestamp', 'version', 'locktime',
    'size', 'vsize', 'input_count', 'output_count', 'input_total', 'output_total',
    'fee', 'is_coinbase', 'hex', 'is_fluxnode_tx', 'fluxnode_type'
  ];

  // Column types for COPY format (txid is BYTEA)
  const types: ColumnType[] = [
    'bytea', 'integer', 'integer', 'integer', 'bigint',
    'integer', 'integer', 'integer', 'integer', 'bigint', 'bigint',
    'bigint', 'boolean', 'text', 'boolean', 'integer'
  ];

  const rows = transactions.map(tx => [
    tx.txid,
    tx.blockHeight,
    tx.timestamp,
    tx.version,
    tx.locktime,
    tx.size,
    tx.vsize,
    tx.inputCount,
    tx.outputCount,
    tx.inputTotal,
    tx.outputTotal,
    tx.fee,
    tx.isCoinbase,
    tx.hex,
    tx.isFluxnodeTx ?? false,
    tx.fluxnodeType ?? null
  ]);

  const result = await bulkUpsert(
    client,
    'transactions',
    columns,
    rows,
    ['block_height', 'txid'],
    ['timestamp'],
    types
  );

  // Also insert into tx_lookup for fast txid -> block_height lookups
  // This enables efficient TimescaleDB chunk exclusion when querying by txid
  if (transactions.length > 0) {
    const lookupRows = transactions.map(tx => [tx.txid, tx.blockHeight]);
    await bulkUpsert(
      client,
      'tx_lookup',
      ['txid', 'block_height'],
      lookupRows,
      ['txid'],  // conflict on txid
      [],        // no updates on conflict
      ['bytea', 'integer']
    );
  }

  return result;
}

/**
 * Bulk insert FluxNode transactions using COPY
 * PHASE 3: txid is now BYTEA
 */
export async function bulkInsertFluxnodeTransactions(
  client: PoolClient,
  transactions: Array<{
    txid: string;  // 64-char hex string
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
  }>
): Promise<number> {
  if (transactions.length === 0) return 0;

  const columns = [
    'txid', 'block_height', 'block_time', 'version', 'type',
    'collateral_hash', 'collateral_index', 'ip_address', 'public_key', 'signature',
    'p2sh_address', 'benchmark_tier', 'extra_data'
  ];

  // Column types (txid is BYTEA)
  const types: ColumnType[] = [
    'bytea', 'integer', 'timestamp', 'integer', 'integer',
    'text', 'integer', 'text', 'text', 'text',
    'text', 'text', 'text'
  ];

  const rows = transactions.map(tx => [
    tx.txid,
    tx.blockHeight,
    tx.blockTime.toISOString(),
    tx.version,
    tx.type,
    tx.collateralHash,
    tx.collateralIndex,
    tx.ipAddress,
    tx.publicKey,
    tx.signature,
    tx.p2shAddress,
    tx.benchmarkTier,
    tx.extraData,
  ]);

  return bulkUpsert(
    client,
    'fluxnode_transactions',
    columns,
    rows,
    ['block_height', 'txid'],
    ['block_time', 'type', 'collateral_hash', 'collateral_index',
     'ip_address', 'public_key', 'signature', 'p2sh_address', 'benchmark_tier', 'extra_data'],
    types
  );
}

/**
 * Bulk insert address transactions using COPY
 * PHASE 3: Uses address_id (INTEGER) and txid (BYTEA)
 */
export async function bulkInsertAddressTransactions(
  client: PoolClient,
  records: Array<{
    addressId: number;  // PHASE 3: Changed from address string
    txid: string;  // 64-char hex string
    blockHeight: number;
    timestamp: number;
    received: string;
    sent: string;
  }>
): Promise<number> {
  if (records.length === 0) return 0;

  const columns = [
    'address_id', 'txid', 'block_height', 'timestamp',
    'direction', 'received_value', 'sent_value'
  ];

  // Column types (address_id is integer, txid is BYTEA)
  const types: ColumnType[] = [
    'integer', 'bytea', 'integer', 'integer',
    'text', 'bigint', 'bigint'
  ];

  const rows = records.map(r => {
    const received = BigInt(r.received);
    const sent = BigInt(r.sent);
    const direction = received >= sent ? 'received' : 'sent';
    return [
      r.addressId,
      r.txid,
      r.blockHeight,
      r.timestamp,
      direction,
      r.received,
      r.sent
    ];
  });

  return bulkUpsert(
    client,
    'address_transactions',
    columns,
    rows,
    ['block_height', 'address_id', 'txid'],
    ['timestamp', 'direction', 'received_value', 'sent_value'],
    types
  );
}

/**
 * Bulk insert UTXOs into utxos_unspent table
 * PHASE 3: Uses address_id (INTEGER) and txid (BYTEA)
 */
export async function bulkInsertUtxos(
  client: PoolClient,
  utxos: Array<{
    txid: string;  // 64-char hex string
    vout: number;
    addressId: number;  // PHASE 3: Changed from address string
    value: string;
    scriptPubkey: string;
    scriptType: string;
    blockHeight: number;
  }>
): Promise<number> {
  const columns = ['txid', 'vout', 'address_id', 'value', 'script_pubkey', 'script_type', 'block_height'];

  // Column types (txid is BYTEA, address_id is integer)
  const types: ColumnType[] = [
    'bytea', 'integer', 'integer', 'bigint', 'text', 'text', 'integer'
  ];

  const rows = utxos.map(u => [
    u.txid,
    u.vout,
    u.addressId,
    u.value,
    u.scriptPubkey,
    u.scriptType,
    u.blockHeight
  ]);

  return bulkUpsert(
    client,
    'utxos_unspent',
    columns,
    rows,
    ['txid', 'vout'],
    ['address_id', 'value', 'script_pubkey', 'script_type', 'block_height'],
    types
  );
}

/**
 * Bulk mark UTXOs as spent by moving them from utxos_unspent to utxos_spent
 * PHASE 3: Uses BYTEA for txid and spent_txid
 */
export async function bulkSpendUtxos(
  client: PoolClient,
  spends: Array<{
    txid: string;  // 64-char hex string
    vout: number;
    spentTxid: string;  // 64-char hex string
    spentBlockHeight: number;
  }>
): Promise<number> {
  if (spends.length === 0) {
    return 0;
  }

  const tempTable = `temp_spent_${Date.now()}`;

  try {
    // Create temp table for spend info (using BYTEA for txids)
    await client.query(`
      CREATE TEMP TABLE ${tempTable} (
        txid BYTEA,
        vout INT,
        spent_txid BYTEA,
        spent_block_height INT,
        PRIMARY KEY (txid, vout)
      ) ON COMMIT DROP
    `);

    // COPY spend data to temp table (with BYTEA types)
    const columns = ['txid', 'vout', 'spent_txid', 'spent_block_height'];
    const types: ColumnType[] = ['bytea', 'integer', 'bytea', 'integer'];
    const rows = spends.map(s => [s.txid, s.vout, s.spentTxid, s.spentBlockHeight]);
    await bulkCopy(client, tempTable, columns, rows, types);

    // Move UTXOs from utxos_unspent to utxos_spent:
    // 1. Insert into utxos_spent with spent info (includes UTXO data from unspent table)
    await client.query(`
      INSERT INTO utxos_spent (
        txid, vout, address_id, value, script_pubkey, script_type, block_height,
        spent_txid, spent_block_height, created_at, spent_at
      )
      SELECT
        u.txid, u.vout, u.address_id, u.value, u.script_pubkey, u.script_type, u.block_height,
        t.spent_txid, t.spent_block_height, u.created_at, NOW()
      FROM utxos_unspent u
      INNER JOIN ${tempTable} t ON u.txid = t.txid AND u.vout = t.vout
      ON CONFLICT (spent_block_height, txid, vout) DO NOTHING
    `);

    // 2. Delete from utxos_unspent
    const result = await client.query(`
      DELETE FROM utxos_unspent u
      USING ${tempTable} t
      WHERE u.txid = t.txid AND u.vout = t.vout
    `);

    return result.rowCount || spends.length;
  } finally {
    await client.query(`DROP TABLE IF EXISTS ${tempTable}`).catch(() => {});
  }
}
