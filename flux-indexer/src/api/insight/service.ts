import type { ClickHouseConnection } from '../../database/connection';
import { extractTransactionFromBlock } from '../../parsers/block-parser';
import type { FluxRPCClient } from '../../rpc/flux-rpc-client';
import { getScriptPubkey } from '../../utils/script-utils';
import type {
  InsightAddressSummaryRow,
  InsightBlockRow,
  InsightInputRow,
  InsightOutputRow,
  InsightTxRow,
  InsightUtxoRow,
} from './types';
import { isValidHash, normalizeHash, parseBlockDate, parseLimit } from './utils';

export type InsightClickHouse = Pick<ClickHouseConnection, 'query' | 'queryOne'>;

export type InsightRPC = Pick<FluxRPCClient, 'estimateFee' | 'sendRawTransaction'> & {
  getBlock(hashOrHeight: string | number, verbosity?: 0 | 1 | 2): Promise<unknown>;
  getRawTransaction(txid: string, verbose?: boolean): Promise<unknown>;
};

export type MempoolAddressDeltas = Map<string, { balanceDelta: bigint; txCount: number }>;
export type GetMempoolAddressDeltas = () => Promise<MempoolAddressDeltas>;

export interface InsightBlockServiceResult {
  block: VersionedInsightBlockRow;
  txids: string[];
  confirmations: number;
  nextBlockHash?: string | null;
}

export interface InsightTransactionServiceResult {
  tx: VersionedInsightTxRow;
  outputs: InsightOutputRow[];
  inputs: InsightInputRow[];
  blockHash?: string | null;
  confirmations: number;
  fluxnode?: InsightFluxnodeTransactionRow | null;
}

export interface InsightAddressSummaryServiceResult {
  summary: InsightAddressSummaryRow | null;
  transactions: string[];
  mempool: { balanceDelta: bigint; txCount: number };
}

export interface InsightListBlocksServiceResult {
  blocks: VersionedInsightBlockRow[];
  blockDate: ReturnType<typeof parseBlockDate> | null;
}

type VersionedInsightBlockRow = InsightBlockRow & { is_valid?: number };
type VersionedInsightTxRow = InsightTxRow & { is_valid?: number };
type InsightUtxoQueryRow = Omit<InsightUtxoRow, 'confirmations'> & {
  confirmations?: number;
  script_type?: string | null;
};

export interface InsightFluxnodeTransactionRow {
  type: number;
  collateral_hash: string;
  collateral_index: number;
  ip_address: string;
  public_key: string;
  signature: string;
  p2sh_address: string;
  benchmark_tier: string;
  extra_data?: string;
}

type BlockLookup =
  | { kind: 'height'; height: number }
  | { kind: 'hash'; hash: string };

const UINT32_MAX = 0xffffffff;
const DEFAULT_MEMPOOL_DELTA = { balanceDelta: 0n, txCount: 0 };
const RECENT_BLOCK_LOOKBACK_BUFFER = 250;
const MAX_UTXO_ADDRESSES = 100;
const MAX_UTXO_ROWS = 5000;

export class InsightCompatibilityService {
  constructor(
    private readonly ch: InsightClickHouse,
    private readonly rpc: InsightRPC,
    private readonly getMempoolAddressDeltas: GetMempoolAddressDeltas
  ) {}

  async getBlock(_heightOrHash: string | number): Promise<InsightBlockServiceResult | null> {
    const lookup = parseBlockLookup(_heightOrHash);
    if (!lookup) {
      return null;
    }

    const block = lookup.kind === 'height'
      ? await this.getLatestBlockByHeight(lookup.height)
      : await this.getLatestBlockByHash(lookup.hash);

    if (!isValidVersionedRow(block)) {
      return null;
    }

    const [txids, currentHeight, nextBlockHash] = await Promise.all([
      this.getBlockTxids(block.height),
      this.getCurrentChainHeight(),
      this.getBlockHashByHeight(block.height + 1),
    ]);

    return {
      block,
      txids,
      confirmations: calculateConfirmations(block.height, currentHeight),
      nextBlockHash,
    };
  }

  async getBlockHashByHeight(_height: number): Promise<string | null> {
    const height = parseHeight(_height);
    if (height === null) {
      return null;
    }

    const row = await this.ch.queryOne<{ hash: string }>(`
      SELECT hash
      FROM (
        SELECT hash, is_valid
        FROM blocks
        WHERE height = {height:UInt32}
        ORDER BY _version DESC
        LIMIT 1
      )
      WHERE is_valid = 1
      LIMIT 1
    `, { height });

    return row?.hash ?? null;
  }

  async getRawBlock(_heightOrHash: string | number): Promise<string | null> {
    const lookup = parseBlockLookup(_heightOrHash);
    if (!lookup) {
      return null;
    }

    try {
      const raw = await this.rpc.getBlock(lookup.kind === 'height' ? lookup.height : lookup.hash, 0);
      return typeof raw === 'string' ? raw : null;
    } catch {
      return null;
    }
  }

  async listBlocks(_query: Record<string, unknown> = {}): Promise<InsightListBlocksServiceResult> {
    const blockDate = hasQueryValue(_query.blockDate) ? parseBlockDate(_query.blockDate) : null;
    const limit = parseLimit(_query.limit, 50, 100);
    const blocks = blockDate
      ? await this.ch.query<VersionedInsightBlockRow>(`
        SELECT height, hash, prev_hash, merkle_root, timestamp, bits, nonce, version,
               size, tx_count, producer, producer_reward, difficulty, chainwork, is_valid
        FROM (
          SELECT height, hash, prev_hash, merkle_root, timestamp, bits, nonce, version,
                 size, tx_count, producer, producer_reward, difficulty, chainwork, is_valid
          FROM blocks
          WHERE timestamp >= {start:UInt32} AND timestamp <= {end:UInt32}
          ORDER BY height, _version DESC
          LIMIT 1 BY height
        )
        WHERE is_valid = 1
        ORDER BY height DESC
        LIMIT {limit:UInt32}
      `, { start: blockDate.start, end: blockDate.end, limit })
      : await this.listRecentBlocks(limit);

    return {
      blocks,
      blockDate,
    };
  }

  private async listRecentBlocks(limit: number): Promise<VersionedInsightBlockRow[]> {
    const maxHeight = await this.getIndexedTipHeight();
    const minHeight = Math.max(0, maxHeight - (limit + RECENT_BLOCK_LOOKBACK_BUFFER));

    return this.ch.query<VersionedInsightBlockRow>(`
        SELECT height, hash, prev_hash, merkle_root, timestamp, bits, nonce, version,
               size, tx_count, producer, producer_reward, difficulty, chainwork, is_valid
        FROM (
          SELECT height, hash, prev_hash, merkle_root, timestamp, bits, nonce, version,
                 size, tx_count, producer, producer_reward, difficulty, chainwork, is_valid
          FROM blocks
          WHERE height <= {maxHeight:UInt32} AND height > {minHeight:UInt32}
          ORDER BY height DESC, _version DESC
          LIMIT 1 BY height
        )
        WHERE is_valid = 1
        ORDER BY height DESC
        LIMIT {limit:UInt32}
      `, { maxHeight, minHeight, limit });
  }

  private async getIndexedTipHeight(): Promise<number> {
    const syncState = await this.ch.queryOne<{ current_height?: string | number }>(`
      SELECT argMax(current_height, updated_at) AS current_height
      FROM sync_state
      WHERE id = 1
    `);
    const syncCurrentHeight = parsePositiveHeightValue(syncState?.current_height);
    if (syncCurrentHeight !== null) {
      return syncCurrentHeight;
    }

    const row = await this.ch.queryOne<{ height?: string | number }>(`
      SELECT height
      FROM (
        SELECT height, is_valid
        FROM blocks
        ORDER BY height DESC, _version DESC
        LIMIT 1 BY height
      )
      WHERE is_valid = 1
      ORDER BY height DESC
      LIMIT 1
    `);

    return parseNonNegativeHeightValue(row?.height) ?? 0;
  }

  async getTransaction(_txid: string): Promise<InsightTransactionServiceResult | null> {
    const txid = normalizeHashOrNull(_txid);
    if (!txid) {
      return null;
    }

    const tx = await this.ch.queryOne<VersionedInsightTxRow>(`
      SELECT txid, version, locktime, block_height, timestamp, input_total,
             output_total, fee, size, is_coinbase, is_fluxnode_tx,
             fluxnode_type, is_valid
      FROM transactions
      WHERE txid = {txid:FixedString(64)}
      ORDER BY _version DESC
      LIMIT 1
    `, { txid });

    if (!isValidVersionedRow(tx)) {
      return null;
    }

    const [outputs, clickHouseInputs, block, currentHeight, fluxnode] = await Promise.all([
      this.getTransactionOutputs(txid),
      this.getTransactionInputs(txid),
      this.getTransactionBlock(tx.block_height),
      this.getCurrentChainHeight(),
      tx.is_fluxnode_tx === 1 ? this.getFluxnodeTransaction(txid) : Promise.resolve(null),
    ]);
    const inputs = tx.is_coinbase === 1
      ? clickHouseInputs
      : await this.orderInputsByDecodedVin(txid, clickHouseInputs);

    return {
      tx,
      outputs,
      inputs,
      blockHash: block?.hash ?? null,
      confirmations: block ? calculateConfirmations(tx.block_height, currentHeight) : 0,
      fluxnode,
    };
  }

  async getRawTransaction(_txid: string): Promise<string | null> {
    const txid = normalizeHashOrNull(_txid);
    if (!txid) {
      return null;
    }

    try {
      const raw = await this.rpc.getRawTransaction(txid, false);
      const hex = getRawTransactionHex(raw);
      if (hex !== null) {
        return hex;
      }
    } catch {
      // Fall back to indexed block extraction below. This covers daemons without txindex.
    }

    return this.getRawTransactionFromIndexedBlock(txid);
  }

  async getAddressSummary(_address: string, _noTxList: boolean): Promise<InsightAddressSummaryServiceResult> {
    const [summary, mempoolDeltas, transactions] = await Promise.all([
      this.ch.queryOne<InsightAddressSummaryRow>(`
        SELECT
          sumMerge(balance) AS balance,
          sumMerge(tx_count) AS tx_count,
          sumMerge(received_total) AS received_total,
          sumMerge(sent_total) AS sent_total
        FROM address_summary_agg
        WHERE address = {address:String}
        GROUP BY address
      `, { address: _address }),
      this.getMempoolAddressDeltas(),
      _noTxList ? Promise.resolve([]) : this.getAddressTransactionIds(_address),
    ]);

    return {
      summary: summary ?? null,
      transactions,
      mempool: mempoolDeltas.get(_address) ?? DEFAULT_MEMPOOL_DELTA,
    };
  }

  async getAddressUtxos(_addresses: string[], _queryMempool: boolean): Promise<InsightUtxoRow[]> {
    void _queryMempool;

    const addresses = [...new Set(_addresses.map((address) => address.trim()).filter(Boolean))]
      .slice(0, MAX_UTXO_ADDRESSES);
    if (addresses.length === 0) {
      return [];
    }

    const [rows, currentHeight] = await Promise.all([
      this.ch.query<InsightUtxoQueryRow>(`
        SELECT address, txid, vout, script_pubkey, script_type, value, block_height
        FROM (
          SELECT address, txid, vout, script_pubkey, script_type, value, block_height, spent
          FROM utxos
          WHERE address IN {addresses:Array(String)}
          ORDER BY txid, vout, version DESC
          LIMIT 1 BY txid, vout
        )
        WHERE spent = 0
        ORDER BY block_height DESC, txid, vout
        LIMIT {limit:UInt32}
      `, { addresses, limit: MAX_UTXO_ROWS }),
      this.getCurrentChainHeight(),
    ]);

    return rows.map((row) => {
      const { script_type: scriptType, ...utxo } = row;
      return {
        ...utxo,
        script_pubkey: normalizeScriptPubkey(row.script_pubkey, scriptType, row.address),
        confirmations: calculateConfirmations(row.block_height, currentHeight),
      };
    });
  }

  async sendRawTransaction(_rawtx: string): Promise<string> {
    return this.rpc.sendRawTransaction(_rawtx);
  }

  async estimateFees(_targets: number[]): Promise<Record<number, number>> {
    const entries = await Promise.all(_targets.map(async (target) => {
      if (!Number.isSafeInteger(target) || target <= 0) {
        throw new Error('Invalid fee target');
      }

      return [target, await this.rpc.estimateFee(target)] as const;
    }));

    return Object.fromEntries(entries) as Record<number, number>;
  }

  private async getLatestBlockByHeight(height: number): Promise<VersionedInsightBlockRow | null> {
    return this.ch.queryOne<VersionedInsightBlockRow>(`
      SELECT height, hash, prev_hash, merkle_root, timestamp, bits, nonce, version,
             size, tx_count, producer, producer_reward, difficulty, chainwork, is_valid
      FROM blocks
      WHERE height = {height:UInt32}
      ORDER BY _version DESC
      LIMIT 1
    `, { height });
  }

  private async getLatestBlockByHash(hash: string): Promise<VersionedInsightBlockRow | null> {
    return this.ch.queryOne<VersionedInsightBlockRow>(`
      SELECT height, hash, prev_hash, merkle_root, timestamp, bits, nonce, version,
             size, tx_count, producer, producer_reward, difficulty, chainwork, is_valid
      FROM blocks
      WHERE hash = {hash:FixedString(64)}
      ORDER BY _version DESC
      LIMIT 1
    `, { hash });
  }

  private async getLatestTransactionLocation(txid: string): Promise<{ block_height: number; is_valid?: number } | null> {
    return this.ch.queryOne<{ block_height: number; is_valid?: number }>(`
      SELECT block_height, is_valid
      FROM transactions
      WHERE txid = {txid:FixedString(64)}
      ORDER BY _version DESC
      LIMIT 1
    `, { txid });
  }

  private async getRawTransactionFromIndexedBlock(txid: string): Promise<string | null> {
    const tx = await this.getLatestTransactionLocation(txid);
    if (!isValidVersionedRow(tx)) {
      return null;
    }

    const block = await this.getLatestBlockByHeight(tx.block_height);
    if (!isValidVersionedRow(block)) {
      return null;
    }

    let rawBlock: unknown;
    try {
      rawBlock = await this.rpc.getBlock(block.hash, 0);
    } catch {
      return null;
    }

    if (typeof rawBlock !== 'string') {
      return null;
    }

    try {
      return extractTransactionFromBlock(rawBlock, txid, tx.block_height);
    } catch {
      return null;
    }
  }

  private async getBlockTxids(height: number): Promise<string[]> {
    const rows = await this.ch.query<{ txid: string }>(`
      SELECT txid
      FROM (
        SELECT txid, tx_index, is_valid
        FROM transactions
        WHERE block_height = {height:UInt32}
        ORDER BY txid, _version DESC
        LIMIT 1 BY txid
      )
      WHERE is_valid = 1
      ORDER BY tx_index ASC
    `, { height });

    return rows.map((row) => row.txid);
  }

  private async getCurrentChainHeight(): Promise<number> {
    const syncState = await this.ch.queryOne<{
      chain_height?: string | number;
      current_height?: string | number;
    }>(`
      SELECT
        argMax(chain_height, updated_at) AS chain_height,
        argMax(current_height, updated_at) AS current_height
      FROM sync_state
      WHERE id = 1
    `);
    const syncStateHeight = parsePositiveHeightValue(syncState?.chain_height)
      ?? parsePositiveHeightValue(syncState?.current_height);
    if (syncStateHeight !== null) {
      return syncStateHeight;
    }

    const row = await this.ch.queryOne<{ height?: string | number; h?: string | number; max_height?: string | number }>(`
      SELECT max(height) AS height
      FROM (
        SELECT height, is_valid
        FROM blocks
        ORDER BY height, _version DESC
        LIMIT 1 BY height
      )
      WHERE is_valid = 1
    `);

    return parseNonNegativeHeightValue(row?.height)
      ?? parseNonNegativeHeightValue(row?.h)
      ?? parseNonNegativeHeightValue(row?.max_height)
      ?? 0;
  }

  private async getTransactionOutputs(txid: string): Promise<InsightOutputRow[]> {
    const rows = await this.ch.query<InsightOutputRow>(`
      SELECT vout, address, value, script_pubkey, script_type, spent,
             spent_txid, spent_block_height
      FROM (
        SELECT vout, address, value, script_pubkey, script_type, spent,
               spent_txid, spent_block_height
        FROM utxos
        WHERE txid = {txid:FixedString(64)}
        ORDER BY vout, version DESC
        LIMIT 1 BY vout
      )
      ORDER BY vout
    `, { txid });

    return rows.map((row) => ({
      ...row,
      script_pubkey: normalizeScriptPubkey(row.script_pubkey, row.script_type, row.address ?? ''),
      spent_txid: normalizeNullableHash(row.spent_txid),
      spent_index: null,
      spent_block_height: row.spent_block_height && row.spent_block_height > 0
        ? row.spent_block_height
        : null,
    }));
  }

  private async getTransactionInputs(txid: string): Promise<InsightInputRow[]> {
    return this.ch.query<InsightInputRow>(`
      SELECT txid, vout, address, value, script_type
      FROM (
        SELECT txid, vout, address, value, script_type
        FROM utxos
        WHERE spent_txid = {txid:FixedString(64)}
        ORDER BY txid, vout, version DESC
        LIMIT 1 BY txid, vout
      )
      ORDER BY txid, vout
    `, { txid });
  }

  private async orderInputsByDecodedVin(txid: string, inputs: InsightInputRow[]): Promise<InsightInputRow[]> {
    if (inputs.length < 2) {
      return inputs;
    }

    let decoded: unknown;
    try {
      decoded = await this.rpc.getRawTransaction(txid, true);
    } catch {
      return inputs;
    }

    const vinOrder = getDecodedVinOrder(decoded);
    if (vinOrder.size === 0) {
      return inputs;
    }

    return inputs
      .map((input, index) => ({
        input,
        index,
        order: vinOrder.get(outpointKey(input.txid, input.vout)) ?? Number.MAX_SAFE_INTEGER,
      }))
      .sort((a, b) => a.order - b.order || a.index - b.index)
      .map(({ input }) => input);
  }

  private async getTransactionBlock(height: number): Promise<VersionedInsightBlockRow | null> {
    const block = await this.getLatestBlockByHeight(height);
    return isValidVersionedRow(block) ? block : null;
  }

  private async getFluxnodeTransaction(txid: string): Promise<InsightFluxnodeTransactionRow | null> {
    return this.ch.queryOne<InsightFluxnodeTransactionRow>(`
      SELECT type, collateral_hash, collateral_index, ip_address, public_key,
             signature, p2sh_address, benchmark_tier, extra_data
      FROM (
        SELECT type, collateral_hash, collateral_index, ip_address, public_key,
               signature, p2sh_address, benchmark_tier, extra_data, is_valid
        FROM fluxnode_transactions
        WHERE txid = {txid:FixedString(64)}
        ORDER BY _version DESC
        LIMIT 1
      )
      WHERE is_valid = 1
      LIMIT 1
    `, { txid });
  }

  private async getAddressTransactionIds(address: string): Promise<string[]> {
    const rows = await this.ch.query<{ txid: string }>(`
      SELECT txid
      FROM (
        SELECT txid, block_height, tx_index, is_valid
        FROM address_transactions
        WHERE address = {address:String}
        ORDER BY txid, _version DESC
        LIMIT 1 BY txid
      )
      WHERE is_valid = 1
      ORDER BY block_height DESC, tx_index ASC, txid ASC
      LIMIT 1000
    `, { address });

    return rows.map((row) => row.txid);
  }
}

function parseBlockLookup(heightOrHash: string | number): BlockLookup | null {
  if (typeof heightOrHash === 'number') {
    const height = parseHeight(heightOrHash);
    return height === null ? null : { kind: 'height', height };
  }

  const trimmed = heightOrHash.trim();
  if (/^\d+$/.test(trimmed)) {
    const parsed = Number(trimmed);
    const height = parseHeight(parsed);
    if (height !== null) {
      return { kind: 'height', height };
    }
  }

  const hash = normalizeHashOrNull(trimmed);
  return hash ? { kind: 'hash', hash } : null;
}

function hasQueryValue(raw: unknown): boolean {
  if (raw === undefined || raw === null) {
    return false;
  }

  if (Array.isArray(raw)) {
    return raw.length > 0 && hasQueryValue(raw[0]);
  }

  return String(raw).trim().length > 0;
}

function parseHeight(height: number): number | null {
  if (!Number.isSafeInteger(height) || height < 0 || height > UINT32_MAX) {
    return null;
  }

  return height;
}

function parsePositiveHeightValue(value: unknown): number | null {
  const height = parseHeightValue(value);
  return height !== null && height > 0 ? height : null;
}

function parseNonNegativeHeightValue(value: unknown): number | null {
  return parseHeightValue(value);
}

function parseHeightValue(value: unknown): number | null {
  if (typeof value === 'number') {
    return parseHeight(value);
  }

  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (!/^\d+$/.test(trimmed)) {
    return null;
  }

  return parseHeight(Number(trimmed));
}

function normalizeHashOrNull(hash: string): string | null {
  if (!isValidHash(hash)) {
    return null;
  }

  try {
    return normalizeHash(hash);
  } catch {
    return null;
  }
}

function isValidVersionedRow<T extends { is_valid?: number }>(row: T | null): row is T {
  return row !== null && row.is_valid === 1;
}

function calculateConfirmations(blockHeight: number | undefined, currentHeight: number): number {
  if (blockHeight === undefined || currentHeight < blockHeight) {
    return 0;
  }

  return currentHeight - blockHeight + 1;
}

function normalizeNullableHash(hash: string | null | undefined): string | null {
  const trimmed = hash?.trim() ?? '';
  if (trimmed.length === 0 || /^0+$/.test(trimmed)) {
    return null;
  }

  return trimmed;
}

function normalizeScriptPubkey(
  storedScript: string | null | undefined,
  scriptType: string | null | undefined,
  address: string | null | undefined
): string {
  return getScriptPubkey(storedScript ?? '', scriptType ?? '', address ?? '') ?? storedScript ?? '';
}

function getRawTransactionHex(raw: unknown): string | null {
  if (typeof raw === 'string') {
    return raw;
  }

  if (isRecord(raw) && typeof raw.hex === 'string') {
    return raw.hex;
  }

  return null;
}

function getDecodedVinOrder(decoded: unknown): Map<string, number> {
  if (!isRecord(decoded) || !Array.isArray(decoded.vin)) {
    return new Map();
  }

  const order = new Map<string, number>();
  decoded.vin.forEach((vin, index) => {
    const key = vinOutpointKey(vin);
    if (key && !order.has(key)) {
      order.set(key, index);
    }
  });

  return order;
}

function vinOutpointKey(vin: unknown): string | null {
  if (!isRecord(vin) || typeof vin.txid !== 'string') {
    return null;
  }

  const vout = parseVout(vin.vout);
  return vout === null ? null : outpointKey(vin.txid, vout);
}

function outpointKey(txid: string, vout: number): string {
  return `${normalizeHashOrNull(txid) ?? txid.trim().toLowerCase()}:${vout}`;
}

function parseVout(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (!/^\d+$/.test(trimmed)) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
