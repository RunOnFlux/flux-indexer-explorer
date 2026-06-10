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
import {
  isValidHash,
  normalizeHash,
  parseBlockDate,
  parseLimit,
  zatoshisToFluxString,
  zatoshisToSafeNumber,
} from './utils';

export type InsightClickHouse = Pick<ClickHouseConnection, 'query' | 'queryOne'>;

export type InsightRPC = Pick<
  FluxRPCClient,
  | 'estimateFee'
  | 'sendRawTransaction'
  | 'getDifficulty'
  | 'getBestBlockHash'
  | 'getMiningInfo'
  | 'getPeerInfo'
  | 'getInfo'
  | 'getVersion'
  | 'verifyMessage'
  | 'viewDeterministicFluxNodeList'
  | 'dosList'
  | 'startList'
> & {
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

export type InsightStatisticSeriesKind =
  | 'supply'
  | 'fees'
  | 'network-hash'
  | 'transactions'
  | 'outputs'
  | 'difficulty'
  | 'active-addresses';

type VersionedInsightBlockRow = InsightBlockRow & { is_valid?: number };
type VersionedInsightTxRow = InsightTxRow & { is_valid?: number };
type InsightUtxoQueryRow = Omit<InsightUtxoRow, 'confirmations'> & {
  confirmations?: number;
  script_type?: string | null;
};
type PoolStatisticRow = {
  address: string;
  poolName: string;
  url: null;
  blocks_found: number;
  percent_total: number;
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
const MAX_BLOCK_TRANSACTION_LOOKUP = 200;
const MAX_ADDRESS_TX_LIMIT = 50;
const DEFAULT_STATISTIC_DAYS = 365;
const MAX_STATISTIC_DAYS = 730;
const SATOSHIS_PER_FLUX = 100000000n;
const RICH_LIST_LIMIT = 200;
const BALANCE_INTERVALS = [
  { label: '0-1 FLUX', min: 0n, max: 1n * SATOSHIS_PER_FLUX },
  { label: '1-10 FLUX', min: 1n * SATOSHIS_PER_FLUX, max: 10n * SATOSHIS_PER_FLUX },
  { label: '10-100 FLUX', min: 10n * SATOSHIS_PER_FLUX, max: 100n * SATOSHIS_PER_FLUX },
  { label: '100-1,000 FLUX', min: 100n * SATOSHIS_PER_FLUX, max: 1000n * SATOSHIS_PER_FLUX },
  { label: '1,000-10,000 FLUX', min: 1000n * SATOSHIS_PER_FLUX, max: 10000n * SATOSHIS_PER_FLUX },
  { label: '10,000+ FLUX', min: 10000n * SATOSHIS_PER_FLUX, max: null },
] as const;
const RICHER_THAN_THRESHOLDS = [
  1n,
  10n,
  100n,
  1000n,
  10000n,
  100000n,
].map((flux) => ({
  flux,
  zatoshis: flux * SATOSHIS_PER_FLUX,
}));

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

  async getTransactionsByBlock(blockHash: string): Promise<InsightTransactionServiceResult[]> {
    const block = await this.getBlock(blockHash);
    if (block === null) {
      return [];
    }

    const transactions = await Promise.all(
      block.txids.slice(0, MAX_BLOCK_TRANSACTION_LOOKUP).map((txid) => this.getTransaction(txid))
    );

    return transactions.filter(isPresent);
  }

  async getTransactionsByAddress(address: string): Promise<InsightTransactionServiceResult[]> {
    const result = await this.getAddressTransactions([address], { from: 0, to: 10, limit: 10 });
    return result.items;
  }

  async getAddressTransactions(
    _addresses: string[],
    range: { from: number; to: number; limit: number }
  ): Promise<{ totalItems: number; items: InsightTransactionServiceResult[] }> {
    const addresses = normalizeAddressList(_addresses);
    if (addresses.length === 0) {
      return { totalItems: 0, items: [] };
    }

    const offset = normalizeNonNegativeSafeInteger(range.from, 0);
    const limit = normalizeRangeLimit(range.limit);
    const [rows, countRow] = await Promise.all([
      this.ch.query<{ txid: string }>(`
        SELECT txid
        FROM (
          SELECT txid, max(block_height) AS block_height, min(tx_index) AS tx_index
          FROM (
            SELECT address, txid, block_height, tx_index, is_valid
            FROM address_transactions
            WHERE address IN {addresses:Array(String)}
            ORDER BY address, txid, _version DESC
            LIMIT 1 BY address, txid
          )
          WHERE is_valid = 1
          GROUP BY txid
        )
        ORDER BY block_height DESC, tx_index ASC, txid ASC
        LIMIT {limit:UInt32}
        OFFSET {offset:UInt32}
      `, { addresses, limit, offset }),
      this.ch.queryOne<{ totalItems?: string | number; total_items?: string | number }>(`
        SELECT toString(uniqExact(txid)) AS totalItems
        FROM (
          SELECT txid
          FROM (
            SELECT address, txid, is_valid
            FROM address_transactions
            WHERE address IN {addresses:Array(String)}
            ORDER BY address, txid, _version DESC
            LIMIT 1 BY address, txid
          )
          WHERE is_valid = 1
        )
      `, { addresses }),
    ]);

    const transactions = await Promise.all(
      rows.map((row) => this.getTransaction(row.txid))
    );

    return {
      totalItems: safeCount(countRow?.totalItems ?? countRow?.total_items),
      items: transactions.filter(isPresent),
    };
  }

  async getAddressBalanceSum(_addresses: string[]): Promise<{
    balance: string | number;
    unconfirmedBalance: string | number;
    immature: string | number;
  }> {
    const addresses = normalizeAddressList(_addresses);
    if (addresses.length === 0) {
      return { balance: 0, unconfirmedBalance: 0, immature: 0 };
    }

    const [row, mempoolDeltas] = await Promise.all([
      this.ch.queryOne<{ balance?: string | number }>(`
        SELECT toString(sumMerge(balance)) AS balance
        FROM address_summary_agg
        WHERE address IN {addresses:Array(String)}
      `, { addresses }),
      this.getMempoolAddressDeltas(),
    ]);

    const confirmedBalance = BigInt(zatoshiString(row?.balance));
    const unconfirmedBalance = addresses.reduce((sum, address) => (
      sum + (mempoolDeltas.get(address)?.balanceDelta ?? 0n)
    ), 0n);

    return {
      balance: zatoshisToSafeNumber(confirmedBalance),
      unconfirmedBalance: zatoshisToSafeNumber(unconfirmedBalance),
      immature: 0,
    };
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

  async getStatus(query: string | undefined): Promise<unknown> {
    switch (query) {
      case 'getDifficulty':
        return { difficulty: await this.rpc.getDifficulty() };
      case 'getBestBlockHash':
        return { bestblockhash: await this.rpc.getBestBlockHash() };
      case 'getLastBlockHash': {
        const hash = await this.rpc.getBestBlockHash();
        return { syncTipHash: hash, lastblockhash: hash };
      }
      case 'getMiningInfo':
        return { miningInfo: await this.rpc.getMiningInfo() };
      case 'getPeerInfo':
        return { peerInfo: await this.rpc.getPeerInfo() };
      case 'getFluxNodes':
        return { fluxNodes: await this.rpc.viewDeterministicFluxNodeList() };
      case 'getZelNodes':
        return { zelNodes: await this.rpc.viewDeterministicFluxNodeList() };
      case 'getInfo':
      default:
        return { info: await this.rpc.getInfo() };
    }
  }

  async getSync(): Promise<{
    status: 'syncing' | 'finished';
    blockChainHeight: number;
    syncPercentage: number;
    height: number;
    error: null;
    type: 'bitcore node';
  }> {
    const sync = await this.ch.queryOne<{
      current_height?: string | number;
      chain_height?: string | number;
      sync_percentage?: string | number;
      is_syncing?: string | number | boolean;
    }>(`
      SELECT
        argMax(current_height, updated_at) AS current_height,
        argMax(chain_height, updated_at) AS chain_height,
        argMax(sync_percentage, updated_at) AS sync_percentage,
        argMax(is_syncing, updated_at) AS is_syncing
      FROM sync_state
      WHERE id = 1
    `);
    const rawHeight = parseSignedSafeInteger(sync?.current_height) ?? 0;
    const height = Math.max(0, rawHeight);
    const blockChainHeight = parseNonNegativeHeightValue(sync?.chain_height)
      ?? height;
    const syncPercentage = normalizePercentage(
      sync?.sync_percentage,
      height,
      blockChainHeight
    );
    const isSyncing = parseBooleanFlag(sync?.is_syncing) === true
      || syncPercentage < 100
      || height < blockChainHeight;

    return {
      status: isSyncing ? 'syncing' : 'finished',
      blockChainHeight,
      syncPercentage,
      height,
      error: null,
      type: 'bitcore node',
    };
  }

  getPeer(): { connected: true; host: '127.0.0.1'; port: null } {
    return { connected: true, host: '127.0.0.1', port: null };
  }

  async getVersion(): Promise<unknown> {
    return this.rpc.getVersion();
  }

  async verifyMessage(address: string, signature: string, message: string): Promise<boolean> {
    return this.rpc.verifyMessage(address, signature, message);
  }

  async listFluxNodes(filter?: string): Promise<unknown> {
    const response = await this.rpc.viewDeterministicFluxNodeList();
    const trimmedFilter = filter?.trim();

    if (!trimmedFilter) {
      return response;
    }

    const envelope = normalizeFluxNodeEnvelope(response);
    return {
      result: filterFluxNodeResult(envelope.result, trimmedFilter),
      error: envelope.error,
      id: envelope.id,
    };
  }

  async getSupply(): Promise<string> {
    const row = await this.ch.queryOne<{ total_supply?: string | number | bigint }>(`
      SELECT toString(total_supply) AS total_supply
      FROM supply_stats
      ORDER BY block_height DESC, _version DESC
      LIMIT 1
    `);

    return zatoshiString(row?.total_supply);
  }

  async getStatisticSeries(kind: InsightStatisticSeriesKind, rawDays?: string): Promise<unknown[]> {
    const days = parseStatisticDays(rawDays);

    switch (kind) {
      case 'supply':
        return this.getSupplyStatisticSeries(days);
      case 'fees':
        return this.getFeeStatisticSeries(days);
      case 'network-hash':
        throw new Error('Network hash statistics are not implemented');
      case 'transactions':
        return this.getTransactionCountStatisticSeries(days);
      case 'outputs':
        return this.getOutputStatisticSeries(days);
      case 'difficulty':
        return this.getBlockStatisticSeries(days, 'difficulty');
      case 'active-addresses':
        return this.getActiveAddressStatisticSeries(days);
      default:
        throw new Error(`Unsupported statistic series: ${kind}`);
    }
  }

  async getStatisticsTotal(): Promise<{
    n_blocks_mined: number;
    time_between_blocks: number;
    mined_currency_amount: string;
    transaction_fees: string;
    number_of_transactions: number;
    outputs_volume: string;
    difficulty: number;
    network_hash_ps: number;
    blocks_by_pool: PoolStatisticRow[];
  }> {
    const cutoff = Math.max(0, Math.floor(Date.now() / 1000) - 86400);
    const [blockStats, txStats, poolRows] = await Promise.all([
      this.ch.queryOne<{
        n_blocks_mined?: string | number;
        time_between_blocks?: string | number;
        mined_currency_amount?: string | number;
        difficulty?: string | number;
      }>(`
        SELECT
          toString(count()) AS n_blocks_mined,
          ifNull(avg(timestamp - previous_timestamp), 0) AS time_between_blocks,
          toString(sum(producer_reward)) AS mined_currency_amount,
          ifNull(avg(difficulty), 0) AS difficulty
        FROM (
          SELECT
            height,
            timestamp,
            producer_reward,
            difficulty,
            lagInFrame(timestamp) OVER (ORDER BY height) AS previous_timestamp
          FROM (
            SELECT height, timestamp, producer_reward, difficulty, is_valid
            FROM blocks
            WHERE timestamp >= {cutoff:UInt32}
            ORDER BY height, _version DESC
            LIMIT 1 BY height
          )
          WHERE is_valid = 1
        )
        WHERE previous_timestamp > 0 OR previous_timestamp IS NULL
      `, { cutoff }),
      this.ch.queryOne<{
        number_of_transactions?: string | number;
        transaction_fees?: string | number;
        outputs_volume?: string | number;
      }>(`
        SELECT
          toString(count()) AS number_of_transactions,
          toString(sum(fee)) AS transaction_fees,
          toString(sum(output_total)) AS outputs_volume
        FROM (
          SELECT txid, fee, output_total, is_valid
          FROM transactions
          WHERE timestamp >= {cutoff:UInt32}
          ORDER BY txid, _version DESC
          LIMIT 1 BY txid
        )
        WHERE is_valid = 1
      `, { cutoff }),
      this.queryPoolRows({ cutoff }),
    ]);
    const blocksByPool = formatPoolRows(poolRows);

    return {
      n_blocks_mined: safeCount(blockStats?.n_blocks_mined),
      time_between_blocks: finiteNumber(blockStats?.time_between_blocks),
      mined_currency_amount: zatoshisToFluxString(zatoshiString(blockStats?.mined_currency_amount)),
      transaction_fees: zatoshisToFluxString(zatoshiString(txStats?.transaction_fees)),
      number_of_transactions: safeCount(txStats?.number_of_transactions),
      outputs_volume: zatoshisToFluxString(zatoshiString(txStats?.outputs_volume)),
      difficulty: finiteNumber(blockStats?.difficulty),
      network_hash_ps: 0,
      blocks_by_pool: blocksByPool,
    };
  }

  async getPools(dateRaw?: string): Promise<{
    date: string;
    n_blocks_mined: number;
    blocks_by_pool: PoolStatisticRow[];
    pagination: { current: string; next: string; prev: string };
  }> {
    const blockDate = parseBlockDate(dateRaw);
    const rows = await this.queryPoolRows({ start: blockDate.start, end: blockDate.end });
    const blocksByPool = formatPoolRows(rows);

    return {
      date: blockDate.current,
      n_blocks_mined: blocksByPool.reduce((sum, pool) => sum + pool.blocks_found, 0),
      blocks_by_pool: blocksByPool,
      pagination: {
        current: blockDate.current,
        next: blockDate.next,
        prev: blockDate.prev,
      },
    };
  }

  async getPoolsLastHour(): Promise<{
    n_blocks_mined: number;
    blocks_by_pool: PoolStatisticRow[];
  }> {
    const cutoff = Math.max(0, Math.floor(Date.now() / 1000) - 3600);
    const blocksByPool = formatPoolRows(await this.queryPoolRows({ cutoff }));

    return {
      n_blocks_mined: blocksByPool.reduce((sum, pool) => sum + pool.blocks_found, 0),
      blocks_by_pool: blocksByPool,
    };
  }

  async getBalanceIntervals(): Promise<Array<{
    min: string;
    max: string | null;
    count: number;
    sum: string | number;
  }>> {
    const rows = await this.ch.query<{ bucket: string; count?: string | number; sum?: string | number }>(`
      SELECT bucket, toString(count()) AS count, toString(sum(balance)) AS sum
      FROM (
        SELECT
          multiIf(
            balance >= {tenThousand:UInt64}, '10,000+ FLUX',
            balance >= {oneThousand:UInt64}, '1,000-10,000 FLUX',
            balance >= {oneHundred:UInt64}, '100-1,000 FLUX',
            balance >= {ten:UInt64}, '10-100 FLUX',
            balance >= {one:UInt64}, '1-10 FLUX',
            '0-1 FLUX'
          ) AS bucket,
          balance
        FROM (
          SELECT address, sumMerge(balance) AS balance
          FROM address_summary_agg
          GROUP BY address
          HAVING balance > 0
        )
      )
      GROUP BY bucket
    `, {
      one: Number(SATOSHIS_PER_FLUX),
      ten: Number(10n * SATOSHIS_PER_FLUX),
      oneHundred: Number(100n * SATOSHIS_PER_FLUX),
      oneThousand: Number(1000n * SATOSHIS_PER_FLUX),
      tenThousand: Number(10000n * SATOSHIS_PER_FLUX),
    });
    const counts = new Map(rows.map((row) => [row.bucket, safeCount(row.count)]));
    const sums = new Map(rows.map((row) => [row.bucket, zatoshisToSafeNumber(zatoshiString(row.sum))]));

    return BALANCE_INTERVALS.map((bucket) => ({
      min: bucket.min.toString(),
      max: bucket.max?.toString() ?? null,
      count: counts.get(bucket.label) ?? 0,
      sum: sums.get(bucket.label) ?? 0,
    }));
  }

  async getRicherThan(): Promise<Array<{
    amount_flux: number;
    count_addresses: number;
  }>> {
    const rows = await this.ch.query<{ threshold?: string | number; count?: string | number }>(`
      SELECT threshold, toString(count()) AS count
      FROM (
        SELECT balance, arrayJoin({thresholds:Array(UInt64)}) AS threshold
        FROM (
          SELECT address, sumMerge(balance) AS balance
          FROM address_summary_agg
          GROUP BY address
          HAVING balance > 0
        )
      )
      WHERE balance >= threshold
      GROUP BY threshold
    `, { thresholds: RICHER_THAN_THRESHOLDS.map((threshold) => Number(threshold.zatoshis)) });
    const counts = new Map(rows.map((row) => [zatoshiString(row.threshold), safeCount(row.count)]));

    return RICHER_THAN_THRESHOLDS.map((threshold) => ({
      amount_flux: Number(threshold.flux),
      count_addresses: counts.get(threshold.zatoshis.toString()) ?? 0,
    }));
  }

  async getRichestAddressesList(): Promise<Array<{
    address: string;
    blocks_mined: number;
    balance: string | number;
  }>> {
    const addresses = await this.ch.query<{ address: string; balance?: string | number }>(`
      SELECT address, toString(balance) AS balance
      FROM (
        SELECT address, sumMerge(balance) AS balance
        FROM address_summary_agg
        GROUP BY address
        HAVING balance > 0
        ORDER BY balance DESC
        LIMIT {limit:UInt32}
      )
    `, { limit: RICH_LIST_LIMIT });
    if (addresses.length === 0) {
      return [];
    }

    const addressList = addresses.map((row) => row.address);
    const minedRows = await this.ch.query<{ address: string; blocks_mined?: string | number }>(`
      SELECT producer AS address, toString(count()) AS blocks_mined
      FROM (
        SELECT height, producer, is_valid
        FROM blocks
        WHERE producer IN {addresses:Array(String)}
        ORDER BY height, _version DESC
        LIMIT 1 BY height
      )
      WHERE is_valid = 1
      GROUP BY producer
    `, { addresses: addressList });
    const mined = new Map(minedRows.map((row) => [row.address, safeCount(row.blocks_mined)]));

    return addresses.map((row) => ({
      address: row.address,
      blocks_mined: mined.get(row.address) ?? 0,
      balance: zatoshisToSafeNumber(zatoshiString(row.balance)),
    }));
  }

  getCurrency(): { status: 'ok'; data: null; timestamp: string } {
    return { status: 'ok', data: null, timestamp: new Date().toISOString() };
  }

  getMarketsInfo(): { rate: null; currency: 'USD'; source: null; timestamp: string } {
    return { rate: null, currency: 'USD', source: null, timestamp: new Date().toISOString() };
  }

  async dosList(): Promise<unknown> {
    return this.rpc.dosList();
  }

  async startList(): Promise<unknown> {
    return this.rpc.startList();
  }

  private async getSupplyStatisticSeries(days: number): Promise<Array<{ date: string; sum: string }>> {
    const rows = await this.ch.query<{ date?: string; day?: string; total_supply?: string | number }>(`
      SELECT toString(day) AS date, toString(total_supply) AS total_supply
      FROM (
        SELECT day, max_height, total_supply
        FROM mv_daily_supply
        WHERE day >= today() - {days:UInt16}
        ORDER BY day DESC, max_height DESC
        LIMIT 1 BY day
      )
      ORDER BY day ASC
    `, { days });

    return rows.map((row) => ({
      date: dateText(row.date ?? row.day),
      sum: zatoshisToFluxString(zatoshiString(row.total_supply)),
    }));
  }

  private async getFeeStatisticSeries(days: number): Promise<Array<{ date: string; fee: number }>> {
    const rows = await this.ch.query<{ date?: string; fee?: string | number }>(`
      SELECT
        toString(toDate(toDateTime(timestamp))) AS date,
        ifNull(avg(fee), 0) / 100000000 AS fee
      FROM (
        SELECT txid, timestamp, fee, is_valid
        FROM transactions
        WHERE timestamp >= toUInt32(toUnixTimestamp(now() - toIntervalDay({days:UInt16})))
        ORDER BY txid, _version DESC
        LIMIT 1 BY txid
      )
      WHERE is_valid = 1
      GROUP BY date
      ORDER BY date ASC
    `, { days });

    return rows.map((row) => ({
      date: dateText(row.date),
      fee: finiteNumber(row.fee),
    }));
  }

  private async getBlockStatisticSeries(
    days: number,
    kind: 'network-hash' | 'difficulty'
  ): Promise<Array<{ date: string; sum: number }>> {
    const rows = await this.ch.query<{ date?: string; sum?: string | number }>(`
      SELECT
        toString(toDate(toDateTime(timestamp))) AS date,
        ifNull(avg(difficulty), 0) AS sum
      FROM (
        SELECT height, timestamp, difficulty, is_valid
        FROM blocks
        WHERE timestamp >= toUInt32(toUnixTimestamp(now() - toIntervalDay({days:UInt16})))
        ORDER BY height, _version DESC
        LIMIT 1 BY height
      )
      WHERE is_valid = 1
      GROUP BY date
      ORDER BY date ASC
    `, { days });

    void kind;
    return rows.map((row) => ({
      date: dateText(row.date),
      sum: finiteNumber(row.sum),
    }));
  }

  private async getTransactionCountStatisticSeries(days: number): Promise<Array<{
    date: string;
    transaction_count: number;
    block_count: number;
  }>> {
    const rows = await this.ch.query<{
      date?: string;
      transaction_count?: string | number;
      block_count?: string | number;
    }>(`
      SELECT
        date,
        toString(ifNull(transaction_count, 0)) AS transaction_count,
        toString(ifNull(block_count, 0)) AS block_count
      FROM (
        SELECT date, transaction_count
        FROM (
          SELECT
            toString(toDate(toDateTime(timestamp))) AS date,
            count() AS transaction_count
          FROM (
            SELECT txid, timestamp, is_valid
            FROM transactions
            WHERE timestamp >= toUInt32(toUnixTimestamp(now() - toIntervalDay({days:UInt16})))
            ORDER BY txid, _version DESC
            LIMIT 1 BY txid
          )
          WHERE is_valid = 1
          GROUP BY date
        )
      ) AS tx_by_day
      FULL OUTER JOIN (
        SELECT date, block_count
        FROM (
          SELECT
            toString(toDate(toDateTime(timestamp))) AS date,
            count() AS block_count
          FROM (
            SELECT height, timestamp, is_valid
            FROM blocks
            WHERE timestamp >= toUInt32(toUnixTimestamp(now() - toIntervalDay({days:UInt16})))
            ORDER BY height, _version DESC
            LIMIT 1 BY height
          )
          WHERE is_valid = 1
          GROUP BY date
        )
      ) AS blocks_by_day USING date
      ORDER BY date ASC
    `, { days });

    return rows.map((row) => ({
      date: dateText(row.date),
      transaction_count: safeCount(row.transaction_count),
      block_count: safeCount(row.block_count),
    }));
  }

  private async getOutputStatisticSeries(days: number): Promise<Array<{ date: string; sum: string }>> {
    const rows = await this.ch.query<{ date?: string; output_total?: string | number }>(`
      SELECT
        toString(toDate(toDateTime(timestamp))) AS date,
        toString(sum(output_total)) AS output_total
      FROM (
        SELECT txid, timestamp, output_total, is_valid
        FROM transactions
        WHERE timestamp >= toUInt32(toUnixTimestamp(now() - toIntervalDay({days:UInt16})))
        ORDER BY txid, _version DESC
        LIMIT 1 BY txid
      )
      WHERE is_valid = 1
      GROUP BY date
      ORDER BY date ASC
    `, { days });

    return rows.map((row) => ({
      date: dateText(row.date),
      sum: zatoshisToFluxString(zatoshiString(row.output_total)),
    }));
  }

  private async getActiveAddressStatisticSeries(days: number): Promise<Array<{ date: string; count: number }>> {
    const rows = await this.ch.query<{ date?: string; count?: string | number }>(`
      SELECT
        toString(toDate(toDateTime(timestamp))) AS date,
        toString(uniqExact(address)) AS count
      FROM (
        SELECT address, txid, timestamp, is_valid
        FROM address_transactions
        WHERE timestamp >= toUInt32(toUnixTimestamp(now() - toIntervalDay({days:UInt16})))
        ORDER BY address, txid, _version DESC
        LIMIT 1 BY address, txid
      )
      WHERE is_valid = 1
      GROUP BY date
      ORDER BY date ASC
    `, { days });

    return rows.map((row) => ({
      date: dateText(row.date),
      count: safeCount(row.count),
    }));
  }

  private async queryPoolRows(range: { start: number; end: number } | { cutoff: number }): Promise<Array<{
    producer?: string | null;
    blocks_found?: string | number;
  }>> {
    if ('cutoff' in range) {
      return this.ch.query<{ producer?: string | null; blocks_found?: string | number }>(`
        SELECT producer, toString(count()) AS blocks_found
        FROM (
          SELECT height, producer, is_valid
          FROM blocks
          WHERE timestamp >= {cutoff:UInt32}
          ORDER BY height, _version DESC
          LIMIT 1 BY height
        )
        WHERE is_valid = 1
        GROUP BY producer
        ORDER BY blocks_found DESC, producer ASC
      `, { cutoff: range.cutoff });
    }

    return this.ch.query<{ producer?: string | null; blocks_found?: string | number }>(`
      SELECT producer, toString(count()) AS blocks_found
      FROM (
        SELECT height, producer, is_valid
        FROM blocks
        WHERE timestamp >= {start:UInt32} AND timestamp <= {end:UInt32}
        ORDER BY height, _version DESC
        LIMIT 1 BY height
      )
      WHERE is_valid = 1
      GROUP BY producer
      ORDER BY blocks_found DESC, producer ASC
    `, { start: range.start, end: range.end });
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

function normalizeAddressList(addresses: string[]): string[] {
  return [...new Set(addresses.map((address) => address.trim()).filter(Boolean))]
    .slice(0, MAX_UTXO_ADDRESSES);
}

function normalizeRangeLimit(raw: number): number {
  if (!Number.isSafeInteger(raw) || raw <= 0) {
    return 10;
  }

  return Math.min(raw, MAX_ADDRESS_TX_LIMIT);
}

function normalizeNonNegativeSafeInteger(raw: number, fallback: number): number {
  return Number.isSafeInteger(raw) && raw >= 0 ? raw : fallback;
}

function parseStatisticDays(raw?: string): number {
  if (raw?.trim().toLowerCase() === 'all') {
    return MAX_STATISTIC_DAYS;
  }

  if (raw === undefined || raw.trim().length === 0) {
    return DEFAULT_STATISTIC_DAYS;
  }

  if (!/^\d+$/.test(raw.trim())) {
    return DEFAULT_STATISTIC_DAYS;
  }

  const parsed = Number(raw.trim());
  if (!Number.isSafeInteger(parsed)) {
    return DEFAULT_STATISTIC_DAYS;
  }

  return Math.min(MAX_STATISTIC_DAYS, Math.max(1, parsed));
}

function safeCount(value: unknown): number {
  const parsed = parseNonNegativeHeightValue(value);
  return parsed ?? 0;
}

function finiteNumber(value: unknown): number {
  return parseFiniteNumber(value) ?? 0;
}

function dateText(value: unknown): string {
  if (typeof value === 'string') {
    return value.slice(0, 10);
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }

  return '';
}

function formatPoolRows(rows: Array<{ producer?: string | null; blocks_found?: string | number }>): PoolStatisticRow[] {
  const pools = rows.map((row) => ({
    name: poolName(row.producer),
    blocks_found: safeCount(row.blocks_found),
  }));
  const total = pools.reduce((sum, pool) => sum + pool.blocks_found, 0);

  return pools.map((pool) => ({
    address: pool.name,
    poolName: pool.name,
    url: null,
    blocks_found: pool.blocks_found,
    percent_total: total > 0 ? roundPercent((pool.blocks_found / total) * 100) : 0,
  }));
}

function poolName(value: string | null | undefined): string {
  const trimmed = value?.trim() ?? '';
  return trimmed.length > 0 ? trimmed : 'Unknown';
}

function roundPercent(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}

function isPresent<T>(value: T | null | undefined): value is T {
  return value !== null && value !== undefined;
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

function parseSignedSafeInteger(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isSafeInteger(value) ? value : null;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (!/^-?\d+$/.test(trimmed)) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function parseFiniteNumber(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizePercentage(value: unknown, height: number, chainHeight: number): number {
  const parsed = parseFiniteNumber(value);
  if (parsed !== null) {
    return clampPercentage(parsed);
  }

  if (chainHeight > 0 && height >= 0) {
    return clampPercentage((height / chainHeight) * 100);
  }

  return 0;
}

function clampPercentage(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }

  return Math.min(100, Math.max(0, value));
}

function parseBooleanFlag(value: unknown): boolean | null {
  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'number') {
    if (value === 1) return true;
    if (value === 0) return false;
    return null;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === '1' || normalized === 'true') {
    return true;
  }

  if (normalized === '0' || normalized === 'false') {
    return false;
  }

  return null;
}

function zatoshiString(value: unknown): string {
  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) && Number.isInteger(value) ? value.toString() : '0';
  }

  if (typeof value !== 'string') {
    return '0';
  }

  const trimmed = value.trim();
  return /^-?\d+$/.test(trimmed) ? trimmed : '0';
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

function normalizeFluxNodeEnvelope(response: unknown): { result: unknown; error: unknown; id: unknown } {
  if (isRecord(response) && 'result' in response) {
    return {
      result: response.result,
      error: 'error' in response ? response.error : null,
      id: 'id' in response ? response.id : null,
    };
  }

  return { result: response, error: null, id: null };
}

function filterFluxNodeResult(result: unknown, filter: string): unknown {
  const filters = filter
    .split(',')
    .map((part) => part.trim().toLowerCase())
    .filter(Boolean);

  if (filters.length === 0) {
    return result;
  }

  if (Array.isArray(result)) {
    return result.filter((node) => filters.some((entry) => fluxNodeMatches(node, entry)));
  }

  if (isRecord(result)) {
    return Object.fromEntries(
      Object.entries(result).filter(([key, node]) => (
        filters.some((entry) => fluxNodeMatches(node, entry, key))
      ))
    );
  }

  return filters.some((entry) => fluxNodeMatches(result, entry)) ? result : [];
}

function fluxNodeMatches(node: unknown, filter: string, key?: string): boolean {
  const collateralFilter = parseCollateralFilter(filter);
  if (collateralFilter) {
    return fluxNodeCollateralMatches(node, collateralFilter, key);
  }

  if (key && key.toLowerCase().includes(filter)) {
    return true;
  }

  if (isRecord(node)) {
    if (fluxNodeOutpoints(node).some((outpoint) => outpoint.toLowerCase().includes(filter))) {
      return true;
    }

    if (fluxNodeIps(node).some((ip) => ip.toLowerCase().includes(filter))) {
      return true;
    }
  }

  return stringifyForSearch(node).includes(filter);
}

function parseCollateralFilter(filter: string): { outpoint: string; txhash: string; outidx: string } | null {
  const match = /^([0-9a-f]{1,64})-(\d+)$/.exec(filter);
  if (!match) {
    return null;
  }

  return {
    outpoint: `${match[1]}-${match[2]}`,
    txhash: match[1],
    outidx: match[2],
  };
}

function fluxNodeCollateralMatches(
  node: unknown,
  filter: { outpoint: string; txhash: string; outidx: string },
  key?: string
): boolean {
  if (key && key.toLowerCase() === filter.outpoint) {
    return true;
  }

  if (!isRecord(node)) {
    return stringValue(node)?.toLowerCase() === filter.outpoint;
  }

  return fluxNodeOutpoints(node).some((outpoint) => outpoint.toLowerCase() === filter.outpoint);
}

function fluxNodeOutpoints(node: Record<string, unknown>): string[] {
  const hashes = [
    node.txhash,
    node.txid,
    node.collateralHash,
    node.collateral_hash,
    node.collateralTxHash,
    node.collateral_txhash,
    node.collateral_txid,
  ].map(stringValue).filter((value): value is string => value !== null);
  const indexes = [
    node.outidx,
    node.vout,
    node.outputIndex,
    node.output_index,
    node.collateralIndex,
    node.collateral_index,
  ].map(stringValue).filter((value): value is string => value !== null);
  const explicit = [
    node.collateral,
    node.outpoint,
    node.vin,
  ].map(stringValue).filter((value): value is string => value !== null);

  return [
    ...explicit,
    ...hashes.flatMap((hash) => indexes.map((index) => `${hash}-${index}`)),
  ];
}

function fluxNodeIps(node: Record<string, unknown>): string[] {
  return [
    node.ip,
    node.addr,
    node.address,
    node.ip_address,
    node.networkAddress,
    node.network_address,
  ].map(stringValue).filter((value): value is string => value !== null);
}

function stringValue(value: unknown): string | null {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }

  if (typeof value === 'bigint') {
    return value.toString();
  }

  return null;
}

function stringifyForSearch(value: unknown): string {
  try {
    return JSON.stringify(value)?.toLowerCase() ?? '';
  } catch {
    return '';
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
