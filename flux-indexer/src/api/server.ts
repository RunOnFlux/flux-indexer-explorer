/**
 * ClickHouse API Server
 *
 * FluxIndexer REST API backed by ClickHouse for Flux PoN blockchain
 */

import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import compression from 'compression';
import path from 'path';
import { ClickHouseConnection } from '../database/connection';
import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { ClickHouseSyncEngine } from '../indexer/sync-engine';
import type { Transaction } from '../types';
import { logger } from '../utils/logger';
import { extractTransactionFromBlock } from '../parsers/block-parser';
import { createInsightCompatibilityRouter } from './insight/router';
import { InsightCompatibilityService } from './insight/service';

export class ClickHouseAPIServer {
  private app: express.Application;
  private server: any;
  private daemonReady = false;
  private static readonly TXID_REGEX = /^[0-9a-fA-F]{1,64}$/;
  private static readonly SATOSHIS_PER_FLUX = 100000000n;

  // Caches
  private statsCache: { data: any | null; timestamp: number } = { data: null, timestamp: 0 };
  private statusCache: { data: any | null; timestamp: number } = { data: null, timestamp: 0 };
  private static readonly STATUS_CACHE_TTL = 30000; // 30s for status (doesn't need to be instant)
  private static readonly STATS_CACHE_TTL = 2000;   // 2s for dashboard stats (matches frontend polling)
  private mempoolAddressCache: { data: Map<string, { balanceDelta: bigint; txCount: number }>; timestamp: number } = {
    data: new Map(),
    timestamp: 0,
  };
  private mempoolAddressCacheInFlight: Promise<Map<string, { balanceDelta: bigint; txCount: number }>> | null = null;
  private static readonly MEMPOOL_ADDRESS_CACHE_TTL = 5000; // 5s (fast enough for UX, low enough RPC load)

  constructor(
    private ch: ClickHouseConnection,
    private rpc: FluxRPCClient,
    private syncEngine: ClickHouseSyncEngine,
    private port: number = 3002
  ) {
    this.app = express();
    this.setupMiddleware();
    this.setupRoutes();
    this.setupErrorHandling();
  }

  private static zatoshisToFluxString(zatoshis: bigint): string {
    const isNegative = zatoshis < 0n;
    const absValue = isNegative ? -zatoshis : zatoshis;

    const whole = absValue / ClickHouseAPIServer.SATOSHIS_PER_FLUX;
    const fractional = absValue % ClickHouseAPIServer.SATOSHIS_PER_FLUX;

    if (fractional === 0n) {
      return `${isNegative ? '-' : ''}${whole.toString()}`;
    }

    const fractionalStr = fractional.toString().padStart(8, '0').replace(/0+$/, '');
    return `${isNegative ? '-' : ''}${whole.toString()}.${fractionalStr}`;
  }

  private static zatoshisToFluxNumber(zatoshis: bigint): number {
    const isNegative = zatoshis < 0n;
    const absValue = isNegative ? -zatoshis : zatoshis;

    const whole = absValue / ClickHouseAPIServer.SATOSHIS_PER_FLUX;
    const fractional = absValue % ClickHouseAPIServer.SATOSHIS_PER_FLUX;

    const value = Number(whole) + (Number(fractional) / 1e8);
    return isNegative ? -value : value;
  }

  private static padFixedString64(value: string): string {
    return value.trim().padStart(64, '0').slice(0, 64);
  }

  private static coerceStringParam(raw: unknown): string | undefined {
    if (raw === undefined || raw === null) return undefined;
    if (Array.isArray(raw)) {
      if (raw.length === 0) return undefined;
      const first = raw[0];
      if (first === undefined || first === null) return undefined;
      return typeof first === 'string' ? first : String(first);
    }
    return typeof raw === 'string' ? raw : String(raw);
  }

  private static parseBoundedInt(
    raw: unknown,
    options: {
      name: string;
      defaultValue?: number;
      min?: number;
      max?: number;
      required?: boolean;
      clamp?: boolean;
    }
  ): { value?: number; error?: string } {
    const rawStr = ClickHouseAPIServer.coerceStringParam(raw);
    const trimmed = rawStr?.trim();
    const isMissing = trimmed === undefined || trimmed === '';

    if (isMissing) {
      if (options.required) {
        return { error: `${options.name} parameter is required` };
      }
      return { value: options.defaultValue };
    }

    if (!/^-?\d+$/.test(trimmed)) {
      return { error: `Invalid ${options.name} (must be an integer)` };
    }

    const parsed = Number(trimmed);
    if (!Number.isSafeInteger(parsed)) {
      return { error: `Invalid ${options.name} (must be a safe integer)` };
    }

    if (options.min !== undefined && parsed < options.min) {
      return { error: `Invalid ${options.name} (must be >= ${options.min})` };
    }

    if (options.max !== undefined && parsed > options.max) {
      if (options.clamp === false) {
        return { error: `Invalid ${options.name} (must be <= ${options.max})` };
      }
      return { value: options.max };
    }

    return { value: parsed };
  }

  private isAddressableUtxoAddress(address: string | null | undefined): address is string {
    return !!address && address !== 'UNKNOWN' && address !== 'SHIELDED_OR_NONSTANDARD';
  }

  private async getMempoolAddressDeltas(): Promise<Map<string, { balanceDelta: bigint; txCount: number }>> {
    const now = Date.now();
    if ((now - this.mempoolAddressCache.timestamp) < ClickHouseAPIServer.MEMPOOL_ADDRESS_CACHE_TTL) {
      return this.mempoolAddressCache.data;
    }

    if (this.mempoolAddressCacheInFlight) {
      return this.mempoolAddressCacheInFlight;
    }

    this.mempoolAddressCacheInFlight = (async () => {
      try {
        const data = await this.computeMempoolAddressDeltas();
        this.mempoolAddressCache = { data, timestamp: Date.now() };
        return data;
      } catch (error: any) {
        logger.debug('Failed to compute mempool address deltas', { error: error?.message || error });
        // Avoid hammering the daemon if it's unavailable.
        this.mempoolAddressCache = { data: new Map(), timestamp: Date.now() };
        return this.mempoolAddressCache.data;
      } finally {
        this.mempoolAddressCacheInFlight = null;
      }
    })();

    return this.mempoolAddressCacheInFlight;
  }

  private async computeMempoolAddressDeltas(): Promise<Map<string, { balanceDelta: bigint; txCount: number }>> {
    const mempoolTxidsResult = await this.rpc.getRawMempool(false);
    const mempoolTxids = Array.isArray(mempoolTxidsResult)
      ? mempoolTxidsResult
      : Object.keys(mempoolTxidsResult || {});

    if (mempoolTxids.length === 0) {
      return new Map();
    }

    const MAX_MEMPOOL_TXS = 5000;
    const txids = mempoolTxids.length > MAX_MEMPOOL_TXS ? mempoolTxids.slice(0, MAX_MEMPOOL_TXS) : mempoolTxids;
    if (mempoolTxids.length > MAX_MEMPOOL_TXS) {
      logger.warn('Mempool too large for full address delta scan, truncating', {
        mempoolSize: mempoolTxids.length,
        max: MAX_MEMPOOL_TXS,
      });
    }

    const mempoolTxs: Transaction[] = [];
    const BATCH_SIZE = 50;

    for (let i = 0; i < txids.length; i += BATCH_SIZE) {
      const batch = txids.slice(i, i + BATCH_SIZE);
      try {
        const results = await this.rpc.batchGetRawTransactions(batch, true);
        for (const result of results) {
          if (typeof result !== 'string') {
            mempoolTxs.push(result);
          }
        }
      } catch {
        // Best-effort fallback: tolerate txs disappearing from the mempool between calls.
        for (const txid of batch) {
          try {
            const result = await this.rpc.getRawTransaction(txid, true);
            if (typeof result !== 'string') {
              mempoolTxs.push(result);
            }
          } catch {
            // ignore missing/evicted mempool txs
          }
        }
      }
    }

    if (mempoolTxs.length === 0) {
      return new Map();
    }

    // Build map of outputs created in the mempool (for same-mempool spends).
    const mempoolOutpointMap = new Map<string, { address: string; value: bigint }>();
    for (const tx of mempoolTxs) {
      const txid = ClickHouseAPIServer.padFixedString64(tx.txid);
      for (const output of tx.vout || []) {
        const address = output.scriptPubKey?.addresses?.[0];
        if (!this.isAddressableUtxoAddress(address)) continue;
        const valueSat = BigInt(Math.round(output.value * 100000000));
        mempoolOutpointMap.set(`${txid}:${output.n}`, { address, value: valueSat });
      }
    }

    // Collect input outpoints that must be resolved from ClickHouse.
    const outpointsToResolve = new Map<string, { txid: string; vout: number }>();
    for (const tx of mempoolTxs) {
      for (const input of tx.vin || []) {
        if (input.coinbase || !input.txid || input.vout === undefined) continue;
        const prevTxid = ClickHouseAPIServer.padFixedString64(input.txid);
        const key = `${prevTxid}:${input.vout}`;
        if (mempoolOutpointMap.has(key)) continue;
        outpointsToResolve.set(key, { txid: prevTxid, vout: input.vout });
      }
    }

    const chOutpointMap = new Map<string, { address: string; value: bigint }>();
    if (outpointsToResolve.size > 0) {
      const outpoints = Array.from(outpointsToResolve.values()).map((o) => [o.txid, o.vout]);
      const rows = await this.ch.query<{
        txid: string;
        vout: number;
        address: string;
        value: string;
      }>(`
        SELECT txid, vout, address, toString(value) as value
        FROM (
          SELECT txid, vout, address, value, spent
          FROM utxos
          WHERE (txid, vout) IN {outpoints:Array(Tuple(FixedString(64), UInt16))}
          ORDER BY txid, vout, version DESC
          LIMIT 1 BY txid, vout
        )
        WHERE spent = 0
      `, { outpoints });

      for (const row of rows) {
        if (!this.isAddressableUtxoAddress(row.address)) continue;
        chOutpointMap.set(`${row.txid}:${row.vout}`, { address: row.address, value: BigInt(row.value) });
      }
    }

    // Aggregate per-address deltas and tx counts.
    const balanceDeltaByAddress = new Map<string, bigint>();
    const txCountByAddress = new Map<string, number>();

    for (const tx of mempoolTxs) {
      const touchedAddresses = new Set<string>();

      for (const output of tx.vout || []) {
        const address = output.scriptPubKey?.addresses?.[0];
        if (!this.isAddressableUtxoAddress(address)) continue;
        const valueSat = BigInt(Math.round(output.value * 100000000));
        balanceDeltaByAddress.set(address, (balanceDeltaByAddress.get(address) ?? BigInt(0)) + valueSat);
        touchedAddresses.add(address);
      }

      for (const input of tx.vin || []) {
        if (input.coinbase || !input.txid || input.vout === undefined) continue;
        const prevTxid = ClickHouseAPIServer.padFixedString64(input.txid);
        const key = `${prevTxid}:${input.vout}`;
        const prevOut = mempoolOutpointMap.get(key) ?? chOutpointMap.get(key);
        if (!prevOut) continue;
        if (!this.isAddressableUtxoAddress(prevOut.address)) continue;
        balanceDeltaByAddress.set(
          prevOut.address,
          (balanceDeltaByAddress.get(prevOut.address) ?? BigInt(0)) - prevOut.value
        );
        touchedAddresses.add(prevOut.address);
      }

      for (const addr of touchedAddresses) {
        txCountByAddress.set(addr, (txCountByAddress.get(addr) ?? 0) + 1);
      }
    }

    const result = new Map<string, { balanceDelta: bigint; txCount: number }>();
    for (const [address, txCount] of txCountByAddress) {
      result.set(address, { balanceDelta: balanceDeltaByAddress.get(address) ?? BigInt(0), txCount });
    }

    return result;
  }

  private setupMiddleware(): void {
    this.app.use(compression({ threshold: 1024, level: 6 }));
    this.app.use(cors());
    this.app.use(express.json());

    this.app.use((req, res, next) => {
      logger.debug(`${req.method} ${req.path}`, { query: req.query });
      next();
    });
  }

  private setupRoutes(): void {
    const insightService = new InsightCompatibilityService(
      this.ch,
      this.rpc,
      this.getMempoolAddressDeltas.bind(this)
    );
    this.app.use('/insight-api', createInsightCompatibilityRouter(insightService));
    this.app.use('/insight-api', (req, res) => {
      res.status(404).json({ status: 404, url: req.originalUrl, error: 'Not found' });
    });

    // Status endpoints
    this.app.get('/api/v1/status', this.getStatus.bind(this));
    this.app.get('/api/v1/sync', this.getSyncStatus.bind(this));

    // Block endpoints
    this.app.get('/api/v1/blocks', this.getBlocks.bind(this));
    this.app.get('/api/v1/blocks/latest', this.getLatestBlocks.bind(this));
    this.app.get('/api/v1/blocks/range', this.getBlocksRange.bind(this));
    this.app.get('/api/v1/blocks/:heightOrHash', this.getBlock.bind(this));

    // Transaction endpoints
    this.app.post('/api/v1/transactions/batch', this.getTransactionsBatch.bind(this));
    this.app.get('/api/v1/transactions/:txid', this.getTransaction.bind(this));

    // Address endpoints
    this.app.get('/api/v1/addresses/:address', this.getAddress.bind(this));
    this.app.get('/api/v1/addresses/:address/transactions', this.getAddressTransactions.bind(this));
    this.app.get('/api/v1/addresses/:address/utxos', this.getAddressUTXOs.bind(this));

    // Rich list
    this.app.get('/api/v1/richlist', this.getRichList.bind(this));

    // Supply stats
    this.app.get('/api/v1/supply', this.getSupplyStats.bind(this));

    // Producers
    this.app.get('/api/v1/producers', this.getProducers.bind(this));
    this.app.get('/api/v1/producers/:identifier', this.getProducer.bind(this));

    // FluxNode endpoints
    this.app.get('/api/v1/nodes', this.getFluxNodes.bind(this));
    this.app.get('/api/v1/nodes/:ip', this.getFluxNodeStatus.bind(this));

    // Network
    this.app.get('/api/v1/network', this.getNetworkInfo.bind(this));
    this.app.get('/api/v1/mempool', this.getMempoolInfo.bind(this));
    this.app.get('/api/v1/stats/dashboard', this.getDashboardStats.bind(this));

    // Analytics (leverages materialized views for instant aggregations)
    this.app.get('/api/v1/analytics/tx-volume', this.getTxVolumeHistory.bind(this));
    this.app.get('/api/v1/analytics/supply-history', this.getSupplyHistory.bind(this));

    // Health check
    this.app.get('/health', (req, res) => {
      res.json({ status: 'ok', backend: 'clickhouse', timestamp: new Date().toISOString() });
    });

    // Serve frontend
    const frontendPath = path.join(__dirname, '../../frontend');
    this.app.use(express.static(frontendPath));

    this.app.get('*', (req, res) => {
      if (req.path.startsWith('/api/') || req.path === '/health') {
        return res.status(404).json({ error: 'Not found' });
      }
      res.sendFile(path.join(frontendPath, 'index.html'));
    });
  }

  public getApp(): express.Application {
    return this.app;
  }

  private setupErrorHandling(): void {
    this.app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      logger.error('API error', { error: err.message, path: req.path });
      res.status(500).json({ error: err.message || 'Internal server error' });
    });
  }

  // ========== Status Endpoints ==========

  private async getStatus(req: Request, res: Response): Promise<void> {
    try {
      const now = Date.now();
      if (this.statusCache.data && (now - this.statusCache.timestamp) < ClickHouseAPIServer.STATUS_CACHE_TTL) {
        res.json(this.statusCache.data);
        return;
      }

      let chainInfo: any = null;
      let networkInfo: any = null;
      try {
        [chainInfo, networkInfo] = await Promise.all([
          this.rpc.getBlockchainInfo(),
          this.rpc.getNetworkInfo(),
        ]);
        this.daemonReady = true;
      } catch (e) {
        // Daemon not ready
      }

      const syncState = await this.ch.queryOne<{
        current_height: number;
        chain_height: number;
        is_syncing: number;
        sync_percentage: number;
      }>(`
        SELECT
          argMax(current_height, updated_at) as current_height,
          argMax(chain_height, updated_at) as chain_height,
          argMax(is_syncing, updated_at) as is_syncing,
          argMax(sync_percentage, updated_at) as sync_percentage
        FROM sync_state
        WHERE id = 1
      `);

      // Use uniqExact() for accurate counts without expensive FINAL
      // This counts unique primary key combinations without full table deduplication
      const blockCount = await this.ch.queryCount('SELECT uniqExact(height) as count FROM blocks WHERE is_valid = 1');
      const txCount = await this.ch.queryCount('SELECT uniqExact(txid, block_height) as count FROM transactions WHERE is_valid = 1');
      // Count unique addresses from aggregated table - use uniqExact() for efficiency
      const addressCount = await this.ch.queryCount('SELECT uniqExact(address) as count FROM address_summary_agg');

      const currentHeight = syncState?.current_height ?? 0;
      const chainHeight = chainInfo?.headers ?? syncState?.chain_height ?? 0;
      const synced = chainHeight > 0 ? currentHeight >= chainHeight : false;
      const percentage = syncState?.sync_percentage ?? (chainHeight > 0 ? (currentHeight / chainHeight) * 100 : 0);

      // Response format expected by frontend (FluxIndexerApiResponse)
      // Note: consensus from RPC is an object {chaintip, nextblock}, frontend expects string "PoN"
      const payload = {
        name: 'FluxIndexer',
        version: '2.0.0',
        network: chainInfo?.chain ?? 'mainnet',
        consensus: 'PoN',  // Flux uses Proof of Node consensus
        indexer: {
          syncing: !synced,
          synced,
          currentHeight,
          chainHeight,
          progress: `${currentHeight}/${chainHeight}`,
          blocksIndexed: blockCount,
          transactionsIndexed: txCount,
          addressesIndexed: addressCount,
          percentage,
          lastSyncTime: new Date().toISOString(),
        },
        daemon: chainInfo ? {
          version: networkInfo?.version?.toString() ?? '0',
          protocolVersion: networkInfo?.protocolversion ?? 0,
          blocks: chainInfo.blocks,
          headers: chainInfo.headers,
          bestBlockHash: chainInfo.bestblockhash,
          difficulty: chainInfo.difficulty,
          chainwork: chainInfo.chainwork ?? '',
          consensus: chainInfo.consensus,  // Keep raw object for daemon section
          connections: networkInfo?.connections ?? 0,
        } : {
          status: 'unavailable',
          version: '0',
          consensus: 'PoN',
        },
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
      };

      this.statusCache = { data: payload, timestamp: now };
      res.json(payload);
    } catch (error: any) {
      logger.error('Failed to get status', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  private async getSyncStatus(req: Request, res: Response): Promise<void> {
    try {
      const syncState = await this.ch.queryOne<{
        current_height: number;
        chain_height: number;
        sync_percentage: number;
        is_syncing: number;
        blocks_per_second: number;
      }>(`
        SELECT
          argMax(current_height, updated_at) as current_height,
          argMax(chain_height, updated_at) as chain_height,
          argMax(sync_percentage, updated_at) as sync_percentage,
          argMax(is_syncing, updated_at) as is_syncing,
          argMax(blocks_per_second, updated_at) as blocks_per_second
        FROM sync_state
        WHERE id = 1
      `);

      const currentHeight = syncState?.current_height ?? 0;
      const chainHeight = syncState?.chain_height ?? 0;
      const percentage = syncState?.sync_percentage ?? 0;
      const isSyncing = (syncState?.is_syncing ?? 0) === 1;

      // Response format expected by frontend dashboard
      res.json({
        indexer: {
          syncing: isSyncing,
          synced: chainHeight > 0 ? currentHeight >= chainHeight : false,
          currentHeight,
          chainHeight,
          progress: `${currentHeight}/${chainHeight}`,
          percentage,
          lastSyncTime: new Date().toISOString(),
        },
        timestamp: new Date().toISOString(),
        // Also include flat format for backwards compatibility
        currentHeight,
        chainHeight,
        syncPercentage: percentage,
        isSyncing,
        blocksPerSecond: syncState?.blocks_per_second ?? 0,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Block Endpoints ==========

  private async getBlocks(req: Request, res: Response): Promise<void> {
    try {
      const limitResult = ClickHouseAPIServer.parseBoundedInt(req.query.limit, {
        name: 'limit',
        defaultValue: 20,
        min: 1,
        max: 100,
      });
      if (limitResult.error) {
        res.status(400).json({ error: limitResult.error });
        return;
      }
      const limit = limitResult.value ?? 20;

      const offsetResult = ClickHouseAPIServer.parseBoundedInt(req.query.offset, {
        name: 'offset',
        defaultValue: 0,
        min: 0,
        max: 5_000_000,
      });
      if (offsetResult.error) {
        res.status(400).json({ error: offsetResult.error });
        return;
      }
      const offset = offsetResult.value ?? 0;

      // Avoid FINAL but still read the latest row per height.
      // Use a height window based on requested offset to keep scans bounded.
      const tip = await this.ch.queryOne<{ height: number }>(`
        SELECT height
        FROM blocks
        WHERE is_valid = 1
        ORDER BY height DESC
        LIMIT 1
      `);
      const tipHeight = tip?.height ?? 0;
      const maxHeight = Math.max(0, tipHeight - offset);
      const minHeight = Math.max(0, maxHeight - (limit + 250));

      const blocks = await this.ch.query<any>(`
        SELECT height, hash, timestamp, tx_count, size, producer, difficulty
        FROM (
          SELECT height, hash, timestamp, tx_count, size, producer, difficulty, is_valid
          FROM blocks
          WHERE height <= {maxHeight:UInt32} AND height > {minHeight:UInt32}
          ORDER BY height DESC, _version DESC
          LIMIT 1 BY height
        )
        WHERE is_valid = 1
        ORDER BY height DESC
        LIMIT ${limit}
      `, { maxHeight, minHeight });

      const totalResult = await this.ch.queryOne<{ count: number }>('SELECT uniqExact(height) as count FROM blocks WHERE is_valid = 1');

      res.json({
        blocks,
        pagination: {
          total: Number(totalResult?.count ?? 0),
          limit,
          offset,
        },
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getLatestBlocks(req: Request, res: Response): Promise<void> {
    try {
      const limitResult = ClickHouseAPIServer.parseBoundedInt(req.query.limit, {
        name: 'limit',
        defaultValue: 10,
        min: 1,
        max: 50,
      });
      if (limitResult.error) {
        res.status(400).json({ error: limitResult.error });
        return;
      }
      const limit = limitResult.value ?? 10;

      const tip = await this.ch.queryOne<{ height: number }>(`
        SELECT height
        FROM blocks
        WHERE is_valid = 1
        ORDER BY height DESC
        LIMIT 1
      `);
      const tipHeight = tip?.height ?? 0;
      const maxHeightBound = tipHeight;
      const minHeightBound = Math.max(0, maxHeightBound - (limit + 250));

      // Step 1: Get latest blocks (latest row per height) without FINAL.
      const blocks = await this.ch.query<any>(`
        SELECT height, hash, timestamp, tx_count, size, producer
        FROM (
          SELECT height, hash, timestamp, tx_count, size, producer, is_valid
          FROM blocks
          WHERE height <= {maxHeight:UInt32} AND height > {minHeight:UInt32}
          ORDER BY height DESC, _version DESC
          LIMIT 1 BY height
        )
        WHERE is_valid = 1
        ORDER BY height DESC
        LIMIT ${limit}
      `, { maxHeight: maxHeightBound, minHeight: minHeightBound });

      if (blocks.length === 0) {
        res.json({ blocks: [] });
        return;
      }

      // Step 2: Get FluxNode transaction counts for ONLY these block heights
      // This is much faster than a full JOIN because we constrain to specific heights
      const minHeight = blocks[blocks.length - 1].height;
      const maxHeight = blocks[0].height;

      const txCounts = await this.ch.query<{
        block_height: number;
        regular_count: string;
        fluxnode_count: string;
        fluxnode_start_count: string;
        fluxnode_confirm_count: string;
      }>(`
        WITH latest AS (
          SELECT txid, block_height, is_fluxnode_tx, fluxnode_type
          FROM (
            SELECT txid, block_height, is_fluxnode_tx, fluxnode_type, is_valid
            FROM transactions
            WHERE block_height >= {minHeight:UInt32} AND block_height <= {maxHeight:UInt32}
            ORDER BY txid, _version DESC
            LIMIT 1 BY txid
          )
          WHERE is_valid = 1
        )
        SELECT
          block_height,
          toString(countIf(is_fluxnode_tx = 0)) as regular_count,
          toString(countIf(is_fluxnode_tx = 1)) as fluxnode_count,
          toString(countIf(fluxnode_type = 2)) as fluxnode_start_count,
          toString(countIf(fluxnode_type = 4)) as fluxnode_confirm_count
        FROM latest
        GROUP BY block_height
      `, { minHeight, maxHeight });

      // Step 3: Get FluxNode tier counts from fluxnode_transactions
      const tierCountsQuery = await this.ch.query<{
        block_height: number;
        cumulus_count: string;
        nimbus_count: string;
        stratus_count: string;
      }>(`
        WITH latest AS (
          SELECT txid, block_height, benchmark_tier, type
          FROM (
            SELECT txid, block_height, benchmark_tier, type, is_valid
            FROM fluxnode_transactions
            WHERE block_height >= {minHeight:UInt32} AND block_height <= {maxHeight:UInt32}
            ORDER BY txid, _version DESC
            LIMIT 1 BY txid
          )
          WHERE is_valid = 1 AND type = 4
        )
        SELECT
          block_height,
          toString(countIf(benchmark_tier = 'CUMULUS')) as cumulus_count,
          toString(countIf(benchmark_tier = 'NIMBUS')) as nimbus_count,
          toString(countIf(benchmark_tier = 'STRATUS')) as stratus_count
        FROM latest
        GROUP BY block_height
      `, { minHeight, maxHeight });

      // Build lookup maps
      const txCountMap = new Map<number, {
        regular: number;
        fluxnode: number;
        start: number;
        confirm: number;
      }>();
      for (const tc of txCounts) {
        txCountMap.set(tc.block_height, {
          regular: parseInt(tc.regular_count) || 0,
          fluxnode: parseInt(tc.fluxnode_count) || 0,
          start: parseInt(tc.fluxnode_start_count) || 0,
          confirm: parseInt(tc.fluxnode_confirm_count) || 0,
        });
      }

      const tierCountsMap = new Map<number, {
        cumulus: number;
        nimbus: number;
        stratus: number;
      }>();
      for (const tc of tierCountsQuery) {
        tierCountsMap.set(tc.block_height, {
          cumulus: parseInt(tc.cumulus_count) || 0,
          nimbus: parseInt(tc.nimbus_count) || 0,
          stratus: parseInt(tc.stratus_count) || 0,
        });
      }

      // Response format expected by frontend (FluxIndexerLatestBlocksResponse)
      res.json({
        blocks: blocks.map(b => {
          const counts = txCountMap.get(b.height) || { regular: 0, fluxnode: 0, start: 0, confirm: 0 };
          const tiers = tierCountsMap.get(b.height) || { cumulus: 0, nimbus: 0, stratus: 0 };
          const unknownCount = counts.confirm - (tiers.cumulus + tiers.nimbus + tiers.stratus);

          return {
            height: b.height,
            hash: b.hash,  // Keep full 64-char hash
            time: b.timestamp,
            timestamp: b.timestamp,
            txCount: b.tx_count,
            tx_count: b.tx_count,
            size: b.size,
            producer: b.producer,
            regularTxCount: counts.regular,
            regular_tx_count: counts.regular,
            nodeConfirmationCount: counts.fluxnode,
            node_confirmation_count: counts.fluxnode,
            tierCounts: {
              cumulus: tiers.cumulus,
              nimbus: tiers.nimbus,
              stratus: tiers.stratus,
              starting: counts.start,
              unknown: Math.max(0, unknownCount)
            },
            tier_counts: {
              cumulus: tiers.cumulus,
              nimbus: tiers.nimbus,
              stratus: tiers.stratus,
              starting: counts.start,
              unknown: Math.max(0, unknownCount)
            },
          };
        }),
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getBlocksRange(req: Request, res: Response): Promise<void> {
    try {
      const startResult = ClickHouseAPIServer.parseBoundedInt(req.query.start, {
        name: 'start',
        required: true,
        min: 0,
        max: 0xffffffff,
        clamp: false,
      });
      if (startResult.error) {
        res.status(400).json({ error: startResult.error });
        return;
      }
      const start = startResult.value as number;

      const endResult = ClickHouseAPIServer.parseBoundedInt(req.query.end, {
        name: 'end',
        required: true,
        min: 0,
        max: 0xffffffff,
        clamp: false,
      });
      if (endResult.error) {
        res.status(400).json({ error: endResult.error });
        return;
      }
      const end = endResult.value as number;

      if (end < start) {
        res.status(400).json({ error: 'end must be >= start' });
        return;
      }

      if (end - start > 100) {
        res.status(400).json({ error: 'Range cannot exceed 100 blocks' });
        return;
      }

      // Return the latest row per height without FINAL.
      const blocks = await this.ch.query<any>(`
        SELECT height, hash, timestamp, tx_count, size, producer, producer_reward, difficulty
        FROM (
          SELECT height, hash, timestamp, tx_count, size, producer, producer_reward, difficulty, is_valid
          FROM blocks
          WHERE height >= {start:UInt32} AND height <= {end:UInt32}
          ORDER BY height ASC, _version DESC
          LIMIT 1 BY height
        )
        WHERE is_valid = 1
        ORDER BY height ASC
      `, { start, end });

      res.json({ blocks });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getBlock(req: Request, res: Response): Promise<void> {
    try {
      const { heightOrHash } = req.params;
      const isHeight = /^\d+$/.test(heightOrHash);

      // Single-row lookups by primary key - pick the latest version without FINAL.
      let block: any;
      if (isHeight) {
        const height = parseInt(heightOrHash, 10);
        if (!Number.isFinite(height) || height < 0 || height > 0xffffffff) {
          res.status(400).json({ error: 'Invalid block height' });
          return;
        }
        block = await this.ch.queryOne<any>(`
          SELECT * FROM blocks
          WHERE height = {height:UInt32}
          ORDER BY _version DESC
          LIMIT 1
        `, { height });
      } else {
        const trimmed = heightOrHash.trim();
        if (!ClickHouseAPIServer.TXID_REGEX.test(trimmed)) {
          res.status(400).json({ error: 'Invalid block hash (must be 1-64 hex chars)' });
          return;
        }
        const hash = trimmed.toLowerCase().padStart(64, '0');
        block = await this.ch.queryOne<any>(`
          SELECT * FROM blocks
          WHERE hash = {hash:FixedString(64)}
          ORDER BY _version DESC
          LIMIT 1
        `, { hash });
      }

      if (!block || block.is_valid !== 1) {
        res.status(404).json({ error: 'Block not found' });
        return;
      }

      // Get transactions for this block - latest row per txid without FINAL.
      const transactions = await this.ch.query<any>(`
        SELECT txid, tx_index, timestamp, input_count, output_count,
               input_total, output_total, fee, is_coinbase,
               is_fluxnode_tx, fluxnode_type, size, is_shielded, version
        FROM (
          SELECT txid, tx_index, timestamp, input_count, output_count,
                 input_total, output_total, fee, is_coinbase,
                 is_fluxnode_tx, fluxnode_type, size, is_shielded, version, is_valid
          FROM transactions
          WHERE block_height = {height:UInt32}
          ORDER BY txid, _version DESC
          LIMIT 1 BY txid
        )
        WHERE is_valid = 1
        ORDER BY tx_index
      `, { height: block.height });

      // Get FluxNode details (tier, IP) for this block's FluxNode transactions
      // Also get first output (to address) for all transactions in parallel
      const [fluxnodeDetails, firstOutputs, firstInputs] = await Promise.all([
        this.ch.query<{
          txid: string;
          benchmark_tier: string;
          ip_address: string;
        }>(`
          SELECT txid, benchmark_tier, ip_address
          FROM (
            SELECT txid, benchmark_tier, ip_address, is_valid
            FROM fluxnode_transactions
            WHERE block_height = {height:UInt32}
            ORDER BY txid, _version DESC
            LIMIT 1 BY txid
          )
          WHERE is_valid = 1
        `, { height: block.height }),
        // Get first output (vout=0) for each transaction - the "to" address
        this.ch.query<{ txid: string; address: string; value: string }>(`
          SELECT txid, address, value
          FROM (
            SELECT txid, address, value
            FROM utxos
            WHERE block_height = {height:UInt32} AND vout = 0
            ORDER BY txid, version DESC
            LIMIT 1 BY txid
          )
        `, { height: block.height }),
        // Get first input for each transaction - find UTXOs spent in this block
        // Use spent_block_height directly instead of subquery for better performance
        this.ch.query<{ spent_txid: string; address: string; value: string }>(`
          SELECT spent_txid, address, value
          FROM (
            SELECT spent_txid, address, value, vout
            FROM (
              SELECT txid, vout, spent_txid, address, value
              FROM utxos
              WHERE spent_block_height = {height:UInt32} AND spent = 1
              ORDER BY txid, vout, version DESC
              LIMIT 1 BY txid, vout
            )
          )
          ORDER BY spent_txid, vout
        `, { height: block.height }),
      ]);

      // Build lookup map for FluxNode details
      const fluxnodeMap = new Map<string, { tier: string; ip: string }>();
      for (const fn of fluxnodeDetails) {
        fluxnodeMap.set(fn.txid, { tier: fn.benchmark_tier, ip: fn.ip_address });
      }

      // Build lookup map for first output (to address)
      const toAddrMap = new Map<string, { address: string; value: string }>();
      for (const out of firstOutputs) {
        toAddrMap.set(out.txid, { address: out.address, value: out.value });
      }

      // Build lookup map for first input (from address) - take first per txid
      const fromAddrMap = new Map<string, { address: string; value: string }>();
      for (const inp of firstInputs) {
        if (!fromAddrMap.has(inp.spent_txid)) {
          fromAddrMap.set(inp.spent_txid, { address: inp.address, value: inp.value });
        }
      }

      // Get next/prev block hashes (latest version per height) without FINAL.
      const [prevBlock, nextBlock] = await Promise.all([
        block.height > 0 ? this.ch.queryOne<{ hash: string }>(`
          SELECT hash
          FROM (
            SELECT hash, is_valid
            FROM blocks
            WHERE height = {h:UInt32}
            ORDER BY _version DESC
            LIMIT 1
          )
          WHERE is_valid = 1
          LIMIT 1
        `, { h: block.height - 1 }) : null,
        this.ch.queryOne<{ hash: string }>(`
          SELECT hash
          FROM (
            SELECT hash, is_valid
            FROM blocks
            WHERE height = {h:UInt32}
            ORDER BY _version DESC
            LIMIT 1
          )
          WHERE is_valid = 1
          LIMIT 1
        `, { h: block.height + 1 }),
      ]);

      // Get current chain height for confirmations
      const chainHeight = await this.ch.queryOne<{ h: number }>(`
        SELECT max(height) as h FROM blocks WHERE is_valid = 1
      `);

      // Calculate tx summary and tier counts
      let coinbaseCount = 0;
      let fluxnodeStartCount = 0;
      let fluxnodeConfirmCount = 0;
      let fluxnodeOtherCount = 0;
      const tierCounts = { cumulus: 0, nimbus: 0, stratus: 0, starting: 0, unknown: 0 };

      const txDetails = transactions.map((tx: any, idx: number) => {
        // txid is already 64 chars from FixedString - don't strip leading zeros
        const txid = tx.txid;
        let kind: 'coinbase' | 'transfer' | 'fluxnode_start' | 'fluxnode_confirm' | 'fluxnode_other' = 'transfer';

        // Get FluxNode details from lookup map
        const fnDetails = fluxnodeMap.get(tx.txid);
        let fluxnodeTier: string | null = fnDetails?.tier || null;
        const fluxnodeIp: string | null = fnDetails?.ip || null;

        if (tx.is_coinbase === 1) {
          kind = 'coinbase';
          coinbaseCount++;
        } else if (tx.is_fluxnode_tx === 1) {
          if (tx.fluxnode_type === 1 || tx.fluxnode_type === 2) {
            kind = 'fluxnode_start';
            fluxnodeStartCount++;
            tierCounts.starting++;
          } else if (tx.fluxnode_type === 4) {
            kind = 'fluxnode_confirm';
            fluxnodeConfirmCount++;
            // Count by tier for type 4 (confirm) transactions
            if (fluxnodeTier) {
              const tierLower = fluxnodeTier.toLowerCase();
              if (tierLower === 'cumulus') tierCounts.cumulus++;
              else if (tierLower === 'nimbus') tierCounts.nimbus++;
              else if (tierLower === 'stratus') tierCounts.stratus++;
              else tierCounts.unknown++;
            } else {
              tierCounts.unknown++;
            }
          } else {
            kind = 'fluxnode_other';
            fluxnodeOtherCount++;
          }
        }

        // Get from/to addresses for display
        const fromInfo = fromAddrMap.get(tx.txid);
        const toInfo = toAddrMap.get(tx.txid);
        const fromAddr = fromInfo?.address && fromInfo.address !== 'SHIELDED_OR_NONSTANDARD' ? fromInfo.address : null;
        const toAddr = toInfo?.address && toInfo.address !== 'SHIELDED_OR_NONSTANDARD' ? toInfo.address : null;

        return {
          txid,
          order: tx.tx_index ?? idx,
          kind,
          isCoinbase: tx.is_coinbase === 1,
          fluxnodeType: tx.is_fluxnode_tx === 1 ? tx.fluxnode_type : null,
          fluxnodeTier,
          fluxnodeIp,
          valueSat: Number(tx.output_total),
          value: Number(tx.output_total) / 1e8,
          valueInSat: Number(tx.input_total),
          valueIn: Number(tx.input_total) / 1e8,
          feeSat: Number(tx.fee),
          fee: Number(tx.fee) / 1e8,
          size: tx.size,
          version: tx.version,
          isShielded: tx.is_shielded === 1,
          // Include from/to addresses to avoid separate batch API call
          fromAddr,
          toAddr,
        };
      });

      // Sort transactions by type: coinbase first, then transfers, then fluxnode transactions
      // This provides logical grouping for block visualization
      const kindOrder: Record<string, number> = {
        coinbase: 0,
        transfer: 1,
        fluxnode_start: 2,
        fluxnode_confirm: 3,
        fluxnode_other: 4,
      };
      txDetails.sort((a, b) => {
        const orderA = kindOrder[a.kind] ?? 5;
        const orderB = kindOrder[b.kind] ?? 5;
        if (orderA !== orderB) return orderA - orderB;
        // Within same type, preserve original block order
        return a.order - b.order;
      });

      // Response format expected by frontend (FluxIndexerBlockResponse)
      res.json({
        hash: block.hash,  // Keep full 64-char hash
        height: block.height,
        size: block.size,
        version: block.version,
        merkleRoot: block.merkle_root,  // Keep full 64-char hash
        time: block.timestamp,
        nonce: block.nonce,
        bits: block.bits,
        difficulty: block.difficulty?.toString() ?? '0',
        chainWork: block.chainwork,
        confirmations: (chainHeight?.h ?? block.height) - block.height + 1,
        previousBlockHash: prevBlock?.hash ?? null,  // Keep full 64-char hash
        nextBlockHash: nextBlock?.hash ?? null,  // Keep full 64-char hash
        reward: block.producer_reward ? block.producer_reward.toString() : '0',
        rewardFlux: block.producer_reward
          ? ClickHouseAPIServer.zatoshisToFluxNumber(BigInt(block.producer_reward.toString()))
          : 0,
        txCount: block.tx_count,
        producer: block.producer || null,
        // Transaction IDs array (for backward compat)
        tx: transactions.map((t: any) => t.txid),
        // Transaction details
        txDetails,
        txSummary: {
          total: transactions.length,
          regular: transactions.length - coinbaseCount - fluxnodeStartCount - fluxnodeConfirmCount - fluxnodeOtherCount,
          coinbase: coinbaseCount,
          transfers: transactions.length - coinbaseCount - fluxnodeStartCount - fluxnodeConfirmCount - fluxnodeOtherCount,
          fluxnodeStart: fluxnodeStartCount,
          fluxnodeConfirm: fluxnodeConfirmCount,
          fluxnodeOther: fluxnodeOtherCount,
          fluxnodeTotal: fluxnodeStartCount + fluxnodeConfirmCount + fluxnodeOtherCount,
          tierCounts,
        },
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Transaction Endpoints ==========

  private async getTransactionsBatch(req: Request, res: Response): Promise<void> {
    try {
      const { txids } = req.body;

      if (!Array.isArray(txids) || txids.length === 0) {
        res.status(400).json({ error: 'txids array is required' });
        return;
      }

      if (txids.length > 100) {
        res.status(400).json({ error: 'Maximum 100 transactions per batch' });
        return;
      }

      const paddedTxids: string[] = [];
      for (const [idx, rawTxid] of txids.entries()) {
        if (typeof rawTxid !== 'string') {
          res.status(400).json({ error: `Invalid txid at index ${idx} (must be a string)` });
          return;
        }
        const trimmed = rawTxid.trim();
        if (!ClickHouseAPIServer.TXID_REGEX.test(trimmed)) {
          res.status(400).json({ error: `Invalid txid at index ${idx} (must be 1-64 hex chars)` });
          return;
        }
        paddedTxids.push(trimmed.toLowerCase().padStart(64, '0'));
      }
      const uniqueTxids = Array.from(new Set(paddedTxids));

      // Don't use FINAL for targeted IN lookups - pick latest row per txid.
      const transactions = await this.ch.query<any>(`
        SELECT txid, block_height, timestamp, input_count, output_count,
               input_total, output_total, fee, is_coinbase, is_shielded
        FROM (
          SELECT txid, block_height, timestamp, input_count, output_count,
                 input_total, output_total, fee, is_coinbase, is_shielded, is_valid
          FROM transactions
          WHERE txid IN {txids:Array(FixedString(64))}
          ORDER BY txid, _version DESC
          LIMIT 1 BY txid
        )
        WHERE is_valid = 1
      `, { txids: uniqueTxids });

      // Get outputs (first output per transaction for display)
      const outputs = await this.ch.query<{ txid: string; address: string; value: string }>(`
        SELECT txid, address, value
        FROM (
          SELECT txid, address, value
          FROM utxos
          WHERE txid IN {txids:Array(FixedString(64))} AND vout = 0
          ORDER BY txid, version DESC
          LIMIT 1 BY txid
        )
      `, { txids: uniqueTxids });
      const outputMap = new Map(outputs.map(o => [o.txid, o]));

      // Get inputs (first input per transaction - the spending UTXO)
      const inputs = await this.ch.query<{ spent_txid: string; address: string; value: string }>(`
        SELECT spent_txid, address, value
        FROM (
          SELECT spent_txid, address, value, vout
          FROM (
            SELECT txid, vout, spent_txid, address, value
            FROM utxos
            WHERE spent_txid IN {txids:Array(FixedString(64))}
            ORDER BY txid, vout, version DESC
            LIMIT 1 BY txid, vout
          )
        )
        ORDER BY spent_txid, vout
      `, { txids: uniqueTxids });
      // Group by spent_txid, take first for each
      const inputMap = new Map<string, { address: string; value: string }>();
      for (const inp of inputs) {
        if (!inputMap.has(inp.spent_txid)) {
          inputMap.set(inp.spent_txid, inp);
        }
      }

      // Build a map of txid -> transaction data for order-preserving lookup
      const txMap = new Map<string, any>();
      for (const tx of transactions) {
        txMap.set(tx.txid, tx);
      }

      // Return transactions in the same order as the input txids array
      const orderedResults = paddedTxids.map(paddedTxid => {
        const tx = txMap.get(paddedTxid);
        if (!tx) {
          // Transaction not found - return placeholder
          return {
            txid: paddedTxid,  // Keep full 64-char txid
            error: 'not_found',
          };
        }

        // Keep full 64-char txid - don't strip leading zeros
        const txidFull = tx.txid;
        const output = outputMap.get(tx.txid);
        const input = inputMap.get(tx.txid);

        // Build minimal vin/vout for display purposes
        const vin = tx.is_coinbase === 1
          ? [{ coinbase: 'coinbase', n: 0 }]
          : input
            ? [{ addr: input.address, value: input.value, n: 0 }]
            : [];

        const vout = output
          ? [{
              value: output.value,
              n: 0,
              scriptPubKey: {
                addresses: output.address && output.address !== 'SHIELDED_OR_NONSTANDARD' ? [output.address] : [],
              },
            }]
          : [];

        return {
          txid: txidFull,
          block_height: tx.block_height,
          blockheight: tx.block_height,
          timestamp: tx.timestamp,
          time: tx.timestamp,
          input_count: tx.input_count,
          output_count: tx.output_count,
          valueIn: tx.input_total?.toString() || '0',
          valueOut: tx.output_total?.toString() || '0',
          fees: tx.fee?.toString() || '0',
          is_coinbase: tx.is_coinbase,
          is_shielded: tx.is_shielded,
          vin,
          vout,
        };
      });

      res.json({ transactions: orderedResults });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getTransaction(req: Request, res: Response): Promise<void> {
    try {
      const { txid } = req.params;
      const includeHex = req.query.includeHex === 'true';
      const trimmedTxid = txid.trim();
      if (!ClickHouseAPIServer.TXID_REGEX.test(trimmedTxid)) {
        res.status(400).json({ error: 'Invalid txid (must be 1-64 hex chars)' });
        return;
      }
      const paddedTxid = trimmedTxid.toLowerCase().padStart(64, '0');

      // Direct lookup - pick latest row per txid without FINAL.
      const tx = await this.ch.queryOne<any>(`
        SELECT * FROM transactions
        WHERE txid = {txid:FixedString(64)}
        ORDER BY _version DESC
        LIMIT 1
      `, { txid: paddedTxid });

      if (!tx || tx.is_valid !== 1) {
        res.status(404).json({ error: 'Transaction not found' });
        return;
      }

      // Get outputs (UTXOs) for this transaction
      // Don't use FINAL for targeted lookups - it processes entire table before filtering
      // For specific txid lookups, duplicates are rare and acceptable vs OOM risk
      const outputs = await this.ch.query<any>(`
        SELECT vout, address, value, script_type, script_pubkey, spent, spent_txid, spent_block_height
        FROM (
          SELECT vout, address, value, script_type, script_pubkey, spent, spent_txid, spent_block_height
          FROM utxos
          WHERE txid = {txid:FixedString(64)}
          ORDER BY vout, version DESC
          LIMIT 1 BY vout
        )
        ORDER BY vout
      `, { txid: paddedTxid });

      // Get inputs by finding UTXOs that were spent by this transaction
      const inputs = await this.ch.query<any>(`
        SELECT txid as prev_txid, vout as prev_vout, address, value, script_type
        FROM (
          SELECT txid, vout, address, value, script_type
          FROM utxos
          WHERE spent_txid = {txid:FixedString(64)}
          ORDER BY txid, vout, version DESC
          LIMIT 1 BY txid, vout
        )
        ORDER BY prev_vout
      `, { txid: paddedTxid });

      // Get block info for confirmations
      const block = await this.ch.queryOne<{ hash: string; timestamp: number }>(`
        SELECT hash, timestamp
        FROM (
          SELECT hash, timestamp, is_valid
          FROM blocks
          WHERE height = {h:UInt32}
          ORDER BY _version DESC
          LIMIT 1
        )
        WHERE is_valid = 1
        LIMIT 1
      `, { h: tx.block_height });

      const chainHeight = await this.ch.queryOne<{ h: number }>(`
        SELECT max(height) as h FROM blocks WHERE is_valid = 1
      `);

      // Build vin array (format expected by frontend)
      const vin = tx.is_coinbase === 1
        ? [{ coinbase: 'coinbase', sequence: 0xffffffff, n: 0 }]
        : inputs.map((inp: any, idx: number) => ({
            txid: inp.prev_txid,  // Keep full 64-char txid
            vout: inp.prev_vout,
            sequence: 0xffffffff,
            n: idx,
            scriptSig: { hex: '', asm: '' },
            addresses: inp.address ? [inp.address] : [],
            value: inp.value.toString(),
          }));

      // Build vout array (format expected by frontend)
      const vout = outputs.map((out: any) => ({
        value: out.value.toString(),
        n: out.vout,
        scriptPubKey: {
          hex: out.script_pubkey || '',
          asm: '',
          addresses: out.address && out.address !== 'SHIELDED_OR_NONSTANDARD' ? [out.address] : [],
          type: out.script_type,
        },
        spentTxId: out.spent === 1 ? out.spent_txid : undefined,  // Keep full 64-char txid
        spentIndex: out.spent === 1 ? 0 : undefined,
        spentHeight: out.spent === 1 ? out.spent_block_height : undefined,
      }));

      // Response format expected by frontend (FluxIndexerTransactionResponse)
      const response: any = {
        txid: tx.txid,  // Keep full 64-char txid
        version: tx.version,
        lockTime: tx.locktime,
        vin,
        vout,
        blockHash: block?.hash ?? null,  // Keep full 64-char hash
        blockHeight: tx.block_height,
        confirmations: (chainHeight?.h ?? tx.block_height) - tx.block_height + 1,
        blockTime: block?.timestamp ?? tx.timestamp,
        value: tx.output_total.toString(),
        size: tx.size,
        vsize: tx.vsize || tx.size,
        valueIn: tx.input_total.toString(),
        fees: tx.fee.toString(),
      };

      // Add FluxNode metadata from indexed table (avoids fragile frontend hex parsing).
      if (tx.is_fluxnode_tx === 1) {
        const fluxnodeTx = await this.ch.queryOne<{
          type: number;
          collateral_hash: string;
          collateral_index: number;
          ip_address: string;
          public_key: string;
          signature: string;
          p2sh_address: string;
          benchmark_tier: string;
        }>(`
          SELECT type, collateral_hash, collateral_index, ip_address, public_key, signature, p2sh_address, benchmark_tier
          FROM (
            SELECT type, collateral_hash, collateral_index, ip_address, public_key, signature, p2sh_address, benchmark_tier, is_valid
            FROM fluxnode_transactions
            WHERE txid = {txid:FixedString(64)}
            ORDER BY _version DESC
            LIMIT 1
          )
          WHERE is_valid = 1
          LIMIT 1
        `, { txid: paddedTxid });

        const nType = fluxnodeTx?.type ?? (tx.fluxnode_type ?? null);
        response.nType = typeof nType === 'number' ? nType : null;

        const collateralHash = fluxnodeTx?.collateral_hash?.trim() || '';
        if (collateralHash) {
          response.collateralOutputHash = collateralHash;
          response.collateralOutputIndex = Number(fluxnodeTx?.collateral_index ?? 0);
        }

        const benchmarkTier = fluxnodeTx?.benchmark_tier?.trim() || '';
        response.benchmarkTier = benchmarkTier || null;

        const ip = fluxnodeTx?.ip_address?.trim() || '';
        response.ip = ip || null;

        const pubKey = fluxnodeTx?.public_key?.trim() || '';
        response.fluxnodePubKey = pubKey || null;

        const sig = fluxnodeTx?.signature?.trim() || '';
        response.sig = sig || null;

        const p2shAddress = fluxnodeTx?.p2sh_address?.trim() || '';
        response.p2shAddress = p2shAddress || null;
      }

      // Add hex if requested (fetch from daemon via RPC)
      if (includeHex) {
        try {
          // verbose=false returns raw hex string instead of decoded object
          // For mined txs, this can fail when txindex=0 (Flux daemon does not support the blockhash parameter)
          const rawTx = await this.rpc.getRawTransaction(paddedTxid, false);
          response.hex = typeof rawTx === 'string' ? rawTx : '';
        } catch (err: any) {
          // Fallback: Extract from raw block hex instead (works with txindex=0)
          const blockhash = block?.hash?.trim();
          if (blockhash) {
            try {
              logger.debug('Extracting tx hex from block', { txid: req.params.txid, blockhash });
              const rawBlockHex = await this.rpc.getBlock(blockhash, 0) as unknown as string;
              const txHex = extractTransactionFromBlock(rawBlockHex, paddedTxid, tx.block_height);

              if (txHex && txHex.length > 0) {
                response.hex = txHex;
                logger.info('Extracted transaction hex from block', {
                  txid: req.params.txid,
                  block: blockhash,
                  size: Math.floor(txHex.length / 2)
                });
              } else {
                logger.warn('Transaction not found in block hex', { txid: req.params.txid, block: blockhash });
                response.hex = '';
              }
            } catch (blockErr: any) {
              logger.warn('Failed to extract transaction from block', {
                txid: req.params.txid,
                block: blockhash,
                error: blockErr?.message || blockErr
              });
              response.hex = '';
            }
          } else {
            logger.warn('Failed to fetch raw tx hex (no block hash)', {
              txid: req.params.txid,
              error: err?.message || err
            });
            response.hex = '';
          }
        }
      }

      res.json(response);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Address Endpoints ==========

  private async getAddress(req: Request, res: Response): Promise<void> {
    try {
      const { address } = req.params;

      // Query address summary and live FluxNode data in parallel
      const [summary, fluxnodeData, mempoolDeltas] = await Promise.all([
        // Address balance/tx data from aggregated table
        this.ch.queryOne<any>(`
          SELECT
            address,
            sumMerge(balance) AS balance,
            sumMerge(tx_count) AS tx_count,
            sumMerge(received_total) AS received_total,
            sumMerge(sent_total) AS sent_total,
            minMerge(first_seen) AS first_seen,
            maxMerge(last_activity) AS last_activity
          FROM address_summary_agg
          WHERE address = {address:String}
          GROUP BY address
        `, { address }),
        // Live FluxNode counts from separate table
        this.ch.queryOne<any>(`
          SELECT cumulus_count, nimbus_count, stratus_count, total_collateral
          FROM live_fluxnodes FINAL
          WHERE address = {address:String}
        `, { address }),
        this.getMempoolAddressDeltas(),
      ]);

      const mempoolDelta = mempoolDeltas.get(address) ?? { balanceDelta: BigInt(0), txCount: 0 };

      // Response format expected by frontend (FluxIndexerAddressResponse)
      // Values as strings (satoshis) for precision
      res.json({
        address,
        balance: summary ? summary.balance.toString() : '0',
        totalReceived: summary ? summary.received_total.toString() : '0',
        totalSent: summary ? summary.sent_total.toString() : '0',
        unconfirmedBalance: mempoolDelta.balanceDelta.toString(),
        unconfirmedTxs: mempoolDelta.txCount,
        txs: summary ? summary.tx_count : 0,
        // Additional fields for convenience
        balanceFlux: summary ? ClickHouseAPIServer.zatoshisToFluxNumber(BigInt(summary.balance.toString())) : 0,
        txCount: summary ? summary.tx_count : 0,
        firstSeen: summary ? summary.first_seen : null,
        lastActivity: summary ? summary.last_activity : null,
        // FluxNode counts from live_fluxnodes table (flat structure for frontend compatibility)
        cumulusCount: fluxnodeData?.cumulus_count || 0,
        nimbusCount: fluxnodeData?.nimbus_count || 0,
        stratusCount: fluxnodeData?.stratus_count || 0,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getAddressTransactions(req: Request, res: Response): Promise<void> {
    try {
      const { address } = req.params;
      // Allow higher limit for CSV exports (up to 10000), default 25 for normal pagination
      const limitResult = ClickHouseAPIServer.parseBoundedInt(req.query.limit, {
        name: 'limit',
        defaultValue: 25,
        min: 1,
        max: 10000,
      });
      if (limitResult.error) {
        res.status(400).json({ error: limitResult.error });
        return;
      }
      const limit = limitResult.value ?? 25;

      const offsetResult = ClickHouseAPIServer.parseBoundedInt(req.query.offset, {
        name: 'offset',
        defaultValue: 0,
        min: 0,
        max: 100000,
        clamp: false,
      });
      if (offsetResult.error) {
        res.status(400).json({ error: offsetResult.error });
        return;
      }
      const offset = offsetResult.value ?? 0;

      // Cursor-based pagination (efficient for ClickHouse - no OFFSET scanning)
      const cursorHeightResult = ClickHouseAPIServer.parseBoundedInt(req.query.cursorHeight, {
        name: 'cursorHeight',
        min: 0,
        max: 0xffffffff,
        clamp: false,
      });
      if (cursorHeightResult.error) {
        res.status(400).json({ error: cursorHeightResult.error });
        return;
      }
      const cursorHeight = cursorHeightResult.value;

      const cursorTxIndexResult = ClickHouseAPIServer.parseBoundedInt(req.query.cursorTxIndex, {
        name: 'cursorTxIndex',
        min: 0,
        max: 65535,
      });
      if (cursorTxIndexResult.error) {
        res.status(400).json({ error: cursorTxIndexResult.error });
        return;
      }
      const cursorTxIndex = cursorTxIndexResult.value;

      const cursorTxidRaw = ClickHouseAPIServer.coerceStringParam(req.query.cursorTxid);
      const cursorTxid = cursorTxidRaw?.trim() || undefined;
      if (cursorTxid !== undefined && !ClickHouseAPIServer.TXID_REGEX.test(cursorTxid)) {
        res.status(400).json({ error: 'Invalid cursorTxid (must be 1-64 hex chars)' });
        return;
      }

      // Timestamp filtering for date range exports
      const fromTimestampResult = ClickHouseAPIServer.parseBoundedInt(req.query.fromTimestamp, {
        name: 'fromTimestamp',
        min: 0,
        max: 0xffffffff,
        clamp: false,
      });
      if (fromTimestampResult.error) {
        res.status(400).json({ error: fromTimestampResult.error });
        return;
      }
      const fromTimestamp = fromTimestampResult.value;

      const toTimestampResult = ClickHouseAPIServer.parseBoundedInt(req.query.toTimestamp, {
        name: 'toTimestamp',
        min: 0,
        max: 0xffffffff,
        clamp: false,
      });
      if (toTimestampResult.error) {
        res.status(400).json({ error: toTimestampResult.error });
        return;
      }
      const toTimestamp = toTimestampResult.value;

      if (fromTimestamp !== undefined && toTimestamp !== undefined && fromTimestamp > toTimestamp) {
        res.status(400).json({ error: 'fromTimestamp must be <= toTimestamp' });
        return;
      }

      const params: Record<string, any> = { address };
      const resultFilterParts: string[] = [];
      const filteredCountFilterParts: string[] = [];

      if (cursorHeight !== undefined && cursorTxid && cursorTxIndex !== undefined) {
        // Cursor-based: get rows after the cursor position
        // ORDER BY (address, block_height DESC, tx_index, txid) matches our schema
        resultFilterParts.push('(block_height < {cursorHeight:UInt32} OR (block_height = {cursorHeight:UInt32} AND (tx_index > {cursorTxIndex:UInt16} OR (tx_index = {cursorTxIndex:UInt16} AND txid > {cursorTxid:FixedString(64)}))))');
        params.cursorHeight = cursorHeight;
        params.cursorTxIndex = Math.max(0, Math.min(65535, cursorTxIndex));
        params.cursorTxid = cursorTxid.padStart(64, '0');
      } else if (cursorHeight !== undefined && cursorTxid) {
        // Backwards-compatible cursor: fall back to txid-only when tx_index is not provided.
        // This can skip/duplicate rows when multiple txs exist at the same height.
        resultFilterParts.push('(block_height < {cursorHeight:UInt32} OR (block_height = {cursorHeight:UInt32} AND txid > {cursorTxid:FixedString(64)}))');
        params.cursorHeight = cursorHeight;
        params.cursorTxid = cursorTxid.padStart(64, '0');
      }

      // Add timestamp range filtering
      if (fromTimestamp !== undefined) {
        resultFilterParts.push('timestamp >= {fromTimestamp:UInt32}');
        filteredCountFilterParts.push('timestamp >= {fromTimestamp:UInt32}');
        params.fromTimestamp = fromTimestamp;
      }
      if (toTimestamp !== undefined) {
        resultFilterParts.push('timestamp <= {toTimestamp:UInt32}');
        filteredCountFilterParts.push('timestamp <= {toTimestamp:UInt32}');
        params.toTimestamp = toTimestamp;
      }

      const resultWhereClause = resultFilterParts.length > 0 ? resultFilterParts.join(' AND ') : '1';
      const filteredCountWhereClause = filteredCountFilterParts.length > 0 ? filteredCountFilterParts.join(' AND ') : '1';

      // Step 1: Get address transactions - pick latest row per (address, txid) without FINAL.
      const addressTxs = await this.ch.query<any>(`
        SELECT txid, block_height, block_hash, tx_index, timestamp,
               direction, received_value, sent_value, is_coinbase
        FROM (
          SELECT txid, block_height, block_hash, tx_index, timestamp,
                 direction, received_value, sent_value, is_coinbase
          FROM (
            SELECT txid, block_height, block_hash, tx_index, timestamp,
                   direction, received_value, sent_value, is_coinbase, is_valid
            FROM address_transactions
            WHERE address = {address:String}
            ORDER BY txid, _version DESC
            LIMIT 1 BY txid
          )
          WHERE is_valid = 1
        )
        WHERE ${resultWhereClause}
        ORDER BY block_height DESC, tx_index ASC, txid ASC
        LIMIT ${limit + 1}
        ${cursorHeight === undefined && offset > 0 ? `OFFSET ${offset}` : ''}
      `, params);

      // Check for more results
      const hasMore = addressTxs.length > limit;
      const resultTxs = hasMore ? addressTxs.slice(0, limit) : addressTxs;

      // Step 2: Get fee/total data for just these specific transactions (targeted lookup)
      let txDataMap = new Map<string, { fee: string; input_total: string; output_total: string }>();
      if (resultTxs.length > 0) {
        const txids = resultTxs.map((tx: any) => tx.txid);
        const txData = await this.ch.query<any>(`
          SELECT txid, fee, input_total, output_total
          FROM (
            SELECT txid, fee, input_total, output_total, is_valid
            FROM transactions
            WHERE txid IN {txids:Array(FixedString(64))}
            ORDER BY txid, _version DESC
            LIMIT 1 BY txid
          )
          WHERE is_valid = 1
        `, { txids });
        for (const td of txData) {
          txDataMap.set(td.txid, { fee: td.fee, input_total: td.input_total, output_total: td.output_total });
        }
      }

      // Get current chain height for confirmations calculation
      const chainHeight = await this.ch.queryOne<{ h: number }>(`
        SELECT max(height) as h FROM blocks WHERE is_valid = 1
      `);
      const currentHeight = chainHeight?.h ?? 0;

      // Merge the data
      const transactions = resultTxs.map((at: any) => {
        const td = txDataMap.get(at.txid) || { fee: '0', input_total: '0', output_total: '0' };
        return { ...at, fee: td.fee, input_total: td.input_total, output_total: td.output_total };
      });

      // Get total count (unfiltered) and filtered count (with timestamp range)
      const [totalResult, filteredResult] = await Promise.all([
        this.ch.queryOne<{ count: number }>(`
          SELECT count() as count
          FROM (
            SELECT txid
            FROM (
              SELECT txid, is_valid
              FROM address_transactions
              WHERE address = {address:String}
              ORDER BY txid, _version DESC
              LIMIT 1 BY txid
            )
            WHERE is_valid = 1
          )
        `, { address }),
        this.ch.queryOne<{ count: number }>(`
          SELECT count() as count
          FROM (
            SELECT txid, timestamp
            FROM (
              SELECT txid, timestamp, is_valid
              FROM address_transactions
              WHERE address = {address:String}
              ORDER BY txid, _version DESC
              LIMIT 1 BY txid
            )
            WHERE is_valid = 1
          )
          WHERE ${filteredCountWhereClause}
        `, params),
      ]);

      // Build next cursor if there are more results
      let nextCursor: { height: number; txIndex: number; txid: string } | undefined;
      if (hasMore && transactions.length > 0) {
        const lastTx = transactions[transactions.length - 1];
        nextCursor = {
          height: lastTx.block_height,
          txIndex: lastTx.tx_index,
          txid: lastTx.txid,  // Keep full 64-char txid
        };
      }

      // Response format expected by frontend (FluxIndexerAddressTransactionsResponse)
      res.json({
        address,
        transactions: transactions.map((tx: any) => {
          const receivedValue = tx.received_value?.toString() ?? '0';
          const sentValue = tx.sent_value?.toString() ?? '0';
          const receivedBig = BigInt(receivedValue);
          const sentBig = BigInt(sentValue);
          const fee = tx.fee?.toString() ?? '0';
          const inputTotal = tx.input_total?.toString() ?? '0';
          const outputTotal = tx.output_total?.toString() ?? '0';
          const outputBig = BigInt(outputTotal);

          // Net value for display - the actual difference (what left or entered the address)
          const isSent = sentBig > receivedBig;
          const netValue = isSent
            ? (sentBig - receivedBig).toString()  // Amount that left this address
            : receivedBig > sentBig
              ? (receivedBig - sentBig).toString()  // Amount that entered this address
              : '0';

          // For sent transactions, "sent to others" = total outputs minus what came back to this address
          // This excludes change that returned to the sender
          const toOthersValue = isSent
            ? (outputBig - receivedBig > BigInt(0) ? (outputBig - receivedBig).toString() : '0')
            : '0';

          return {
            txid: tx.txid,  // Keep full 64-char txid
            blockHeight: tx.block_height,
            timestamp: tx.timestamp,
            blockHash: tx.block_hash,  // Keep full 64-char hash
            confirmations: currentHeight - tx.block_height + 1,
            direction: tx.direction || (isSent ? 'sent' : 'received'),
            value: netValue,
            receivedValue,
            sentValue,
            feeValue: fee,
            toOthersValue,
            inputTotal,
            outputTotal,
            fromAddresses: [],
            fromAddressCount: 0,
            toAddresses: [],
            toAddressCount: 0,
            // True self-transfer: address both sends AND receives, but nothing goes to other addresses
            // (all outputs return to the same address, only fee is lost)
            selfTransfer: receivedBig > BigInt(0) && sentBig > BigInt(0) && (outputBig - receivedBig) <= BigInt(0),
            isCoinbase: tx.is_coinbase === 1,
          };
        }),
        total: Number(totalResult?.count ?? 0),
        filteredTotal: Number(filteredResult?.count ?? totalResult?.count ?? 0),
        limit,
        offset: cursorHeight !== undefined ? 0 : offset,
        nextCursor,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getAddressUTXOs(req: Request, res: Response): Promise<void> {
    try {
      const { address } = req.params;

      // Return latest row per outpoint without FINAL, then filter unspent.
      const utxos = await this.ch.query<any>(`
        SELECT txid, vout, value, script_type, block_height
        FROM (
          SELECT txid, vout, value, script_type, block_height, spent
          FROM utxos
          WHERE address = {address:String}
          ORDER BY txid, vout, version DESC
          LIMIT 1 BY txid, vout
        )
        WHERE spent = 0
        ORDER BY block_height DESC
        LIMIT 1000
      `, { address });

      res.json({
        utxos: utxos.map(u => ({
          txid: u.txid,  // Keep full 64-char txid
          vout: u.vout,
          value: Number(u.value) / 1e8,
          valueSat: u.value,
          scriptType: u.script_type,
          blockHeight: u.block_height,
        })),
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Rich List ==========

  private async getRichList(req: Request, res: Response): Promise<void> {
    try {
      // Support both limit/offset and page/pageSize styles
      const pageSizeResult = ClickHouseAPIServer.parseBoundedInt(req.query.pageSize ?? req.query.limit, {
        name: 'pageSize',
        defaultValue: 100,
        min: 1,
        max: 1000,
      });
      if (pageSizeResult.error) {
        res.status(400).json({ error: pageSizeResult.error });
        return;
      }
      const pageSize = pageSizeResult.value ?? 100;

      const pageResult = ClickHouseAPIServer.parseBoundedInt(req.query.page, {
        name: 'page',
        defaultValue: 1,
        min: 1,
        max: 1_000_000,
      });
      if (pageResult.error) {
        res.status(400).json({ error: pageResult.error });
        return;
      }
      const page = pageResult.value ?? 1;

      const MAX_RICHLIST_OFFSET = 100000;
      let offset = (page - 1) * pageSize;

      if (req.query.offset !== undefined) {
        const offsetResult = ClickHouseAPIServer.parseBoundedInt(req.query.offset, {
          name: 'offset',
          required: true,
          min: 0,
          max: MAX_RICHLIST_OFFSET,
          clamp: false,
        });
        if (offsetResult.error) {
          res.status(400).json({ error: offsetResult.error });
          return;
        }
        offset = offsetResult.value as number;
      } else if (offset > MAX_RICHLIST_OFFSET) {
        res.status(400).json({ error: `offset too large (max ${MAX_RICHLIST_OFFSET}); use smaller page/pageSize` });
        return;
      }

      // Query addresses with balances, then join with live_fluxnodes for node counts
      const addresses = await this.ch.query<any>(`
        SELECT
          a.address,
          a.balance,
          a.tx_count,
          a.last_activity,
          fn.cumulus_count,
          fn.nimbus_count,
          fn.stratus_count
        FROM (
          SELECT
            address,
            sumMerge(balance) AS balance,
            sumMerge(tx_count) AS tx_count,
            maxMerge(last_activity) AS last_activity
          FROM address_summary_agg
          GROUP BY address
          HAVING balance > 0
          ORDER BY balance DESC
          LIMIT ${pageSize}
          OFFSET ${offset}
        ) a
        LEFT JOIN live_fluxnodes fn ON a.address = fn.address
      `);

      // Cache-friendly: separate count query (can be cached longer)
      const totalResult = await this.ch.queryOne<{ count: number; total: string }>(`
        SELECT count() as count, toString(sum(balance)) as total
        FROM (
          SELECT sumMerge(balance) AS balance
          FROM address_summary_agg
          GROUP BY address
          HAVING balance > 0
        )
      `);

      // Get latest block for lastBlockHeight - use ORDER BY LIMIT 1 instead of FINAL with max()
      const latestBlock = await this.ch.queryOne<{ height: number }>(`
        SELECT height FROM blocks WHERE is_valid = 1 ORDER BY height DESC LIMIT 1
      `);

      const totalAddresses = Number(totalResult?.count ?? 0);
      const totalSupplySat = totalResult?.total?.toString() ?? '0';
      const totalSupplyFlux = ClickHouseAPIServer.zatoshisToFluxNumber(BigInt(totalSupplySat));

      // Return format expected by flux-explorer frontend
      res.json({
        lastUpdate: new Date().toISOString(),
        lastBlockHeight: Number(latestBlock?.height ?? 0),
        totalSupply: totalSupplySat, // Keep as string in satoshis for explorer compatibility
        totalAddresses,
        page,
        pageSize,
        totalPages: Math.ceil(totalAddresses / pageSize),
        addresses: addresses.map((a: any, idx: number) => ({
          rank: offset + idx + 1,
          address: a.address,
          balance: a.balance.toString(), // Keep as string in satoshis
          txCount: Number(a.tx_count),
          cumulusCount: Number(a.cumulus_count) || 0,
          nimbusCount: Number(a.nimbus_count) || 0,
          stratusCount: Number(a.stratus_count) || 0,
        })),
        // Also include pagination format for backwards compatibility
        pagination: {
          total: totalAddresses,
          limit: pageSize,
          offset,
        },
        totalSupplyFlux, // Also include FLUX value for convenience
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Supply Stats ==========

  /**
   * Calculate mainchain-only supply (without parallel asset distributions)
   * Based on SupplyHelper.getCirculatingSupplyByHeight from insight-api
   * Does NOT include: exchange fund, snapshot amounts, chain funds, or parallel asset mining
   */
  private calculateMainchainSupply(height: number): bigint {
    const PON_HEIGHT = 2020000;
    const FIRST_HALVING = 657850;
    const HALVING_INTERVAL = 655350;

    // Time-locked fund releases (12 releases)
    const FUND_RELEASES = [
      { height: 836274, amount: 7500000 },
      { height: 836994, amount: 2500000 },
      { height: 837714, amount: 22000000 },
      { height: 859314, amount: 22000000 },
      { height: 880914, amount: 22000000 },
      { height: 902514, amount: 22000000 },
      { height: 924114, amount: 22000000 },
      { height: 945714, amount: 22000000 },
      { height: 967314, amount: 22000000 },
      { height: 988914, amount: 22000000 },
      { height: 1010514, amount: 22000000 },
      { height: 1032114, amount: 22000000 },
    ];

    let subsidy = 150;
    const miningHeight = Math.min(height, PON_HEIGHT - 1);
    const halvings = Math.min(2, Math.floor((miningHeight - 2500) / HALVING_INTERVAL));

    // Initial supply: slow start + premine + dev fund
    let coins = (FIRST_HALVING - 5000) * 150 + 375000 + 13020000;

    // Calculate traditional mining rewards through halvings
    for (let i = 1; i <= halvings; i++) {
      subsidy = subsidy / 2;

      if (i === halvings) {
        const nBlocksMain = miningHeight - FIRST_HALVING - ((i - 1) * HALVING_INTERVAL);
        coins += nBlocksMain * subsidy;
      } else {
        coins += HALVING_INTERVAL * subsidy;
      }
    }

    // Add time-locked fund releases
    for (const release of FUND_RELEASES) {
      if (height >= release.height) {
        coins += release.amount;
      }
    }

    // Add PON rewards after PON_HEIGHT (mainchain only)
    if (height >= PON_HEIGHT) {
      coins += (height - PON_HEIGHT + 1) * 14;
    }

    return BigInt(Math.floor(coins * 100000000));
  }

  /**
   * Calculate circulating supply including all parallel asset chains
   * Based on insight-api getCirculatingSupplyAllChains implementation
   */
  private calculateCirculatingSupplyAllChains(height: number): bigint {
    const PON_HEIGHT = 2020000;
    const ASSET_MINING_START = 825000;
    const FIRST_HALVING = 657850;
    const HALVING_INTERVAL = 655350;
    const EXCHANGE_FUND_HEIGHT = 835554;
    const EXCHANGE_FUND_AMOUNT = 10000000;
    const CHAIN_FUND_AMOUNT = 1000000; // dev + exchange fund allocated per chain launch
    const SNAPSHOT_AMOUNT = 12313785.94991485; // user snapshot per chain

    // Parallel asset chain launch heights
    const CHAINS = [
      { name: 'KDA', launchHeight: 825000 },
      { name: 'BSC', launchHeight: 883000 },
      { name: 'ETH', launchHeight: 883000 },
      { name: 'SOL', launchHeight: 969500 },
      { name: 'TRX', launchHeight: 969500 },
      { name: 'AVAX', launchHeight: 1170000 },
      { name: 'ERGO', launchHeight: 1210000 },
      { name: 'ALGO', launchHeight: 1330000 },
      { name: 'MATIC', launchHeight: 1414000 },
      { name: 'BASE', launchHeight: 1738000 },
    ];

    let subsidy = 150;
    const miningHeight = Math.min(height, PON_HEIGHT - 1);
    const halvings = Math.min(2, Math.floor((miningHeight - 2500) / HALVING_INTERVAL));

    // Initial supply: slow start + premine + dev fund
    let coins = (FIRST_HALVING - 5000) * 150 + 375000 + 13020000;

    // Add exchange fund if height reached
    if (height >= EXCHANGE_FUND_HEIGHT) {
      coins += EXCHANGE_FUND_AMOUNT;
    }

    // Add snapshot amounts and chain funds for launched chains
    for (const chain of CHAINS) {
      if (height > chain.launchHeight) {
        coins += CHAIN_FUND_AMOUNT + SNAPSHOT_AMOUNT;
      }
    }

    // Calculate traditional mining rewards through halvings
    for (let i = 1; i <= halvings; i++) {
      subsidy = subsidy / 2;

      if (i === halvings) {
        // Current/last halving period - partial blocks
        const nBlocksMain = miningHeight - FIRST_HALVING - ((i - 1) * HALVING_INTERVAL);
        coins += nBlocksMain * subsidy;

        // Add parallel asset mining rewards (1/10 of main chain subsidy)
        if (miningHeight > ASSET_MINING_START) {
          const activeChains = CHAINS.filter(chain => miningHeight > chain.launchHeight).length;
          coins += nBlocksMain * subsidy * activeChains / 10;
        }
      } else {
        // Completed halving period - full interval
        coins += HALVING_INTERVAL * subsidy;

        // Add parallel asset mining for the completed period
        if (miningHeight > ASSET_MINING_START) {
          const nBlocksAsset = HALVING_INTERVAL - (ASSET_MINING_START - FIRST_HALVING);
          const activeChains = CHAINS.filter(chain => miningHeight > chain.launchHeight).length;
          coins += nBlocksAsset * subsidy * activeChains / 10;
        }
      }
    }

    // Add PON (Proof of Node) rewards after PON_HEIGHT - × 2 for parallel assets
    if (height >= PON_HEIGHT) {
      coins += (height - PON_HEIGHT + 1) * 14 * 2;
    }

    // Convert to zatoshis (1 FLUX = 100,000,000 zatoshis)
    // Use Math.floor to handle decimal from SNAPSHOT_AMOUNT
    return BigInt(Math.floor(coins * 100000000));
  }

  private async getSupplyStats(_req: Request, res: Response): Promise<void> {
    try {
      // FINAL needed for ReplacingMergeTree deduplication
      const stats = await this.ch.queryOne<any>(`
        SELECT block_height, transparent_supply, shielded_pool, total_supply
        FROM supply_stats FINAL
        ORDER BY block_height DESC
        LIMIT 1
      `);

      if (!stats) {
        res.json({
          blockHeight: 0,
          transparentSupply: 0,
          shieldedPool: 0,
          circulatingSupply: 0,
          totalSupply: 0,
          maxSupply: 560000000,
        });
        return;
      }

      const blockHeight = Number(stats.block_height);
      const transparentSupply = BigInt(stats.transparent_supply);
      const shieldedPool = BigInt(stats.shielded_pool);
      const totalSupply = transparentSupply + shieldedPool;

      // Calculate circulating supply
      // Circulating = Total supply - Locked parallel assets
      // Where locked = theoretical mainchain supply - theoretical distributed to parallel chains
      const theoreticalMainchain = this.calculateMainchainSupply(blockHeight);
      const theoreticalAllChains = this.calculateCirculatingSupplyAllChains(blockHeight);
      const lockedParallelAssets = theoreticalMainchain - theoreticalAllChains;
      const circulatingSupply = totalSupply - lockedParallelAssets;

      // Max supply is 560 million FLUX
      const maxSupply = BigInt(560000000) * BigInt(100000000); // in zatoshis

      res.json({
        blockHeight,
        transparentSupply: ClickHouseAPIServer.zatoshisToFluxString(transparentSupply),
        shieldedPool: ClickHouseAPIServer.zatoshisToFluxString(shieldedPool),
        circulatingSupply: ClickHouseAPIServer.zatoshisToFluxString(circulatingSupply),
        totalSupply: ClickHouseAPIServer.zatoshisToFluxString(totalSupply),
        maxSupply: ClickHouseAPIServer.zatoshisToFluxString(maxSupply),
        timestamp: new Date().toISOString(),
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Producers ==========

  private async getProducers(req: Request, res: Response): Promise<void> {
    try {
      const limitResult = ClickHouseAPIServer.parseBoundedInt(req.query.limit, {
        name: 'limit',
        defaultValue: 100,
        min: 1,
        max: 500,
      });
      if (limitResult.error) {
        res.status(400).json({ error: limitResult.error });
        return;
      }
      const limit = limitResult.value ?? 100;

      const offsetResult = ClickHouseAPIServer.parseBoundedInt(req.query.offset, {
        name: 'offset',
        defaultValue: 0,
        min: 0,
        max: 100000,
        clamp: false,
      });
      if (offsetResult.error) {
        res.status(400).json({ error: offsetResult.error });
        return;
      }
      const offset = offsetResult.value ?? 0;

      // Pick the latest row per fluxnode without FINAL.
      const producers = await this.ch.query<any>(`
        SELECT fluxnode, blocks_produced, first_block, last_block, total_rewards
        FROM (
          SELECT fluxnode, blocks_produced, first_block, last_block, total_rewards
          FROM producers
          ORDER BY fluxnode, updated_at DESC
          LIMIT 1 BY fluxnode
        )
        ORDER BY blocks_produced DESC
        LIMIT ${limit}
        OFFSET ${offset}
      `);

      // Use uniqExact() for accurate count without expensive FINAL
      const totalResult = await this.ch.queryOne<{ count: number }>('SELECT uniqExact(fluxnode) as count FROM producers');

      res.json({
        producers: producers.map((p, idx) => ({
          rank: offset + idx + 1,
          fluxnode: p.fluxnode,
          blocksProduced: Number(p.blocks_produced),
          firstBlock: Number(p.first_block),
          lastBlock: Number(p.last_block),
          totalRewards: ClickHouseAPIServer.zatoshisToFluxNumber(BigInt(p.total_rewards.toString())),
        })),
        pagination: {
          total: Number(totalResult?.count ?? 0),
          limit,
          offset,
        },
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getProducer(req: Request, res: Response): Promise<void> {
    try {
      const { identifier } = req.params;

      // Single-row lookup - pick the latest row for this fluxnode without FINAL.
      const producer = await this.ch.queryOne<any>(`
        SELECT fluxnode, blocks_produced, first_block, last_block, total_rewards
        FROM producers
        WHERE fluxnode = {identifier:String}
        ORDER BY updated_at DESC
        LIMIT 1
      `, { identifier });

      if (!producer) {
        res.status(404).json({ error: 'Producer not found' });
        return;
      }

      // Get recent blocks.
      // Phase 1: use the producer projection for a fast candidate list.
      const candidateBlocks = await this.ch.query<any>(`
        SELECT height, hash, timestamp, tx_count
        FROM (
          SELECT height, hash, timestamp, tx_count, is_valid
          FROM blocks
          WHERE producer = {identifier:String}
          ORDER BY height DESC, _version DESC
          LIMIT 1 BY height
        )
        WHERE is_valid = 1
        ORDER BY height DESC
        LIMIT 50
      `, { identifier });

      // Phase 2: re-select latest rows for those heights without filtering by producer first
      // (prevents stale results if a reorg replaced blocks at the same height with a new producer).
      let recentBlocks: any[] = [];
      if (candidateBlocks.length > 0) {
        const heights = candidateBlocks.map((b: any) => b.height);
        recentBlocks = await this.ch.query<any>(`
          SELECT height, hash, timestamp, tx_count
          FROM (
            SELECT height, hash, timestamp, tx_count, producer, is_valid
            FROM blocks
            WHERE height IN {heights:Array(UInt32)}
            ORDER BY height, _version DESC
            LIMIT 1 BY height
          )
          WHERE is_valid = 1 AND producer = {identifier:String}
          ORDER BY height DESC
          LIMIT 10
        `, { heights, identifier });
      }

      res.json({
        fluxnode: producer.fluxnode,
        blocksProduced: producer.blocks_produced,
        firstBlock: producer.first_block,
        lastBlock: producer.last_block,
        totalRewards: ClickHouseAPIServer.zatoshisToFluxNumber(BigInt(producer.total_rewards.toString())),
        recentBlocks,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== FluxNode Endpoints ==========

  private async getFluxNodes(req: Request, res: Response): Promise<void> {
    try {
      const limitResult = ClickHouseAPIServer.parseBoundedInt(req.query.limit, {
        name: 'limit',
        defaultValue: 100,
        min: 1,
        max: 1000,
      });
      if (limitResult.error) {
        res.status(400).json({ error: limitResult.error });
        return;
      }
      const limit = limitResult.value ?? 100;

      const offsetResult = ClickHouseAPIServer.parseBoundedInt(req.query.offset, {
        name: 'offset',
        defaultValue: 0,
        min: 0,
        max: 100000,
        clamp: false,
      });
      if (offsetResult.error) {
        res.status(400).json({ error: offsetResult.error });
        return;
      }
      const offset = offsetResult.value ?? 0;
      const tier = req.query.tier as string;

      // Validate tier to prevent SQL injection and build WHERE clause
      const validTiers = ['CUMULUS', 'NIMBUS', 'STRATUS'];
      let tierFilter = '';
      if (tier && validTiers.includes(tier.toUpperCase())) {
        tierFilter = `AND benchmark_tier = '${tier.toUpperCase()}'`;
      }

      // Pick the latest row per txid without FINAL, then filter is_valid.
      const nodes = await this.ch.query<any>(`
        SELECT txid, block_height, block_time, type, ip_address, public_key,
               benchmark_tier, p2sh_address
        FROM (
          SELECT txid, block_height, block_time, type, ip_address, public_key,
                 benchmark_tier, p2sh_address, is_valid
          FROM fluxnode_transactions
          WHERE 1 = 1 ${tierFilter}
          ORDER BY txid, _version DESC
          LIMIT 1 BY txid
        )
        WHERE is_valid = 1
        ORDER BY block_height DESC
        LIMIT ${limit}
        OFFSET ${offset}
      `);

      const totalResult = await this.ch.queryOne<{ count: number }>(`
        SELECT count() as count
        FROM (
          SELECT txid
          FROM (
            SELECT txid, is_valid
            FROM fluxnode_transactions
            WHERE 1 = 1 ${tierFilter}
            ORDER BY txid, _version DESC
            LIMIT 1 BY txid
          )
          WHERE is_valid = 1
        )
      `);

      res.json({
        nodes: nodes.map(n => ({
          ...n,
          txid: n.txid,  // Keep full 64-char txid
        })),
        pagination: {
          total: Number(totalResult?.count ?? 0),
          limit,
          offset,
        },
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getFluxNodeStatus(req: Request, res: Response): Promise<void> {
    try {
      const { ip } = req.params;

      // Single-row lookup - pick the latest row per txid without FINAL, then filter is_valid.
      const node = await this.ch.queryOne<any>(`
        SELECT txid, block_height, block_time, type, ip_address, public_key,
               benchmark_tier, p2sh_address, collateral_hash, collateral_index
        FROM (
          SELECT txid, block_height, block_time, type, ip_address, public_key,
                 benchmark_tier, p2sh_address, collateral_hash, collateral_index, is_valid
          FROM fluxnode_transactions
          WHERE ip_address = {ip:String}
          ORDER BY txid, _version DESC
          LIMIT 1 BY txid
        )
        WHERE is_valid = 1
        ORDER BY block_height DESC
        LIMIT 1
      `, { ip });

      if (!node) {
        res.status(404).json({ error: 'FluxNode not found' });
        return;
      }

      res.json({
        ...node,
        txid: node.txid,  // Keep full 64-char txid
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Network ==========

  private async getNetworkInfo(_req: Request, res: Response): Promise<void> {
    try {
      const [chainInfo, networkInfo] = await Promise.all([
        this.rpc.getBlockchainInfo(),
        this.rpc.getNetworkInfo(),
      ]);

      res.json({
        chain: chainInfo.chain,
        blocks: chainInfo.blocks,
        headers: chainInfo.headers,
        bestBlockHash: chainInfo.bestblockhash,
        difficulty: chainInfo.difficulty,
        version: networkInfo.version,
        subversion: networkInfo.subversion,
        protocolVersion: networkInfo.protocolversion,
        connections: networkInfo.connections,
      });
    } catch (error: any) {
      // Return unavailable status instead of 500 when daemon is not ready
      logger.debug('Network info unavailable', { error: error.message });
      res.json({
        status: 'unavailable',
        message: 'Daemon not ready',
        chain: 'mainnet',
        blocks: 0,
        headers: 0,
        connections: 0,
      });
    }
  }

  private async getMempoolInfo(_req: Request, res: Response): Promise<void> {
    try {
      const mempoolInfo = await this.rpc.getMempoolInfo();
      res.json(mempoolInfo);
    } catch (error: any) {
      // Return unavailable status instead of 500 when daemon is not ready
      logger.debug('Mempool info unavailable', { error: error.message });
      res.json({
        status: 'unavailable',
        message: 'Daemon not ready',
        size: 0,
        bytes: 0,
        usage: 0,
      });
    }
  }

  private async getDashboardStats(_req: Request, res: Response): Promise<void> {
    try {
      const now = Date.now();
      if (this.statsCache.data && (now - this.statsCache.timestamp) < ClickHouseAPIServer.STATS_CACHE_TTL) {
        res.json(this.statsCache.data);
        return;
      }

      const nowSeconds = Math.floor(now / 1000);

      // Phase 1: Get latest blocks and general stats in parallel
      // The latest 5 blocks query is very fast (uses primary key ORDER BY)
      const [latestBlocks, avgBlockTime, tx24h] = await Promise.all([
        // Get latest 5 blocks - fast primary key scan
        this.ch.query<any>(`
          WITH
            (SELECT max(height) FROM blocks WHERE is_valid = 1) AS maxHeight,
            toUInt32(greatest(toInt64(maxHeight) - 50, 0)) AS minHeight
          SELECT height, hash, timestamp
          FROM (
            SELECT height, hash, timestamp, is_valid
            FROM blocks
            WHERE height <= maxHeight AND height > minHeight
            ORDER BY height DESC, _version DESC
            LIMIT 1 BY height
          )
          WHERE is_valid = 1
          ORDER BY height DESC
          LIMIT 5
        `),
        this.ch.queryOne<{ avg_interval: number }>(`
          WITH
            (SELECT max(height) FROM blocks WHERE is_valid = 1) AS maxHeight,
            toUInt32(greatest(toInt64(maxHeight) - 400, 0)) AS minHeight,
            latest AS (
              SELECT height, timestamp
              FROM (
                SELECT height, timestamp, is_valid
                FROM blocks
                WHERE height <= maxHeight AND height > minHeight
                ORDER BY height DESC, _version DESC
                LIMIT 1 BY height
              )
              WHERE is_valid = 1
              ORDER BY height DESC
              LIMIT 121
            ),
            recent AS (
              SELECT height, timestamp, lagInFrame(timestamp) OVER (ORDER BY height) AS prev_timestamp
              FROM latest
            )
          SELECT avg(timestamp - prev_timestamp) as avg_interval
          FROM recent
          WHERE prev_timestamp > 0
        `),
        this.ch.queryOne<{ tx_24h: string }>(`
          WITH
            (SELECT max(height) FROM blocks WHERE is_valid = 1) AS maxHeight,
            toUInt32(greatest(toInt64(maxHeight) - 3000, 0)) AS minHeight
          SELECT toString(sum(tx_count)) as tx_24h
          FROM (
            SELECT tx_count
            FROM (
              SELECT height, tx_count, timestamp, is_valid
              FROM blocks
              WHERE height <= maxHeight AND height > minHeight
              ORDER BY height DESC, _version DESC
              LIMIT 1 BY height
            )
            WHERE is_valid = 1 AND timestamp >= {cutoff:UInt32}
          )
        `, { cutoff: nowSeconds - 86400 }),
      ]);

      const latestBlock = latestBlocks[0] || null;

      // Phase 2: Get coinbase transactions and their outputs for just those 5 block heights
      // This uses block_height partition key for efficient filtering instead of scanning all coinbase txs
      let recentCoinbaseTxs: any[] = [];
      let outputsByTxid = new Map<string, Array<{ address: string; value: bigint }>>();

      if (latestBlocks.length > 0) {
        const heights = latestBlocks.map((b: any) => b.height);

        // Get coinbase transactions for just these specific heights (partition-efficient)
	        const coinbaseTxs = await this.ch.query<any>(`
	          SELECT txid, block_height, output_total
	          FROM (
	            SELECT txid, block_height, output_total, is_valid
            FROM transactions
            WHERE block_height IN {heights:Array(UInt32)} AND is_coinbase = 1
            ORDER BY txid, _version DESC
            LIMIT 1 BY txid
	          )
	          WHERE is_valid = 1
	        `, { heights });

	        // Build block lookup map
	        const blockMap = new Map(latestBlocks.map((b: any) => [b.height, b]));

        // Merge block info with coinbase transactions
        recentCoinbaseTxs = coinbaseTxs.map((t: any) => {
          const block = blockMap.get(t.block_height);
          return {
            height: t.block_height,
            hash: block?.hash,
            timestamp: block?.timestamp,
            txid: t.txid,
            output_total: t.output_total,
          };
        }).sort((a: any, b: any) => b.height - a.height);

        // Get outputs for coinbase transactions
        if (coinbaseTxs.length > 0) {
          const txids = coinbaseTxs.map((t: any) => t.txid);
          const outputs = await this.ch.query<{ txid: string; address: string; value: string }>(`
            SELECT txid, address, value
            FROM (
              SELECT txid, vout, address, value
              FROM utxos
              WHERE txid IN {txids:Array(FixedString(64))}
              ORDER BY txid, vout, version DESC
              LIMIT 1 BY txid, vout
            )
            ORDER BY txid, vout
          `, { txids });

          for (const out of outputs) {
            const existing = outputsByTxid.get(out.txid) || [];
            existing.push({ address: out.address, value: BigInt(out.value) });
            outputsByTxid.set(out.txid, existing);
          }
        }
      }

      // Build response format expected by frontend (DashboardStats)
      const payload = {
        latestBlock: {
          height: latestBlock?.height ?? 0,
          hash: latestBlock?.hash ?? null,  // Keep full 64-char hash
          timestamp: latestBlock?.timestamp ?? null,
        },
        averages: {
          blockTimeSeconds: avgBlockTime?.avg_interval ?? 120,
        },
        transactions24h: parseInt(tx24h?.tx_24h ?? '0'),
        latestRewards: recentCoinbaseTxs.map((r: any) => {
          const outputs = outputsByTxid.get(r.txid) || [];
          const totalRewardSat = Number(r.output_total);

          return {
            height: r.height,
            hash: r.hash,  // Keep full 64-char hash
            timestamp: r.timestamp,
            txid: r.txid,  // Keep full 64-char txid
            totalRewardSat,
            totalReward: totalRewardSat / 1e8,
            outputs: outputs.map(o => ({
              address: o.address !== 'SHIELDED_OR_NONSTANDARD' ? o.address : null,
              valueSat: Number(o.value),
              value: Number(o.value) / 1e8,
            })),
          };
        }),
        generatedAt: new Date().toISOString(),
      };

      this.statsCache = { data: payload, timestamp: now };
      res.json(payload);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Analytics (Materialized Views) ==========

  private async getTxVolumeHistory(req: Request, res: Response): Promise<void> {
    try {
      const daysResult = ClickHouseAPIServer.parseBoundedInt(req.query.days, {
        name: 'days',
        defaultValue: 30,
        min: 1,
        max: 365,
      });
      if (daysResult.error) {
        res.status(400).json({ error: daysResult.error });
        return;
      }
      const days = daysResult.value ?? 30;
      const cutoff = Math.floor(Date.now() / 1000) - (days * 86400);

      // Leverages mv_hourly_tx_count materialized view for instant aggregation
      const hourlyData = await this.ch.query<{
        hour: string;
        tx_count: string;
        block_count: number;
      }>(`
        SELECT
          hour,
          toString(tx_count) as tx_count,
          block_count
        FROM mv_hourly_tx_count
        WHERE hour >= toDateTime(${cutoff})
        ORDER BY hour DESC
        LIMIT ${days * 24}
      `);

      // Also get daily aggregates
      const dailyData = await this.ch.query<{
        day: string;
        tx_count: string;
        block_count: string;
      }>(`
        SELECT
          toDate(hour) as day,
          toString(sum(tx_count)) as tx_count,
          toString(sum(block_count)) as block_count
        FROM mv_hourly_tx_count
        WHERE hour >= toDateTime(${cutoff})
        GROUP BY day
        ORDER BY day DESC
      `);

      res.json({
        hourly: hourlyData.map(h => ({
          hour: h.hour,
          txCount: parseInt(h.tx_count),
          blockCount: h.block_count,
        })),
        daily: dailyData.map(d => ({
          day: d.day,
          txCount: parseInt(d.tx_count),
          blockCount: parseInt(d.block_count),
        })),
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  private async getSupplyHistory(req: Request, res: Response): Promise<void> {
    try {
      const daysResult = ClickHouseAPIServer.parseBoundedInt(req.query.days, {
        name: 'days',
        defaultValue: 90,
        min: 1,
        max: 365,
      });
      if (daysResult.error) {
        res.status(400).json({ error: daysResult.error });
        return;
      }
      const days = daysResult.value ?? 90;

      // Leverages mv_daily_supply materialized view
      const data = await this.ch.query<{
        day: string;
        max_height: number;
        transparent_supply: string;
        shielded_pool: string;
        total_supply: string;
      }>(`
        SELECT
          day,
          max_height,
          toString(transparent_supply) as transparent_supply,
          toString(shielded_pool) as shielded_pool,
          toString(total_supply) as total_supply
        FROM (
          SELECT day, max_height, transparent_supply, shielded_pool, total_supply
          FROM mv_daily_supply
          ORDER BY day DESC, max_height DESC
          LIMIT 1 BY day
        )
        ORDER BY day DESC
        LIMIT ${days}
      `);

      res.json({
        history: data.map(d => ({
          day: d.day,
          height: d.max_height,
          transparentSupply: ClickHouseAPIServer.zatoshisToFluxNumber(BigInt(d.transparent_supply)),
          shieldedPool: ClickHouseAPIServer.zatoshisToFluxNumber(BigInt(d.shielded_pool)),
          totalSupply: ClickHouseAPIServer.zatoshisToFluxNumber(BigInt(d.total_supply)),
        })),
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  // ========== Server Control ==========

  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.server = this.app.listen(this.port, () => {
        logger.info(`ClickHouse API server listening on port ${this.port}`);
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (this.server) {
        this.server.close(() => {
          logger.info('ClickHouse API server stopped');
          resolve();
        });
      } else {
        resolve();
      }
    });
  }
}
