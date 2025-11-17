/**
 * API Server
 *
 * FluxIndexer REST API for Flux PoN blockchain
 */

import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import compression from 'compression';
import path from 'path';
import { DatabaseConnection } from '../database/connection';
import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { SyncEngine } from '../indexer/sync-engine';
import { logger } from '../utils/logger';
import { Transaction } from '../types';
import { extractTransactionFromBlock } from '../parsers/block-parser';

function decodeOpReturnData(scriptHex?: string | null): { hex: string; text: string | null } | null {
  if (!scriptHex) return null;

  try {
    const buffer = Buffer.from(scriptHex, 'hex');
    if (buffer.length === 0 || buffer[0] !== 0x6a) {
      return null;
    }

    let index = 1;
    if (index >= buffer.length) return null;

    let length = buffer[index];
    index += 1;

    if (length === 0x4c) {
      if (index >= buffer.length) return null;
      length = buffer[index];
      index += 1;
    } else if (length === 0x4d) {
      if (index + 1 >= buffer.length) return null;
      length = buffer[index] | (buffer[index + 1] << 8);
      index += 2;
    } else if (length === 0x4e) {
      if (index + 3 >= buffer.length) return null;
      length = buffer.readUInt32LE(index);
      index += 4;
    }

    const data = buffer.slice(index, index + length);
    if (data.length === 0) {
      return { hex: '', text: null };
    }

    const hex = data.toString('hex');
    let text: string | null = null;

    const ascii = data.toString('utf8');
    if (/^[\x09\x0A\x0D\x20-\x7E]+$/.test(ascii)) {
      text = ascii;
    }

    return { hex, text };
  } catch (error) {
    logger.debug('Failed to decode OP_RETURN data', { error: (error as Error).message });
    return null;
  }
}

export class APIServer {
  private app: express.Application;
  private server: any;
  private daemonReady = false;
  private statusCheckFailures = 0;

  // Cache for expensive count queries
  private statsCache: {
    data: { blocks: number; transactions: number; addresses: number } | null;
    timestamp: number;
  } = { data: null, timestamp: 0 };
  private static readonly STATS_CACHE_TTL = 30000; // 30 seconds
  private statsRefreshPromise: Promise<any> | null = null; // Track in-progress refresh

  // Cache for status endpoint
  private statusCache: { data: any | null; timestamp: number } = { data: null, timestamp: 0 };
  private static readonly STATUS_CACHE_TTL = 5000; // 5 seconds
  private statusRefreshPromise: Promise<any> | null = null; // Track in-progress refresh

  // Cache for rich list supply calculation (expensive SUM on utxos table)
  private richListSupplyCache: {
    data: { totalSupply: string; totalAddresses: number } | null;
    timestamp: number;
  } = { data: null, timestamp: 0 };
  private static readonly RICH_LIST_SUPPLY_CACHE_TTL = 30000; // 30 seconds
  private richListSupplyRefreshPromise: Promise<any> | null = null;

  // Block reward constants
  private static readonly FIRST_HALVING_HEIGHT = 657850;
  private static readonly SECOND_HALVING_HEIGHT = 1313200;
  private static readonly FOUNDATION_ACTIVATION_HEIGHT = 2020000; // PON fork

  /**
   * Calculate expected block reward at a given height
   * Matches the logic from flux-explorer/src/lib/block-rewards.ts
   */
  private getExpectedBlockReward(height: number): number {
    if (height < 1) return 0;

    // Before first halving: 150 FLUX
    if (height < APIServer.FIRST_HALVING_HEIGHT) {
      return 150;
    }

    // After first halving, before second: 75 FLUX
    if (height < APIServer.SECOND_HALVING_HEIGHT) {
      return 75;
    }

    // After second halving: 37.5 FLUX
    // 3rd halving was canceled, so it stays at 37.5 until PON fork
    if (height < APIServer.FOUNDATION_ACTIVATION_HEIGHT) {
      return 37.5;
    }

    // PON era: fixed rewards totaling 14 FLUX
    // (Cumulus: 1 + Nimbus: 3.5 + Stratus: 9 + Foundation: 0.5 = 14)
    return 14;
  }

  constructor(
    private db: DatabaseConnection,
    private rpc: FluxRPCClient,
    private syncEngine: SyncEngine,
    private port: number = 3002
  ) {
    this.app = express();
    this.setupMiddleware();
    this.setupRoutes();
    this.setupErrorHandling();
  }

  /**
   * Setup Express middleware
   */
  private setupMiddleware(): void {
    // Enable gzip/brotli compression for API responses
    // Reduces response size by 70-90% for JSON payloads
    this.app.use(compression({
      filter: (req: Request, res: Response) => {
        // Don't compress if client explicitly requests no compression
        if (req.headers['x-no-compression']) {
          return false;
        }
        // Otherwise, use compression for responses > 1KB
        return compression.filter(req, res);
      },
      threshold: 1024, // Only compress responses larger than 1KB
      level: 6, // Compression level (0-9, 6 is good balance of speed/ratio)
    }));

    this.app.use(cors());
    this.app.use(express.json());

    // Request logging
    this.app.use((req, res, next) => {
      logger.debug(`${req.method} ${req.path}`, {
        query: req.query,
        ip: req.ip,
      });
      next();
    });
  }

  /**
   * Setup API routes
   */
  private setupRoutes(): void {
    // FluxIndexer API v1
    // Status endpoints
    this.app.get('/api/v1/status', this.getStatus.bind(this));
    this.app.get('/api/v1/sync', this.getSyncStatus.bind(this));

    // Block endpoints
    this.app.get('/api/v1/blocks', this.getBlocks.bind(this));
    this.app.get('/api/v1/blocks/latest', this.getLatestBlocks.bind(this));
    this.app.get('/api/v1/blocks/:heightOrHash', this.getBlock.bind(this));

    // Transaction endpoints
    this.app.get('/api/v1/transactions/:txid', this.getTransaction.bind(this));

    // Address endpoints
    this.app.get('/api/v1/addresses/:address', this.getAddress.bind(this));
    this.app.get('/api/v1/addresses/:address/transactions', this.getAddressTransactions.bind(this));
    this.app.get('/api/v1/addresses/:address/utxos', this.getAddressUTXOs.bind(this));

    // Rich list endpoint
    this.app.get('/api/v1/richlist', this.getRichList.bind(this));

    // Supply stats endpoint
    this.app.get('/api/v1/supply', this.getSupplyStats.bind(this));

    // Producer endpoints (PoN-specific)
    this.app.get('/api/v1/producers', this.getProducers.bind(this));
    this.app.get('/api/v1/producers/:identifier', this.getProducer.bind(this));

    // FluxNode endpoints (Flux-specific)
    this.app.get('/api/v1/nodes', this.getFluxNodes.bind(this));
    this.app.get('/api/v1/nodes/:ip', this.getFluxNodeStatus.bind(this));

    // Network endpoints
    this.app.get('/api/v1/network', this.getNetworkInfo.bind(this));
    this.app.get('/api/v1/mempool', this.getMempoolInfo.bind(this));
    this.app.get('/api/v1/stats/dashboard', this.getDashboardStats.bind(this));

    // Health check (no /api prefix for monitoring tools)
    this.app.get('/health', (req, res) => {
      res.json({ status: 'ok', timestamp: new Date().toISOString() });
    });

    // Serve frontend static files (dashboard)
    const frontendPath = path.join(__dirname, '../../frontend');
    this.app.use(express.static(frontendPath));

    // Serve index.html for all non-API routes (SPA fallback)
    this.app.get('*', (req, res) => {
      // Don't serve index.html for API routes
      if (req.path.startsWith('/api/') || req.path === '/health') {
        return res.status(404).json({ error: 'Not found' });
      }
      res.sendFile(path.join(frontendPath, 'index.html'));
    });
  }

  /**
   * GET /api/v1/stats/dashboard - Aggregated explorer stats
   */
  private dashboardCache: {
    data: any | null;
    timestamp: number;
  } = { data: null, timestamp: 0 };
  private static readonly DASHBOARD_CACHE_TTL = 5000; // 5 seconds

  private blockCache: Map<string, { data: any; timestamp: number }> = new Map();
  private static readonly BLOCK_CACHE_LIMIT = 200;
  private static readonly BLOCK_CACHE_TTL = 10000; // 10 seconds for recent blocks

  private async getDashboardStats(req: Request, res: Response): Promise<void> {
    try {
      const now = Date.now();
      if (this.dashboardCache.data && (now - this.dashboardCache.timestamp) < APIServer.DASHBOARD_CACHE_TTL) {
        res.json(this.dashboardCache.data);
        return;
      }

      const nowSeconds = Math.floor(now / 1000);

      const [latestBlockResult, avgBlockTimeResult, tx24hResult, rewardsResult] = await Promise.all([
        this.db.query(`
          SELECT height, hash, timestamp
          FROM blocks
          ORDER BY height DESC
          LIMIT 1
        `),
        this.db.query(`
          WITH recent AS (
            SELECT
              height,
              timestamp,
              LAG(timestamp) OVER (ORDER BY height) AS prev_timestamp
            FROM blocks
            ORDER BY height DESC
            LIMIT 121
          )
          SELECT COALESCE(AVG(timestamp - prev_timestamp), 0)::numeric AS avg_interval
          FROM recent
          WHERE prev_timestamp IS NOT NULL
        `),
        this.db.query(
          `SELECT COALESCE(SUM(tx_count), 0)::bigint AS tx_24h
           FROM blocks
           WHERE timestamp >= $1`,
          [nowSeconds - 86400]
        ),
        this.db.query(`
          SELECT
            b.height,
            b.hash,
            b.timestamp,
            t.txid,
            t.output_total
          FROM blocks b
          JOIN transactions t
            ON t.block_height = b.height AND t.is_coinbase = TRUE
          ORDER BY b.height DESC
          LIMIT 5
        `),
      ]);

      const latestBlock = latestBlockResult.rows[0] || { height: 0, hash: null, timestamp: null };
      const avgBlockTime = Number(avgBlockTimeResult.rows[0]?.avg_interval || 0);
      const tx24h = Number(tx24hResult.rows[0]?.tx_24h || 0);

      const coinbaseTxids = rewardsResult.rows.map((row: any) => row.txid);
      let outputsByTxid = new Map<string, Array<{ address: string | null; valueSat: number }>>();

      if (coinbaseTxids.length > 0) {
        const outputsResult = await this.db.query(
          `SELECT txid, address, value
           FROM utxos
           WHERE txid = ANY($1::text[])
           ORDER BY value DESC`,
          [coinbaseTxids]
        );

        outputsByTxid = outputsResult.rows.reduce((map: Map<string, Array<{ address: string | null; valueSat: number }>>, row: any) => {
          const list = map.get(row.txid) || [];
          const valueSat = row.value ? Number(row.value) : 0;
          list.push({
            address: row.address || null,
            valueSat,
          });
          map.set(row.txid, list);
          return map;
        }, new Map<string, Array<{ address: string | null; valueSat: number }>>());
      }

      const latestRewards = rewardsResult.rows.map((row: any) => {
        const valueSat = row.output_total ? Number(row.output_total) : 0;
        const outputs = (outputsByTxid.get(row.txid) || []).map(output => ({
          address: output.address,
          valueSat: output.valueSat,
          value: output.valueSat / 1e8,
        }));

        return {
          height: Number(row.height),
          hash: row.hash,
          timestamp: Number(row.timestamp),
          txid: row.txid,
          totalRewardSat: valueSat,
          totalReward: valueSat / 1e8,
          outputs,
        };
      });

      const payload = {
        latestBlock: {
          height: Number(latestBlock.height || 0),
          hash: latestBlock.hash,
          timestamp: latestBlock.timestamp ? Number(latestBlock.timestamp) : null,
        },
        averages: {
          blockTimeSeconds: avgBlockTime,
        },
        transactions24h: tx24h,
        latestRewards,
        generatedAt: new Date().toISOString(),
      };

      this.dashboardCache = { data: payload, timestamp: now };
      res.json(payload);
    } catch (error: any) {
      logger.error('Failed to get dashboard stats', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Setup error handling
   */
  private setupErrorHandling(): void {
    this.app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      logger.error('API error', {
        error: err.message,
        stack: err.stack,
        path: req.path,
      });

      res.status(500).json({
        error: err.message || 'Internal server error',
      });
    });
  }

  /**
   * GET /api/v1/status - Get status (CACHED)
   */
  private async getStatus(req: Request, res: Response): Promise<void> {
    try {
      // Check cache first
      const now = Date.now();
      if (this.statusCache.data && (now - this.statusCache.timestamp) < APIServer.STATUS_CACHE_TTL) {
        res.json(this.statusCache.data);
        return;
      }

      // If a refresh is already in progress, wait for it (request coalescing)
      if (this.statusRefreshPromise) {
        await this.statusRefreshPromise;
        if (this.statusCache.data) {
          res.json(this.statusCache.data);
          return;
        }
      }

      // Mark that we're refreshing to prevent concurrent requests
      const refreshPromise = (async () => {
        return await this.doStatusRefresh(now);
      })();

      this.statusRefreshPromise = refreshPromise;

      try {
        const payload = await refreshPromise;
        res.json(payload);
      } finally {
        this.statusRefreshPromise = null;
      }
    } catch (error: any) {
      logger.error('Failed to get status', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Internal method to perform the actual status refresh
   */
  private async doStatusRefresh(now: number): Promise<any> {
    try {
      // Try to get blockchain and network info from daemon
      let chainInfo: any = null;
      let networkInfo: any = null;
      let daemonHeight: number | null = null;

      try {
        chainInfo = await this.rpc.getBlockchainInfo();
        networkInfo = await this.rpc.getNetworkInfo();

        // Daemon is now responding
        if (!this.daemonReady) {
          this.daemonReady = true;
          logger.info('Daemon RPC is now responding to status checks');
        }
        this.statusCheckFailures = 0;
      } catch (rpcError) {
        this.statusCheckFailures++;

        // Log differently based on whether daemon was previously ready
        if (this.daemonReady) {
          // Was ready, now failing - this is concerning
          logger.warn('RPC not available for status check (daemon may be restarting)');
          this.daemonReady = false;
        } else if (this.statusCheckFailures === 1) {
          // First failure during warmup
          logger.info('Daemon not responding yet (warming up...)');
        } else if (this.statusCheckFailures % 20 === 0) {
          // Every 20 failures (~2 minutes), log progress
          logger.debug('Still waiting for daemon warmup', { attempts: this.statusCheckFailures });
        }

        // If RPC fails, try to get height from daemon log file
        try {
          const { execSync } = require('child_process');
          const logOutput = execSync('tail -1 /var/log/supervisor/fluxd.log 2>/dev/null || echo ""', { encoding: 'utf8' });
          const heightMatch = logOutput.match(/height=(\d+)/);
          if (heightMatch) {
            daemonHeight = parseInt(heightMatch[1]);
            logger.debug('Got daemon height from log file', { height: daemonHeight });
          }
        } catch (logError) {
          // Ignore log parsing errors
        }
      }

      const syncState = await this.db.query('SELECT * FROM sync_state WHERE id = 1');
      const state = syncState.rows[0];

      // Use cached stats to avoid expensive COUNT queries (with request coalescing)
      let stats = this.statsCache.data;

      if (!stats || (now - this.statsCache.timestamp) > APIServer.STATS_CACHE_TTL) {
        // If a stats refresh is already in progress, wait for it
        if (this.statsRefreshPromise) {
          await this.statsRefreshPromise;
          stats = this.statsCache.data;
        } else {
          // Start a new refresh using pg_class statistics (instant, no table scan)
          const refreshPromise = (async () => {
            const statsQuery = `
              SELECT
                schemaname,
                relname,
                n_live_tup as count
              FROM pg_stat_user_tables
              WHERE schemaname = 'public'
                AND relname IN ('blocks', 'transactions', 'address_summary')
            `;

            const result = await this.db.query(statsQuery);

            const statsMap = new Map(
              result.rows.map((row: any) => [row.relname, parseInt(row.count || '0')])
            );

            const newStats = {
              blocks: statsMap.get('blocks') || 0,
              transactions: statsMap.get('transactions') || 0,
              addresses: statsMap.get('address_summary') || 0,
            };

            this.statsCache = { data: newStats, timestamp: now };
            return newStats;
          })();

          this.statsRefreshPromise = refreshPromise;

          try {
            stats = await refreshPromise;
          } finally {
            this.statsRefreshPromise = null;
          }
        }
      }

      const currentHeight = state?.current_height || 0;
      const chainHeight = state?.chain_height || 0;
      const isSynced = chainInfo ? (currentHeight >= chainHeight - 1) : false;

      // Fallback if stats are still unavailable
      if (!stats) {
        stats = { blocks: 0, transactions: 0, addresses: 0 };
      }

      const payload = {
        name: 'FluxIndexer',
        version: '1.0.0',
        network: chainInfo?.chain || 'mainnet',
        consensus: 'PoN',
        indexer: {
          syncing: state?.is_syncing || false,
          synced: isSynced,
          currentHeight,
          chainHeight,
          progress: chainHeight > 0 ? ((currentHeight / chainHeight) * 100).toFixed(2) + '%' : '0%',
          blocksIndexed: stats.blocks,
          transactionsIndexed: stats.transactions,
          addressesIndexed: stats.addresses,
          lastSyncTime: state?.last_sync_time || null,
        },
        daemon: chainInfo ? {
          version: networkInfo?.subversion || '/Flux:9.0.0/',
          protocolVersion: networkInfo?.protocolversion || 170015,
          blocks: chainInfo.blocks,
          headers: chainInfo.headers,
          bestBlockHash: chainInfo.bestblockhash,
          difficulty: chainInfo.difficulty,
          chainwork: chainInfo.chainwork,
          consensus: 'PoN',
          connections: networkInfo?.connections || 0,
          networkActive: networkInfo?.networkactive || false,
        } : {
          status: 'warming up',
          version: '/Flux:9.0.0/',
          protocolVersion: 170015,
          blocks: daemonHeight || 0,
          headers: chainHeight,
          bestBlockHash: daemonHeight ? `Syncing... (${daemonHeight.toLocaleString()})` : 'Syncing from bootstrap...',
          difficulty: 0,
          chainwork: '',
          consensus: 'PoN',
          connections: 0,
          networkActive: false,
        },
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
      };

      // Cache the response
      this.statusCache = { data: payload, timestamp: now };
      return payload;
    } catch (error: any) {
      logger.error('Failed to refresh status', { error: error.message });
      throw error;
    }
  }

  /**
   * GET /api/v1/sync - Get sync status
   */
  private async getSyncStatus(req: Request, res: Response): Promise<void> {
    try {
      const syncState = await this.db.query('SELECT * FROM sync_state WHERE id = 1');
      const state = syncState.rows[0];

      const currentHeight = state?.current_height || 0;

      // Try to get live chain height from daemon headers, fallback to database if daemon unavailable
      let chainHeight = state?.chain_height || 0;
      try {
        const chainInfo = await this.rpc.getBlockchainInfo();
        chainHeight = chainInfo.headers;
      } catch (rpcError: any) {
        // Daemon not ready or error - use stale database value
        logger.debug('Failed to get live chain height from daemon, using database value', { error: rpcError.message });
      }

      const percentageRaw = chainHeight > 0
        ? (currentHeight / chainHeight) * 100
        : 0;
      const synced = chainHeight > 0 ? currentHeight >= chainHeight - 1 : false;

      res.json({
        indexer: {
          syncing: state?.is_syncing || false,
          synced,
          currentHeight,
          chainHeight,
          progress: `${percentageRaw.toFixed(2)}%`,
          percentage: parseFloat(percentageRaw.toFixed(2)),
          lastSyncTime: state?.last_sync_time || null,
        },
        timestamp: new Date().toISOString(),
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/blocks - Get list of recent blocks
   */
  private async getBlocks(req: Request, res: Response): Promise<void> {
    try {
      const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 20));
      const offset = Math.max(0, parseInt(req.query.offset as string) || 0);

      // Get total count
      const countResult = await this.db.query('SELECT COUNT(*) as total_count FROM blocks');
      const totalBlocks = parseInt(countResult.rows[0]?.total_count || '0', 10);

      // Get blocks
      const blocksResult = await this.db.query(`
        SELECT
          height,
          hash,
          timestamp,
          tx_count,
          size,
          producer
        FROM blocks
        ORDER BY height DESC
        LIMIT $1 OFFSET $2
      `, [limit, offset]);

      res.json({
        blocks: blocksResult.rows.map(row => ({
          height: row.height,
          hash: row.hash,
          timestamp: row.timestamp,
          txCount: row.tx_count,
          size: row.size,
          producer: row.producer,
        })),
        total: totalBlocks,
        limit,
        offset,
      });
    } catch (error: any) {
      logger.error('Failed to get blocks', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  private latestBlocksCache: {
    data: any | null;
    timestamp: number;
    currentHeight: number;
    limit: number; // Track the limit used for this cache
  } = { data: null, timestamp: 0, currentHeight: -1, limit: 0 };
  private static readonly LATEST_BLOCKS_CACHE_TTL = 5000; // 5 seconds (increased from 2s to reduce query load)
  private latestBlocksRefreshPromise: Promise<any> | null = null; // Track in-progress refresh

  /**
   * GET /api/v1/blocks/latest - Get latest blocks with aggregated counts (OPTIMIZED with request coalescing)
   */
  private async getLatestBlocks(req: Request, res: Response): Promise<void> {
    try {
      const limit = Math.min(50, Math.max(1, parseInt(req.query.limit as string) || 10));

      // Check cache first (quick check without querying DB)
      // Cache is valid only if it has the same limit AND is not expired
      const now = Date.now();
      if (
        this.latestBlocksCache.data &&
        this.latestBlocksCache.limit === limit &&
        (now - this.latestBlocksCache.timestamp) < APIServer.LATEST_BLOCKS_CACHE_TTL
      ) {
        res.json(this.latestBlocksCache.data);
        return;
      }

      // If a refresh is already in progress, wait for it (request coalescing)
      if (this.latestBlocksRefreshPromise) {
        await this.latestBlocksRefreshPromise;
        // After waiting, check if the cache now has the data we need (matching limit)
        if (this.latestBlocksCache.data && this.latestBlocksCache.limit === limit) {
          res.json(this.latestBlocksCache.data);
          return;
        }
        // Otherwise fall through to refresh with correct limit
      }

      // Start refresh and track it
      const refreshPromise = this.refreshLatestBlocks(limit, now);
      this.latestBlocksRefreshPromise = refreshPromise;

      try {
        const data = await refreshPromise;
        res.json(data);
      } finally {
        this.latestBlocksRefreshPromise = null;
      }
    } catch (error: any) {
      logger.error('Failed to get latest blocks', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Internal method to refresh latest blocks cache
   */
  private async refreshLatestBlocks(limit: number, now: number): Promise<any> {
    try {
      // Get current height
      const heightResult = await this.db.query('SELECT MAX(height) as max_height FROM blocks');
      const currentHeight = heightResult.rows[0]?.max_height || 0;

      // OPTIMIZED: Simplified query - only count total FluxNode txs, no tier breakdown needed
      // Replaces slow LATERAL JOIN with simple GROUP BY + IN clause
      const query = `
        WITH latest_blocks AS (
          SELECT height, hash, timestamp, tx_count, size
          FROM blocks
          ORDER BY height DESC
          LIMIT $1
        ),
        fluxnode_counts AS (
          SELECT
            fn.block_height,
            COUNT(*) AS node_count
          FROM fluxnode_transactions fn
          WHERE fn.block_height IN (SELECT height FROM latest_blocks)
          GROUP BY fn.block_height
        )
        SELECT
          lb.height,
          lb.hash,
          lb.timestamp,
          lb.tx_count,
          lb.size,
          COALESCE(fc.node_count, 0) AS node_confirmation_count,
          (lb.tx_count - COALESCE(fc.node_count, 0)) AS regular_tx_count
        FROM latest_blocks lb
        LEFT JOIN fluxnode_counts fc ON fc.block_height = lb.height
        ORDER BY lb.height DESC
      `;

      const result = await this.db.query(query, [limit]);

      const blocks = result.rows.map(row => ({
        height: Number(row.height),
        hash: row.hash,
        time: Number(row.timestamp),
        txCount: Number((row.tx_count ?? row.txcount) ?? 0),
        size: Number(row.size ?? 0),
        regularTxCount: Number(row.regular_tx_count ?? 0),
        nodeConfirmationCount: Number(row.node_confirmation_count ?? 0),
      }));

      const responseData = { blocks };

      // Update cache
      this.latestBlocksCache = {
        data: responseData,
        timestamp: now,
        currentHeight,
        limit,
      };

      return responseData;
    } catch (error: any) {
      logger.error('Failed to refresh latest blocks', { error: error.message });
      throw error;
    }
  }

  /**
   * GET /api/v1/blocks/:heightOrHash - Get block by height or hash
   */
  private async getBlock(req: Request, res: Response): Promise<void> {
    try {
      const { heightOrHash } = req.params;
      const page = parseInt(req.query.page as string) || 1;
      const pageSize = 200;

      const cacheKey = `${heightOrHash}:${page}:${pageSize}`;
      const cached = this.blockCache.get(cacheKey);
      if (cached && (Date.now() - cached.timestamp) < APIServer.BLOCK_CACHE_TTL) {
        res.json(cached.data);
        return;
      }

      let block;

      // Check if it's a height or hash
      if (/^\d+$/.test(heightOrHash)) {
        const height = parseInt(heightOrHash);
        block = await this.db.query('SELECT * FROM blocks WHERE height = $1', [height]);
      } else {
        block = await this.db.query('SELECT * FROM blocks WHERE hash = $1', [heightOrHash]);
      }

      if (block.rows.length === 0) {
        res.status(404).json({ error: 'Block not found' });
        return;
      }

      const blockData = block.rows[0];

      // Get current block height to calculate confirmations
      const currentHeightResult = await this.db.query('SELECT MAX(height) as max_height FROM blocks');
      const currentHeight = currentHeightResult.rows[0]?.max_height || 0;
      const confirmations = blockData.height ? Math.max(0, currentHeight - blockData.height + 1) : 0;

      // Get next block hash
      let nextBlockHash: string | null = null;
      if (blockData.height !== null && blockData.height !== undefined) {
        const nextBlock = await this.db.query('SELECT hash FROM blocks WHERE height = $1', [blockData.height + 1]);
        if (nextBlock.rows.length > 0) {
          nextBlockHash = nextBlock.rows[0].hash;
        }
      }

      const txResult = await this.db.query(`
        SELECT
          t.txid,
          t.is_coinbase,
          t.output_total,
          t.fee,
          t.size,
          t.vsize,
          fn.type AS fluxnode_type,
          fn.benchmark_tier,
          fn.ip_address,
          fn.public_key,
          fn.signature
        FROM transactions t
        LEFT JOIN fluxnode_transactions fn ON fn.txid = t.txid
        WHERE t.block_height = $1
        ORDER BY
          CASE
            WHEN t.is_coinbase THEN 0
            WHEN fn.type IS NULL THEN 1
            WHEN fn.type = 2 THEN 2
            ELSE 3
          END,
          t.txid
      `, [blockData.height]);

      const allTxRows = txResult.rows;
      const totalTxs = allTxRows.length;

      const summary = {
        total: totalTxs,
        coinbase: 0,
        transfers: 0,
        fluxnodeStart: 0,
        fluxnodeConfirm: 0,
        fluxnodeOther: 0,
        tierCounts: {
          cumulus: 0,
          nimbus: 0,
          stratus: 0,
          starting: 0,
          unknown: 0,
        },
      };

      const txDetails = allTxRows.map((row: any, index: number) => {
        let kind: 'coinbase' | 'transfer' | 'fluxnode_start' | 'fluxnode_confirm' | 'fluxnode_other' = 'transfer';
        if (row.is_coinbase) {
          kind = 'coinbase';
          summary.coinbase += 1;
        } else if (row.fluxnode_type !== null && row.fluxnode_type !== undefined) {
          if (row.fluxnode_type === 2) {
            kind = 'fluxnode_start';
            summary.fluxnodeStart += 1;
            summary.tierCounts.starting += 1;
          } else if (row.fluxnode_type === 4) {
            kind = 'fluxnode_confirm';
            summary.fluxnodeConfirm += 1;
          } else {
            kind = 'fluxnode_other';
            summary.fluxnodeOther += 1;
          }

          if (row.benchmark_tier) {
            const tier = String(row.benchmark_tier).toUpperCase();
            if (tier === 'CUMULUS') summary.tierCounts.cumulus += 1;
            else if (tier === 'NIMBUS') summary.tierCounts.nimbus += 1;
            else if (tier === 'STRATUS') summary.tierCounts.stratus += 1;
            else summary.tierCounts.unknown += 1;
          } else if (kind === 'fluxnode_confirm') {
            summary.tierCounts.unknown += 1;
          }
        } else {
          summary.transfers += 1;
        }

        const valueSat = row.output_total ? Number(row.output_total) : 0;

        // Calculate fee correctly for coinbase transactions
        // Fee = MAX(0, total_output - expected_block_reward)
        // Negative values indicate unclaimed mining rewards (burned), not fees
        let feeSat: number;
        if (row.is_coinbase) {
          const expectedReward = this.getExpectedBlockReward(blockData.height);
          const expectedRewardSat = Math.floor(expectedReward * 1e8);
          const calculatedFee = valueSat - expectedRewardSat;
          // If output < expected, unclaimed rewards are burned - fee should be 0
          feeSat = calculatedFee > 0 ? calculatedFee : 0;
        } else {
          feeSat = row.fee ? Number(row.fee) : 0;
        }

        // Convert benchmark tier to tier name (handles both numeric and string values)
        let fluxnodeTier: string | null = null;
        if (row.benchmark_tier !== null && row.benchmark_tier !== undefined) {
          const tierStr = String(row.benchmark_tier).toUpperCase().trim();

          // Handle numeric tiers (1=CUMULUS, 2=NIMBUS, 3=STRATUS)
          if (tierStr === '1') fluxnodeTier = 'CUMULUS';
          else if (tierStr === '2') fluxnodeTier = 'NIMBUS';
          else if (tierStr === '3') fluxnodeTier = 'STRATUS';
          // Handle string tiers (already correct format)
          else if (tierStr === 'CUMULUS' || tierStr === 'NIMBUS' || tierStr === 'STRATUS') {
            fluxnodeTier = tierStr;
          }
          // Unknown tier value
          else {
            fluxnodeTier = null;
          }
        }

        return {
          txid: row.txid,
          order: index,
          kind,
          isCoinbase: !!row.is_coinbase,
          fluxnodeType: row.fluxnode_type ?? null,
          fluxnodeTier,
          fluxnodeIp: row.ip_address || null,
          fluxnodePubKey: row.public_key || null,
          fluxnodeSignature: row.signature || null,
          valueSat,
          value: valueSat / 1e8,
          feeSat,
          fee: feeSat / 1e8,
          size: row.size !== null && row.size !== undefined
            ? Number(row.size)
            : row.vsize !== null && row.vsize !== undefined
              ? Number(row.vsize)
              : 0,
        };
      });

      const pagedDetails = txDetails.slice((page - 1) * pageSize, page * pageSize);

      const payload = {
        page,
        totalPages: Math.max(1, Math.ceil(Math.max(0, totalTxs) / pageSize)),
        itemsOnPage: pagedDetails.length,
        hash: blockData.hash,
        previousBlockHash: blockData.prev_hash,
        nextBlockHash,
        height: blockData.height,
        confirmations,
        size: blockData.size,
        time: blockData.timestamp,
        version: blockData.version,
        merkleRoot: blockData.merkle_root,
        nonce: blockData.nonce !== null && blockData.nonce !== undefined ? blockData.nonce.toString() : null,
        bits: blockData.bits,
        difficulty: blockData.difficulty,
        txCount: blockData.tx_count,
        producer: blockData.producer,
        producerReward: blockData.producer_reward ? blockData.producer_reward.toString() : null,
        txs: pagedDetails.map(detail => ({ txid: detail.txid })),
        txDetails,
        txSummary: {
          ...summary,
          regular: summary.coinbase + summary.transfers,
          fluxnodeTotal: summary.fluxnodeStart + summary.fluxnodeConfirm + summary.fluxnodeOther,
        },
      };

      this.blockCache.set(cacheKey, { data: payload, timestamp: Date.now() });
      if (this.blockCache.size > APIServer.BLOCK_CACHE_LIMIT) {
        const oldestKey = this.blockCache.keys().next().value;
        if (oldestKey) {
          this.blockCache.delete(oldestKey);
        }
      }

      res.json(payload);
    } catch (error: any) {
      logger.error('Failed to get block', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/blocks/:height - Get block by height
   */
  private async getBlockByHeight(req: Request, res: Response): Promise<void> {
    try {
      const height = parseInt(req.params.height);
      const block = await this.db.query('SELECT hash FROM blocks WHERE height = $1', [height]);

      if (block.rows.length === 0) {
        res.status(404).json({ error: 'Block not found' });
        return;
      }

      res.json({
        blockHash: block.rows[0].hash,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/transactions/:txid - Get transaction
   */
  private async getTransaction(req: Request, res: Response): Promise<void> {
    try {
      const { txid } = req.params;

      // Get transaction
      const tx = await this.db.query('SELECT * FROM transactions WHERE txid = $1', [txid]);

      if (tx.rows.length === 0) {
        res.status(404).json({ error: 'Transaction not found' });
        return;
      }

      const txData = tx.rows[0];

      // Get current block height to calculate confirmations
      const currentHeightResult = await this.db.query('SELECT MAX(height) as max_height FROM blocks');
      const currentHeight = currentHeightResult.rows[0]?.max_height || 0;
      const confirmations = txData.block_height ? Math.max(0, currentHeight - txData.block_height + 1) : 0;

      // Get inputs
      const inputs = await this.db.query(
        `SELECT u.*, t.block_height
         FROM utxos u
         JOIN transactions t ON t.txid = u.spent_txid
         WHERE u.spent_txid = $1
         ORDER BY u.vout`,
        [txid]
      );

      // Get outputs
      const outputs = await this.db.query(
        'SELECT * FROM utxos WHERE txid = $1 ORDER BY vout',
        [txid]
      );

      let sizeBytes = txData.size && Number(txData.size) > 0
        ? Number(txData.size)
        : 0;
      let vsizeBytes = txData.vsize && Number(txData.vsize) > 0
        ? Number(txData.vsize)
        : 0;
      let hexValue = txData.hex as string | null;

      if (!hexValue || hexValue.length === 0) {
        hexValue = null;
      } else if (sizeBytes === 0) {
        sizeBytes = Math.floor(hexValue.length / 2);
      }

      if (sizeBytes === 0 || vsizeBytes === 0 || !hexValue) {
        try {
          // Flux daemon doesn't support blockhash parameter, requires txindex=1
          const rawTx = await this.rpc.getRawTransaction(txid, false);
          let fetchedHex: string | null = null;
          let fetchedSize = sizeBytes;
          let fetchedVSize = vsizeBytes;

          if (typeof rawTx === 'string' && rawTx.length > 0) {
            fetchedHex = rawTx;
            fetchedSize = Math.floor(rawTx.length / 2);
            fetchedVSize = fetchedSize;
          }

          const updateSize = fetchedSize > 0 ? fetchedSize : null;
          const updateVSize = fetchedVSize > 0 ? fetchedVSize : null;
          const updateHex = fetchedHex && fetchedHex.length > 0 ? fetchedHex : null;

          if (updateSize !== null || updateVSize !== null || updateHex !== null) {
            await this.db.query(
              `
                UPDATE transactions
                SET
                  size = COALESCE($1, size),
                  vsize = COALESCE($2, vsize),
                  hex = COALESCE($3, hex)
                WHERE txid = $4
              `,
              [updateSize, updateVSize, updateHex, txid]
            );
          }

          if (updateSize !== null) sizeBytes = updateSize;
          if (updateVSize !== null) vsizeBytes = updateVSize;
          if (updateHex !== null) hexValue = updateHex;
        } catch (error: any) {
          // Flux daemon returns HTTP 500 for many transactions
          // Extract them from raw block hex instead
          if (txData.block_hash) {
            try {
              const rawBlockHex = await this.rpc.getBlock(txData.block_hash, 0) as unknown as string;
              const txHex = extractTransactionFromBlock(rawBlockHex, txid, txData.block_height);

              if (txHex && txHex.length > 0) {
                const fetchedSize = Math.floor(txHex.length / 2);

                await this.db.query(
                  `UPDATE transactions SET size = $1, vsize = $2, hex = $3 WHERE txid = $4`,
                  [fetchedSize, fetchedSize, txHex, txid]
                );

                sizeBytes = fetchedSize;
                vsizeBytes = fetchedSize;
                hexValue = txHex;

                logger.info('Successfully extracted transaction hex from block for API', {
                  txid,
                  block: txData.block_hash,
                  size: fetchedSize
                });
              } else {
                logger.warn('Transaction not found in block hex for API', {
                  txid,
                  block: txData.block_hash
                });
              }
            } catch (blockError: any) {
              logger.warn('Failed to extract transaction from block hex for API', {
                txid,
                block: txData.block_hash,
                error: blockError.message
              });
            }
          } else {
            logger.warn('Failed to hydrate transaction size', { txid, error: error.message });
          }
        }
      }

      // Calculate fee correctly for coinbase transactions
      // Coinbase transactions have no inputs (inputs.rows.length === 0)
      // Fee = MAX(0, total_output - expected_block_reward)
      // Negative values indicate unclaimed mining rewards (burned), not fees
      const isCoinbase = inputs.rows.length === 0;
      let feeZatoshis: bigint;

      if (isCoinbase && txData.block_height) {
        const expectedReward = this.getExpectedBlockReward(txData.block_height);
        const expectedRewardZatoshis = BigInt(Math.floor(expectedReward * 100000000));
        const outputTotal = BigInt(txData.output_total || 0);
        const calculatedFee = outputTotal - expectedRewardZatoshis;
        // If output < expected, unclaimed rewards are burned - fee should be 0
        feeZatoshis = calculatedFee > BigInt(0) ? calculatedFee : BigInt(0);
      } else {
        feeZatoshis = txData.fee ? BigInt(txData.fee) : BigInt(0);
      }

      res.json({
        txid: txData.txid,
        version: txData.version,
        locktime: txData.locktime,
        vin: inputs.rows.map(row => ({
          txid: row.txid,
          vout: row.vout,
          sequence: 0,
          n: row.vout,
          addresses: row.address && row.address !== 'SHIELDED_OR_NONSTANDARD' ? [row.address] : [],
          value: row.value ? row.value.toString() : '0',
        })),
        vout: outputs.rows.map(row => {
          const scriptType = row.script_type || 'unknown';
          const normalizedAddress = row.address === 'SHIELDED_OR_NONSTANDARD' ? null : row.address;
          const opReturn = scriptType === 'nulldata' ? decodeOpReturnData(row.script_pubkey) : null;

          return {
            value: row.value ? row.value.toString() : '0',
            n: row.vout,
            scriptPubKey: {
              hex: row.script_pubkey || '',
              asm: '',
              addresses: normalizedAddress ? [normalizedAddress] : [],
              type: scriptType,
              opReturnHex: opReturn?.hex ?? null,
              opReturnText: opReturn?.text ?? null,
            },
            spentTxId: row.spent_txid || undefined,
            spentHeight: row.spent_block_height || undefined,
          };
        }),
        blockHash: txData.block_hash,
        blockHeight: txData.block_height,
        confirmations,
        blockTime: txData.timestamp,
        size: sizeBytes,
        vsize: vsizeBytes,
        value: txData.output_total ? txData.output_total.toString() : '0',
        valueIn: txData.input_total ? txData.input_total.toString() : '0',
        fees: feeZatoshis.toString(),
        hex: hexValue,
      });
    } catch (error: any) {
      logger.error('Failed to get transaction', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/addresses/:address - Get address info
   */
  private async getAddress(req: Request, res: Response): Promise<void> {
    try {
      const { address } = req.params;
      const page = parseInt(req.query.page as string) || 1;
      // Limit page size to 100, default to 25 (was 1000 before)
      // This prevents loading excessive transactions and improves response time
      const pageSize = Math.min(100, Math.max(1, parseInt(req.query.pageSize as string) || 25));

      // Get address summary
      const summary = await this.db.query(
        'SELECT * FROM address_summary WHERE address = $1',
        [address]
      );

      if (summary.rows.length === 0) {
        res.status(404).json({ error: 'Address not found' });
        return;
      }

      const addressData = summary.rows[0];

      // Get transactions using optimized address_transactions materialized table
      // This is 5-10x faster than joining transactions + utxos tables
      // Uses idx_address_tx_pagination index for optimal performance
      const txQuery = `
        SELECT txid, block_height, timestamp, block_hash
        FROM address_transactions
        WHERE address = $1
        ORDER BY block_height DESC, txid DESC
        LIMIT $2 OFFSET $3
      `;

      const transactions = await this.db.query(txQuery, [
        address,
        pageSize,
        (page - 1) * pageSize,
      ]);

      res.json({
        page,
        totalPages: Math.ceil(addressData.tx_count / pageSize),
        itemsOnPage: transactions.rows.length,
        address: addressData.address,
        balance: addressData.balance ? addressData.balance.toString() : '0',
        totalReceived: addressData.received_total ? addressData.received_total.toString() : '0',
        totalSent: addressData.sent_total ? addressData.sent_total.toString() : '0',
        unconfirmedBalance: addressData.unconfirmed_balance ? addressData.unconfirmed_balance.toString() : '0',
        unconfirmedTxs: 0,
        txs: addressData.tx_count,
        transactions: transactions.rows,
      });
    } catch (error: any) {
      logger.error('Failed to get address', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/addresses/:address/transactions - Get address transactions
   */
  private async getAddressTransactions(req: Request, res: Response): Promise<void> {
    try {
      const { address } = req.params;
      const fromTimestamp = req.query.fromTimestamp ? parseInt(req.query.fromTimestamp as string) : undefined;
      const toTimestamp = req.query.toTimestamp ? parseInt(req.query.toTimestamp as string) : undefined;

      // Cursor-based pagination parameters (preferred method)
      const cursorHeight = req.query.cursorHeight ? parseInt(req.query.cursorHeight as string) : undefined;
      const cursorTxid = req.query.cursorTxid as string | undefined;

      // Legacy offset pagination (kept for backward compatibility with CSV exports)
      const offset = Math.max(0, parseInt(req.query.offset as string) || 0);

      // For date range exports, allow larger batch sizes (up to 10000)
      // Otherwise cap at 100 for standard pagination
      const maxLimit = (fromTimestamp !== undefined || toTimestamp !== undefined) ? 10000 : 100;
      const limit = Math.min(maxLimit, Math.max(1, parseInt(req.query.limit as string) || 20));

      // TEMPORARY: Disable transaction history during initial sync to prevent blocking
      // Check if we're in fast sync mode (far behind)
      const syncState = await this.db.query('SELECT current_height, chain_height FROM sync_state WHERE id = 1');
      if (syncState.rows.length > 0) {
        const currentHeight = syncState.rows[0].current_height;
        const chainHeight = syncState.rows[0].chain_height;
        const blocksBehind = chainHeight - currentHeight;

        if (blocksBehind > 1000) {
          res.status(503).json({
            error: 'Transaction history temporarily unavailable during initial blockchain sync',
            message: `Indexer is ${blocksBehind.toLocaleString()} blocks behind. Transaction history will be available once sync is complete.`,
            currentHeight,
            chainHeight,
            progress: `${((currentHeight / chainHeight) * 100).toFixed(2)}%`
          });
          return;
        }
      }

      // Get transaction count from summary
      const countQuery = await this.db.query(
        'SELECT tx_count FROM address_summary WHERE address = $1',
        [address]
      );

      if (countQuery.rows.length === 0) {
        res.status(404).json({ error: 'Address not found' });
        return;
      }

      const totalTxs = parseInt(countQuery.rows[0].tx_count, 10) || 0;

      // ULTRA-OPTIMIZED: Use materialized address_transactions table (migration 007)
      // This eliminates expensive UNION and LATERAL JOIN queries entirely
      // Falls back to old query if table doesn't exist yet
      // Support timestamp filtering for date range exports
      const whereConditions = ['at.address = $1'];
      const queryParams: any[] = [address];
      let paramIndex = 2;

      if (fromTimestamp !== undefined) {
        whereConditions.push(`at.timestamp >= $${paramIndex}`);
        queryParams.push(fromTimestamp);
        paramIndex++;
      }

      if (toTimestamp !== undefined) {
        whereConditions.push(`at.timestamp <= $${paramIndex}`);
        queryParams.push(toTimestamp);
        paramIndex++;
      }

      // Cursor-based pagination: Use composite key (block_height, txid) for efficient seeks
      // This replaces OFFSET which becomes extremely slow with large datasets
      if (cursorHeight !== undefined && cursorTxid) {
        whereConditions.push(`(at.block_height, at.txid) < ($${paramIndex}, $${paramIndex + 1})`);
        queryParams.push(cursorHeight, cursorTxid);
        paramIndex += 2;
      }

      const txQuery = `
        SELECT
          at.txid,
          at.block_height,
          at.timestamp,
          at.block_hash,
          at.direction,
          at.received_value,
          at.sent_value,
          COALESCE(t.is_coinbase, FALSE) AS is_coinbase,
          COALESCE(t.fee, 0)::bigint AS fee_value
        FROM address_transactions at
        LEFT JOIN transactions t ON t.txid = at.txid
        WHERE ${whereConditions.join(' AND ')}
        ORDER BY at.block_height DESC, at.txid DESC
        LIMIT $${paramIndex}${cursorHeight === undefined && offset > 0 ? ` OFFSET $${paramIndex + 1}` : ''}
      `;

      // Use cursor-based pagination if cursor is provided, otherwise fall back to offset
      if (cursorHeight === undefined && offset > 0) {
        queryParams.push(limit, offset);
      } else {
        queryParams.push(limit);
      }

      const transactions = await this.db.query(txQuery, queryParams);

      // Get filtered count if timestamp filters are applied
      let filteredTotal = totalTxs;
      if (fromTimestamp !== undefined || toTimestamp !== undefined) {
        // Build count query with WHERE conditions excluding cursor (cursor is for pagination, not filtering)
        const countWhereConditions = ['at.address = $1'];
        const countParams: any[] = [address];
        let countParamIndex = 2;

        if (fromTimestamp !== undefined) {
          countWhereConditions.push(`at.timestamp >= $${countParamIndex}`);
          countParams.push(fromTimestamp);
          countParamIndex++;
        }

        if (toTimestamp !== undefined) {
          countWhereConditions.push(`at.timestamp <= $${countParamIndex}`);
          countParams.push(toTimestamp);
          countParamIndex++;
        }

        const countQueryFiltered = `
          SELECT COUNT(*) as filtered_count
          FROM address_transactions at
          WHERE ${countWhereConditions.join(' AND ')}
        `;
        const filteredCountResult = await this.db.query(countQueryFiltered, countParams);
        filteredTotal = parseInt(filteredCountResult.rows[0]?.filtered_count || '0', 10);
      }

      const currentHeightResult = await this.db.query('SELECT MAX(height) as max_height FROM blocks');
      const currentHeight = currentHeightResult.rows[0]?.max_height || 0;

      const txids = transactions.rows.map((row: any) => row.txid);
      const participantsMap = new Map<string, { inputs: string[]; outputs: string[]; inputCount: number; outputCount: number }>();

      if (txids.length > 0) {
        try {
          const participantResult = await this.db.query(
            `
            SELECT
              txid,
              input_addresses,
              input_count,
              output_addresses,
              output_count
              FROM transaction_participants
              WHERE txid = ANY($1)
            `,
            [txids]
          );

          for (const row of participantResult.rows) {
            const inputs: string[] = Array.isArray(row.input_addresses)
              ? row.input_addresses.filter((addr: string | null) => !!addr)
              : [];
            const outputs: string[] = Array.isArray(row.output_addresses)
              ? row.output_addresses.filter((addr: string | null) => !!addr)
              : [];
            const inputCount = typeof row.input_count === 'number'
              ? row.input_count
              : inputs.length;
            const outputCount = typeof row.output_count === 'number'
              ? row.output_count
              : outputs.length;
            participantsMap.set(row.txid, { inputs, outputs, inputCount, outputCount });
          }
        } catch (error: any) {
          // Fallback for deployments that have not run migration 010 yet.
          if (error.code !== '42P01') {
            throw error;
          }

          const inputResult = await this.db.query(
            `
              SELECT
                spent_txid AS txid,
                ARRAY_AGG(DISTINCT address) FILTER (
                  WHERE address IS NOT NULL AND address <> 'SHIELDED_OR_NONSTANDARD'
                ) AS input_addresses
              FROM utxos
              WHERE spent_txid = ANY($1)
              GROUP BY spent_txid
            `,
            [txids]
          );

          for (const row of inputResult.rows) {
            const addresses: string[] = Array.isArray(row.input_addresses)
              ? row.input_addresses.filter((addr: string | null) => !!addr)
              : [];
            participantsMap.set(row.txid, {
              inputs: addresses,
              outputs: [],
              inputCount: addresses.length,
              outputCount: 0,
            });
          }

          const outputResult = await this.db.query(
            `
              SELECT
                txid,
                ARRAY_AGG(DISTINCT address) FILTER (
                  WHERE address IS NOT NULL AND address <> 'SHIELDED_OR_NONSTANDARD'
                ) AS output_addresses
              FROM utxos
              WHERE txid = ANY($1)
              GROUP BY txid
            `,
            [txids]
          );

          for (const row of outputResult.rows) {
            const addresses: string[] = Array.isArray(row.output_addresses)
              ? row.output_addresses.filter((addr: string | null) => !!addr)
              : [];
            const existing = participantsMap.get(row.txid) || {
              inputs: [],
              outputs: [],
              inputCount: 0,
              outputCount: 0,
            };
            existing.outputs = addresses;
            existing.outputCount = addresses.length;
            participantsMap.set(row.txid, existing);
          }
        }
      }

      // Calculate next cursor for efficient pagination
      const lastTx = transactions.rows[transactions.rows.length - 1];
      const nextCursor = lastTx ? {
        height: Number(lastTx.block_height),
        txid: lastTx.txid
      } : null;

      const responseData: any = {
        address,
        transactions: transactions.rows.map(row => {
          const receivedValue = BigInt(row.received_value || 0);
          const sentValue = BigInt(row.sent_value || 0);
          const direction = row.direction || (receivedValue >= sentValue ? 'received' : 'sent');
          const feeValue = BigInt(row.fee_value || 0);
          const participants = participantsMap.get(row.txid) || { inputs: [], outputs: [], inputCount: 0, outputCount: 0 };
          const dedupe = (list: string[]) => Array.from(new Set(list)).slice(0, 8);
          const rawInputs = participants.inputs || [];
          const rawOutputs = participants.outputs || [];
          const counterpartInputs = rawInputs.filter(addr => addr !== address);
          const counterpartOutputs = rawOutputs.filter(addr => addr !== address);
          const totalInputCount = typeof participants.inputCount === 'number' ? participants.inputCount : rawInputs.length;
          const totalOutputCount = typeof participants.outputCount === 'number' ? participants.outputCount : rawOutputs.length;
          const includesSelfInput = sentValue > BigInt(0);
          const includesSelfOutput = receivedValue > BigInt(0);
          const fromAddressCount = Math.max(0, totalInputCount - (includesSelfInput ? 1 : 0));
          const toAddressCount = Math.max(0, totalOutputCount - (includesSelfOutput ? 1 : 0));

          let changeValue = BigInt(0);
          let toOthersValue = BigInt(0);
          let selfTransfer = false;

          if (direction === 'sent') {
            changeValue = receivedValue > BigInt(0) ? receivedValue : BigInt(0);
            const computed = sentValue - changeValue - feeValue;
            toOthersValue = computed > BigInt(0) ? computed : BigInt(0);
            selfTransfer = toOthersValue === BigInt(0);
          }

          return {
            txid: row.txid,
            blockHeight: Number(row.block_height),
            timestamp: Number(row.timestamp),
            blockHash: row.block_hash,
            direction,
            value: (receivedValue - sentValue).toString(),
            receivedValue: receivedValue.toString(),
            sentValue: sentValue.toString(),
            fromAddresses: dedupe(counterpartInputs),
            fromAddressCount,
            toAddresses: dedupe(counterpartOutputs),
            toAddressCount,
            selfTransfer,
            feeValue: feeValue.toString(),
            changeValue: changeValue.toString(),
            toOthersValue: toOthersValue.toString(),
            confirmations: row.block_height ? Math.max(0, currentHeight - Number(row.block_height) + 1) : 0,
            isCoinbase: row.is_coinbase === true,
          };
        }),
        total: totalTxs,
        filteredTotal: filteredTotal, // Filtered count (respects timestamp range)
        limit,
      };

      // Include cursor in response for cursor-based pagination
      if (nextCursor && transactions.rows.length === limit) {
        responseData.nextCursor = nextCursor;
      }

      // Include offset in response for legacy offset-based pagination
      if (cursorHeight === undefined) {
        responseData.offset = offset;
      }

      res.json(responseData);
    } catch (error: any) {
      logger.error('Failed to get address transactions', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/addresses/:address/utxos - Get address UTXOs
   */
  private async getAddressUTXOs(req: Request, res: Response): Promise<void> {
    try {
      const { address } = req.params;

      const utxos = await this.db.query(
        `SELECT * FROM utxos
         WHERE address = $1 AND spent = false
         ORDER BY block_height DESC, vout`,
        [address]
      );

      res.json(
        utxos.rows.map(row => ({
          txid: row.txid,
          vout: row.vout,
          value: row.value ? row.value.toString() : '0',
          height: row.block_height,
          confirmations: 0, // Calculate from current height
        }))
      );
    } catch (error: any) {
      logger.error('Failed to get UTXOs', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/producers - Get all producers
   */
  private async getProducers(req: Request, res: Response): Promise<void> {
    try {
      const limit = parseInt(req.query.limit as string) || 100;

      const producers = await this.db.query(
        `SELECT * FROM producers
         ORDER BY blocks_produced DESC
         LIMIT $1`,
        [limit]
      );

      const totalBlocks = await this.db.query('SELECT COUNT(*) as count FROM blocks WHERE producer IS NOT NULL');

      res.json({
        producers: producers.rows.map(row => ({
          fluxnode: row.fluxnode,
          blocksProduced: row.blocks_produced,
          firstBlock: row.first_block,
          lastBlock: row.last_block,
          totalRewards: row.total_rewards ? row.total_rewards.toString() : '0',
          averageBlockTime: row.avg_block_time,
          percentageOfBlocks: totalBlocks.rows[0].count > 0
            ? ((row.blocks_produced / totalBlocks.rows[0].count) * 100).toFixed(2)
            : '0.00',
        })),
        totalProducers: producers.rows.length,
      });
    } catch (error: any) {
      logger.error('Failed to get producers', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/producers/:identifier - Get specific producer
   */
  private async getProducer(req: Request, res: Response): Promise<void> {
    try {
      const { identifier } = req.params;

      const producer = await this.db.query(
        'SELECT * FROM producers WHERE fluxnode = $1',
        [identifier]
      );

      if (producer.rows.length === 0) {
        res.status(404).json({ error: 'Producer not found' });
        return;
      }

      const row = producer.rows[0];

      res.json({
        fluxnode: row.fluxnode,
        blocksProduced: row.blocks_produced,
        firstBlock: row.first_block,
        lastBlock: row.last_block,
        totalRewards: row.total_rewards ? row.total_rewards.toString() : '0',
        averageBlockTime: row.avg_block_time,
      });
    } catch (error: any) {
      logger.error('Failed to get producer', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/richlist
   * Get rich list with pagination (CACHED - supply calculation cached for 30s)
   */
  private async getRichList(req: Request, res: Response): Promise<void> {
    try {
      const page = Math.max(1, parseInt(req.query.page as string) || 1);
      const pageSize = Math.min(1000, Math.max(1, parseInt(req.query.pageSize as string) || 100));
      const minBalance = parseInt(req.query.minBalance as string) || 1;
      const offset = (page - 1) * pageSize;

      // Get supply stats from cache or refresh
      const now = Date.now();
      let supplyStats = this.richListSupplyCache.data;

      if (!supplyStats || (now - this.richListSupplyCache.timestamp) > APIServer.RICH_LIST_SUPPLY_CACHE_TTL) {
        // If a refresh is already in progress, wait for it (request coalescing)
        if (this.richListSupplyRefreshPromise) {
          await this.richListSupplyRefreshPromise;
          supplyStats = this.richListSupplyCache.data;
        } else {
          // Start a new refresh
          const refreshPromise = (async () => {
            // Get total count of addresses (using pg_stat_user_tables for speed)
            const totalAddresses = await this.db.query(`
              SELECT n_live_tup as count
              FROM pg_stat_user_tables
              WHERE schemaname = 'public' AND relname = 'address_summary'
            `);

            // Get correct total supply from address balances + shielded pool
            // Using address_summary is 13x faster than summing all UTXOs (127ms vs 1735ms)
            const supplyQuery = await this.db.query(`
              SELECT
                COALESCE(SUM(balance), 0) as transparent_supply,
                (SELECT COALESCE(shielded_pool, 0) FROM supply_stats ORDER BY block_height DESC LIMIT 1) as shielded_pool
              FROM address_summary
              WHERE balance > 0
            `);

            const transparentSupply = BigInt(supplyQuery.rows[0]?.transparent_supply || '0');
            const shieldedPool = BigInt(supplyQuery.rows[0]?.shielded_pool || '0');
            const totalSupply = (transparentSupply + shieldedPool).toString();

            const newStats = {
              totalSupply,
              totalAddresses: parseInt(totalAddresses.rows[0]?.count || '0'),
            };

            this.richListSupplyCache = { data: newStats, timestamp: now };
            return newStats;
          })();

          this.richListSupplyRefreshPromise = refreshPromise;

          try {
            supplyStats = await refreshPromise;
          } finally {
            this.richListSupplyRefreshPromise = null;
          }
        }
      }

      // Fallback if stats are still unavailable
      if (!supplyStats) {
        supplyStats = { totalSupply: '0', totalAddresses: 0 };
      }

      // Get paginated rich list
      const richListQuery = await this.db.query(`
        SELECT
          address,
          balance,
          tx_count,
          received_total,
          sent_total,
          unspent_count,
          first_seen,
          last_activity
        FROM address_summary
        WHERE balance >= $1
        ORDER BY balance DESC
        LIMIT $2 OFFSET $3
      `, [minBalance * 1e8, pageSize, offset]);

      // Get current block height for metadata
      const syncState = await this.db.query('SELECT current_height, chain_height FROM sync_state WHERE id = 1');
      const currentHeight = syncState.rows[0]?.current_height || 0;

      // Format addresses
      const addresses = richListQuery.rows.map((row, index) => ({
        rank: offset + index + 1,
        address: row.address,
        balance: row.balance ? row.balance.toString() : '0',
        txCount: row.tx_count,
        receivedTotal: row.received_total ? row.received_total.toString() : '0',
        sentTotal: row.sent_total ? row.sent_total.toString() : '0',
        unspentCount: row.unspent_count,
        firstSeen: row.first_seen,
        lastActivity: row.last_activity,
      }));

      res.json({
        lastUpdate: new Date().toISOString(),
        lastBlockHeight: currentHeight,
        totalSupply: supplyStats.totalSupply,
        totalAddresses: supplyStats.totalAddresses,
        page,
        pageSize,
        totalPages: Math.ceil(supplyStats.totalAddresses / pageSize),
        addresses,
      });
    } catch (error: any) {
      logger.error('Failed to get rich list', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

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

  /**
   * GET /api/v1/supply
   * Get supply statistics (transparent, shielded, circulating, total, max)
   */
  private async getSupplyStats(req: Request, res: Response): Promise<void> {
    try {
      // Get current block height
      const syncState = await this.db.query('SELECT current_height FROM sync_state WHERE id = 1');
      const currentHeight = syncState.rows[0]?.current_height || 0;

      if (currentHeight === 0) {
        res.status(503).json({
          error: 'Indexer not synced',
          message: 'Supply statistics are not yet available',
        });
        return;
      }

      // Get latest supply stats (shielded pool)
      const supplyQuery = await this.db.query(`
        SELECT
          block_height,
          shielded_pool,
          updated_at
        FROM supply_stats
        ORDER BY block_height DESC
        LIMIT 1
      `);

      if (supplyQuery.rows.length === 0) {
        res.status(503).json({
          error: 'Supply data not available',
          message: 'Supply statistics have not been calculated yet',
        });
        return;
      }

      const stats = supplyQuery.rows[0];

      // Calculate transparent supply using address_summary balances
      // 13x faster than summing all UTXOs (127ms vs 1735ms)
      const transparentQuery = await this.db.query(`
        SELECT COALESCE(SUM(balance), 0) as transparent_supply
        FROM address_summary
        WHERE balance > 0
      `);

      // Database stores values in zatoshis (integers), but PostgreSQL numeric type may have decimals
      // Convert to string and remove any decimal places
      const transparentSupplyRaw = String(transparentQuery.rows[0]?.transparent_supply || '0');
      const shieldedPoolRaw = String(stats.shielded_pool || '0');

      // Remove decimal point and fractional part if present (convert to zatoshis)
      const transparentSupply = BigInt(transparentSupplyRaw.split('.')[0]);
      const shieldedPool = BigInt(shieldedPoolRaw.split('.')[0]);
      const totalSupply = transparentSupply + shieldedPool;

      // Calculate circulating supply
      // Circulating = Total supply - Locked parallel assets
      // Where locked = theoretical mainchain supply - theoretical distributed to parallel chains
      const theoreticalMainchain = this.calculateMainchainSupply(stats.block_height);
      const theoreticalAllChains = this.calculateCirculatingSupplyAllChains(stats.block_height);
      const lockedParallelAssets = theoreticalMainchain - theoreticalAllChains;
      const circulatingSupply = totalSupply - lockedParallelAssets;

      // Max supply is 560 million FLUX
      const maxSupply = BigInt(560000000) * BigInt(100000000); // in zatoshis

      // Helper function to convert zatoshis to FLUX with decimal places
      const toFlux = (zatoshis: bigint): string => {
        const flux = Number(zatoshis) / 100000000;
        return flux.toString();
      };

      res.json({
        blockHeight: stats.block_height,
        transparentSupply: toFlux(transparentSupply),
        shieldedPool: toFlux(shieldedPool),
        circulatingSupply: toFlux(circulatingSupply),
        totalSupply: toFlux(totalSupply),
        maxSupply: toFlux(maxSupply),
        lastUpdate: stats.updated_at,
        timestamp: new Date().toISOString(),
      });
    } catch (error: any) {
      logger.error('Failed to get supply stats', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/nodes - Get FluxNode list
   */
  private async getFluxNodes(req: Request, res: Response): Promise<void> {
    try {
      const filter = (req.query.filter as string) || 'all'; // all, confirmed, starting

      const nodeList = await this.rpc.getFluxNodeList();

      // Filter nodes if requested
      let filteredNodes = nodeList;
      if (filter !== 'all' && Array.isArray(nodeList)) {
        filteredNodes = nodeList.filter((node: any) => node.status === filter);
      }

      res.json({
        nodes: filteredNodes,
        total: Array.isArray(filteredNodes) ? filteredNodes.length : 0,
        filter,
        timestamp: new Date().toISOString(),
      });
    } catch (error: any) {
      logger.error('Failed to get FluxNode list', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/nodes/:ip - Get FluxNode status
   */
  private async getFluxNodeStatus(req: Request, res: Response): Promise<void> {
    try {
      const { ip } = req.params;

      const nodeStatus = await this.rpc.getFluxNodeStatus(ip);

      res.json({
        ip,
        status: nodeStatus,
        timestamp: new Date().toISOString(),
      });
    } catch (error: any) {
      logger.error('Failed to get FluxNode status', { error: error.message, ip: req.params.ip });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/network - Get network information
   */
  private async getNetworkInfo(req: Request, res: Response): Promise<void> {
    try {
      const networkInfo = await this.rpc.getNetworkInfo();
      const blockchainInfo = await this.rpc.getBlockchainInfo();

      res.json({
        network: {
          version: networkInfo.subversion,
          protocolVersion: networkInfo.protocolversion,
          connections: networkInfo.connections,
          relayfee: networkInfo.relayfee,
          localServices: networkInfo.localservices,
          networks: networkInfo.networks,
        },
        blockchain: {
          chain: blockchainInfo.chain,
          blocks: blockchainInfo.blocks,
          headers: blockchainInfo.headers,
          bestBlockHash: blockchainInfo.bestblockhash,
          difficulty: blockchainInfo.difficulty,
          medianTime: blockchainInfo.mediantime,
          verificationProgress: blockchainInfo.verificationprogress,
          chainwork: blockchainInfo.chainwork,
          pruned: blockchainInfo.pruned,
          consensus: 'PoN',
        },
        timestamp: new Date().toISOString(),
      });
    } catch (error: any) {
      logger.error('Failed to get network info', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * GET /api/v1/mempool - Get mempool information
   */
  private async getMempoolInfo(req: Request, res: Response): Promise<void> {
    try {
      const includeTxs = req.query.includeTxs === 'true';

      const mempoolInfo = await this.rpc.getMempoolInfo();
      let transactions = [];

      if (includeTxs) {
        const rawMempool = await this.rpc.getRawMempool(true);
        transactions = Array.isArray(rawMempool) ? rawMempool : Object.keys(rawMempool);
      }

      res.json({
        size: mempoolInfo.size,
        bytes: mempoolInfo.bytes,
        usage: mempoolInfo.usage,
        maxmempool: mempoolInfo.maxmempool,
        mempoolminfee: mempoolInfo.mempoolminfee,
        ...(includeTxs && { transactions }),
        timestamp: new Date().toISOString(),
      });
    } catch (error: any) {
      logger.error('Failed to get mempool info', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Start API server
   */
  async start(): Promise<void> {
    return new Promise((resolve) => {
      this.server = this.app.listen(this.port, () => {
        logger.info(`API server listening on port ${this.port}`);
        resolve();
      });
    });
  }

  /**
   * Stop API server
   */
  async stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.server) {
        this.server.close((err: Error) => {
          if (err) {
            reject(err);
          } else {
            logger.info('API server stopped');
            resolve();
          }
        });
      } else {
        resolve();
      }
    });
  }
}
