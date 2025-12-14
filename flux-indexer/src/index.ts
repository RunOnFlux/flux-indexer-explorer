/**
 * FluxIndexer - ClickHouse Entry Point
 *
 * Custom blockchain indexer for Flux v9.0.0+ PoN consensus
 * Using ClickHouse for high-performance storage
 */

import { config } from './config';
import { FluxRPCClient } from './rpc/flux-rpc-client';
import { ClickHouseConnection, getClickHouse, closeClickHouse } from './database/connection';
import { ClickHouseSyncEngine } from './indexer/sync-engine';
import { ClickHouseAPIServer } from './api/server';
import { ClickHouseFluxNodeSyncService } from './services/fluxnode-sync';
import { logger, printStartupBanner } from './utils/logger';
import * as fs from 'fs';
import * as path from 'path';

class ClickHouseFluxIndexer {
  private rpc!: FluxRPCClient;
  private ch!: ClickHouseConnection;
  private syncEngine!: ClickHouseSyncEngine;
  private apiServer!: ClickHouseAPIServer;
  private fluxNodeSync!: ClickHouseFluxNodeSyncService;
  private isShuttingDown = false;

  /**
   * Initialize FluxIndexer with ClickHouse
   */
  async initialize(): Promise<void> {
    logger.info('Initializing FluxIndexer with ClickHouse...');
    logger.info('Configuration', {
      rpcUrl: config.rpc.url,
      clickhouseHost: config.clickhouse.host,
      clickhousePort: config.clickhouse.port,
      clickhouseDb: config.clickhouse.database,
      apiPort: config.api.port,
      batchSize: config.indexer.batchSize,
      pollingInterval: config.indexer.pollingInterval,
    });

    // Initialize RPC client
    this.rpc = new FluxRPCClient(config.rpc);
    logger.info('RPC client initialized');

    // Initialize ClickHouse connection with retry logic
    // ClickHouse may still be bootstrapping/restoring data on first deploy
    // Retry every 10 seconds for up to 30 minutes (180 attempts)
    this.ch = getClickHouse(config.clickhouse);
    await this.ch.connectWithRetry(180, 10000);
    logger.info('ClickHouse connected');

    // Initialize schema if needed
    await this.initializeSchema();

    // Initialize sync engine
    this.syncEngine = new ClickHouseSyncEngine(this.rpc, this.ch, {
      batchSize: config.indexer.batchSize,
      pollingInterval: config.indexer.pollingInterval,
      startHeight: config.indexer.startHeight,
      maxReorgDepth: config.indexer.maxReorgDepth,
    });
    logger.info('ClickHouse sync engine initialized');

    // Initialize FluxNode sync service
    this.fluxNodeSync = new ClickHouseFluxNodeSyncService(this.ch, this.rpc);
    logger.info('FluxNode sync service initialized');

    // Initialize API server
    this.apiServer = new ClickHouseAPIServer(this.ch, this.rpc, this.syncEngine, config.api.port);
    logger.info('ClickHouse API server initialized');

    logger.info('FluxIndexer initialization complete');
  }

  /**
   * Initialize ClickHouse schema from file
   */
  private async initializeSchema(): Promise<void> {
    const schemaPath = path.join(__dirname, 'database', 'schema.sql');

    if (!fs.existsSync(schemaPath)) {
      logger.warn('Schema file not found, assuming schema already exists', { path: schemaPath });
      return;
    }

    logger.info('Checking ClickHouse schema...');

    // Check if tables exist
    const blocksExist = await this.ch.tableExists('blocks');

    if (blocksExist) {
      logger.info('ClickHouse schema already initialized');
      return;
    }

    logger.info('Initializing ClickHouse schema...');
    const schemaSQL = fs.readFileSync(schemaPath, 'utf-8');
    await this.ch.executeSchema(schemaSQL);
    logger.info('ClickHouse schema initialized');
  }

  /**
   * Validate data consistency on startup and repair if needed
   */
  private async validateAndRepairOnStartup(): Promise<void> {
    logger.info('🔍 Running startup data consistency checks...');

    // Check address_summary consistency with UTXOs
    const isConsistent = await this.syncEngine.getBlockIndexer().validateAddressSummaryConsistency();

    if (!isConsistent) {
      logger.warn('⚠️ address_summary inconsistency detected, rebuilding from UTXOs...');
      await this.syncEngine.getBlockIndexer().rebuildAddressSummaryFromUtxos();

      // Verify after rebuild
      const isNowConsistent = await this.syncEngine.getBlockIndexer().validateAddressSummaryConsistency();
      if (!isNowConsistent) {
        logger.error('❌ address_summary still inconsistent after rebuild - manual intervention may be needed');
      }
    }

    logger.info('✅ Startup data consistency checks complete');
  }

  /**
   * Start FluxIndexer
   */
  async start(): Promise<void> {
    logger.info('Starting FluxIndexer with ClickHouse...');

    // Validate and repair data consistency before starting sync
    await this.validateAndRepairOnStartup();

    // Start API server
    await this.apiServer.start();
    logger.info(`API server started on port ${config.api.port}`);

    // Start sync engine
    await this.syncEngine.start();
    logger.info('Sync engine started');

    // Start FluxNode sync service
    this.fluxNodeSync.start();
    logger.info('FluxNode sync service started');

    logger.info('FluxIndexer is running with ClickHouse backend');
    logger.info(`API available at http://${config.api.host}:${config.api.port}/api/v1`);
  }

  /**
   * Stop FluxIndexer
   */
  async stop(): Promise<void> {
    if (this.isShuttingDown) {
      return;
    }

    this.isShuttingDown = true;
    logger.info('Stopping FluxIndexer...');

    // Stop FluxNode sync service
    if (this.fluxNodeSync) {
      this.fluxNodeSync.stop();
      logger.info('FluxNode sync service stopped');
    }

    // Stop sync engine
    if (this.syncEngine) {
      await this.syncEngine.stop();
      logger.info('Sync engine stopped');
    }

    // Stop API server
    if (this.apiServer) {
      await this.apiServer.stop();
      logger.info('API server stopped');
    }

    // Close ClickHouse connection
    await closeClickHouse();
    logger.info('ClickHouse connection closed');

    logger.info('FluxIndexer stopped');
  }

  /**
   * Check for block gaps in ClickHouse
   */
  async findMissingBlocks(): Promise<number[]> {
    // Get max height and count - no FINAL for MergeTree tables
    const maxResult = await this.ch.queryOne<{ max_height: string | number; block_count: string | number }>(`
      SELECT max(height) as max_height, count() as block_count FROM blocks WHERE is_valid = 1
    `);

    const maxHeight = Number(maxResult?.max_height ?? -1);
    const blockCount = Number(maxResult?.block_count ?? 0);

    // Empty database - no blocks indexed yet
    if (blockCount === 0 || isNaN(blockCount)) {
      return [];
    }

    if (maxHeight < 0 || isNaN(maxHeight)) {
      return []; // Empty database
    }

    // Find gaps using a range query - no FINAL for MergeTree tables
    const gaps = await this.ch.query<{ missing: number }>(`
      WITH numbers AS (
        SELECT number FROM numbers(${maxHeight + 1})
      )
      SELECT number as missing
      FROM numbers n
      LEFT ANTI JOIN (
        SELECT height FROM blocks WHERE is_valid = 1
      ) b ON n.number = b.height
      ORDER BY missing
    `);

    return gaps.map(g => g.missing);
  }

  /**
   * Backfill missing blocks
   */
  async backfillMissingBlocks(): Promise<number> {
    const missingHeights = await this.findMissingBlocks();

    if (missingHeights.length === 0) {
      logger.info('No missing blocks found');
      return 0;
    }

    logger.info(`Found ${missingHeights.length} missing blocks`);

    const blockIndexer = this.syncEngine.getBlockIndexer();
    for (const height of missingHeights) {
      try {
        logger.info(`Backfilling block ${height}...`);
        await blockIndexer.indexBlock(height);
        logger.info(`Successfully backfilled block ${height}`);
      } catch (error: any) {
        logger.error(`Failed to backfill block ${height}`, { error: error.message });
      }
    }

    return missingHeights.length;
  }

  /**
   * Get status
   */
  async getStatus(): Promise<any> {
    const chainInfo = await this.rpc.getBlockchainInfo();
    const syncStats = this.syncEngine.getSyncStats();

    return {
      indexer: {
        version: '1.0.0-clickhouse',
        backend: 'ClickHouse',
        uptime: syncStats.uptimeSeconds,
        isRunning: syncStats.isRunning,
        blocksIndexed: syncStats.blocksIndexed,
        lastSyncTime: syncStats.lastSyncTime,
      },
      chain: {
        name: chainInfo.chain,
        blocks: chainInfo.blocks,
        headers: chainInfo.headers,
        bestBlockHash: chainInfo.bestblockhash,
        difficulty: chainInfo.difficulty,
        consensus: 'PoN',
      },
    };
  }
}

// Main execution
async function main() {
  printStartupBanner();
  logger.info('🚀 Starting FluxIndexer with ClickHouse backend');

  const indexer = new ClickHouseFluxIndexer();

  // Handle shutdown signals
  const shutdown = async (signal: string) => {
    logger.info(`Received ${signal}, shutting down...`);
    await indexer.stop();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception', { error: error.message, stack: error.stack });
    shutdown('uncaughtException');
  });

  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled rejection', { reason, promise });
    shutdown('unhandledRejection');
  });

  try {
    await indexer.initialize();

    // Check for block gaps
    logger.info('Checking for block gaps...');
    const gaps = await indexer.findMissingBlocks();
    if (gaps.length > 0) {
      logger.error(`❌ BLOCK GAPS DETECTED: ${gaps.slice(0, 10).join(', ')}${gaps.length > 10 ? ` (and ${gaps.length - 10} more)` : ''}`);

      if (process.env.BACKFILL_GAPS === 'true') {
        logger.warn('BACKFILL_GAPS=true - Attempting to backfill gaps...');
        const backfilled = await indexer.backfillMissingBlocks();
        logger.warn(`Backfilled ${backfilled} blocks.`);
      } else {
        logger.error('Refusing to start with block gaps. Set BACKFILL_GAPS=true to override.');
        process.exit(1);
      }
    }

    await indexer.start();

    // Log status periodically
    setInterval(async () => {
      try {
        const status = await indexer.getStatus();
        logger.info('Status', status);
      } catch (error: any) {
        logger.debug('Status check failed', { error: error.message });
      }
    }, 60000);
  } catch (error: any) {
    logger.error('Failed to start FluxIndexer', { error: error.message, stack: error.stack });
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  main().catch((error) => {
    logger.error('Fatal error', { error });
    process.exit(1);
  });
}

export { ClickHouseFluxIndexer };
