/**
 * FluxIndexer - Main Entry Point
 *
 * Custom blockchain indexer for Flux v9.0.0+ PoN consensus
 */

import { config } from './config';
import { FluxRPCClient } from './rpc/flux-rpc-client';
import { DatabaseConnection } from './database/connection';
import { SyncEngine } from './indexer/sync-engine';
import { APIServer } from './api/server';
import { runMigration } from './database/migrate';
import { logger, printStartupBanner } from './utils/logger';
import { BootstrapImporter } from './bootstrap/importer';
import { FluxNodeSyncService } from './services/fluxnode-sync';

class FluxIndexer {
  private rpc!: FluxRPCClient;
  private db!: DatabaseConnection;
  private syncEngine!: SyncEngine;
  private apiServer!: APIServer;
  private fluxNodeSync!: FluxNodeSyncService;
  private isShuttingDown = false;

  /**
   * Initialize FluxIndexer
   */
  async initialize(): Promise<void> {
    logger.info('Initializing FluxIndexer...');
    logger.info('Configuration', {
      rpcUrl: config.rpc.url,
      dbHost: config.database.host,
      dbName: config.database.database,
      apiPort: config.api.port,
      batchSize: config.indexer.batchSize,
      pollingInterval: config.indexer.pollingInterval,
    });

    // Initialize RPC client (but don't block on connection)
    this.rpc = new FluxRPCClient(config.rpc);
    logger.info('RPC client initialized');

    // Initialize database connection
    this.db = new DatabaseConnection(config.database);
    await this.db.connect();
    logger.info('Database connected');

    // Run migrations
    if (process.env.SKIP_MIGRATIONS !== 'true') {
      logger.info('Running database migrations...');
      await runMigration(this.db);
      logger.info('Migrations complete');
    }

    // Check and import bootstrap if needed
    const bootstrapImporter = new BootstrapImporter(this.db.getPool());
    const bootstrapImported = await bootstrapImporter.checkAndImport();
    if (bootstrapImported) {
      logger.info('Bootstrap import completed, database is ready');
    }

    // Initialize sync engine (will start when RPC is ready)
    this.syncEngine = new SyncEngine(this.rpc, this.db, {
      batchSize: config.indexer.batchSize,
      pollingInterval: config.indexer.pollingInterval,
      startHeight: config.indexer.startHeight,
      maxReorgDepth: config.indexer.maxReorgDepth,
    });
    logger.info('Sync engine initialized');

    // Initialize FluxNode sync service
    this.fluxNodeSync = new FluxNodeSyncService(this.db.getPool(), this.rpc);
    logger.info('FluxNode sync service initialized');

    // Initialize API server
    this.apiServer = new APIServer(this.db, this.rpc, this.syncEngine, config.api.port);
    logger.info('API server initialized');

    logger.info('FluxIndexer initialization complete');
  }

  /**
   * Start FluxIndexer
   */
  async start(): Promise<void> {
    logger.info('Starting FluxIndexer...');

    // Start API server
    await this.apiServer.start();
    logger.info(`API server started on port ${config.api.port}`);

    // Start sync engine
    await this.syncEngine.start();
    logger.info('Sync engine started');

    // Start FluxNode sync service
    this.fluxNodeSync.start();
    logger.info('FluxNode sync service started');

    logger.info('FluxIndexer is running');
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

    // Close database connection
    if (this.db) {
      await this.db.close();
      logger.info('Database connection closed');
    }

    logger.info('FluxIndexer stopped');
  }

  /**
   * Find missing blocks (gaps in block height sequence)
   */
  async findMissingBlocks(): Promise<number[]> {
    const pool = this.db.getPool();

    // Check if we have any blocks first
    const countResult = await pool.query('SELECT COUNT(*) as count FROM blocks');
    if (parseInt(countResult.rows[0].count) === 0) {
      return []; // Empty database, no gaps
    }

    // Find all missing blocks
    const result = await pool.query(`
      WITH block_range AS (
        SELECT generate_series(0, (SELECT MAX(height) FROM blocks)) as expected_height
      ),
      actual_blocks AS (
        SELECT height FROM blocks
      )
      SELECT array_agg(expected_height ORDER BY expected_height) as missing_heights
      FROM block_range br
      LEFT JOIN actual_blocks ab ON br.expected_height = ab.height
      WHERE ab.height IS NULL;
    `);

    return result.rows[0]?.missing_heights || [];
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

    logger.info(`Found ${missingHeights.length} missing blocks: ${missingHeights.join(', ')}`);

    // Index each missing block
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
    const poolStats = this.db.getPoolStats();

    return {
      indexer: {
        version: '1.0.0',
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
      database: {
        pool: poolStats,
      },
    };
  }
}

// Main execution
async function main() {
  // Show the awesome startup banner
  printStartupBanner();

  const indexer = new FluxIndexer();

  // Handle shutdown signals
  const shutdown = async (signal: string) => {
    logger.info(`Received ${signal}, shutting down...`);
    await indexer.stop();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  // Handle uncaught errors
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

    // ALWAYS check for block gaps on startup
    // Gaps indicate data corruption - cumulative stats like supply_stats will be wrong
    logger.info('Checking for block gaps...');
    const gaps = await indexer.findMissingBlocks();
    if (gaps.length > 0) {
      logger.error(`❌ BLOCK GAPS DETECTED: ${gaps.slice(0, 10).join(', ')}${gaps.length > 10 ? ` (and ${gaps.length - 10} more)` : ''}`);
      logger.error('Cumulative stats (supply_stats) are likely corrupted.');
      logger.error('Options:');
      logger.error('  1. Run with BACKFILL_GAPS=true to attempt repair (WARNING: may not fix cumulative stats)');
      logger.error('  2. Truncate database and resync from block 0 (RECOMMENDED for data integrity)');

      if (process.env.BACKFILL_GAPS === 'true') {
        logger.warn('BACKFILL_GAPS=true - Attempting to backfill gaps...');
        const backfilled = await indexer.backfillMissingBlocks();
        logger.warn(`Backfilled ${backfilled} blocks. ⚠️  WARNING: supply_stats is likely still incorrect!`);
      } else {
        logger.error('Refusing to start with block gaps. Set BACKFILL_GAPS=true to override (not recommended).');
        process.exit(1);
      }
    }

    await indexer.start();

    // Log status periodically (less frequently during heavy sync)
    setInterval(async () => {
      try {
        const status = await indexer.getStatus();
        logger.info('Status', status);
      } catch (error: any) {
        // During heavy sync, RPC might be too busy - log as debug, not error
        // This prevents noisy logs when parallel fetcher is overwhelming the daemon
        logger.debug('Status check failed (daemon busy during sync)', { error: error.message });
      }
    }, 60000); // Every minute
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

export { FluxIndexer };
