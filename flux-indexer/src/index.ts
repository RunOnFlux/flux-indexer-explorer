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
import { logger } from './utils/logger';
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
    await indexer.start();

    // Log status periodically
    setInterval(async () => {
      try {
        const status = await indexer.getStatus();
        logger.info('Status', status);
      } catch (error) {
        logger.error('Failed to get status', { error });
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
