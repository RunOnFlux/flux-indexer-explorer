/**
 * Sync Engine
 *
 * Manages blockchain synchronization and continuous indexing
 * with automatic performance optimization
 */

import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { DatabaseConnection } from '../database/connection';
import { BlockIndexer } from './block-indexer';
import { DatabaseOptimizer } from '../database/optimizer';
import { ParallelBlockFetcher } from './parallel-fetcher';
import { logger } from '../utils/logger';
import { SyncError, Block } from '../types';

// Memory profiling helper - logs heap usage at key points
let lastMemLog = 0;
let baselineHeap = 0;
function logMemory(label: string, force = false): void {
  const now = Date.now();
  // Only log every 10 seconds unless forced
  if (!force && now - lastMemLog < 10000) return;
  lastMemLog = now;

  const mem = process.memoryUsage();
  const heapMB = Math.round(mem.heapUsed / 1024 / 1024);
  const rssMB = Math.round(mem.rss / 1024 / 1024);
  const externalMB = Math.round(mem.external / 1024 / 1024);

  if (baselineHeap === 0) baselineHeap = heapMB;
  const delta = heapMB - baselineHeap;

  logger.info('Memory snapshot', {
    label,
    heapMB,
    rssMB,
    externalMB,
    deltaHeapMB: delta,
  });
}

export interface SyncConfig {
  batchSize: number;
  pollingInterval: number;
  startHeight?: number;
  maxReorgDepth: number;
}

export class SyncEngine {
  private blockIndexer: BlockIndexer;
  private optimizer: DatabaseOptimizer;
  private isRunning = false;
  private syncInterval: NodeJS.Timeout | null = null;
  private lastSyncTime = Date.now();
  private blocksIndexed = 0;
  private syncInProgress = false;
  private consecutiveErrors = 0;
  private daemonReady = false;

  constructor(
    private rpc: FluxRPCClient,
    private db: DatabaseConnection,
    private config: SyncConfig
  ) {
    this.blockIndexer = new BlockIndexer(rpc, db);
    this.optimizer = new DatabaseOptimizer(db);
  }

  /**
   * Start synchronization
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn('Sync engine already running');
      return;
    }

    logger.info('Starting sync engine...');
    this.isRunning = true;

    // Ensure performance indexes exist on startup
    try {
      await this.optimizer.ensureIndexes();
      logger.info('✅ Performance indexes verified');
    } catch (error: any) {
      logger.warn('Failed to ensure performance indexes (will continue anyway)', { error: error.message });
    }

    // Set up polling interval (will retry if RPC not ready)
    this.syncInterval = setInterval(async () => {
      try {
        await this.sync();
        this.consecutiveErrors = 0; // Reset on success
      } catch (error: any) {
        this.consecutiveErrors++;
        // During daemon warmup, log minimally at debug level
        if (this.daemonReady) {
          // Daemon was ready but now failing - log as warning
          logger.warn('Sync error (will retry)', { error: error.message });
        } else if (this.consecutiveErrors === 1) {
          // First warmup message only
          logger.info('Waiting for Flux daemon to respond to RPC calls...');
        } else if (this.consecutiveErrors % 30 === 0) {
          // Every 30 attempts (~2.5 minutes) show we're still waiting
          logger.debug('Still waiting for daemon warmup', { attempts: this.consecutiveErrors });
        }
      }
    }, this.config.pollingInterval);

    logger.info('Sync engine started', {
      pollingInterval: this.config.pollingInterval,
      batchSize: this.config.batchSize,
    });
    logger.info('Waiting for Flux daemon to be ready...');

    // Try initial sync in background (don't block startup)
    // Use debug level since failures are expected during daemon warmup
    this.sync().catch(error => {
      logger.debug('Initial sync attempt failed (daemon warmup)', { error: error.message });
    });
  }

  /**
   * Stop synchronization
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    logger.info('Stopping sync engine...');

    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }

    this.isRunning = false;
    await this.blockIndexer.setSyncingStatus(false);

    logger.info('Sync engine stopped');
  }

  /**
   * Perform synchronization
   */
  private async sync(): Promise<void> {
    if (this.syncInProgress) {
      logger.debug('Sync already in progress, skipping trigger');
      return;
    }

    this.syncInProgress = true;

    try {
      // Get chain info from RPC - use headers as chain height since it represents
      // the actual network height that the daemon is aware of
      const chainInfo = await this.rpc.getBlockchainInfo();
      const chainHeight = chainInfo.headers;

      // Mark daemon as ready on first successful RPC call
      if (!this.daemonReady) {
        this.daemonReady = true;
        logger.info('✅ Flux daemon is ready and responding to RPC calls');
      }

      // Get current indexed height from database
      const syncState = await this.blockIndexer.getSyncState();
      let currentHeight = syncState.currentHeight;

      // Use start height from config if specified and higher
      if (this.config.startHeight !== undefined && currentHeight < this.config.startHeight) {
        currentHeight = this.config.startHeight - 1;
      }

      // During daemon initial sync, stay 1000 blocks behind to avoid constant reorgs.
      // Once daemon is caught up (blocks == headers), index all the way to the tip.
      const safetyBuffer = 1000;
      const blocksBehind = chainHeight - currentHeight;
      const daemonIsSyncing = chainInfo.blocks < chainInfo.headers;

      const indexingTarget = daemonIsSyncing && blocksBehind > safetyBuffer
        ? Math.max(0, chainHeight - safetyBuffer)  // Keep buffer during daemon sync
        : chainHeight;  // Chase tip when daemon is synced

      // Update chain height in database (use actual chain height, not buffered)
      await this.blockIndexer.setSyncingStatus(true, chainHeight);

      // Check if we're in sync (compare against buffered height)
      if (currentHeight >= indexingTarget) {
        logger.debug('In sync with buffer', { currentHeight, indexingTarget, actualChainHeight: chainHeight });
        await this.blockIndexer.setSyncingStatus(false, chainHeight);
        return;
      }

      // Calculate blocks to sync (to buffered target, not latest chain tip)
      const blocksToSync = indexingTarget - currentHeight;
      const batchSize = Math.min(this.config.batchSize, blocksToSync);

      // Threshold: use simple sequential fetch for small catches (tip-following)
      // Use parallel fetcher only for bulk sync (> 10 blocks)
      const PARALLEL_FETCH_THRESHOLD = 10;

      const startTime = Date.now();
      let lastHeight = currentHeight;
      let processedThisBatch = 0;

      if (blocksToSync <= PARALLEL_FETCH_THRESHOLD) {
        // ===== SIMPLE TIP-FOLLOWING MODE =====
        // For small catches (1-10 blocks), use simple sequential fetch
        // This avoids the overhead of starting/stopping the parallel fetcher
        logger.debug('Tip-following mode', {
          blocksToSync,
          startBlock: currentHeight + 1,
          endBlock: currentHeight + batchSize,
        });

        for (let height = currentHeight + 1; height <= currentHeight + batchSize; height++) {
          try {
            await this.blockIndexer.indexBlock(height);
            lastHeight = height;
            processedThisBatch++;
            this.blocksIndexed++;
          } catch (error: any) {
            logger.error('Failed to index block in tip-following mode', { height, error: error.message });
            throw error;
          }
        }
      } else {
        // ===== BULK SYNC MODE =====
        // For large syncs (> 10 blocks), use parallel fetcher for speed
        logger.info('Batch starting', {
          startBlock: currentHeight + 1,
          endBlock: currentHeight + batchSize,
          blocksToSync,
          batchSize,
          currentHeight,
          chainHeight,
        });

        const batchStartHeight = currentHeight + 1;
        const batchEndHeight = currentHeight + batchSize;
        // Aggressive settings for high-performance server (32GB RAM, memory leak fixed)
        const fetchBatchSize = 200;   // 200 blocks per RPC batch (2x faster fetching)
        const prefetchBatches = 10;   // 10 batches in flight = 2000 blocks max in memory
        const parallelWorkers = 8;    // 8 concurrent fetch workers

        logger.debug('Using pipelined block fetcher', {
          batchStartHeight,
          batchEndHeight,
          fetchBatchSize,
          prefetchBatches,
          parallelWorkers,
          targetBatchSize: batchSize,
        });

        const fetcher = new ParallelBlockFetcher(this.rpc, {
          batchSize: fetchBatchSize,
          prefetchBatches,
          parallelWorkers,
        });

        await fetcher.start(batchStartHeight, batchEndHeight);
        logMemory('batch-start', true);

        try {
          while (true) {
            const fetchedBlocks = await fetcher.getNextBatch();
            if (!fetchedBlocks || fetchedBlocks.length === 0) {
              break;
            }
            logMemory(`fetched-${fetchedBlocks.length}-blocks`);

            // Separate valid blocks from missing ones
            const validBlocks: Block[] = [];
            const missingHeights: number[] = [];

            for (let i = 0; i < fetchedBlocks.length; i++) {
              const height = lastHeight + 1 + i;
              if (fetchedBlocks[i]) {
                validBlocks.push(fetchedBlocks[i]!);
              } else {
                missingHeights.push(height);
              }
            }

            // Process valid blocks in batch (MUCH faster - single DB transaction, batchUtxoMap)
            if (validBlocks.length > 0) {
              const batchStartHeight = lastHeight + 1;
              const blocksProcessed = await this.blockIndexer.indexBlocksBatch(validBlocks, batchStartHeight);
              lastHeight += blocksProcessed;
              processedThisBatch += blocksProcessed;
              this.blocksIndexed += blocksProcessed;
            }

            // Handle any missing blocks individually (should be rare)
            for (const height of missingHeights) {
              logger.warn('Missing block from pipelined fetch, refetching individually', { height });
              await this.blockIndexer.indexBlock(height);
              lastHeight = height;
              processedThisBatch++;
              this.blocksIndexed++;
            }

            // Clear batch array to allow V8 to reclaim memory between fetches
            fetchedBlocks.length = 0;
            validBlocks.length = 0;

            // Trigger FULL GC after each batch to properly reclaim memory
            if (typeof global.gc === 'function') {
              global.gc(true);
            }
            logMemory('post-batch-gc', true);

            // Log progress after each batch
            const elapsed = (Date.now() - startTime) / 1000;
            const processed = lastHeight - currentHeight;
            const blocksPerSecond = processed > 0 && elapsed > 0 ? processed / elapsed : 0;
            const remaining = chainHeight - lastHeight;
            const eta = blocksPerSecond > 0 ? remaining / blocksPerSecond : Infinity;

            const etaStr = Number.isFinite(eta) ? `${Math.floor(eta / 60)}m ${Math.floor(eta % 60)}s` : 'unknown';
            logger.info(`Bulk sync progress ${lastHeight}/${chainHeight}`, {
              height: lastHeight,
              chainHeight,
              blocksPerSecond: blocksPerSecond.toFixed(1),
              eta: etaStr,
              remaining,
            });

            // Verify supply accuracy every 10000 blocks
            if (lastHeight % 10000 < fetchedBlocks.length) {
              const checkHeight = Math.floor(lastHeight / 10000) * 10000;
              if (checkHeight > 0) {
                await this.verifySupplyAccuracy(checkHeight);
              }
            }
          }
        } finally {
          fetcher.stop();
        }
      }

      // Check for reorgs
      if (lastHeight > 0) {
        await this.checkForReorg(lastHeight);
      }

      const syncTime = Date.now() - startTime;
      const batchBlocksPerSec = processedThisBatch > 0 && syncTime > 0
        ? processedThisBatch / (syncTime / 1000)
        : 0;
      logger.info('Batch complete', {
        blocks: processedThisBatch,
        time: `${(syncTime / 1000).toFixed(1)}s`,
        blocksPerSecond: batchBlocksPerSec.toFixed(1),
      });

      // Force FULL GC after every batch to prevent long-term heap drift
      logMemory('batch-end-pre-gc', true);
      if (typeof global.gc === 'function') {
        global.gc(true); // true = full GC, not just incremental
      }
      logMemory('batch-end-post-gc', true);

      await this.blockIndexer.setSyncingStatus(false, chainHeight);

      // Update metrics
      this.lastSyncTime = Date.now();

      // If still far behind, schedule next sync batch
      const stillBehind = chainHeight - lastHeight > safetyBuffer;
      if (stillBehind) {
        // Use setImmediate to allow event loop to breathe between batches
        setImmediate(() => {
          this.sync().catch(err => {
            logger.warn('Continuous sync error', { error: err.message });
          });
        });
      }

    } catch (error: any) {
      logger.error('Sync error', { error: error.message, stack: error.stack });
      await this.blockIndexer.setSyncingStatus(false);
      throw error;
    } finally {
      this.syncInProgress = false;
    }
  }

  /**
   * Check for blockchain reorganization
   */
  private async checkForReorg(currentHeight: number): Promise<void> {
    try {
      // Get last indexed block hash from database
      const syncState = await this.blockIndexer.getSyncState();
      const dbHash = syncState.lastBlockHash;

      if (!dbHash) return;

      // Get current block hash from RPC
      const rpcHash = await this.rpc.getBlockHash(currentHeight);

      // If hashes match, no reorg
      if (dbHash === rpcHash) {
        return;
      }

      logger.warn('Reorg detected!', {
        height: currentHeight,
        dbHash,
        rpcHash,
      });

      // Find common ancestor
      let commonAncestor = currentHeight - 1;
      let foundCommonAncestor = false;
      for (let i = 1; i <= this.config.maxReorgDepth; i++) {
        const height = currentHeight - i;
        if (height < 0) break;

        const result = await this.db.query(
          'SELECT hash FROM blocks WHERE height = $1',
          [height]
        );

        if (result.rows.length === 0) {
          break;
        }

        const dbBlockHash = result.rows[0].hash;
        const rpcBlockHash = await this.rpc.getBlockHash(height);

        if (dbBlockHash === rpcBlockHash) {
          commonAncestor = height;
          foundCommonAncestor = true;
          break;
        }
      }

      if (!foundCommonAncestor) {
        throw new SyncError('Failed to find common ancestor within max reorg depth', {
          currentHeight,
          maxDepth: this.config.maxReorgDepth,
        });
      }

      logger.info('Reorg common ancestor found', {
        commonAncestor,
        blocksToRollback: currentHeight - commonAncestor,
      });

      // Rollback to common ancestor
      await this.handleReorg(commonAncestor, currentHeight, dbHash, rpcHash);

    } catch (error: any) {
      logger.error('Reorg check failed', { error: error.message });
      throw error;
    }
  }

  /**
   * Handle blockchain reorganization
   */
  private async handleReorg(
    commonAncestor: number,
    currentHeight: number,
    oldHash: string,
    newHash: string
  ): Promise<void> {
    logger.info('Handling reorg', {
      from: currentHeight,
      to: commonAncestor,
      blocksAffected: currentHeight - commonAncestor,
    });

    await this.db.transaction(async (client) => {
      // Get affected addresses from both unspent and spent tables
      // PHASE 3: JOIN addresses table since address_id replaces address column
      const affectedAddressRows = await client.query(
        `SELECT DISTINCT a.address FROM (
           SELECT address_id FROM utxos_unspent WHERE block_height > $1
           UNION
           SELECT address_id FROM utxos_spent WHERE block_height > $1 OR spent_block_height > $1
         ) combined
         JOIN addresses a ON combined.address_id = a.id`,
        [commonAncestor]
      );

      const affectedAddresses = new Set<string>();
      for (const row of affectedAddressRows.rows) {
        if (row.address) {
          affectedAddresses.add(row.address);
        }
      }

      // Log reorg event
      await client.query(
        `INSERT INTO reorgs (from_height, to_height, common_ancestor, old_hash, new_hash, blocks_affected)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [currentHeight, commonAncestor, commonAncestor, oldHash, newHash, currentHeight - commonAncestor]
      );

      // Move UTXOs that were spent in rolled-back blocks back to unspent table
      // Step 1: Insert back into utxos_unspent from utxos_spent
      // PHASE 3: Use address_id instead of address column
      await client.query(
        `INSERT INTO utxos_unspent (txid, vout, address_id, value, script_pubkey, script_type, block_height, created_at)
         SELECT txid, vout, address_id, value, script_pubkey, script_type, block_height, created_at
         FROM utxos_spent
         WHERE spent_block_height > $1
         ON CONFLICT (txid, vout) DO NOTHING`,
        [commonAncestor]
      );

      // Step 2: Delete from utxos_spent
      await client.query(
        'DELETE FROM utxos_spent WHERE spent_block_height > $1',
        [commonAncestor]
      );

      // Delete UTXOs created in rolled-back blocks from utxos_unspent
      await client.query(
        'DELETE FROM utxos_unspent WHERE block_height > $1',
        [commonAncestor]
      );

      // Delete transactions from rolled-back blocks
      await client.query(
        'DELETE FROM transactions WHERE block_height > $1',
        [commonAncestor]
      );

      // Delete rolled-back blocks
      await client.query(
        'DELETE FROM blocks WHERE height > $1',
        [commonAncestor]
      );

      // Delete supply stats from rolled-back blocks
      await client.query(
        'DELETE FROM supply_stats WHERE block_height > $1',
        [commonAncestor]
      );

      // Update sync state
      const lastValidBlock = await client.query(
        'SELECT hash FROM blocks WHERE height = $1',
        [commonAncestor]
      );

      if (lastValidBlock.rows.length > 0) {
        await client.query(
          `UPDATE sync_state
           SET current_height = $1,
               last_block_hash = $2,
               last_sync_time = NOW()
           WHERE id = 1`,
          [commonAncestor, lastValidBlock.rows[0].hash]
        );
      } else {
        await client.query(
          `UPDATE sync_state
           SET current_height = $1,
               last_block_hash = NULL,
               last_sync_time = NOW()
           WHERE id = 1`,
          [commonAncestor]
        );
      }

      // Recalculate address summaries for affected addresses
      for (const address of affectedAddresses) {
        await client.query('SELECT update_address_summary($1)', [address]);
      }
    });

    logger.info('Reorg handled successfully', {
      rolledBackTo: commonAncestor,
    });
  }

  /**
   * Get sync statistics
   */
  getSyncStats() {
    return {
      isRunning: this.isRunning,
      blocksIndexed: this.blocksIndexed,
      lastSyncTime: new Date(this.lastSyncTime),
      uptimeSeconds: (Date.now() - this.lastSyncTime) / 1000,
    };
  }

  /**
   * Get block indexer instance for backfill operations
   */
  getBlockIndexer(): BlockIndexer {
    return this.blockIndexer;
  }

  /**
   * Force sync now (bypasses interval)
   */
  async syncNow(): Promise<void> {
    if (!this.isRunning) {
      throw new Error('Sync engine not running');
    }
    await this.sync();
  }

  /**
   * Verify supply accuracy against daemon at specific height
   * Compares both transparent and shielded pool values
   */
  private async verifySupplyAccuracy(height: number): Promise<void> {
    try {
      // Get daemon's blockchain info at current height
      const chainInfo = await this.rpc.getBlockchainInfo();

      // Only verify if daemon is at or past this height
      if (chainInfo.blocks < height) {
        logger.debug('Skipping supply verification - daemon not at height yet', {
          daemonHeight: chainInfo.blocks,
          verifyHeight: height
        });
        return;
      }

      // Get indexer's calculated transparent supply from utxos_unspent table
      const result = await this.db.getPool().query(`
        SELECT
          SUM(value)::numeric / 100000000 as transparent_supply,
          COUNT(*) as utxo_count
        FROM utxos_unspent
        WHERE block_height <= $1
      `, [height]);

      const indexerTransparent = parseFloat(result.rows[0]?.transparent_supply || '0');
      const utxoCount = parseInt(result.rows[0]?.utxo_count || '0');

      // Get daemon's value pools
      const valuePools = chainInfo.valuePools || [];
      const daemonTransparent = valuePools.find((p: any) => p.id === 'transparent')?.chainValue || 0;
      const daemonSapling = valuePools.find((p: any) => p.id === 'sapling')?.chainValue || 0;
      const daemonSprout = valuePools.find((p: any) => p.id === 'sprout')?.chainValue || 0;
      const daemonShielded = daemonSapling + daemonSprout;

      // Calculate discrepancy
      const transparentDiff = indexerTransparent - daemonTransparent;
      const transparentDiffPercent = daemonTransparent > 0
        ? (Math.abs(transparentDiff) / daemonTransparent * 100).toFixed(4)
        : '0';

      // Get shielded supply from supply_stats table (if exists)
      const shieldedResult = await this.db.getPool().query(`
        SELECT shielded_pool as shielded_supply
        FROM supply_stats
        WHERE block_height = (
          SELECT MAX(block_height) FROM supply_stats WHERE block_height <= $1
        )
      `, [height]).catch(() => ({ rows: [{ shielded_supply: null }] }));

      // Convert from satoshis to FLUX for comparison with daemon
      const indexerShieldedSats = parseFloat(shieldedResult.rows[0]?.shielded_supply || '0');
      const indexerShielded = indexerShieldedSats / 1e8;
      const shieldedDiff = indexerShielded - daemonShielded;

      // Log comparison
      const logLevel = Math.abs(transparentDiff) > 1.0 ? 'warn' : 'info';

      logger[logLevel](`Supply verification at height ${height}`, {
        transparent: {
          indexer: indexerTransparent.toFixed(8),
          daemon: daemonTransparent.toFixed(8),
          difference: transparentDiff.toFixed(8),
          diffPercent: `${transparentDiffPercent}%`,
          utxoCount: utxoCount
        },
        shielded: {
          indexer: indexerShielded.toFixed(8),
          daemon: daemonShielded.toFixed(8),
          difference: shieldedDiff.toFixed(8),
          sapling: daemonSapling.toFixed(8),
          sprout: daemonSprout.toFixed(8)
        },
        total: {
          indexer: (indexerTransparent + indexerShielded).toFixed(8),
          daemon: (daemonTransparent + daemonShielded).toFixed(8)
        }
      });

      // Alert if significant discrepancy (>1 FLUX)
      if (Math.abs(transparentDiff) > 1.0) {
        logger.error('⚠️  SUPPLY DISCREPANCY DETECTED', {
          height,
          transparentDiff: transparentDiff.toFixed(8),
          percentOff: `${transparentDiffPercent}%`,
          message: 'Indexer transparent supply does not match daemon!'
        });
      }

    } catch (error: any) {
      logger.warn('Failed to verify supply accuracy', {
        height,
        error: error.message
      });
    }
  }
}
