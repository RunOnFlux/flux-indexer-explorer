/**
 * ClickHouse Sync Engine
 *
 * Manages blockchain synchronization and continuous indexing
 * with ClickHouse as the storage backend
 */

import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { ClickHouseConnection, getClickHouse } from '../database/connection';
import { ClickHouseBlockIndexer } from './block-indexer';
import { ParallelBlockFetcher } from './parallel-fetcher';
import { logger } from '../utils/logger';
import { Block } from '../types';
import {
  updateSyncState,
  recordReorg,
  invalidateFromHeight,
  getSyncState,
} from '../database/bulk-loader';

// Memory profiling helper - throttled to avoid log spam
let lastMemLog = 0;
let baselineHeap = 0;
const MEMORY_LOG_INTERVAL = 30000; // Only log memory every 30 seconds

// Progress bar throttling - only show every 10 seconds
let lastProgressLog = 0;
const PROGRESS_LOG_INTERVAL = 10000;

// Waiting message throttling - only show every 30 seconds
let lastWaitingLog = 0;
const WAITING_LOG_INTERVAL = 30000;

function logMemory(label: string): void {
  const now = Date.now();
  if (now - lastMemLog < MEMORY_LOG_INTERVAL) return;
  lastMemLog = now;

  const mem = process.memoryUsage();
  const heapMB = Math.round(mem.heapUsed / 1024 / 1024);
  const rssMB = Math.round(mem.rss / 1024 / 1024);

  if (baselineHeap === 0) baselineHeap = heapMB;
  const delta = heapMB - baselineHeap;

  logger.debug('Memory snapshot', {
    label,
    heapMB,
    rssMB,
    deltaHeapMB: delta,
  });
}

export interface SyncConfig {
  batchSize: number;
  pollingInterval: number;
  startHeight?: number;
  maxReorgDepth: number;
}

export class ClickHouseSyncEngine {
  private blockIndexer: ClickHouseBlockIndexer;
  private ch: ClickHouseConnection;
  private isRunning = false;
  private syncInterval: NodeJS.Timeout | null = null;
  private lastSyncTime = Date.now();
  private blocksIndexed = 0;
  private syncInProgress = false;
  private consecutiveErrors = 0;
  private daemonReady = false;
  // Track height in memory to avoid stale reads from async inserts
  private lastKnownHeight: number = -1;

  constructor(
    private rpc: FluxRPCClient,
    ch?: ClickHouseConnection,
    private config?: SyncConfig
  ) {
    this.ch = ch || getClickHouse();
    this.blockIndexer = new ClickHouseBlockIndexer(rpc, this.ch);
    this.config = config || {
      batchSize: 200,  // Increased from 100 to reduce insert frequency and part creation
      pollingInterval: 5000,
      maxReorgDepth: 100,
    };
  }

  /**
   * Start synchronization
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn('Sync engine already running');
      return;
    }

    logger.info('Starting ClickHouse sync engine...');
    this.isRunning = true;

    // Set up polling interval
    this.syncInterval = setInterval(async () => {
      try {
        await this.sync();
        this.consecutiveErrors = 0;
      } catch (error: any) {
        this.consecutiveErrors++;
        if (this.daemonReady) {
          logger.warn('Sync error (will retry)', { error: error.message });
        } else if (this.consecutiveErrors === 1) {
          logger.info('Waiting for Flux daemon to respond to RPC calls...');
        } else if (this.consecutiveErrors % 30 === 0) {
          logger.debug('Still waiting for daemon warmup', { attempts: this.consecutiveErrors });
        }
      }
    }, this.config!.pollingInterval);

    logger.info('ClickHouse sync engine started', {
      pollingInterval: this.config!.pollingInterval,
      batchSize: this.config!.batchSize,
    });

    // Try initial sync in background
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

    logger.info('Stopping ClickHouse sync engine...');

    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }

    this.isRunning = false;
    await this.blockIndexer.setSyncingStatus(false);

    logger.info('ClickHouse sync engine stopped');
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
      // Get chain info from RPC
      const chainInfo = await this.rpc.getBlockchainInfo();
      const chainHeight = chainInfo.headers;  // Total chain height (from headers)
      const daemonBlocks = chainInfo.blocks;  // Blocks daemon has actually downloaded

      // Daemon is "syncing" if it hasn't downloaded all blocks OR hasn't discovered chain yet
      // A fresh daemon starts with headers=0 until it connects to peers
      const MIN_CHAIN_HEIGHT = 1000; // Flux mainnet has 2M+ blocks, so 1000 means daemon is still discovering
      const daemonDiscoveringChain = chainHeight < MIN_CHAIN_HEIGHT;
      const daemonIsSyncing = daemonDiscoveringChain || daemonBlocks < chainHeight;
      const daemonSyncPercent = chainHeight > 0 ? ((daemonBlocks / chainHeight) * 100).toFixed(1) : '0';

      // Mark daemon as ready on first successful RPC call
      if (!this.daemonReady) {
        this.daemonReady = true;
        if (daemonDiscoveringChain) {
          logger.info('✅ Flux daemon responding - connecting to network...', {
            headers: chainHeight,
            blocks: daemonBlocks,
          });
        } else if (daemonIsSyncing) {
          logger.info('✅ Flux daemon responding - blockchain sync in progress', {
            daemonBlocks,
            chainHeight,
            syncPercent: `${daemonSyncPercent}%`,
          });
        } else {
          logger.info('✅ Flux daemon is ready and fully synced');
        }
      }

      // Get current indexed height - prefer in-memory tracking to avoid stale reads
      let currentHeight: number;
      if (this.lastKnownHeight >= 0) {
        // Use memory-tracked height
        currentHeight = this.lastKnownHeight;
      } else {
        // First sync or after restart - query database
        const syncState = await this.blockIndexer.getSyncState();
        currentHeight = syncState.currentHeight;
        this.lastKnownHeight = currentHeight;
      }

      // Use start height from config if specified and higher
      if (this.config!.startHeight !== undefined && currentHeight < this.config!.startHeight) {
        currentHeight = this.config!.startHeight - 1;
        this.lastKnownHeight = currentHeight;
      }

      // CRITICAL: Only index up to blocks the daemon has actually downloaded
      // This prevents errors from trying to fetch non-existent blocks
      // When daemon is fully synced, we can safely index to chain tip
      const isDaemonFullySynced = daemonBlocks >= chainHeight;
      const safetyBuffer = isDaemonFullySynced ? 0 : 10; // No buffer when fully synced
      const safeTarget = Math.max(0, daemonBlocks - safetyBuffer);
      const indexingTarget = Math.min(safeTarget, chainHeight);

      // Log daemon sync status periodically when waiting (throttled)
      if (daemonIsSyncing && currentHeight >= indexingTarget) {
        const now = Date.now();
        if (now - lastWaitingLog >= WAITING_LOG_INTERVAL) {
          lastWaitingLog = now;
          if (daemonDiscoveringChain) {
            logger.info('⏳ Waiting for daemon to connect to network and discover chain', {
              indexerHeight: currentHeight,
              headers: chainHeight,
              blocks: daemonBlocks,
            });
          } else {
            logger.info('⏳ Waiting for daemon to download more blocks', {
              indexerHeight: currentHeight,
              daemonBlocks,
              chainHeight,
              daemonSync: `${daemonSyncPercent}%`,
            });
          }
        }
        await this.blockIndexer.setSyncingStatus(false, chainHeight);
        return;
      }

      // Update chain height
      await this.blockIndexer.setSyncingStatus(true, chainHeight);

      // Check if we're in sync
      if (currentHeight >= indexingTarget) {
        logger.debug('In sync with buffer', { currentHeight, indexingTarget, actualChainHeight: chainHeight });
        await this.blockIndexer.setSyncingStatus(false, chainHeight);
        return;
      }

      // Calculate blocks to sync
      const blocksToSync = indexingTarget - currentHeight;
      const batchSize = Math.min(this.config!.batchSize, blocksToSync);

      const PARALLEL_FETCH_THRESHOLD = 10;
      const startTime = Date.now();
      let lastHeight = currentHeight;
      let processedThisBatch = 0;

      if (blocksToSync <= PARALLEL_FETCH_THRESHOLD) {
        // Simple tip-following mode
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
        // Bulk sync mode with parallel fetcher
        logger.info('Batch starting', {
          startBlock: currentHeight + 1,
          endBlock: currentHeight + batchSize,
          blocksToSync,
          batchSize,
        });

        const batchStartHeight = currentHeight + 1;
        const batchEndHeight = currentHeight + batchSize;
        // Memory-optimized settings - reduced to prevent OOM
        // At block 1M+, transactions are larger and more numerous
        const fetchBatchSize = 100;  // 100 blocks per RPC batch (was 200)
        const prefetchBatches = 3;   // 3 batches in flight (was 6)
        const parallelWorkers = 3;   // 3 concurrent workers (was 4)

        const fetcher = new ParallelBlockFetcher(this.rpc, {
          batchSize: fetchBatchSize,
          prefetchBatches,
          parallelWorkers,
        });

        await fetcher.start(batchStartHeight, batchEndHeight);
        logMemory('batch-start');

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

            // Process valid blocks in batch
            if (validBlocks.length > 0) {
              const batchStartHeight = lastHeight + 1;
              const blocksProcessed = await this.blockIndexer.indexBlocksBatch(validBlocks, batchStartHeight);
              lastHeight += blocksProcessed;
              processedThisBatch += blocksProcessed;
              this.blocksIndexed += blocksProcessed;
            }

            // Handle any missing blocks individually
            for (const height of missingHeights) {
              logger.warn('Missing block from pipelined fetch, refetching', { height });
              await this.blockIndexer.indexBlock(height);
              lastHeight = height;
              processedThisBatch++;
              this.blocksIndexed++;
            }

            // Clear arrays for GC
            fetchedBlocks.length = 0;
            validBlocks.length = 0;

            // Trigger GC
            if (typeof global.gc === 'function') {
              global.gc(true);
            }
            logMemory('post-batch-gc');

            // Log progress (throttled to avoid log spam)
            const now = Date.now();
            if (now - lastProgressLog >= PROGRESS_LOG_INTERVAL) {
              lastProgressLog = now;
              const elapsed = (now - startTime) / 1000;
              const processed = lastHeight - currentHeight;
              const blocksPerSecond = processed > 0 && elapsed > 0 ? processed / elapsed : 0;
              const remaining = chainHeight - lastHeight;
              const eta = blocksPerSecond > 0 ? remaining / blocksPerSecond : Infinity;

              const etaStr = Number.isFinite(eta) ? `${Math.floor(eta / 60)}m ${Math.floor(eta % 60)}s` : 'unknown';
              const progressInfo: Record<string, any> = {
                height: lastHeight,
                chainHeight,
                blocksPerSecond: blocksPerSecond.toFixed(1),
                eta: etaStr,
                remaining,
              };
              // Show daemon sync status if daemon is still syncing
              if (daemonIsSyncing) {
                progressInfo.daemonBlocks = daemonBlocks;
                progressInfo.daemonSync = `${daemonSyncPercent}%`;
              }
              logger.info(`Bulk sync progress ${lastHeight}/${chainHeight}`, progressInfo);
            }

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

      // Check for reorgs - but ONLY when daemon is fully synced
      // During daemon sync, hash changes are expected and not actual reorgs
      const daemonFullySynced = chainInfo.blocks >= chainInfo.headers;
      if (lastHeight > 0 && daemonFullySynced) {
        await this.checkForReorg(lastHeight);
      } else if (lastHeight > 0 && !daemonFullySynced) {
        logger.debug('Skipping reorg check - daemon still syncing', {
          daemonBlocks: chainInfo.blocks,
          daemonHeaders: chainInfo.headers,
          indexerHeight: lastHeight,
        });
      }

      // Update in-memory height tracking
      this.lastKnownHeight = lastHeight;

      const syncTime = Date.now() - startTime;
      const batchBlocksPerSec = processedThisBatch > 0 && syncTime > 0
        ? processedThisBatch / (syncTime / 1000)
        : 0;
      logger.info('Batch complete', {
        blocks: processedThisBatch,
        time: `${(syncTime / 1000).toFixed(1)}s`,
        blocksPerSecond: batchBlocksPerSec.toFixed(1),
      });

      // Force GC
      logMemory('batch-end');
      if (typeof global.gc === 'function') {
        global.gc(true);
      }

      await this.blockIndexer.setSyncingStatus(false, chainHeight);
      this.lastSyncTime = Date.now();

      // Continue syncing if still behind
      const stillBehind = chainHeight - lastHeight > safetyBuffer;
      if (stillBehind) {
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
      const syncState = await this.blockIndexer.getSyncState();
      const dbHash = syncState.lastBlockHash;
      const dbHeight = syncState.currentHeight;

      if (!dbHash) return;

      // Skip if dbHash is the empty/default hash
      if (dbHash === '0000000000000000000000000000000000000000000000000000000000000000') {
        return;
      }

      // CRITICAL: Use the height from sync_state, not the passed-in height
      // The passed-in height might not match the height that lastBlockHash corresponds to
      // This can happen if sync_state wasn't updated correctly or there's a race condition
      if (dbHeight !== currentHeight) {
        logger.debug('Skipping reorg check - height mismatch', {
          passedHeight: currentHeight,
          syncStateHeight: dbHeight,
          syncStateHash: dbHash?.slice(0, 16),
        });
        return;
      }

      let rpcHash: string;
      try {
        rpcHash = await this.rpc.getBlockHash(dbHeight);
      } catch (rpcError: any) {
        // Block may not exist yet or daemon may not have it
        // This is NOT a reorg - just wait for next sync cycle
        logger.debug('Skipping reorg check - block not available from daemon', {
          height: currentHeight,
          error: rpcError.message,
        });
        return;
      }

      if (dbHash === rpcHash) {
        return;
      }

      // Before declaring reorg, double-check that this isn't just a timing issue
      // Wait a moment and retry the hash check
      await new Promise(r => setTimeout(r, 1000));

      try {
        const retryHash = await this.rpc.getBlockHash(currentHeight);
        if (dbHash === retryHash) {
          logger.debug('Reorg false positive resolved on retry', { height: currentHeight });
          return;
        }
        rpcHash = retryHash;
      } catch {
        // If retry fails, skip reorg check entirely
        logger.debug('Skipping reorg check - block unavailable on retry', { height: currentHeight });
        return;
      }

      // Log full details to debug false positive reorgs
      const syncStateDebug = await this.blockIndexer.getSyncState();
      logger.warn('Reorg detected!', {
        height: currentHeight,
        dbHash,
        rpcHash,
        syncStateHeight: syncStateDebug.currentHeight,
        syncStateHash: syncStateDebug.lastBlockHash,
      });

      // Find common ancestor
      let commonAncestor = currentHeight - 1;
      let foundCommonAncestor = false;

      for (let i = 1; i <= this.config!.maxReorgDepth; i++) {
        const height = currentHeight - i;
        if (height < 0) break;

        // FINAL for ReplacingMergeTree (blocks)
        const result = await this.ch.queryOne<{ hash: string }>(`
          SELECT hash FROM blocks FINAL
          WHERE height = {height:UInt32} AND is_valid = 1
        `, { height });

        if (!result) break;

        let rpcBlockHash: string;
        try {
          rpcBlockHash = await this.rpc.getBlockHash(height);
        } catch {
          // Block not available, can't determine common ancestor
          logger.warn('Block not available from daemon during reorg search', { height });
          break;
        }

        if (result.hash === rpcBlockHash) {
          commonAncestor = height;
          foundCommonAncestor = true;
          break;
        }
      }

      if (!foundCommonAncestor) {
        // Don't throw - just log and wait for next sync cycle
        // This can happen during daemon sync when blocks are being reorganized internally
        logger.warn('Could not find common ancestor - will retry on next sync cycle', {
          currentHeight,
          maxDepth: this.config!.maxReorgDepth,
        });
        return;
      }

      logger.info('Reorg common ancestor found', {
        commonAncestor,
        blocksToRollback: currentHeight - commonAncestor,
      });

      await this.handleReorg(commonAncestor, currentHeight, dbHash, rpcHash);

    } catch (error: any) {
      // Don't rethrow - log and continue
      // Reorg check failures should not stop the sync engine
      logger.error('Reorg check failed (will retry on next cycle)', { error: error.message });
    }
  }

  /**
   * Handle blockchain reorganization using ClickHouse soft deletes
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

    // Record the reorg
    await recordReorg(this.ch, {
      fromHeight: currentHeight,
      toHeight: commonAncestor,
      commonAncestor,
      oldHash,
      newHash,
      blocksAffected: currentHeight - commonAncestor,
    });

    // Invalidate data from rolled-back blocks (soft delete using is_valid = 0)
    await invalidateFromHeight(this.ch, commonAncestor + 1);

    // Clear cross-batch UTXO cache to avoid stale data from rolled-back blocks
    this.blockIndexer.clearCrossBatchUtxoCache();

    // Update sync state - FINAL for ReplacingMergeTree (blocks)
    const lastValidBlock = await this.ch.queryOne<{ hash: string }>(`
      SELECT hash FROM blocks FINAL
      WHERE height = {height:UInt32} AND is_valid = 1
    `, { height: commonAncestor });

    await updateSyncState(this.ch, {
      currentHeight: commonAncestor,
      chainHeight: currentHeight,
      syncPercentage: 0,
      lastBlockHash: lastValidBlock?.hash || '',
      isSyncing: true,
      blocksPerSecond: 0,
    });

    // Reset in-memory height tracking to the rollback point
    this.lastKnownHeight = commonAncestor;

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
   * Get block indexer instance
   */
  getBlockIndexer(): ClickHouseBlockIndexer {
    return this.blockIndexer;
  }

  /**
   * Force sync now
   */
  async syncNow(): Promise<void> {
    if (!this.isRunning) {
      throw new Error('Sync engine not running');
    }
    await this.sync();
  }

  /**
   * Verify supply accuracy against daemon
   */
  private async verifySupplyAccuracy(height: number): Promise<void> {
    try {
      const chainInfo = await this.rpc.getBlockchainInfo();

      if (chainInfo.blocks < height) {
        return;
      }

      // Get indexer's calculated supply from ClickHouse
      const result = await this.ch.queryOne<{
        transparent_supply: string;
        utxo_count: string;
      }>(`
        SELECT
          toString(sum(value)) as transparent_supply,
          toString(count()) as utxo_count
        FROM utxos FINAL
        WHERE block_height <= {height:UInt32} AND spent = 0
      `, { height });

      const indexerTransparent = parseFloat(result?.transparent_supply || '0') / 1e8;
      const utxoCount = parseInt(result?.utxo_count || '0');

      // Get daemon's value pools
      const valuePools = chainInfo.valuePools || [];
      const daemonTransparent = valuePools.find((p: any) => p.id === 'transparent')?.chainValue || 0;
      const daemonSapling = valuePools.find((p: any) => p.id === 'sapling')?.chainValue || 0;
      const daemonSprout = valuePools.find((p: any) => p.id === 'sprout')?.chainValue || 0;
      const daemonShielded = daemonSapling + daemonSprout;

      const transparentDiff = indexerTransparent - daemonTransparent;
      const transparentDiffPercent = daemonTransparent > 0
        ? (Math.abs(transparentDiff) / daemonTransparent * 100).toFixed(4)
        : '0';

      // Get shielded supply from supply_stats - FINAL for ReplacingMergeTree
      const shieldedResult = await this.ch.queryOne<{ shielded_pool: string }>(`
        SELECT shielded_pool
        FROM supply_stats FINAL
        WHERE block_height <= {height:UInt32}
        ORDER BY block_height DESC
        LIMIT 1
      `, { height });

      const indexerShielded = parseFloat(shieldedResult?.shielded_pool || '0') / 1e8;
      const shieldedDiff = indexerShielded - daemonShielded;

      const logLevel = Math.abs(transparentDiff) > 1.0 ? 'warn' : 'info';

      logger[logLevel](`Supply verification at height ${height}`, {
        transparent: {
          indexer: indexerTransparent.toFixed(8),
          daemon: daemonTransparent.toFixed(8),
          difference: transparentDiff.toFixed(8),
          diffPercent: `${transparentDiffPercent}%`,
          utxoCount
        },
        shielded: {
          indexer: indexerShielded.toFixed(8),
          daemon: daemonShielded.toFixed(8),
          difference: shieldedDiff.toFixed(8),
        },
        total: {
          indexer: (indexerTransparent + indexerShielded).toFixed(8),
          daemon: (daemonTransparent + daemonShielded).toFixed(8)
        }
      });

      if (Math.abs(transparentDiff) > 1.0) {
        logger.error('⚠️  SUPPLY DISCREPANCY DETECTED', {
          height,
          transparentDiff: transparentDiff.toFixed(8),
          percentOff: `${transparentDiffPercent}%`,
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
