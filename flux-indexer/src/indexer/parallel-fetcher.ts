/**
 * Parallel Block Fetcher
 *
 * Implements pipelined block fetching for extreme sync performance.
 * Fetches blocks ahead in parallel while processing continues sequentially.
 *
 * Key optimizations:
 * - Prefetch buffer: Fetch N batches ahead while processing current batch
 * - Parallel fetch workers: Multiple concurrent RPC batch calls
 * - In-order delivery: Blocks are delivered in height order for sequential processing
 */

import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { Block } from '../types';
import { logger } from '../utils/logger';

export interface ParallelFetcherConfig {
  /** Number of blocks per RPC batch call (default: 50) */
  batchSize: number;
  /** Number of batches to prefetch ahead (default: 4) */
  prefetchBatches: number;
  /** Number of parallel fetch workers (default: 4) */
  parallelWorkers: number;
}

interface FetchJob {
  startHeight: number;
  endHeight: number;
  promise: Promise<Block[]>;
  blocks?: Block[];
  error?: Error;
  completed: boolean;
}

export class ParallelBlockFetcher {
  private config: ParallelFetcherConfig;
  private fetchQueue: FetchJob[] = [];
  private nextFetchHeight: number = 0;
  private targetHeight: number = 0;
  private isRunning: boolean = false;
  private activeWorkers: number = 0;
  private totalFetched: number = 0;
  private totalDelivered: number = 0;
  private fetchStartTime: number = 0;

  constructor(
    private rpc: FluxRPCClient,
    config?: Partial<ParallelFetcherConfig>
  ) {
    this.config = {
      batchSize: config?.batchSize || 50,
      prefetchBatches: config?.prefetchBatches || 4,
      parallelWorkers: config?.parallelWorkers || 4,
    };
  }

  /**
   * Start prefetching blocks from startHeight to targetHeight
   */
  async start(startHeight: number, targetHeight: number): Promise<void> {
    if (this.isRunning) {
      throw new Error('Parallel fetcher already running');
    }

    this.isRunning = true;
    this.nextFetchHeight = startHeight;
    this.targetHeight = targetHeight;
    this.fetchQueue = [];
    this.activeWorkers = 0;
    this.totalFetched = 0;
    this.totalDelivered = 0;
    this.fetchStartTime = Date.now();

    logger.info('Parallel block fetcher started', {
      startHeight,
      targetHeight,
      batchSize: this.config.batchSize,
      prefetchBatches: this.config.prefetchBatches,
      parallelWorkers: this.config.parallelWorkers,
      totalBlocks: targetHeight - startHeight + 1,
    });

    // Start initial prefetch workers
    this.scheduleFetches();
  }

  /**
   * Stop the fetcher
   */
  stop(): void {
    this.isRunning = false;
    const elapsed = (Date.now() - this.fetchStartTime) / 1000;
    const blocksPerSecond = elapsed > 0 ? this.totalDelivered / elapsed : 0;

    logger.info('Parallel block fetcher stopped', {
      totalFetched: this.totalFetched,
      totalDelivered: this.totalDelivered,
      elapsed: `${elapsed.toFixed(1)}s`,
      avgBlocksPerSecond: blocksPerSecond.toFixed(2),
    });
  }

  /**
   * Get the next batch of blocks in order
   * Blocks until the next batch is ready
   */
  async getNextBatch(): Promise<Block[] | null> {
    if (!this.isRunning) {
      return null;
    }

    // Schedule more fetches if needed
    this.scheduleFetches();

    // Wait for the first job in queue to complete
    if (this.fetchQueue.length === 0) {
      // No more blocks to fetch
      if (this.nextFetchHeight > this.targetHeight) {
        return null;
      }
      // Wait a bit and try again
      await new Promise(resolve => setTimeout(resolve, 100));
      return this.getNextBatch();
    }

    const job = this.fetchQueue[0];

    // Wait for this job to complete
    if (!job.completed) {
      try {
        job.blocks = await job.promise;
        job.completed = true;
        this.totalFetched += job.blocks.length;
      } catch (error: any) {
        job.error = error;
        job.completed = true;
        logger.warn('Batch fetch failed', {
          startHeight: job.startHeight,
          endHeight: job.endHeight,
          error: error.message,
        });
      }
    }

    // Remove from queue
    this.fetchQueue.shift();

    // Schedule more fetches
    this.scheduleFetches();

    // Return blocks or throw error
    if (job.error) {
      throw job.error;
    }

    if (job.blocks) {
      this.totalDelivered += job.blocks.length;

      // Log progress periodically
      if (this.totalDelivered % 1000 < this.config.batchSize) {
        const elapsed = (Date.now() - this.fetchStartTime) / 1000;
        const blocksPerSecond = elapsed > 0 ? this.totalDelivered / elapsed : 0;
        const remaining = this.targetHeight - job.endHeight;
        const eta = blocksPerSecond > 0 ? remaining / blocksPerSecond : Infinity;

        logger.debug('Parallel fetcher progress', {
          delivered: this.totalDelivered,
          queued: this.fetchQueue.length,
          activeWorkers: this.activeWorkers,
          blocksPerSecond: blocksPerSecond.toFixed(2),
          eta: Number.isFinite(eta) ? `${Math.floor(eta / 60)}m` : 'unknown',
        });
      }
    }

    // Extract blocks and clear job reference to help GC
    const blocks = job.blocks || [];
    job.blocks = null as any;
    job.promise = null as any;

    // Clear rawHex from blocks before returning to free memory sooner
    // (rawHex will be used in indexBlocksBatch but cleared there after use)
    // Note: We keep rawHex for now as it's needed for shielded parsing
    // The indexBlocksBatch will clear it after extracting shielded data

    return blocks;
  }

  /**
   * Get fetcher statistics
   */
  getStats(): {
    totalFetched: number;
    totalDelivered: number;
    queuedBatches: number;
    activeWorkers: number;
    nextFetchHeight: number;
    targetHeight: number;
    queuedBlocksInMemory: number;
  } {
    // Count blocks currently held in memory by completed jobs in queue
    let queuedBlocksInMemory = 0;
    for (const job of this.fetchQueue) {
      if (job.completed && job.blocks) {
        queuedBlocksInMemory += job.blocks.length;
      }
    }

    return {
      totalFetched: this.totalFetched,
      totalDelivered: this.totalDelivered,
      queuedBatches: this.fetchQueue.length,
      activeWorkers: this.activeWorkers,
      nextFetchHeight: this.nextFetchHeight,
      targetHeight: this.targetHeight,
      queuedBlocksInMemory,
    };
  }

  /**
   * Schedule fetch jobs to keep the prefetch buffer full
   */
  private scheduleFetches(): void {
    if (!this.isRunning) return;

    // Calculate how many batches we should have in flight
    const targetQueueSize = this.config.prefetchBatches;

    while (
      this.fetchQueue.length < targetQueueSize &&
      this.nextFetchHeight <= this.targetHeight &&
      this.activeWorkers < this.config.parallelWorkers
    ) {
      const startHeight = this.nextFetchHeight;
      const endHeight = Math.min(
        startHeight + this.config.batchSize - 1,
        this.targetHeight
      );

      if (startHeight > this.targetHeight) break;

      const heights: number[] = [];
      for (let h = startHeight; h <= endHeight; h++) {
        heights.push(h);
      }

      this.nextFetchHeight = endHeight + 1;
      this.activeWorkers++;

      const job: FetchJob = {
        startHeight,
        endHeight,
        completed: false,
        promise: this.fetchBatch(heights),
      };

      // Handle completion to track active workers
      job.promise
        .then(() => {
          this.activeWorkers--;
          this.scheduleFetches(); // Try to schedule more
        })
        .catch(() => {
          this.activeWorkers--;
        });

      this.fetchQueue.push(job);
    }
  }

  /**
   * Fetch a batch of blocks
   */
  private async fetchBatch(heights: number[]): Promise<Block[]> {
    try {
      // Include raw hex for shielded transaction parsing (vjoinsplit/valueBalance)
      return await this.rpc.batchGetBlocks(heights, true);
    } catch (error: any) {
      logger.warn('Batch fetch failed, retrying individually', {
        heights: `${heights[0]}-${heights[heights.length - 1]}`,
        error: error.message,
      });

      // Fallback: fetch blocks individually
      const blocks: Block[] = [];
      for (const height of heights) {
        try {
          const block = await this.rpc.getBlock(height, 2);
          blocks.push(block);
        } catch (individualError: any) {
          logger.warn('Individual block fetch failed', {
            height,
            error: individualError.message,
          });
          throw individualError;
        }
      }
      return blocks;
    }
  }
}
