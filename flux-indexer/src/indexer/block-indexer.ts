/**
 * ClickHouse Block Indexer
 *
 * Indexes blocks, transactions, and UTXOs from Flux blockchain into ClickHouse.
 * Optimized for high-throughput bulk inserts using ClickHouse's columnar storage.
 */

import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { ClickHouseConnection, getClickHouse } from '../database/connection';
import { Block, Transaction, SyncError } from '../types';
import { logger } from '../utils/logger';
import { isReconstructableScriptType } from '../utils/script-utils';
import { determineFluxNodeTier, parseFluxNodeTransaction } from '../parsers/fluxnode-parser';
import {
  extractFluxNodeTransaction,
  extractTransactionFromBlock,
  scanBlockTransactions,
  parseTransactionShieldedData,
} from '../parsers/block-parser';
import {
  bulkInsertBlocks,
  bulkInsertTransactions,
  bulkInsertFluxnodeTransactions,
  bulkInsertUtxos,
  bulkSpendUtxos,
  bulkInsertAddressTransactions,
  bulkUpdateAddressSummary,
  bulkInsertSupplyStats,
  bulkUpdateProducers,
  updateSyncState,
  fetchExistingUtxos,
  BlockInsert,
  TransactionInsert,
  UtxoInsert,
  UtxoSpend,
  ExistingUtxo,
  AddressTransactionInsert,
  AddressSummaryUpdate,
  SupplyStatsInsert,
  FluxnodeTransactionInsert,
  ProducerUpdate,
} from '../database/bulk-loader';

// Memory profiling
let blockMemBaseline = 0;
let blockCount = 0;
let totalTxProcessed = 0;
let totalUtxosCreated = 0;

interface MemoryProfile {
  cacheSize: number;
  cacheQueueSize: number;
}

function logDetailedMem(label: string, profile: MemoryProfile): void {
  blockCount++;
  if (blockCount % 100 !== 0) return;

  const mem = process.memoryUsage();
  const heapMB = Math.round(mem.heapUsed / 1024 / 1024);
  const rssMB = Math.round(mem.rss / 1024 / 1024);

  if (blockMemBaseline === 0) blockMemBaseline = heapMB;
  const delta = heapMB - blockMemBaseline;
  const mbPerBlock = blockCount > 0 ? (delta / blockCount).toFixed(3) : '0';

  logger.info('Memory details', {
    label,
    heapMB,
    rssMB,
    deltaHeapMB: delta,
    mbPerBlock,
    cacheSize: profile.cacheSize,
    blocks: blockCount,
    txs: totalTxProcessed,
  });
}

export class ClickHouseBlockIndexer {
  private ch: ClickHouseConnection;

  // Track supply in memory to avoid stale reads from async inserts
  private lastSupplyHeight: number = -1;
  private lastTransparentSupply: bigint = BigInt(0);
  private lastShieldedPool: bigint = BigInt(0);

  // Cross-batch UTXO cache to handle async insert visibility issues
  // This fixes the root cause of address_summary inflation:
  // When batch A creates a UTXO and batch B spends it, the async insert
  // from batch A may not be visible via database query yet. This cache
  // bridges that gap by keeping recently created UTXOs in memory.
  private crossBatchUtxoCache = new Map<string, {
    address: string;
    value: bigint;
    scriptPubkey: string;
    scriptType: string;
    blockHeight: number;
    createdAt: number; // For cache eviction
  }>();
  private readonly CROSS_BATCH_UTXO_CACHE_MAX_SIZE = 500000; // ~50MB for 500K entries
  private readonly CROSS_BATCH_UTXO_CACHE_MAX_AGE_MS = 300000; // 5 minutes

  constructor(
    private rpc: FluxRPCClient,
    ch?: ClickHouseConnection
  ) {
    this.ch = ch || getClickHouse();
  }

  /**
   * Validate address_summary consistency with UTXOs
   * Returns true if consistent, false if rebuild is needed
   */
  async validateAddressSummaryConsistency(): Promise<boolean> {
    logger.info('🔍 Validating address_summary consistency...');

    // Get total balance from UTXOs (unspent)
    const utxoResult = await this.ch.queryOne<{ total: string }>(`
      SELECT SUM(value) as total FROM utxos FINAL WHERE spent = 0
    `);
    const utxoTotal = BigInt(utxoResult?.total || '0');

    // Get total balance from address_summary (sum all merged balances)
    const summaryResult = await this.ch.queryOne<{ total: string }>(`
      SELECT sum(balance) as total FROM address_summary
    `);
    const summaryTotal = BigInt(summaryResult?.total || '0');

    // Allow 1 FLUX tolerance for rounding
    const tolerance = BigInt(100000000);
    const diff = utxoTotal > summaryTotal ? utxoTotal - summaryTotal : summaryTotal - utxoTotal;

    if (diff > tolerance) {
      logger.warn('⚠️ address_summary inconsistency detected', {
        utxoTotal: (Number(utxoTotal) / 1e8).toFixed(2),
        summaryTotal: (Number(summaryTotal) / 1e8).toFixed(2),
        difference: (Number(diff) / 1e8).toFixed(2),
      });
      return false;
    }

    logger.info('✅ address_summary is consistent with UTXOs');
    return true;
  }

  /**
   * Rebuild address_summary from UTXOs and address_transactions
   * This cleans up any corruption from duplicate batch processing
   */
  async rebuildAddressSummaryFromUtxos(): Promise<void> {
    logger.info('🔄 Rebuilding address_summary from UTXOs...');
    const startTime = Date.now();

    // First optimize tables to force deduplication
    logger.info('  Optimizing UTXOs table...');
    await this.ch.command('OPTIMIZE TABLE utxos FINAL');
    logger.info('  Optimizing address_transactions table...');
    await this.ch.command('OPTIMIZE TABLE address_transactions FINAL');

    // Truncate existing data
    await this.ch.command('TRUNCATE TABLE address_summary');
    await this.ch.command('TRUNCATE TABLE address_summary_agg');

    // Rebuild from deduplicated UTXOs and address_transactions
    logger.info('  Inserting recalculated balances...');
    await this.ch.command(`
      INSERT INTO address_summary
        (address, balance, tx_count, received_total, sent_total, unspent_count, first_seen, last_activity)
      SELECT
        COALESCE(u.address, at.address) as address,
        toInt64(COALESCE(u.balance, 0)) as balance,
        toUInt32(COALESCE(at.tx_count, 0)) as tx_count,
        toUInt64(COALESCE(at.received_total, 0)) as received_total,
        toUInt64(COALESCE(at.sent_total, 0)) as sent_total,
        toUInt32(COALESCE(u.unspent_count, 0)) as unspent_count,
        COALESCE(at.first_seen, u.first_block, 0) as first_seen,
        COALESCE(at.last_activity, u.last_block, 0) as last_activity
      FROM (
        SELECT
          address,
          SUM(CASE WHEN spent = 0 THEN value ELSE 0 END) as balance,
          SUM(CASE WHEN spent = 0 THEN 1 ELSE 0 END) as unspent_count,
          MIN(block_height) as first_block,
          MAX(CASE WHEN spent = 1 THEN spent_block_height ELSE block_height END) as last_block
        FROM utxos FINAL
        WHERE address != '' AND address != 'UNKNOWN' AND address != 'SHIELDED_OR_NONSTANDARD'
        GROUP BY address
      ) u
      FULL OUTER JOIN (
        SELECT
          address,
          uniqExact(txid) as tx_count,
          SUM(received_value) as received_total,
          SUM(sent_value) as sent_total,
          MIN(block_height) as first_seen,
          MAX(block_height) as last_activity
        FROM address_transactions FINAL
        GROUP BY address
      ) at ON u.address = at.address
      WHERE COALESCE(u.address, at.address) != ''
    `);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    logger.info(`✅ address_summary rebuild complete in ${elapsed}s`);
  }

  // Lightweight cache for same-batch UTXO lookups
  private readonly RAW_TX_CACHE_LIMIT = 100;
  private rawTransactionCache = new Map<string, Transaction>();
  private rawTransactionCacheQueue: string[] = [];

  /**
   * Add UTXOs to the cross-batch cache
   * Called after creating UTXOs in a batch
   */
  private addToCrossBatchUtxoCache(utxos: Array<{
    txid: string;
    vout: number;
    address: string;
    value: bigint;
    scriptPubkey: string;
    scriptType: string;
    blockHeight: number;
  }>): void {
    const now = Date.now();

    // Evict old entries if cache is getting large
    if (this.crossBatchUtxoCache.size > this.CROSS_BATCH_UTXO_CACHE_MAX_SIZE * 0.9) {
      this.evictOldCacheEntries();
    }

    for (const utxo of utxos) {
      const key = `${utxo.txid}:${utxo.vout}`;
      this.crossBatchUtxoCache.set(key, {
        address: utxo.address,
        value: utxo.value,
        scriptPubkey: utxo.scriptPubkey,
        scriptType: utxo.scriptType,
        blockHeight: utxo.blockHeight,
        createdAt: now,
      });
    }
  }

  /**
   * Remove spent UTXOs from the cross-batch cache
   * Called after spending UTXOs in a batch
   */
  private removeFromCrossBatchUtxoCache(spends: Array<{ txid: string; vout: number }>): void {
    for (const spend of spends) {
      const key = `${spend.txid}:${spend.vout}`;
      this.crossBatchUtxoCache.delete(key);
    }
  }

  /**
   * Lookup UTXOs from the cross-batch cache
   * Returns found UTXOs and remaining keys that weren't in cache
   */
  private lookupFromCrossBatchUtxoCache(
    outpoints: Array<{ txid: string; vout: number }>
  ): {
    found: Map<string, ExistingUtxo>;
    notFound: Array<{ txid: string; vout: number }>;
  } {
    const found = new Map<string, ExistingUtxo>();
    const notFound: Array<{ txid: string; vout: number }> = [];

    for (const op of outpoints) {
      const key = `${op.txid}:${op.vout}`;
      const cached = this.crossBatchUtxoCache.get(key);
      if (cached) {
        found.set(key, {
          address: cached.address,
          value: cached.value,
          scriptPubkey: cached.scriptPubkey,
          scriptType: cached.scriptType,
          blockHeight: cached.blockHeight,
        });
      } else {
        notFound.push(op);
      }
    }

    return { found, notFound };
  }

  /**
   * Clear the cross-batch UTXO cache
   * Should be called on reorg to avoid stale data
   */
  public clearCrossBatchUtxoCache(): void {
    const size = this.crossBatchUtxoCache.size;
    this.crossBatchUtxoCache.clear();
    if (size > 0) {
      logger.info('Cleared cross-batch UTXO cache', { entriesCleared: size });
    }
  }

  /**
   * Evict old entries from the cross-batch UTXO cache
   */
  private evictOldCacheEntries(): void {
    const now = Date.now();
    const cutoff = now - this.CROSS_BATCH_UTXO_CACHE_MAX_AGE_MS;
    let evicted = 0;

    for (const [key, entry] of this.crossBatchUtxoCache) {
      if (entry.createdAt < cutoff) {
        this.crossBatchUtxoCache.delete(key);
        evicted++;
      }
    }

    if (evicted > 0) {
      logger.debug('Evicted old entries from cross-batch UTXO cache', {
        evicted,
        remaining: this.crossBatchUtxoCache.size,
      });
    }

    // If still over limit, remove oldest entries
    if (this.crossBatchUtxoCache.size > this.CROSS_BATCH_UTXO_CACHE_MAX_SIZE) {
      const entries = Array.from(this.crossBatchUtxoCache.entries())
        .sort((a, b) => a[1].createdAt - b[1].createdAt);

      const toRemove = entries.slice(0, this.crossBatchUtxoCache.size - this.CROSS_BATCH_UTXO_CACHE_MAX_SIZE);
      for (const [key] of toRemove) {
        this.crossBatchUtxoCache.delete(key);
      }

      logger.debug('Force-evicted entries from cross-batch UTXO cache', {
        forceEvicted: toRemove.length,
        remaining: this.crossBatchUtxoCache.size,
      });
    }
  }

  /**
   * Index a single block
   */
  async indexBlock(height: number): Promise<void> {
    try {
      const block = await this.rpc.getBlock(height, 2);
      await this.processBlock(block, height);
      logger.debug(`Indexed block ${height} (${block.hash})`);
    } catch (error: any) {
      logger.error(`Failed to index block ${height}`, { error: error.message });
      throw new SyncError(`Block indexing failed at height ${height}`, { error: error.message });
    }
  }

  async indexBlockData(block: Block, expectedHeight?: number): Promise<void> {
    try {
      await this.processBlock(block, expectedHeight);
      const height = block.height ?? expectedHeight;
      logger.debug(`Indexed block ${height} (${block.hash})`);
    } catch (error: any) {
      const height = block.height ?? expectedHeight ?? 'unknown';
      logger.error(`Failed to index block ${height}`, { error: error.message });
      throw new SyncError(`Block indexing failed at height ${height}`, { error: error.message });
    }
  }

  /**
   * Index multiple blocks in a batch using ClickHouse bulk inserts
   * This is significantly faster than processing blocks individually
   * @param blocks - Array of blocks to index
   * @param startHeight - Starting height for the batch
   * @param options - Optional settings:
   *   - syncFluxnodeInsert: Use synchronous insert for fluxnode_transactions for immediate visibility
   */
  async indexBlocksBatch(blocks: Block[], startHeight: number, options?: { syncFluxnodeInsert?: boolean }): Promise<number> {
    if (blocks.length === 0) return 0;

    const startTime = Date.now();

    // Fetch raw block hex for ALL blocks to extract transaction hex
    let blockRawHexMap: Map<string, string> | null = new Map<string, string>();

    // Collect all block hashes
    const blockHashes: Array<{ hash: string; height: number }> = [];
    let heightCheck = startHeight;
    for (const block of blocks) {
      if (block) {
        blockHashes.push({ hash: block.hash, height: heightCheck });
      }
      heightCheck++;
    }

    // Fetch raw block hex in parallel batches with retry logic
    const BATCH_SIZE = 15;  // Moderate parallelism to balance speed vs daemon load
    const MAX_RETRIES = 5;
    const RETRY_BASE_DELAY = 1000;

    const fetchRawHexWithRetry = async (b: { hash: string; height: number }): Promise<{ hash: string; height: number; rawHex: string | null }> => {
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
          const rawHex = await this.rpc.getBlock(b.hash, 0) as unknown as string;
          return { hash: b.hash, height: b.height, rawHex };
        } catch (error) {
          const delay = RETRY_BASE_DELAY * Math.pow(2, attempt - 1);
          if (attempt < MAX_RETRIES) {
            logger.warn('Failed to fetch raw block hex, retrying', {
              height: b.height,
              attempt,
              maxRetries: MAX_RETRIES,
              retryInMs: delay,
              error: (error as Error).message
            });
            await new Promise(resolve => setTimeout(resolve, delay));
          } else {
            logger.error('Failed to fetch raw block hex after all retries', {
              height: b.height,
              attempts: MAX_RETRIES,
              error: (error as Error).message
            });
            return { hash: b.hash, height: b.height, rawHex: null };
          }
        }
      }
      return { hash: b.hash, height: b.height, rawHex: null };
    };

    for (let i = 0; i < blockHashes.length; i += BATCH_SIZE) {
      const batch = blockHashes.slice(i, i + BATCH_SIZE);
      const rawHexResults = await Promise.all(batch.map(fetchRawHexWithRetry));

      for (const result of rawHexResults) {
        if (result.rawHex) {
          blockRawHexMap!.set(result.hash, result.rawHex);
        }
      }
    }

    // Maps for parsed data
    let txHexMap: Map<string, string> | null = new Map<string, string>();
    let parsedShieldedData: Map<string, { vjoinsplit?: Array<{ vpub_old: bigint; vpub_new: bigint }>; valueBalance?: bigint }> | null = new Map();
    let parsedFluxNodeData: Map<string, {
      type: number;
      collateralHash?: string;
      collateralIndex?: number;
      ip?: string;
      publicKey?: string;
      signature?: string;
      tier?: string;
      p2shAddress?: string;
    }> | null = new Map();

    // Parse all transactions from raw blocks
    heightCheck = startHeight;
    for (const block of blocks) {
      if (!block) {
        heightCheck++;
        continue;
      }

      const rawHex = blockRawHexMap!.get(block.hash);
      if (!rawHex) {
        heightCheck++;
        continue;
      }

      const transactions = block.tx as Transaction[];
      if (!transactions || transactions.length === 0) {
        heightCheck++;
        continue;
      }

      try {
        const scannedTxs = scanBlockTransactions(rawHex, heightCheck);
        const scannedTxMap = new Map<string, { hex: string; version: number; fluxNodeType?: number }>();
        for (const scanned of scannedTxs) {
          scannedTxMap.set(scanned.txid, {
            hex: scanned.hex,
            version: scanned.version,
            fluxNodeType: scanned.fluxNodeType
          });
        }

        // Verify we got all expected transactions
        const missingTxids: string[] = [];
        let fluxnodeInScanned = 0;
        for (const scanned of scannedTxs) {
          if (scanned.fluxNodeType !== undefined) {
            fluxnodeInScanned++;
          }
        }
        for (const tx of transactions) {
          if (!tx || !tx.txid) continue;
          if (!scannedTxMap.has(tx.txid)) {
            missingTxids.push(tx.txid);
          }
        }

        if (missingTxids.length > 0) {
          logger.warn('Scan missing transactions, falling back to per-tx extraction', {
            height: heightCheck,
            missing: missingTxids.length,
            fluxnodeInScanned,
            firstMissing: missingTxids.slice(0, 3)
          });
          throw new Error(`Scan missing ${missingTxids.length} transactions`);
        }

        // Process each transaction using the pre-built map
        for (const tx of transactions) {
          if (!tx || !tx.txid) continue;

          const scanned = scannedTxMap.get(tx.txid)!;
          txHexMap!.set(tx.txid, scanned.hex);

          // Parse shielded data for v2/v4 transactions
          if (tx.version === 2 || tx.version === 4) {
            const shieldedData = parseTransactionShieldedData(scanned.hex);
            if (shieldedData.vjoinsplit || shieldedData.valueBalance !== undefined) {
              parsedShieldedData!.set(tx.txid, shieldedData);
            }
          }

          // Parse FluxNode data for v3/v5/v6 transactions
          if (scanned.fluxNodeType !== undefined) {
            try {
              const fluxNodeData = parseFluxNodeTransaction(scanned.hex);
              if (fluxNodeData) {
                parsedFluxNodeData!.set(tx.txid, {
                  type: fluxNodeData.type,
                  collateralHash: fluxNodeData.collateralHash,
                  collateralIndex: fluxNodeData.collateralIndex,
                  ip: fluxNodeData.ipAddress,
                  publicKey: fluxNodeData.publicKey,
                  signature: fluxNodeData.signature,
                  tier: fluxNodeData.benchmarkTier,
                  p2shAddress: fluxNodeData.p2shAddress,
                });
              }
            } catch (fnError) {
              logger.debug('Failed to parse FluxNode transaction', { txid: tx.txid, height: heightCheck });
            }
          }
        }
      } catch (error) {
        // Fallback to per-tx extraction
        for (const tx of transactions) {
          if (!tx || !tx.txid) continue;
          try {
            const txHex = extractTransactionFromBlock(rawHex, tx.txid, heightCheck);
            if (txHex) {
              txHexMap!.set(tx.txid, txHex);
              if (tx.version === 2 || tx.version === 4) {
                const shieldedData = parseTransactionShieldedData(txHex);
                if (shieldedData.vjoinsplit || shieldedData.valueBalance !== undefined) {
                  parsedShieldedData!.set(tx.txid, shieldedData);
                }
              }
              if (tx.version === 3 || tx.version === 5 || tx.version === 6) {
                const vin = tx.vin || [];
                const vout = tx.vout || [];
                if (vin.length === 0 && vout.length === 0) {
                  const fluxNodeData = parseFluxNodeTransaction(txHex);
                  if (fluxNodeData) {
                    parsedFluxNodeData!.set(tx.txid, {
                      type: fluxNodeData.type,
                      collateralHash: fluxNodeData.collateralHash,
                      collateralIndex: fluxNodeData.collateralIndex,
                      ip: fluxNodeData.ipAddress,
                      publicKey: fluxNodeData.publicKey,
                      signature: fluxNodeData.signature,
                      tier: fluxNodeData.benchmarkTier,
                      p2shAddress: fluxNodeData.p2shAddress,
                    });
                  }
                }
              }
            }
          } catch (err) {
            logger.error('Exception extracting tx hex', { height: heightCheck, txid: tx.txid });
          }
        }
      }

      heightCheck++;
    }

    // Collect all data from all blocks
    const blockRecords: BlockInsert[] = [];
    const txRecords: TransactionInsert[] = [];
    const fluxnodeRecords: FluxnodeTransactionInsert[] = [];
    const utxoRecords: UtxoInsert[] = [];
    const spendRecords: UtxoSpend[] = [];

    // Address transactions map - tracks per-address involvement in transactions
    const addressTxMap = new Map<string, {
      address: string;
      txid: string;
      blockHeight: number;
      blockHash: string;      // Denormalized for fast queries
      txIndex: number;        // Position in block
      timestamp: number;
      received: bigint;
      sent: bigint;
      isCoinbase: boolean;
    }>();

    // Track txid -> tx metadata for address_transactions
    const txMetaMap = new Map<string, { blockHash: string; txIndex: number; isCoinbase: boolean }>();

    // Map of outputs created in this batch (for same-batch UTXO lookups)
    const batchUtxoMap = new Map<string, { value: bigint; address: string; scriptPubkey: string; scriptType: string }>();

    // Collect input references that need UTXO lookups
    const inputRefs: Array<{
      key: string;
      txid: string;
      vout: number;
      spentByTxid: string;
      spentBlockHeight: number;
      spentTimestamp: number;
    }> = [];

    // Supply tracking per block (includes timestamp for accurate date grouping in materialized views)
    const supplyChanges: Array<{ height: number; timestamp: number; coinbaseReward: bigint; shieldedChange: bigint }> = [];

    // First pass: collect all data
    let currentHeight = startHeight;
    for (const block of blocks) {
      if (!block) {
        currentHeight++;
        continue;
      }

      const transactions = block.tx as Transaction[];
      if (!transactions || transactions.length === 0 || typeof transactions[0] === 'string') {
        currentHeight++;
        continue;
      }

      blockRecords.push({
        height: currentHeight,
        hash: block.hash,
        prevHash: block.previousblockhash || null,
        merkleRoot: block.merkleroot || null,
        timestamp: block.time,
        bits: block.bits || null,
        nonce: block.nonce?.toString() || null,
        version: block.version ?? null,
        size: block.size ?? null,
        txCount: transactions.length,
        producer: block.producer || null,
        producerReward: block.producerReward ? BigInt(Math.round(block.producerReward * 1e8)) : null,
        difficulty: block.difficulty ?? null,
        chainwork: block.chainwork || null,
      });

      // Track supply changes
      let blockCoinbaseReward = BigInt(0);
      let blockShieldedChange = BigInt(0);

      let txIndexInBlock = 0;
      for (const tx of transactions) {
        if (!tx || !tx.txid) continue;

        const vin = tx.vin || [];
        const vout = tx.vout || [];
        const isCoinbase = vin.length > 0 && !!vin[0].coinbase;

        // Store tx metadata for address_transactions (denormalized)
        txMetaMap.set(tx.txid, { blockHash: block.hash, txIndex: txIndexInBlock, isCoinbase });
        txIndexInBlock++;

        // Collect outputs
        let outputTotal = BigInt(0);

        for (let voutIdx = 0; voutIdx < vout.length; voutIdx++) {
          const output = vout[voutIdx];
          const valueSats = BigInt(Math.round(output.value * 100000000));
          outputTotal += valueSats;

          let address = 'SHIELDED_OR_NONSTANDARD';
          if (output.scriptPubKey?.addresses && output.scriptPubKey.addresses.length > 0) {
            address = output.scriptPubKey.addresses[0];
          }

          const scriptType = output.scriptPubKey?.type || 'unknown';
          const scriptHex = output.scriptPubKey?.hex || '';

          // OPTIMIZATION: Skip storing script_pubkey for standard types (P2PKH, P2SH)
          // These can be reconstructed from address + type, saving ~12GB storage
          // Only store script_pubkey for non-standard types (nulldata, nonstandard, etc.)
          const scriptPubkeyToStore = isReconstructableScriptType(scriptType) ? '' : scriptHex;

          // Add to batch map for same-batch lookups
          const key = `${tx.txid}:${voutIdx}`;
          batchUtxoMap.set(key, { value: valueSats, address, scriptPubkey: scriptPubkeyToStore, scriptType });

          utxoRecords.push({
            txid: tx.txid,
            vout: voutIdx,
            address,
            value: valueSats,
            scriptPubkey: scriptPubkeyToStore,
            scriptType,
            blockHeight: currentHeight,
          });

          // Track address transaction (received)
          if (address !== 'SHIELDED_OR_NONSTANDARD') {
            const addrTxKey = `${address}:${tx.txid}:${currentHeight}`;
            const existing = addressTxMap.get(addrTxKey);
            if (existing) {
              existing.received += valueSats;
            } else {
              const meta = txMetaMap.get(tx.txid)!;
              addressTxMap.set(addrTxKey, {
                address,
                txid: tx.txid,
                blockHeight: currentHeight,
                blockHash: meta.blockHash,
                txIndex: meta.txIndex,
                timestamp: block.time,
                received: valueSats,
                sent: BigInt(0),
                isCoinbase: meta.isCoinbase,
              });
            }
          }
        }

        // Collect inputs
        let inputTotal = BigInt(0);
        for (const input of vin) {
          if (input.coinbase || !input.txid) continue;

          const key = `${input.txid}:${input.vout}`;
          const batchUtxo = batchUtxoMap.get(key);

          if (batchUtxo) {
            inputTotal += batchUtxo.value;

            if (batchUtxo.address !== 'SHIELDED_OR_NONSTANDARD') {
              const addrTxKey = `${batchUtxo.address}:${tx.txid}:${currentHeight}`;
              const existing = addressTxMap.get(addrTxKey);
              if (existing) {
                existing.sent += batchUtxo.value;
              } else {
                const meta = txMetaMap.get(tx.txid)!;
                addressTxMap.set(addrTxKey, {
                  address: batchUtxo.address,
                  txid: tx.txid,
                  blockHeight: currentHeight,
                  blockHash: meta.blockHash,
                  txIndex: meta.txIndex,
                  timestamp: block.time,
                  received: BigInt(0),
                  sent: batchUtxo.value,
                  isCoinbase: meta.isCoinbase,
                });
              }
            }
          } else {
            inputRefs.push({
              key,
              txid: input.txid,
              vout: input.vout!,
              spentByTxid: tx.txid,
              spentBlockHeight: currentHeight,
              spentTimestamp: block.time,
            });
          }

          spendRecords.push({
            txid: input.txid,
            vout: input.vout!,
            spentTxid: tx.txid,
            spentBlockHeight: currentHeight,
          });
        }

        // FluxNode transaction handling
        const fluxNodeData = parsedFluxNodeData!.get(tx.txid);
        const isFluxnodeTx = !!fluxNodeData;
        const fluxnodeType = fluxNodeData?.type ?? null;

        const txHexForSize = txHexMap!.get(tx.txid) || null;
        const computedSize = tx.size || (txHexForSize ? Math.floor(txHexForSize.length / 2) : 0);
        const computedVSize = tx.vsize || computedSize;

        txRecords.push({
          txid: tx.txid,
          blockHeight: currentHeight,
          txIndex: txRecords.length,
          timestamp: block.time,
          version: tx.version,
          locktime: tx.locktime || 0,
          size: computedSize,
          vsize: computedVSize,
          inputCount: vin.length,
          outputCount: vout.length,
          inputTotal: BigInt(0), // Updated after UTXO lookups
          outputTotal: outputTotal,
          fee: BigInt(0), // Calculated after UTXO lookups
          isCoinbase,
          isFluxnodeTx,
          fluxnodeType,
          isShielded: !!(parsedShieldedData!.get(tx.txid)?.vjoinsplit?.length || parsedShieldedData!.get(tx.txid)?.valueBalance),
        });

        // Add to fluxnodeRecords if FluxNode transaction
        if (isFluxnodeTx && txHexForSize) {
          fluxnodeRecords.push({
            txid: tx.txid,
            blockHeight: currentHeight,
            blockTime: new Date(block.time * 1000),
            version: tx.version,
            type: fluxnodeType!,
            collateralHash: fluxNodeData!.collateralHash || null,
            collateralIndex: fluxNodeData!.collateralIndex ?? null,
            ipAddress: fluxNodeData!.ip || null,
            publicKey: fluxNodeData!.publicKey || null,
            signature: fluxNodeData!.signature || null,
            p2shAddress: fluxNodeData!.p2shAddress || null,
            benchmarkTier: fluxNodeData!.tier || null,
            extraData: null,
          });
        }

        // Track supply changes
        if (isCoinbase) {
          blockCoinbaseReward = outputTotal;
        }

        // Extract shielded pool changes
        if (tx.version === 2 || tx.version === 4) {
          const shieldedData = parsedShieldedData!.get(tx.txid);
          if (shieldedData) {
            const MAX_REASONABLE_VALUE = BigInt(1_000_000_000) * BigInt(100_000_000);

            if (shieldedData.vjoinsplit && shieldedData.vjoinsplit.length > 0) {
              for (const js of shieldedData.vjoinsplit) {
                const absVpubOld = js.vpub_old < BigInt(0) ? -js.vpub_old : js.vpub_old;
                const absVpubNew = js.vpub_new < BigInt(0) ? -js.vpub_new : js.vpub_new;
                if (absVpubOld > MAX_REASONABLE_VALUE || absVpubNew > MAX_REASONABLE_VALUE) {
                  break;
                }
                blockShieldedChange += js.vpub_old - js.vpub_new;
              }
            }

            if (tx.version === 4 && shieldedData.valueBalance !== undefined) {
              const absValueBalance = shieldedData.valueBalance < BigInt(0) ? -shieldedData.valueBalance : shieldedData.valueBalance;
              if (absValueBalance <= MAX_REASONABLE_VALUE) {
                blockShieldedChange -= shieldedData.valueBalance;
              }
            }
          }
        }
      }

      supplyChanges.push({
        height: currentHeight,
        timestamp: block.time,
        coinbaseReward: blockCoinbaseReward,
        shieldedChange: blockShieldedChange,
      });

      currentHeight++;
    }

    // Batch lookup UTXOs from database for inputs not in this batch
    // IMPORTANT: First check the cross-batch cache to handle async insert visibility issues
    // This is the ROOT CAUSE fix for address_summary inflation - without this cache,
    // UTXOs created in batch A may not be visible in batch B's database query due to
    // ClickHouse async inserts not being flushed yet.
    const utxoLookupMap = new Map<string, ExistingUtxo>();
    if (inputRefs.length > 0) {
      const uniqueRefs = [...new Map(inputRefs.map(r => [r.key, r])).values()];
      const outpoints = uniqueRefs.map(r => ({ txid: r.txid, vout: r.vout }));

      // Step 1: Check cross-batch cache first (handles async insert visibility)
      const { found: cacheHits, notFound: cacheMisses } = this.lookupFromCrossBatchUtxoCache(outpoints);

      // Add cache hits to lookup map
      for (const [key, utxo] of cacheHits) {
        utxoLookupMap.set(key, utxo);
      }

      // Step 2: Query database only for UTXOs not in cache
      if (cacheMisses.length > 0) {
        const existingUtxos = await fetchExistingUtxos(this.ch, cacheMisses);

        // Merge into lookup map
        for (const [key, utxo] of existingUtxos) {
          utxoLookupMap.set(key, utxo);
        }
      }

      // Log cache effectiveness periodically
      if (cacheHits.size > 0 || startHeight % 10000 === 0) {
        logger.debug('Cross-batch UTXO cache stats', {
          cacheHits: cacheHits.size,
          cacheMisses: cacheMisses.length,
          dbFound: utxoLookupMap.size - cacheHits.size,
          cacheSize: this.crossBatchUtxoCache.size,
          hitRate: outpoints.length > 0 ? ((cacheHits.size / outpoints.length) * 100).toFixed(1) + '%' : 'N/A',
        });
      }
    }

    // Process inputRefs to update addressTxMap with looked-up addresses
    // Track missing UTXOs for warning (indicates potential data integrity issue)
    let missingUtxoCount = 0;
    for (const ref of inputRefs) {
      const utxo = utxoLookupMap.get(ref.key);
      if (utxo && utxo.address !== 'SHIELDED_OR_NONSTANDARD' && utxo.address !== 'UNKNOWN') {
        const addrTxKey = `${utxo.address}:${ref.spentByTxid}:${ref.spentBlockHeight}`;
        const existing = addressTxMap.get(addrTxKey);
        if (existing) {
          existing.sent += utxo.value;
        } else {
          // Get tx metadata (may be from same batch or need lookup)
          const meta = txMetaMap.get(ref.spentByTxid);
          if (meta) {
            addressTxMap.set(addrTxKey, {
              address: utxo.address,
              txid: ref.spentByTxid,
              blockHeight: ref.spentBlockHeight,
              blockHash: meta.blockHash,
              txIndex: meta.txIndex,
              timestamp: ref.spentTimestamp,
              received: BigInt(0),
              sent: utxo.value,
              isCoinbase: meta.isCoinbase,
            });
          }
        }
      } else if (!utxo) {
        // UTXO not found in cache OR database - this would cause address_summary corruption
        missingUtxoCount++;
      }
    }

    // Log warning if UTXOs were not found (indicates potential data integrity issue)
    if (missingUtxoCount > 0) {
      logger.warn('⚠️ UTXOs not found for spending inputs - address balances may be affected', {
        missingCount: missingUtxoCount,
        totalInputRefs: inputRefs.length,
        startHeight,
        endHeight: startHeight + blocks.length - 1,
      });
    }

    // Update transaction inputTotal and fee based on UTXO lookups
    // Also track total fees per block to assign to coinbase transactions
    let txIdx = 0;
    currentHeight = startHeight;
    for (const block of blocks) {
      if (!block) {
        currentHeight++;
        continue;
      }

      const transactions = block.tx as Transaction[];
      if (!transactions || transactions.length === 0 || typeof transactions[0] === 'string') {
        currentHeight++;
        continue;
      }

      // Track coinbase txRecord and total fees collected for this block
      let coinbaseTxRecord: TransactionInsert | null = null;
      let blockTotalFees = BigInt(0);

      for (const tx of transactions) {
        if (!tx || !tx.txid) continue;
        const txRecord = txRecords[txIdx];
        if (!txRecord) {
          txIdx++;
          continue;
        }

        const vin = tx.vin || [];
        const isCoinbase = vin.length > 0 && !!vin[0].coinbase;

        if (isCoinbase) {
          // Remember the coinbase txRecord to assign total fees later
          coinbaseTxRecord = txRecord;
        } else {
          let inputTotal = BigInt(0);
          for (const input of vin) {
            if (input.coinbase || !input.txid) continue;
            const key = `${input.txid}:${input.vout}`;
            const batchUtxo = batchUtxoMap.get(key);
            const lookupUtxo = utxoLookupMap.get(key);
            if (batchUtxo) {
              inputTotal += batchUtxo.value;
            } else if (lookupUtxo) {
              inputTotal += lookupUtxo.value;
            }
          }
          txRecord.inputTotal = inputTotal;
          const outputTotal = typeof txRecord.outputTotal === 'bigint' ? txRecord.outputTotal : BigInt(txRecord.outputTotal);

          // Calculate fee correctly for shielded transactions
          // For shielded transactions: fee = inputs - outputs - shieldedPoolChange
          // where shieldedPoolChange accounts for coins moving between transparent and shielded pools
          let shieldedPoolChange = BigInt(0);
          const shieldedData = parsedShieldedData!.get(tx.txid);

          if (shieldedData) {
            const MAX_REASONABLE_VALUE = BigInt(1_000_000_000) * BigInt(100_000_000);

            // V2/V4 transactions with JoinSplits (Sprout)
            // vpub_old = transparent value entering shielded pool (shielding)
            // vpub_new = shielded value leaving to transparent (deshielding)
            // shieldedPoolChange = net flow FROM shielded pool (positive = deshielding, negative = shielding)
            if (shieldedData.vjoinsplit && shieldedData.vjoinsplit.length > 0) {
              for (const js of shieldedData.vjoinsplit) {
                const absVpubOld = js.vpub_old < BigInt(0) ? -js.vpub_old : js.vpub_old;
                const absVpubNew = js.vpub_new < BigInt(0) ? -js.vpub_new : js.vpub_new;
                if (absVpubOld > MAX_REASONABLE_VALUE || absVpubNew > MAX_REASONABLE_VALUE) {
                  break;
                }
                // vpub_new - vpub_old = net flow FROM shielded pool
                shieldedPoolChange += js.vpub_new - js.vpub_old;
              }
            }

            // V4 Sapling transactions with valueBalance
            // valueBalance = sapling spends - sapling outputs
            // Positive = net withdrawal from shielded (deshielding)
            // Negative = net deposit to shielded (shielding)
            if (tx.version === 4 && shieldedData.valueBalance !== undefined) {
              const absValueBalance = shieldedData.valueBalance < BigInt(0) ? -shieldedData.valueBalance : shieldedData.valueBalance;
              if (absValueBalance <= MAX_REASONABLE_VALUE) {
                // valueBalance represents net flow FROM shielded pool (same convention as JoinSplit)
                shieldedPoolChange += shieldedData.valueBalance;
              }
            }
          }

          // Fee formula: fee = transparent_inputs + shielded_contribution - transparent_outputs
          // shieldedPoolChange = net flow FROM shielded pool (positive = funds coming from shielded)
          const fee = inputTotal - outputTotal + shieldedPoolChange;
          const safeFee = fee < BigInt(0) ? BigInt(0) : fee;
          txRecord.fee = safeFee;

          // Accumulate fees for this block (to assign to coinbase)
          blockTotalFees += safeFee;
        }

        txIdx++;
      }

      // Assign total collected fees to the coinbase transaction
      if (coinbaseTxRecord) {
        coinbaseTxRecord.fee = blockTotalFees;
      }

      currentHeight++;
    }

    // Build existing UTXO map for spend operations
    const existingUtxoMapForSpends = new Map<string, ExistingUtxo>();
    for (const [key, utxo] of utxoLookupMap) {
      existingUtxoMapForSpends.set(key, utxo);
    }
    // Also add batch UTXOs
    for (const utxo of utxoRecords) {
      const key = `${utxo.txid}:${utxo.vout}`;
      existingUtxoMapForSpends.set(key, {
        address: utxo.address,
        value: typeof utxo.value === 'bigint' ? utxo.value : BigInt(utxo.value),
        scriptPubkey: utxo.scriptPubkey,
        scriptType: utxo.scriptType,
        blockHeight: utxo.blockHeight,
      });
    }

    // Now do bulk inserts into ClickHouse
    const timings: Record<string, number> = {};
    let t0 = Date.now();

    await bulkInsertBlocks(this.ch, blockRecords);
    timings.blocks = Date.now() - t0; t0 = Date.now();

    await bulkInsertTransactions(this.ch, txRecords);
    timings.txs = Date.now() - t0; t0 = Date.now();

    if (fluxnodeRecords.length > 0) {
      const useSync = options?.syncFluxnodeInsert ?? false;
      logger.info('Inserting FluxNode transaction records', {
        count: fluxnodeRecords.length,
        startHeight,
        endHeight: startHeight + blocks.length - 1,
        firstTxid: fluxnodeRecords[0]?.txid?.slice(0, 16),
        firstTier: fluxnodeRecords[0]?.benchmarkTier,
        firstIp: fluxnodeRecords[0]?.ipAddress,
        syncInsert: useSync,
      });
      await bulkInsertFluxnodeTransactions(this.ch, fluxnodeRecords, { sync: useSync });
      timings.fluxnode = Date.now() - t0; t0 = Date.now();
    }

    await bulkInsertUtxos(this.ch, utxoRecords);
    timings.utxos = Date.now() - t0; t0 = Date.now();

    // Update cross-batch UTXO cache with newly created UTXOs
    // This ensures subsequent batches can find these UTXOs even if async inserts haven't flushed
    if (utxoRecords.length > 0) {
      const utxosForCache = utxoRecords.map(u => ({
        txid: u.txid,
        vout: u.vout,
        address: u.address,
        value: typeof u.value === 'bigint' ? u.value : BigInt(u.value),
        scriptPubkey: u.scriptPubkey || '',
        scriptType: u.scriptType,
        blockHeight: u.blockHeight,
      }));
      this.addToCrossBatchUtxoCache(utxosForCache);
    }

    if (spendRecords.length > 0) {
      await bulkSpendUtxos(this.ch, existingUtxoMapForSpends, spendRecords);
      timings.spends = Date.now() - t0; t0 = Date.now();

      // Remove spent UTXOs from cross-batch cache (they won't be needed again)
      this.removeFromCrossBatchUtxoCache(spendRecords.map(s => ({ txid: s.txid, vout: s.vout })));
    }

    // Bulk insert address_transactions
    if (addressTxMap.size > 0) {
      const addressTxRecords: AddressTransactionInsert[] = Array.from(addressTxMap.values()).map(r => ({
        address: r.address,
        txid: r.txid,
        blockHeight: r.blockHeight,
        blockHash: r.blockHash,
        txIndex: r.txIndex,
        timestamp: r.timestamp,
        received: r.received,
        sent: r.sent,
        isCoinbase: r.isCoinbase,
      }));
      await bulkInsertAddressTransactions(this.ch, addressTxRecords);
      timings.addrTx = Date.now() - t0; t0 = Date.now();
    }

    // Bulk update address_summary
    if (addressTxMap.size > 0) {
      const addressChanges = new Map<string, {
        received: bigint;
        sent: bigint;
        txCount: number;
        minHeight: number;
        maxHeight: number;
      }>();

      for (const record of addressTxMap.values()) {
        const existing = addressChanges.get(record.address);
        if (existing) {
          existing.received += record.received;
          existing.sent += record.sent;
          existing.txCount++;
          existing.minHeight = Math.min(existing.minHeight, record.blockHeight);
          existing.maxHeight = Math.max(existing.maxHeight, record.blockHeight);
        } else {
          addressChanges.set(record.address, {
            received: record.received,
            sent: record.sent,
            txCount: 1,
            minHeight: record.blockHeight,
            maxHeight: record.blockHeight
          });
        }
      }

      const summaryUpdates: AddressSummaryUpdate[] = [];
      for (const [address, changes] of addressChanges) {
        summaryUpdates.push({
          address,
          balance: changes.received - changes.sent,
          txCount: changes.txCount,
          receivedTotal: changes.received,
          sentTotal: changes.sent,
          unspentCount: 0, // Will be calculated separately if needed
          firstSeen: changes.minHeight,
          lastActivity: changes.maxHeight,
        });
      }

      await bulkUpdateAddressSummary(this.ch, summaryUpdates);
      timings.addrSummary = Date.now() - t0; t0 = Date.now();
    }

    // Bulk update supply stats
    if (supplyChanges.length > 0) {
      // Use in-memory tracking to avoid stale reads from async inserts
      // Only query database if we haven't initialized yet or there's a gap
      const expectedPrevHeight = startHeight - 1;

      if (this.lastSupplyHeight < 0 || this.lastSupplyHeight !== expectedPrevHeight) {
        if (expectedPrevHeight < 0) {
          // Starting from genesis - no need to query database
          this.lastSupplyHeight = -1;
          this.lastTransparentSupply = BigInt(0);
          this.lastShieldedPool = BigInt(0);
        } else {
          // Need to initialize or re-sync from database
          const prevStats = await this.ch.queryOne<{
            block_height: number;
            transparent_supply: string;
            shielded_pool: string;
          }>(`
            SELECT block_height, transparent_supply, shielded_pool
            FROM supply_stats FINAL
            WHERE block_height = {height:UInt32}
          `, { height: expectedPrevHeight });

          if (prevStats) {
            this.lastSupplyHeight = prevStats.block_height;
            this.lastTransparentSupply = BigInt(prevStats.transparent_supply);
            this.lastShieldedPool = BigInt(prevStats.shielded_pool);
          } else {
            // Gap detected - query for latest available
            const latestStats = await this.ch.queryOne<{
              block_height: number;
              transparent_supply: string;
              shielded_pool: string;
            }>(`
              SELECT block_height, transparent_supply, shielded_pool
              FROM supply_stats FINAL
              ORDER BY block_height DESC
              LIMIT 1
            `);

            if (latestStats) {
              this.lastSupplyHeight = latestStats.block_height;
              this.lastTransparentSupply = BigInt(latestStats.transparent_supply);
              this.lastShieldedPool = BigInt(latestStats.shielded_pool);
              logger.warn('Supply stats gap detected, using latest available', {
                expected: expectedPrevHeight,
                found: latestStats.block_height
              });
            } else {
              // No data at all - start from zero
              this.lastSupplyHeight = -1;
              this.lastTransparentSupply = BigInt(0);
              this.lastShieldedPool = BigInt(0);
            }
          }
        }
      }

      let transparentSupply = this.lastTransparentSupply;
      let shieldedPool = this.lastShieldedPool;

      const supplyStatsRecords: SupplyStatsInsert[] = [];
      for (const change of supplyChanges) {
        transparentSupply += change.coinbaseReward - change.shieldedChange;
        shieldedPool += change.shieldedChange;
        const totalSupply = transparentSupply + shieldedPool;
        supplyStatsRecords.push({
          blockHeight: change.height,
          timestamp: change.timestamp,
          transparentSupply,
          shieldedPool,
          totalSupply,
        });
      }

      await bulkInsertSupplyStats(this.ch, supplyStatsRecords);

      // Update in-memory tracking with the last values from this batch
      const lastChange = supplyChanges[supplyChanges.length - 1];
      this.lastSupplyHeight = lastChange.height;
      this.lastTransparentSupply = transparentSupply;
      this.lastShieldedPool = shieldedPool;

      timings.supply = Date.now() - t0; t0 = Date.now();
    }

    // Track totals for memory profiling
    totalTxProcessed += txRecords.length;
    totalUtxosCreated += utxoRecords.length;

    // Update sync state to last block
    const lastBlock = blocks[blocks.length - 1];
    if (lastBlock) {
      await updateSyncState(this.ch, {
        currentHeight: startHeight + blocks.length - 1,
        chainHeight: 0, // Updated separately
        syncPercentage: 0,
        lastBlockHash: lastBlock.hash,
        isSyncing: true,
        blocksPerSecond: blocks.length / ((Date.now() - startTime) / 1000),
      });
    }

    const elapsed = Date.now() - startTime;
    logger.debug(`Batch indexed ${blocks.length} blocks in ${elapsed}ms (${(blocks.length / (elapsed / 1000)).toFixed(1)} blocks/sec)`);

    // Clean up
    blockRawHexMap?.clear();
    blockRawHexMap = null;
    txHexMap?.clear();
    txHexMap = null;
    parsedShieldedData?.clear();
    parsedShieldedData = null;
    parsedFluxNodeData?.clear();
    parsedFluxNodeData = null;
    batchUtxoMap.clear();
    utxoLookupMap.clear();
    addressTxMap.clear();

    return blocks.length;
  }

  /**
   * Process a single block
   */
  private async processBlock(block: Block, expectedHeight?: number, options?: { syncFluxnodeInsert?: boolean }): Promise<void> {
    const blockHeight = block.height ?? expectedHeight;
    if (blockHeight === undefined) {
      throw new SyncError('Block height is undefined', { blockHash: block.hash });
    }

    block.height = blockHeight;
    // Ensure block transactions are normalized (fetches any missing tx data)
    await this.normalizeBlockTransactions(block);

    // Process as a single-block batch
    // Use sync insert for fluxnode data when processing single blocks (tip-following mode)
    await this.indexBlocksBatch([block], blockHeight, { syncFluxnodeInsert: options?.syncFluxnodeInsert ?? true });

    // Update producer stats if applicable
    if (block.producer) {
      await this.updateProducerStats(block);
    }

    logDetailedMem(`block-${blockHeight}-cleared`, {
      cacheSize: this.rawTransactionCache.size,
      cacheQueueSize: this.rawTransactionCacheQueue.length,
    });
  }

  /**
   * Normalize block transactions (ensure full tx objects)
   */
  private async normalizeBlockTransactions(block: Block): Promise<Transaction[]> {
    if (!Array.isArray(block.tx) || block.tx.length === 0) {
      return [];
    }

    const normalized: (Transaction | null)[] = new Array(block.tx.length).fill(null);
    const missing: Array<{ txid: string; index: number }> = [];

    block.tx.forEach((tx, index) => {
      if (typeof tx === 'string') {
        const cached = this.rawTransactionCache.get(tx);
        if (cached) {
          normalized[index] = cached;
        } else {
          missing.push({ txid: tx, index });
        }
      } else {
        this.cacheRawTransaction(tx);
        normalized[index] = tx;
      }
    });

    if (missing.length > 0) {
      const uniqueTxids = Array.from(new Set(missing.map(item => item.txid)));
      const fetched = await this.rpc.batchGetRawTransactions(uniqueTxids, true, block.hash);

      uniqueTxids.forEach((_txid, idx) => {
        const fetchedTx = fetched[idx];
        if (typeof fetchedTx !== 'string') {
          this.cacheRawTransaction(fetchedTx);
        }
      });

      for (const { txid, index } of missing) {
        const raw = this.rawTransactionCache.get(txid);
        if (raw) {
          normalized[index] = raw;
        }
      }
    }

    return normalized.filter((entry): entry is Transaction => entry !== null);
  }

  private cacheRawTransaction(tx: Transaction): void {
    if (!tx?.txid || this.rawTransactionCache.has(tx.txid)) {
      return;
    }

    const lightweightVout = tx.vout?.map(out => ({
      value: out.value,
      n: out.n,
      scriptPubKey: {
        type: out.scriptPubKey?.type || 'unknown',
        addresses: out.scriptPubKey?.addresses,
        hex: '',
        asm: '',
      },
    })) || [];

    const lightweightTx: Transaction = {
      txid: tx.txid,
      hash: tx.hash || tx.txid,
      version: tx.version,
      vin: [],
      vout: lightweightVout,
      size: tx.size || 0,
      vsize: tx.vsize || 0,
      locktime: tx.locktime || 0,
    };

    this.rawTransactionCache.set(tx.txid, lightweightTx);
    this.rawTransactionCacheQueue.push(tx.txid);

    if (this.rawTransactionCacheQueue.length > this.RAW_TX_CACHE_LIMIT) {
      const oldest = this.rawTransactionCacheQueue.shift();
      if (oldest) {
        this.rawTransactionCache.delete(oldest);
      }
    }
  }

  /**
   * Update FluxNode producer statistics
   */
  private async updateProducerStats(block: Block): Promise<void> {
    if (!block.producer || block.height === undefined) return;

    const reward = block.producerReward ? BigInt(Math.floor(block.producerReward * 1e8)) : BigInt(0);

    // Get existing producer stats
    const existing = await this.ch.queryOne<{
      blocks_produced: number;
      first_block: number;
      total_rewards: string;
    }>(`
      SELECT blocks_produced, first_block, total_rewards
      FROM producers FINAL
      WHERE fluxnode = {fluxnode:String}
    `, { fluxnode: block.producer });

    const update: ProducerUpdate = {
      fluxnode: block.producer,
      blocksProduced: (existing?.blocks_produced || 0) + 1,
      firstBlock: existing?.first_block || block.height,
      lastBlock: block.height,
      totalRewards: BigInt(existing?.total_rewards || 0) + reward,
      avgBlockTime: 0,
    };

    await bulkUpdateProducers(this.ch, [update]);
  }

  /**
   * Get current sync state
   */
  async getSyncState(): Promise<{
    currentHeight: number;
    chainHeight: number;
    lastBlockHash: string | null;
    isSyncing: boolean;
  }> {
    // Use argMax to get the row with the latest updated_at
    // This is more reliable than FINAL which may return stale data from unmerged parts
    const result = await this.ch.queryOne<{
      current_height: number;
      chain_height: number;
      last_block_hash: string;
      is_syncing: number;
    }>(`
      SELECT
        argMax(current_height, updated_at) as current_height,
        argMax(chain_height, updated_at) as chain_height,
        argMax(last_block_hash, updated_at) as last_block_hash,
        argMax(is_syncing, updated_at) as is_syncing
      FROM sync_state
      WHERE id = 1
    `);

    return {
      currentHeight: result?.current_height ?? 0,
      chainHeight: result?.chain_height ?? 0,
      // Don't strip leading zeros - they are significant for block hashes
      lastBlockHash: result?.last_block_hash || null,
      isSyncing: (result?.is_syncing ?? 0) === 1,
    };
  }

  /**
   * Set syncing status
   * Uses INSERT ... SELECT with argMax to preserve currentHeight from unmerged parts
   * This prevents race conditions where getSyncState() returns stale data
   */
  async setSyncingStatus(isSyncing: boolean, chainHeight?: number): Promise<void> {
    const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
    const isSyncingInt = isSyncing ? 1 : 0;

    // Use a single atomic INSERT ... SELECT to avoid reading stale data
    // argMax gets the latest value even from unmerged parts
    if (chainHeight !== undefined) {
      await this.ch.command(`
        INSERT INTO sync_state (id, current_height, chain_height, sync_percentage, last_block_hash, last_sync_time, is_syncing, blocks_per_second)
        SELECT
          1,
          argMax(current_height, updated_at),
          ${chainHeight},
          argMax(current_height, updated_at) * 100.0 / ${chainHeight},
          argMax(last_block_hash, updated_at),
          '${now}',
          ${isSyncingInt},
          0
        FROM sync_state WHERE id = 1
      `);
    } else {
      await this.ch.command(`
        INSERT INTO sync_state (id, current_height, chain_height, sync_percentage, last_block_hash, last_sync_time, is_syncing, blocks_per_second)
        SELECT
          1,
          argMax(current_height, updated_at),
          argMax(chain_height, updated_at),
          argMax(sync_percentage, updated_at),
          argMax(last_block_hash, updated_at),
          '${now}',
          ${isSyncingInt},
          0
        FROM sync_state WHERE id = 1
      `);
    }
  }
}
