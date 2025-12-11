/**
 * Block Indexer
 *
 * Indexes blocks, transactions, and UTXOs from Flux blockchain
 */

import { PoolClient } from 'pg';
import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { DatabaseConnection } from '../database/connection';
import { Block, Transaction, SyncError } from '../types';
import { logger } from '../utils/logger';
import { isReconstructableScriptType } from '../utils/script-utils';
import { determineFluxNodeTier } from '../parsers/fluxnode-parser';
import { extractFluxNodeTransaction, extractTransactionFromBlock, scanBlockTransactions } from '../parsers/block-parser';
import {
  bulkInsertBlocks,
  bulkInsertTransactions,
  bulkInsertFluxnodeTransactions,
  bulkInsertUtxos,
  bulkSpendUtxos,
  bulkInsertAddressTransactions,
  bulkCreateAddresses,
  sanitizeUtf8,
  sanitizeHex,
} from '../database/bulk-loader';

// Detailed memory profiling to find leak source
let blockMemBaseline = 0;
let blockCount = 0;
let totalTxProcessed = 0;
let totalUtxosCreated = 0;

// Track object counts to identify what's accumulating
interface MemoryProfile {
  cacheSize: number;
  cacheQueueSize: number;
}

function logDetailedMem(label: string, profile: MemoryProfile): void {
  blockCount++;
  // Only log every 100 blocks to reduce spam
  if (blockCount % 100 !== 0) return;

  const mem = process.memoryUsage();
  const heapMB = Math.round(mem.heapUsed / 1024 / 1024);
  const rssMB = Math.round(mem.rss / 1024 / 1024);
  const extMB = Math.round(mem.external / 1024 / 1024);
  const arrBufMB = Math.round(mem.arrayBuffers / 1024 / 1024);

  if (blockMemBaseline === 0) blockMemBaseline = heapMB;
  const delta = heapMB - blockMemBaseline;
  const mbPerBlock = blockCount > 0 ? (delta / blockCount).toFixed(3) : '0';

  logger.info('Memory details', {
    label,
    heapMB,
    rssMB,
    extMB,
    arrBufMB,
    deltaHeapMB: delta,
    mbPerBlock,
    cacheSize: profile.cacheSize,
    cacheQueueSize: profile.cacheQueueSize,
    blocks: blockCount,
    txs: totalTxProcessed,
  });
}

export class BlockIndexer {
  constructor(
    private rpc: FluxRPCClient,
    private db: DatabaseConnection
  ) {}

  // Lightweight cache for performance - memory leak is elsewhere
  private readonly RAW_TX_CACHE_LIMIT = 100;
  private rawTransactionCache = new Map<string, Transaction>();
  private rawTransactionCacheQueue: string[] = [];

  // PostgreSQL bigint maximum value
  private readonly MAX_BIGINT = BigInt('9223372036854775807');

  /**
   * Safely convert BigInt to string with overflow protection
   * Clamps values that exceed PostgreSQL bigint limits
   */
  private safeBigIntToString(value: bigint, context?: string): string {
    if (value > this.MAX_BIGINT) {
      logger.error('BigInt value exceeds PostgreSQL max, clamping to max', {
        value: value.toString(),
        maxValue: this.MAX_BIGINT.toString(),
        context: context || 'unknown',
      });
      return this.MAX_BIGINT.toString();
    }
    if (value < BigInt(0)) {
      logger.warn('Negative BigInt value detected, clamping to 0', {
        value: value.toString(),
        context: context || 'unknown',
      });
      return '0';
    }
    return value.toString();
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
   * Index multiple blocks in a single transaction using COPY for bulk loading
   * This is significantly faster than processing blocks individually
   */
  async indexBlocksBatch(blocks: Block[], startHeight: number): Promise<number> {
    if (blocks.length === 0) return 0;

    const startTime = Date.now();

    // Import parsers for raw block parsing
    const { extractTransactionFromBlock, parseTransactionShieldedData } = await import('../parsers/block-parser');
    const { parseFluxNodeTransaction } = await import('../parsers/fluxnode-parser');

    // Fetch raw block hex for ALL blocks to extract transaction hex
    // getblock verbosity 2 does NOT include hex field or vjoinsplit data
    // MEMORY FIX: Use let instead of const so we can null out AFTER DB writes to help GC
    let blockRawHexMap: Map<string, string> | null = new Map<string, string>(); // block hash -> raw hex

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
    const BATCH_SIZE = 10;
    const MAX_RETRIES = 5;
    const RETRY_BASE_DELAY = 1000; // 1 second base delay

    const fetchRawHexWithRetry = async (b: { hash: string; height: number }): Promise<{ hash: string; height: number; rawHex: string | null }> => {
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
          const rawHex = await this.rpc.getBlock(b.hash, 0) as unknown as string;
          return { hash: b.hash, height: b.height, rawHex };
        } catch (error) {
          const delay = RETRY_BASE_DELAY * Math.pow(2, attempt - 1); // Exponential backoff: 1s, 2s, 4s, 8s, 16s
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
            logger.error('Failed to fetch raw block hex after all retries - SUPPLY DATA WILL BE INACCURATE', {
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

    // Maps for parsed data - use let for GC cleanup after DB writes
    // txid -> hex string
    let txHexMap: Map<string, string> | null = new Map<string, string>();
    // txid -> shielded data { vjoinsplit, valueBalance }
    let parsedShieldedData: Map<string, { vjoinsplit?: Array<{ vpub_old: bigint; vpub_new: bigint }>; valueBalance?: bigint }> | null = new Map();
    // txid -> FluxNode data { type, collateralHash, collateralIndex, ip, publicKey, signature, tier }
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

    // Parse all transactions from raw blocks using single-pass scan (OPTIMIZED)
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

      // OPTIMIZED: Scan block ONCE to extract all transactions (instead of O(n*m) per-tx lookups)
      try {
        const scannedTxs = scanBlockTransactions(rawHex, heightCheck);

        // Build a quick lookup map from scanned results
        const scannedTxMap = new Map<string, { hex: string; version: number; fluxNodeType?: number }>();
        for (const scanned of scannedTxs) {
          scannedTxMap.set(scanned.txid, {
            hex: scanned.hex,
            version: scanned.version,
            fluxNodeType: scanned.fluxNodeType
          });
        }

        // Verify we got all expected transactions before processing
        const missingTxids: string[] = [];
        for (const tx of transactions) {
          if (!tx || !tx.txid) continue;
          if (!scannedTxMap.has(tx.txid)) {
            missingTxids.push(tx.txid);
          }
        }

        // If any transactions are missing from scan, fall back to per-tx extraction for ALL
        if (missingTxids.length > 0) {
          logger.warn('Block scan missing transactions, using fallback extraction', {
            height: heightCheck,
            missing: missingTxids.length,
            total: transactions.length,
            missingTxids: missingTxids.slice(0, 5) // Log first 5 missing
          });
          throw new Error(`Scan missing ${missingTxids.length} transactions`);
        }

        // Process each transaction using the pre-built map
        for (const tx of transactions) {
          if (!tx || !tx.txid) continue;

          const scanned = scannedTxMap.get(tx.txid)!; // Safe - we verified above
          txHexMap!.set(tx.txid, scanned.hex);

          // Parse shielded data for v2/v4 transactions
          if (tx.version === 2 || tx.version === 4) {
            const shieldedData = parseTransactionShieldedData(scanned.hex);
            if (shieldedData.vjoinsplit || shieldedData.valueBalance !== undefined) {
              parsedShieldedData!.set(tx.txid, shieldedData);
            }
          }

          // Parse FluxNode data for v3/v5/v6 transactions (already parsed during scan)
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
              logger.debug('Failed to parse FluxNode transaction', { txid: tx.txid, height: heightCheck, error: (fnError as Error).message });
            }
          }
        }
      } catch (error) {
        logger.warn('Using per-tx extraction fallback', {
          height: heightCheck,
          error: (error as Error).message
        });
        // Fallback to old per-tx method - slower but reliable
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
                  try {
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
                  } catch (fnError) {
                    logger.debug('Failed to parse FluxNode transaction', { txid: tx.txid, height: heightCheck, error: (fnError as Error).message });
                  }
                }
              }
            } else {
              // CRITICAL: If we can't extract hex for a transaction, log error
              logger.error('CRITICAL: Failed to extract transaction hex - supply may be inaccurate', {
                height: heightCheck,
                txid: tx.txid,
                version: tx.version
              });
            }
          } catch (err) {
            logger.error('CRITICAL: Exception extracting tx hex - supply may be inaccurate', {
              height: heightCheck,
              txid: tx.txid,
              error: (err as Error).message
            });
          }
        }
      }

      heightCheck++;
    }

    logger.debug(`Extracted hex for ${txHexMap!.size} transactions, ${parsedShieldedData!.size} shielded, ${parsedFluxNodeData!.size} FluxNode`)

    return await this.db.transaction(async (client) => {
      // Collect all data from all blocks
      const blockRecords: Array<{
        height: number;
        hash: string;
        prevHash: string | null;
        merkleRoot: string | null;
        timestamp: number;
        bits: string | null;
        nonce: string | null;
        version: number | null;
        size: number | null;
        txCount: number;
        producer: string | null;
        producerReward: string | null;
        difficulty: number | null;
        chainwork: string | null;
      }> = [];
      // OPTIMIZED: Removed blockHash - JOIN from blocks table when needed
      const txRecords: Array<{
        txid: string;
        blockHeight: number;
        timestamp: number;
        version: number;
        locktime: number;
        size: number;
        vsize: number;
        inputCount: number;
        outputCount: number;
        inputTotal: string;
        outputTotal: string;
        fee: string;
        isCoinbase: boolean;
        hex: string | null;
        isFluxnodeTx: boolean;
        fluxnodeType: number | null;
      }> = [];

      // FluxNode transaction records for fluxnode_transactions table
      // OPTIMIZED: Removed blockHash - JOIN from blocks table when needed
      const fluxnodeRecords: Array<{
        txid: string;
        blockHeight: number;
        blockTime: Date;
        version: number;
        type: number;
        collateralHash: string | null;
        collateralIndex: number | null;
        ipAddress: string | null;
        publicKey: string | null;
        signature: string | null;
        p2shAddress: string | null;
        benchmarkTier: string | null;
        extraData: string | null;
      }> = [];
      const utxoRecords: Array<{
        txid: string;
        vout: number;
        address: string;
        value: string;
        scriptPubkey: string;
        scriptType: string;
        blockHeight: number;
      }> = [];
      const spendRecords: Array<{
        txid: string;
        vout: number;
        spentTxid: string;
        spentBlockHeight: number;
      }> = [];

      // Address transactions records (for address_transactions table)
      // Key: `${address}:${txid}:${blockHeight}` -> { received, sent }
      // OPTIMIZED: Removed blockHash - JOIN from blocks table when needed
      const addressTxMap = new Map<string, {
        address: string;
        txid: string;
        blockHeight: number;
        timestamp: number;
        received: bigint;
        sent: bigint;
      }>();

      // Transaction participants map (for transaction_participants table)
      // Key: txid -> { inputs: Set<address>, outputs: Set<address> }
      const txParticipantsMap = new Map<string, {
        inputs: Set<string>;
        outputs: Set<string>;
      }>();

      // Map of outputs created in this batch (for same-batch UTXO lookups)
      const batchUtxoMap = new Map<string, { value: bigint; address: string }>();

      // Collect all input references that need UTXO lookups
      // OPTIMIZED: Removed spentBlockHash - not needed for address_transactions
      const inputRefs: Array<{
        key: string;
        txid: string;
        vout: number;
        spentByTxid: string;
        spentBlockHeight: number;
        spentTimestamp: number;
      }> = [];

      // Supply tracking per block (height -> { coinbase, shieldedChange })
      const supplyChanges: Array<{ height: number; coinbaseReward: bigint; shieldedChange: bigint }> = [];

      // Phase timing for performance analysis
      const phaseTimings: Record<string, number> = {};
      let phaseStart = Date.now();

      // First pass: collect all data and create batch UTXO map
      let currentHeight = startHeight;
      for (const block of blocks) {
        if (!block) {
          currentHeight++;
          continue;
        }

        // Get transactions - they should be full Transaction objects from verbose getblock
        const transactions = block.tx as Transaction[];
        if (!transactions || transactions.length === 0 || typeof transactions[0] === 'string') {
          logger.warn('Block has no verbose transactions, skipping batch processing', { height: currentHeight });
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
          producerReward: block.producerReward ? BigInt(Math.round(block.producerReward * 1e8)).toString() : null,
          difficulty: block.difficulty ?? null,
          chainwork: block.chainwork || null,
        });

        // Track supply changes for this block
        let blockCoinbaseReward = BigInt(0);
        let blockShieldedChange = BigInt(0);

        for (const tx of transactions) {
          // Skip completely malformed transactions (no txid)
          if (!tx || !tx.txid) {
            logger.warn('Skipping malformed transaction without txid in batch', {
              height: currentHeight,
            });
            continue;
          }

          // Handle shielded transactions: vin/vout may be undefined, null, or empty
          // Flux is a Zcash fork - shielded transactions use vShieldedSpend/vShieldedOutput instead
          const vin = tx.vin || [];
          const vout = tx.vout || [];

          const isCoinbase = vin.length > 0 && !!vin[0].coinbase;

          // Collect outputs - add to batch map and utxo records
          let outputTotal = BigInt(0);

          // Initialize transaction participants for this tx
          if (!txParticipantsMap.has(tx.txid)) {
            txParticipantsMap.set(tx.txid, { inputs: new Set(), outputs: new Set() });
          }
          const txParticipants = txParticipantsMap.get(tx.txid)!;

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
            batchUtxoMap.set(key, { value: valueSats, address });

            utxoRecords.push({
              txid: tx.txid,
              vout: voutIdx,
              address,
              value: valueSats.toString(),
              scriptPubkey: scriptPubkeyToStore,
              scriptType,
              blockHeight: currentHeight,
            });

            // Track output address for transaction_participants (skip non-standard)
            if (address !== 'SHIELDED_OR_NONSTANDARD') {
              txParticipants.outputs.add(address);

              // Track address transaction (received)
              const addrTxKey = `${address}:${tx.txid}:${currentHeight}`;
              const existing = addressTxMap.get(addrTxKey);
              if (existing) {
                existing.received += valueSats;
              } else {
                addressTxMap.set(addrTxKey, {
                  address,
                  txid: tx.txid,
                  blockHeight: currentHeight,
                  timestamp: block.time,
                  received: valueSats,
                  sent: BigInt(0),
                });
              }
            }
          }

          // Collect inputs
          let inputTotal = BigInt(0);
          for (const input of vin) {
            if (input.coinbase || !input.txid) continue;

            const key = `${input.txid}:${input.vout}`;

            // Check batch map first (for UTXOs created in earlier blocks of this batch)
            const batchUtxo = batchUtxoMap.get(key);
            if (batchUtxo) {
              inputTotal += batchUtxo.value;

              // Track input address for transaction_participants (skip non-standard)
              if (batchUtxo.address !== 'SHIELDED_OR_NONSTANDARD') {
                txParticipants.inputs.add(batchUtxo.address);

                // Track address transaction (sent)
                const addrTxKey = `${batchUtxo.address}:${tx.txid}:${currentHeight}`;
                const existing = addressTxMap.get(addrTxKey);
                if (existing) {
                  existing.sent += batchUtxo.value;
                } else {
                  addressTxMap.set(addrTxKey, {
                    address: batchUtxo.address,
                    txid: tx.txid,
                    blockHeight: currentHeight,
                    timestamp: block.time,
                    received: BigInt(0),
                    sent: batchUtxo.value,
                  });
                }
              }
            } else {
              // Need to look up from database - store additional info for later processing
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

          // We'll calculate final inputTotal after UTXO lookups
          // For now, store a placeholder

          // Check if this is a FluxNode transaction
          const fluxNodeData = parsedFluxNodeData!.get(tx.txid);
          const isFluxnodeTx = !!fluxNodeData;
          const fluxnodeType = fluxNodeData?.type ?? null;

          // Get hex from txHexMap (extracted from raw block) - used for size calculation only
          const txHexForSize = txHexMap!.get(tx.txid) || null;

          // Calculate size from hex if RPC didn't provide it (common for FluxNode txs)
          const computedSize = tx.size || (txHexForSize ? Math.floor(txHexForSize.length / 2) : 0);
          const computedVSize = tx.vsize || computedSize;

          txRecords.push({
            txid: tx.txid,
            blockHeight: currentHeight,
            timestamp: block.time,
            version: tx.version,
            locktime: tx.locktime || 0,
            size: computedSize,
            vsize: computedVSize,
            inputCount: vin.length,
            outputCount: vout.length,
            inputTotal: '0', // Will be updated after UTXO lookups
            outputTotal: outputTotal.toString(),
            fee: '0', // Will be calculated after UTXO lookups
            isCoinbase,
            hex: null, // Don't store hex during sync - fetched on-demand via API (saves ~30GB)
            isFluxnodeTx,
            fluxnodeType,
          });

          // Add to fluxnodeRecords if this is a FluxNode transaction
          if (isFluxnodeTx && txHexForSize) {
            // Build extra_data JSON to match normal sync
            const extraData = JSON.stringify({
              sigTime: (fluxNodeData as any)?.sigTime ?? null,
              benchmarkSigTime: (fluxNodeData as any)?.benchmarkSigTime ?? null,
              updateType: (fluxNodeData as any)?.updateType ?? null,
              nFluxNodeTxVersion: (fluxNodeData as any)?.nFluxNodeTxVersion ?? null,
            });

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
              extraData,
            });
          }

          // Track supply changes
          if (isCoinbase) {
            blockCoinbaseReward = outputTotal;
          }

          // Extract shielded pool changes from pre-parsed JoinSplit data
          // RPC getblock verbosity 2 does NOT include vjoinsplit data, so we use parsed raw hex
          const version = tx.version || 1;

          if (version === 2 || version === 4) {
            const shieldedData = parsedShieldedData!.get(tx.txid);
            if (shieldedData) {
              // Maximum reasonable value for sanity check (1 billion FLUX in satoshis)
              const MAX_REASONABLE_VALUE = BigInt(1_000_000_000) * BigInt(100_000_000);
              let hasInsaneValue = false;

              // Process JoinSplits (Sprout) - values are already in satoshis from parser
              if (shieldedData.vjoinsplit && shieldedData.vjoinsplit.length > 0) {
                for (const js of shieldedData.vjoinsplit) {
                  // vpub_old: value entering shielded pool (from transparent)
                  // vpub_new: value exiting shielded pool (to transparent)
                  const absVpubOld = js.vpub_old < BigInt(0) ? -js.vpub_old : js.vpub_old;
                  const absVpubNew = js.vpub_new < BigInt(0) ? -js.vpub_new : js.vpub_new;
                  if (absVpubOld > MAX_REASONABLE_VALUE || absVpubNew > MAX_REASONABLE_VALUE) {
                    hasInsaneValue = true;
                    logger.warn('Insane JoinSplit value detected, skipping', { txid: tx.txid, height: currentHeight });
                    break;
                  }
                  blockShieldedChange += js.vpub_old - js.vpub_new;
                }
              }

              // Process Sapling valueBalance (V4 only) - already in satoshis from parser
              if (!hasInsaneValue && version === 4 && shieldedData.valueBalance !== undefined) {
                const absValueBalance = shieldedData.valueBalance < BigInt(0) ? -shieldedData.valueBalance : shieldedData.valueBalance;
                if (absValueBalance > MAX_REASONABLE_VALUE) {
                  hasInsaneValue = true;
                  logger.warn('Insane valueBalance detected, skipping', { txid: tx.txid, height: currentHeight });
                } else {
                  // Positive valueBalance = value leaving shielded pool (pool decreases)
                  // Negative valueBalance = value entering shielded pool (pool increases)
                  // So we SUBTRACT valueBalance from change
                  blockShieldedChange -= shieldedData.valueBalance;
                }
              }
            }
          }
        }

        // Record supply changes for this block
        supplyChanges.push({
          height: currentHeight,
          coinbaseReward: blockCoinbaseReward,
          shieldedChange: blockShieldedChange,
        });

        currentHeight++;
      }

      phaseTimings.firstPass = Date.now() - phaseStart;
      phaseStart = Date.now();

      // Batch lookup UTXOs from database for inputs not in this batch
      const utxoLookupMap = new Map<string, { value: bigint; address: string }>();
      if (inputRefs.length > 0) {
        // Deduplicate
        const uniqueRefs = [...new Map(inputRefs.map(r => [r.key, r])).values()];

        const chunkSize = 1000;
        for (let i = 0; i < uniqueRefs.length; i += chunkSize) {
          const chunk = uniqueRefs.slice(i, i + chunkSize);
          if (chunk.length === 0) continue;

          const params: Array<string | number> = [];
          const placeholders = chunk.map((ref, idx) => {
            params.push(ref.txid, ref.vout);
            const base = idx * 2;
            return `($${base + 1}, $${base + 2})`;
          }).join(', ');

          const result = await client.query(
            `SELECT txid, vout, value, address FROM utxos WHERE (txid, vout) IN (${placeholders})`,
            params
          );

          for (const row of result.rows) {
            utxoLookupMap.set(`${row.txid}:${row.vout}`, {
              value: BigInt(row.value),
              address: row.address,
            });
          }
        }
      }

      phaseTimings.utxoLookup = Date.now() - phaseStart;
      phaseStart = Date.now();

      // Now process inputRefs to update addressTxMap and txParticipantsMap with looked-up addresses
      for (const ref of inputRefs) {
        const utxo = utxoLookupMap.get(ref.key);
        if (utxo && utxo.address !== 'SHIELDED_OR_NONSTANDARD') {
          // Add to transaction participants
          const participants = txParticipantsMap.get(ref.spentByTxid);
          if (participants) {
            participants.inputs.add(utxo.address);
          }

          // Add to address transactions (sent)
          const addrTxKey = `${utxo.address}:${ref.spentByTxid}:${ref.spentBlockHeight}`;
          const existing = addressTxMap.get(addrTxKey);
          if (existing) {
            existing.sent += utxo.value;
          } else {
            addressTxMap.set(addrTxKey, {
              address: utxo.address,
              txid: ref.spentByTxid,
              blockHeight: ref.spentBlockHeight,
              timestamp: ref.spentTimestamp,
              received: BigInt(0),
              sent: utxo.value,
            });
          }
        }
      }

      // Update transaction inputTotal and fee based on UTXO lookups
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

        for (const tx of transactions) {
          // Skip transactions without txid - must match first loop's skip logic
          if (!tx || !tx.txid) {
            continue;
          }
          const txRecord = txRecords[txIdx];
          // Defensive check in case txRecords is out of sync
          if (!txRecord) {
            logger.warn('txRecord undefined at index - skipping', { txIdx, txRecordsLength: txRecords.length, txid: tx.txid });
            txIdx++;
            continue;
          }

          // Handle shielded transactions: vin/vout may be undefined, null, or empty
          const vin = tx.vin || [];
          const isCoinbase = vin.length > 0 && !!vin[0].coinbase;

          if (!isCoinbase) {
            let inputTotal = BigInt(0);
            for (const input of vin) {
              if (input.coinbase || !input.txid) continue;
              const key = `${input.txid}:${input.vout}`;
              const utxo = batchUtxoMap.get(key) || utxoLookupMap.get(key);
              if (utxo) {
                inputTotal += utxo.value;
              }
            }
            txRecord.inputTotal = inputTotal.toString();
            const outputTotal = BigInt(txRecord.outputTotal);
            txRecord.fee = (inputTotal - outputTotal).toString();
          }

          txIdx++;
        }

        // Clear block.tx array NOW - we've extracted all needed data
        // This is the BIGGEST memory saver - tx arrays can be huge
        (block as any).tx = null;

        currentHeight++;
      }

      phaseTimings.secondPass = Date.now() - phaseStart;
      phaseStart = Date.now();

      // Now do bulk inserts using COPY - with timing to identify bottlenecks
      const timings: Record<string, number> = {};
      let t0 = Date.now();

      // PHASE 3: Collect all unique addresses and get/create address IDs
      // Include SHIELDED_OR_NONSTANDARD to satisfy FK constraint
      const allAddresses = new Set<string>();
      for (const u of utxoRecords) {
        if (u.address) {
          allAddresses.add(u.address);
        }
      }
      for (const r of addressTxMap.values()) {
        if (r.address) {
          allAddresses.add(r.address);
        }
      }

      // Create/get address IDs in bulk (much faster than individual lookups)
      const addressIdMap = await bulkCreateAddresses(client, Array.from(allAddresses), startHeight);
      timings.addresses = Date.now() - t0; t0 = Date.now();

      await bulkInsertBlocks(client, blockRecords);
      timings.blocks = Date.now() - t0; t0 = Date.now();

      await bulkInsertTransactions(client, txRecords);
      timings.txs = Date.now() - t0; t0 = Date.now();

      await bulkInsertFluxnodeTransactions(client, fluxnodeRecords);
      timings.fluxnode = Date.now() - t0; t0 = Date.now();

      // PHASE 3: Transform utxoRecords to use addressId instead of address
      const utxoRecordsWithIds = utxoRecords.map(u => ({
        txid: u.txid,
        vout: u.vout,
        addressId: addressIdMap.get(u.address) || 0,  // 0 for SHIELDED_OR_NONSTANDARD
        value: u.value,
        scriptPubkey: u.scriptPubkey,
        scriptType: u.scriptType,
        blockHeight: u.blockHeight
      }));
      await bulkInsertUtxos(client, utxoRecordsWithIds);
      timings.utxos = Date.now() - t0; t0 = Date.now();

      await bulkSpendUtxos(client, spendRecords);
      timings.spends = Date.now() - t0; t0 = Date.now();

      // Bulk insert address_transactions (cache table for address history)
      // PHASE 3: Transform to use addressId instead of address
      if (addressTxMap.size > 0) {
        const addressTxRecords = Array.from(addressTxMap.values()).map(r => ({
          addressId: addressIdMap.get(r.address) || 0,
          txid: r.txid,
          blockHeight: r.blockHeight,
          timestamp: r.timestamp,
          received: r.received.toString(),
          sent: r.sent.toString()
        }));
        await bulkInsertAddressTransactions(client, addressTxRecords);
        timings.addrTx = Date.now() - t0; t0 = Date.now();
      }

      // Bulk update address_summary (incremental updates during sync)
      if (addressTxMap.size > 0) {
        // Collect unique addresses and their aggregated changes
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

        // Build VALUES clause for bulk upsert
        const addressValues: string[] = [];
        for (const [address, changes] of addressChanges) {
          const balance = changes.received - changes.sent;
          addressValues.push(`('${address}', ${balance.toString()}, ${changes.txCount}, ${changes.received.toString()}, ${changes.sent.toString()}, 0, ${changes.minHeight}, ${changes.maxHeight}, NOW())`);
        }

        if (addressValues.length > 0) {
          await client.query(`
            INSERT INTO address_summary (address, balance, tx_count, received_total, sent_total, unspent_count, first_seen, last_activity, updated_at)
            VALUES ${addressValues.join(', ')}
            ON CONFLICT (address) DO UPDATE SET
              balance = address_summary.balance + EXCLUDED.balance,
              tx_count = address_summary.tx_count + EXCLUDED.tx_count,
              received_total = address_summary.received_total + EXCLUDED.received_total,
              sent_total = address_summary.sent_total + EXCLUDED.sent_total,
              first_seen = LEAST(address_summary.first_seen, EXCLUDED.first_seen),
              last_activity = GREATEST(address_summary.last_activity, EXCLUDED.last_activity),
              updated_at = NOW()
          `);
        }
        timings.addrSummary = Date.now() - t0; t0 = Date.now();
      }

      // Bulk update supply stats
      if (supplyChanges.length > 0) {
        // Get previous supply state
        const prevStats = await client.query(
          'SELECT transparent_supply, shielded_pool FROM supply_stats ORDER BY block_height DESC LIMIT 1'
        );
        let transparentSupply = BigInt(prevStats.rows[0]?.transparent_supply || 0);
        let shieldedPool = BigInt(prevStats.rows[0]?.shielded_pool || 0);

        // Build bulk insert values
        const supplyValues: string[] = [];
        for (const change of supplyChanges) {
          transparentSupply += change.coinbaseReward - change.shieldedChange;
          shieldedPool += change.shieldedChange;
          const totalSupply = transparentSupply + shieldedPool;
          supplyValues.push(`(${change.height}, ${this.safeBigIntToString(transparentSupply)}, ${this.safeBigIntToString(shieldedPool)}, ${this.safeBigIntToString(totalSupply)}, NOW())`);
        }

        // Bulk insert supply stats
        if (supplyValues.length > 0) {
          await client.query(`
            INSERT INTO supply_stats (block_height, transparent_supply, shielded_pool, total_supply, updated_at)
            VALUES ${supplyValues.join(', ')}
            ON CONFLICT (block_height) DO UPDATE SET
              transparent_supply = EXCLUDED.transparent_supply,
              shielded_pool = EXCLUDED.shielded_pool,
              total_supply = EXCLUDED.total_supply,
              updated_at = NOW()
          `);
        }
        timings.supply = Date.now() - t0; t0 = Date.now();
      }

      // Track totals for memory profiling
      totalTxProcessed += txRecords.length;
      totalUtxosCreated += utxoRecords.length;

      // Update sync state to last block
      const lastBlock = blocks[blocks.length - 1];
      if (lastBlock) {
        await this.updateSyncState(client, startHeight + blocks.length - 1, lastBlock.hash);
      }

      // Timing data available for debugging if needed (LOG_LEVEL=debug)
      const totalDbTime = Object.values(timings).reduce((a, b) => a + b, 0);
      logger.debug(`Timing breakdown: parse=${phaseTimings.firstPass}ms utxoLookup=${phaseTimings.utxoLookup}ms process=${phaseTimings.secondPass}ms dbWrites=${totalDbTime}ms`);

      const elapsed = Date.now() - startTime;
      const blocksProcessed = blocks.length;
      logger.debug(`Batch indexed ${blocksProcessed} blocks in ${elapsed}ms (${(blocksProcessed / (elapsed / 1000)).toFixed(1)} blocks/sec)`);

      // CRITICAL: Clear all large data structures to help GC reclaim memory
      blockRecords.length = 0;
      txRecords.length = 0;
      fluxnodeRecords.length = 0;
      utxoRecords.length = 0;
      spendRecords.length = 0;
      inputRefs.length = 0;
      supplyChanges.length = 0;
      batchUtxoMap.clear();
      utxoLookupMap.clear();
      addressTxMap.clear();
      txParticipantsMap.clear();

      // CRITICAL: Clear and null out the large Maps from outer scope that hold raw hex data
      // These were causing the memory leak - blockRawHexMap and txHexMap hold large strings
      // Nulling them allows V8 to fully release the memory
      if (blockRawHexMap) { blockRawHexMap.clear(); blockRawHexMap = null; }
      if (txHexMap) { txHexMap.clear(); txHexMap = null; }
      if (parsedShieldedData) { parsedShieldedData.clear(); parsedShieldedData = null; }
      if (parsedFluxNodeData) { parsedFluxNodeData.clear(); parsedFluxNodeData = null; }

      // Clear input blocks array to break closure reference chain
      // This allows GC to reclaim the Block objects even if this closure survives
      for (let i = 0; i < blocksProcessed; i++) {
        blocks[i] = null as any;
      }
      blocks.length = 0;

      // NOTE: GC is now handled in sync-engine.ts every 500 blocks
      // Doing GC here inside the transaction was blocking for ~6 seconds per batch

      return blocksProcessed;
    });
  }
  private async processBlock(block: Block, expectedHeight?: number): Promise<void> {
    const blockHeight = block.height ?? expectedHeight;
    if (blockHeight === undefined) {
      throw new SyncError('Block height is undefined', { blockHash: block.hash });
    }

    block.height = blockHeight;

    const transactions = await this.normalizeBlockTransactions(block);

    await this.db.transaction(async (client) => {
      await this.insertBlock(client, block);

      if (transactions.length > 0) {
        await this.indexTransactionsBatch(client, block, transactions);
      }

      // Track shielded pool and supply statistics
      await this.updateSupplyStats(client, blockHeight, transactions);

      if (block.producer) {
        await this.updateProducerStats(client, block);
      }

      await this.updateSyncState(client, blockHeight, block.hash);
    });

    // Explicitly clear references to help GC reclaim memory
    // The transactions have been written to DB, we no longer need them in memory
    for (let i = 0; i < transactions.length; i++) {
      transactions[i] = null as any;
    }
    transactions.length = 0;

    // Clear block.tx array to release transaction references
    if (Array.isArray(block.tx)) {
      block.tx.length = 0;
    }

    // Memory profiling - log after block processing complete
    logDetailedMem(`block-${blockHeight}-cleared`, {
      cacheSize: this.rawTransactionCache.size,
      cacheQueueSize: this.rawTransactionCacheQueue.length,
    });
  }

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

      uniqueTxids.forEach((txid, idx) => {
        const fetchedTx = fetched[idx];
        if (typeof fetchedTx === 'string') {
          logger.warn('Received raw transaction hex when expecting verbose JSON', { txid });
          return;
        }
        this.cacheRawTransaction(fetchedTx);
      });

      for (const { txid, index } of missing) {
        const raw = this.rawTransactionCache.get(txid);
        if (raw) {
          normalized[index] = raw;
        } else {
          logger.warn('Unable to resolve transaction data during normalization', { txid, block: block.hash });
        }
      }
    }

    return normalized.map((entry, idx) => {
      if (!entry) {
        throw new SyncError('Failed to normalize transaction', { blockHash: block.hash, index: idx });
      }
      return entry;
    });
  }

  private cacheRawTransaction(tx: Transaction): void {
    if (!tx?.txid) {
      return;
    }

    if (this.rawTransactionCache.has(tx.txid)) {
      return;
    }

    // Create a MINIMAL cache entry to prevent memory bloat
    // The cache is ONLY used for UTXO value lookups (vout.value and vout.scriptPubKey.addresses)
    // Strip ALL unnecessary data including scriptPubKey.hex/asm which can be very large
    // Database writes use the original transaction objects, NOT from this cache
    const lightweightVout = tx.vout?.map(out => ({
      value: out.value,
      n: out.n,
      scriptPubKey: {
        type: out.scriptPubKey?.type || 'unknown',
        addresses: out.scriptPubKey?.addresses,
        // Stripped: hex, asm, reqSigs - large and not needed for UTXO lookups
        hex: '',
        asm: '',
      },
    })) || [];

    const lightweightTx: Transaction = {
      txid: tx.txid,
      hash: tx.hash || tx.txid,
      version: tx.version,
      vin: [], // Empty - vin not needed for UTXO value lookups
      vout: lightweightVout,
      size: tx.size || 0,
      vsize: tx.vsize || 0,
      locktime: tx.locktime || 0,
      // Explicitly omit: hex, blockhash, confirmations, time, blocktime
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

  private async populateInputValuesFromRawTransactions(
    client: PoolClient,
    missingByTx: Map<string, number[]>,
    utxoInfoMap: Map<string, { value: bigint; address: string | null }>
  ): Promise<void> {
    const txids = Array.from(missingByTx.keys());
    const uncachedTxids = txids.filter((txid) => !this.rawTransactionCache.has(txid));
    const blockHashMap = new Map<string, string>();

    if (uncachedTxids.length > 0) {
      try {
        // OPTIMIZED: JOIN to blocks table to get hash since block_hash column was removed
        const blockRows = await client.query(
          `SELECT t.txid, b.hash as block_hash
           FROM transactions t
           JOIN blocks b ON t.block_height = b.height
           WHERE t.txid = ANY($1)`,
          [uncachedTxids]
        );

        for (const row of blockRows.rows) {
          if (row.block_hash) {
            blockHashMap.set(row.txid, row.block_hash);
          }
        }
      } catch (error: any) {
        logger.warn('Failed to resolve block hashes for previous transactions', {
          txids: uncachedTxids,
          error: error.message,
        });
      }
    }

    // Fetch uncached transactions for input value resolution
    // Store in a temporary map instead of the main cache to avoid memory bloat
    // from large consolidation transactions (1000+ inputs)
    const tempTxMap = new Map<string, Transaction>();

    if (uncachedTxids.length > 0) {
      try {
        const batchHashes = uncachedTxids.map((txid) => blockHashMap.get(txid));
        const fetched = await this.rpc.batchGetRawTransactions(uncachedTxids, true, batchHashes);
        uncachedTxids.forEach((txid, index) => {
          const result = fetched[index];
          if (typeof result === 'string') {
            logger.warn('Received raw transaction hex when expecting verbose JSON', { txid });
            return;
          }
          // Store in temp map, NOT the main cache - these are historical lookups
          // that rarely need to be accessed again
          tempTxMap.set(txid, result);
        });
      } catch (error: any) {
        logger.warn('Failed to fetch raw transactions during input hydration', { txids: uncachedTxids, error: error.message });
      }
    }

    for (const [txid, vouts] of missingByTx.entries()) {
      // Check main cache first, then temp map from batch fetch
      let rawTx = this.rawTransactionCache.get(txid) || tempTxMap.get(txid);

      if (!rawTx) {
        // Last resort: individual RPC fetch (but don't cache to avoid bloat)
        const blockHash = blockHashMap.get(txid);
        try {
          rawTx = await this.rpc.getRawTransaction(txid, true, blockHash) as Transaction;
          // Don't cache - this is a one-time historical lookup
        } catch (error: any) {
          logger.warn('Failed to hydrate input transaction via verbose RPC', {
            txid,
            blockHash,
            error: error.message,
          });
        }
      }

      if (!rawTx) {
        logger.warn('Missing raw transaction data after batch fetch', { txid });
        continue;
      }

      for (const voutIndex of vouts) {
        const referencedOutput = rawTx?.vout?.[voutIndex];
        if (referencedOutput) {
          const value = this.toSatoshis(referencedOutput.value);
          if (value !== null) {
            const address = referencedOutput?.scriptPubKey?.addresses?.[0] || null;
            utxoInfoMap.set(`${txid}:${voutIndex}`, { value, address });
            continue;
          }
        }
        logger.warn('Referenced output missing when backfilling input value', {
          txid,
          vout: voutIndex,
        });
      }
    }
  }

  /**
   * Detect if transaction is fully shielded (no transparent inputs or outputs)
   */
  private isFullyShieldedTransaction(tx: Transaction): boolean {
    // Check for shielded components
    const hasShieldedComponents = !!(
      tx.vShieldedOutput?.length ||
      tx.vShieldedOutput2?.length ||
      tx.vShieldedSpend?.length ||
      tx.vShieldedSpend2?.length ||
      tx.vjoinsplit?.length
    );

    if (!hasShieldedComponents) {
      return false;
    }

    // Check if there are no transparent inputs (excluding coinbase)
    const hasTransparentInputs = tx.vin?.some(input =>
      input.txid && input.vout !== undefined && !input.coinbase
    );

    // Check if there are no transparent outputs
    const hasTransparentOutputs = tx.vout && tx.vout.length > 0;

    // Fully shielded = has shielded components but no transparent ins/outs
    return !hasTransparentInputs && !hasTransparentOutputs;
  }

  private toSatoshis(value: number | string | undefined): bigint | null {
    if (value === undefined || value === null) {
      return null;
    }

    let numericValue: number;

    if (typeof value === 'number') {
      numericValue = value;
    } else {
      numericValue = Number(value);
    }

    if (!Number.isFinite(numericValue)) {
      return null;
    }

    // Flux/Zcash can have -1 for shielded outputs
    // Clamp negative values to 0 to avoid bigint overflow
    const clampedValue = numericValue < 0 ? 0 : numericValue;

    return BigInt(Math.round(clampedValue * 1e8));
  }

  /**
   * Insert block into database
   */
  private async insertBlock(client: PoolClient, block: Block): Promise<void> {
    const query = `
      INSERT INTO blocks (
        height, hash, prev_hash, merkle_root, timestamp, bits, nonce,
        version, size, tx_count, producer, producer_reward, difficulty, chainwork
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      ON CONFLICT (height) DO UPDATE SET
        hash = EXCLUDED.hash,
        prev_hash = EXCLUDED.prev_hash,
        merkle_root = EXCLUDED.merkle_root,
        timestamp = EXCLUDED.timestamp,
        bits = EXCLUDED.bits,
        nonce = EXCLUDED.nonce,
        version = EXCLUDED.version,
        size = EXCLUDED.size,
        tx_count = EXCLUDED.tx_count,
        producer = EXCLUDED.producer,
        producer_reward = EXCLUDED.producer_reward,
        difficulty = EXCLUDED.difficulty,
        chainwork = EXCLUDED.chainwork
    `;

    const values = [
      block.height,
      block.hash,
      block.previousblockhash || null,
      block.merkleroot,
      block.time,
      block.bits ? String(block.bits) : null,  // Ensure TEXT
      block.nonce !== null && block.nonce !== undefined ? String(block.nonce) : null,  // Store nonce as TEXT
      block.version,
      block.size,
      Array.isArray(block.tx) ? block.tx.length : 0,
      block.producer || null,
      block.producerReward ? BigInt(Math.round(block.producerReward * 1e8)) : null,
      block.difficulty,
      block.chainwork ? String(block.chainwork) : null,  // Ensure TEXT
    ];

    await client.query(query, values);
  }

  /**
   * Index all transactions in a block using batch operations (OPTIMIZED)
   */
  private async indexTransactionsBatch(
    client: PoolClient,
    block: Block,
    transactions: Transaction[]
  ): Promise<void> {
    if (transactions.length === 0) return;

    if (block.height === undefined) {
      throw new SyncError('Block height missing during transaction batch indexing', { blockHash: block.hash });
    }

    type InputRef = { txid: string; vout: number };

    const txValues: any[][] = [];
    const utxosToCreate: any[][] = [];
    const utxosToSpend: any[][] = [];
    const utxoInfoMap = new Map<string, { value: bigint; address: string | null }>();
    const txAddressTotals = new Map<string, Map<string, { received: bigint; sent: bigint }>>();
    const txParticipants = new Map<string, { inputs: Set<string>; outputs: Set<string> }>();
    const txMap = new Map<string, Transaction>();
    const txidsMissingDetails: string[] = [];

    const txPreparations: Array<{
      tx: Transaction;
      isCoinbase: boolean;
      inputs: InputRef[];
      outputTotal: bigint;
    }> = [];

    const inputRefKeys = new Set<string>();
    let fluxnodeCount = 0;
    const fluxnodeTransactions: Transaction[] = [];

    // First pass: separate FluxNode transactions from regular transactions
    for (const tx of transactions) {
      // Handle FluxNode confirmations and other special transactions
      // FluxNode confirmations have empty vin/vout arrays (version 5, nType 4)
      // FluxNode start transactions are version 6
      if (!Array.isArray(tx.vin) || !Array.isArray(tx.vout)) {
        // Check if this is a FluxNode transaction (confirmations v3/v5, starts v6)
        const isFluxNodeTransaction = tx.version === 3 || tx.version === 5 || tx.version === 6;

        if (isFluxNodeTransaction) {
          fluxnodeTransactions.push(tx);
          fluxnodeCount++;
          continue;
        }

        // Unknown transaction type - log for investigation
        logger.warn('Transaction has invalid vin/vout structure, skipping', {
          txid: tx.txid,
          hasVin: !!tx.vin,
          hasVout: !!tx.vout,
          vinType: typeof tx.vin,
          voutType: typeof tx.vout,
          version: tx.version,
          size: tx.size,
          blockHeight: block.height,
        });
        continue;
      }

      const isCoinbase = tx.vin.length > 0 && !!tx.vin[0].coinbase;
      const inputs: InputRef[] = [];
      const participantEntry = { inputs: new Set<string>(), outputs: new Set<string>() };
      txParticipants.set(tx.txid, participantEntry);

      for (const input of tx.vin) {
        if (input.coinbase) continue;
        if (input.txid && input.vout !== undefined) {
          const ref: InputRef = { txid: input.txid, vout: input.vout };
          inputs.push(ref);
          inputRefKeys.add(`${ref.txid}:${ref.vout}`);
          utxosToSpend.push([ref.txid, ref.vout, tx.txid, block.height]);
        }
      }

      let outputTotal = tx.vout.reduce((sum, output) => {
        const sat = this.toSatoshis(output.value);
        return sat !== null ? sum + sat : sum;
      }, BigInt(0));

      // Clamp outputTotal to PostgreSQL bigint max
      const MAX_BIGINT = BigInt('9223372036854775807');
      if (outputTotal > MAX_BIGINT) {
        logger.error('Prep outputTotal exceeds PostgreSQL bigint max, clamping', {
          txid: tx.txid,
          outputTotal: outputTotal.toString(),
          outputTotalFlux: (Number(outputTotal) / 1e8).toFixed(2),
          clampedTo: MAX_BIGINT.toString(),
          outputCount: tx.vout.length,
        });
        outputTotal = MAX_BIGINT;
      }

      for (const output of tx.vout) {
        const address = output.scriptPubKey?.addresses?.[0];
        const value = this.toSatoshis(output.value);

        if (value !== null) {
          // For outputs without addresses (shielded/OP_RETURN), use a placeholder address
          const utxoAddress = address || 'SHIELDED_OR_NONSTANDARD';

          utxosToCreate.push([
            tx.txid,
            output.n,
            utxoAddress,
            this.safeBigIntToString(value, `utxo:${tx.txid}:${output.n}`),
            sanitizeHex(output.scriptPubKey.hex || ''),
            sanitizeUtf8(output.scriptPubKey.type || 'unknown'),
            block.height,
          ]);

          // Pre-populate utxoInfoMap with this output for intra-block reference resolution
          // This allows later transactions in the same block to resolve inputs from earlier txs
          utxoInfoMap.set(`${tx.txid}:${output.n}`, {
            value: value,
            address: address || null,
          });
        }
      }

      txMap.set(tx.txid, tx);
      if (
        !tx.hex ||
        tx.hex.length === 0 ||
        typeof tx.size !== 'number' ||
        tx.size <= 0 ||
        typeof tx.vsize !== 'number' ||
        tx.vsize <= 0
      ) {
        txidsMissingDetails.push(tx.txid);
      }

      txPreparations.push({
        tx,
        isCoinbase,
        inputs,
        outputTotal,
      });
    }

    const inputKeys = Array.from(inputRefKeys);

    if (txidsMissingDetails.length > 0) {
      const uniqueMissing = Array.from(new Set(txidsMissingDetails));

      // OPTIMIZATION: With txindex=0, fetching raw block hex once and extracting all transactions
      // is faster than individual or batch getrawtransaction calls (which fall back to scanning anyway)
      try {
        // Fetch raw block hex once for all missing transactions
        const rawBlockHex = await this.rpc.getBlock(block.hash, 0) as unknown as string;

        for (const missingTxid of uniqueMissing) {
          const target = txMap.get(missingTxid);
          if (!target) continue;

          const txHex = extractTransactionFromBlock(rawBlockHex, missingTxid, block.height);

          if (txHex && txHex.length > 0) {
            target.hex = txHex;
            const computedSize = Math.floor(txHex.length / 2);
            target.size = computedSize;
            target.vsize = computedSize;
          } else {
            logger.warn('Transaction not found in block hex', {
              txid: missingTxid,
              block: block.hash
            });
          }
        }
      } catch (blockError: any) {
        logger.error('Failed to fetch and extract transactions from block hex', {
          block: block.hash,
          error: blockError.message,
          missingCount: uniqueMissing.length
        });
        // Fatal error - can't proceed without transaction data
        throw blockError;
      }
    }

    // Filter out keys already resolved (same-block outputs are pre-populated in utxoInfoMap)
    const keysToLookup = inputKeys.filter(key => !utxoInfoMap.has(key));

    if (keysToLookup.length > 0) {
      const chunkSize = 500;
      for (let i = 0; i < keysToLookup.length; i += chunkSize) {
        const chunk = keysToLookup.slice(i, i + chunkSize);
        if (chunk.length === 0) continue;

        const params: Array<string | number> = [];
        const placeholders = chunk.map((key, idx) => {
          const [txid, voutStr] = key.split(':');
          params.push(txid, Number(voutStr));
          const base = idx * 2;
          return `($${base + 1}, $${base + 2})`;
        }).join(', ');

        if (!placeholders) continue;

        const result = await client.query(
          `SELECT txid, vout, value, address FROM utxos WHERE (txid, vout) IN (${placeholders})`,
          params
        );

        for (const row of result.rows) {
          const normalizedAddress = row.address && row.address !== 'SHIELDED_OR_NONSTANDARD'
            ? row.address
            : null;

          // Validate UTXO value before converting to BigInt to prevent overflow
          // Max valid value is PostgreSQL bigint max: 9,223,372,036,854,775,807 satoshis (~92B FLUX)
          const rawValue = row.value;
          const MAX_BIGINT = BigInt('9223372036854775807');
          let value: bigint;

          try {
            value = BigInt(rawValue);
            if (value < BigInt(0) || value > MAX_BIGINT) {
              logger.warn('Invalid UTXO value detected, clamping to 0', {
                txid: row.txid,
                vout: row.vout,
                value: rawValue.toString(),
              });
              value = BigInt(0);
            }
          } catch (e) {
            logger.warn('Failed to convert UTXO value to BigInt, using 0', {
              txid: row.txid,
              vout: row.vout,
              value: rawValue,
              error: e instanceof Error ? e.message : String(e),
            });
            value = BigInt(0);
          }

          utxoInfoMap.set(`${row.txid}:${row.vout}`, {
            value,
            address: normalizedAddress,
          });
        }
      }
    }

    // Fallback to RPC for any missing inputs (should be rare)
    const missingKeys = inputKeys.filter((key) => !utxoInfoMap.has(key));
    if (missingKeys.length > 0) {
      const missingByTx = new Map<string, number[]>();

      for (const key of missingKeys) {
        const [txid, voutStr] = key.split(':');
        const vout = Number(voutStr);
        const list = missingByTx.get(txid) || [];
        list.push(vout);
        missingByTx.set(txid, list);
      }

      await this.populateInputValuesFromRawTransactions(client, missingByTx, utxoInfoMap);
    }

    // Fetch raw block hex for parsing JoinSplits in v2/v4 transactions
    // This is necessary because getBlock with verbosity 2 doesn't include vjoinsplit data
    const needsRawBlockForFees = transactions.some(tx => tx.version === 2 || tx.version === 4);
    let rawBlockHexForFees: string | null = null;
    const parsedJoinSplitsMap = new Map<string, Array<{ vpub_old: bigint; vpub_new: bigint }>>();
    const parsedValueBalanceMap = new Map<string, bigint>();

    if (needsRawBlockForFees) {
      try {
        rawBlockHexForFees = await this.rpc.getBlock(block.hash, 0) as unknown as string;

        // Parse JoinSplits for all v2/v4 transactions
        const { parseTransactionShieldedData, extractTransactionFromBlock } = await import('../parsers/block-parser');

        for (const tx of transactions) {
          if (tx.version === 2 || tx.version === 4) {
            try {
              let txHex: string | undefined | null = tx.hex;

              if (!txHex && rawBlockHexForFees) {
                txHex = extractTransactionFromBlock(rawBlockHexForFees, tx.txid, block.height);
              }

              if (txHex) {
                const parsed = parseTransactionShieldedData(txHex);

                if (parsed.vjoinsplit && parsed.vjoinsplit.length > 0) {
                  parsedJoinSplitsMap.set(tx.txid, parsed.vjoinsplit);
                }

                if (parsed.valueBalance !== undefined) {
                  parsedValueBalanceMap.set(tx.txid, parsed.valueBalance);
                }
              }
            } catch (error) {
              logger.warn('Failed to parse JoinSplits for fee calculation', {
                txid: tx.txid,
                error: (error as Error).message,
              });
            }
          }
        }
      } catch (error) {
        logger.warn('Failed to fetch raw block hex for fee calculation', {
          blockHeight: block.height,
          blockHash: block.hash,
          error: (error as Error).message,
        });
      }
    }

    for (const prep of txPreparations) {
      // Skip UTXO and address processing for fully shielded transactions
      if (prep.tx.is_shielded) {
        // Still need to add transaction to txValues for database insertion
        const computedSize = prep.tx.size || 0;
        const computedVSize = prep.tx.vsize || computedSize;

        // OPTIMIZED: Removed block.hash - JOIN from blocks table when needed
        txValues.push([
          prep.tx.txid,
          block.height,
          block.time,
          prep.tx.version || 0,
          prep.tx.locktime || 0,
          computedSize,
          computedVSize,
          0,  // input_count
          0,  // output_count
          '0',  // input_total
          '0',  // output_total
          '0',  // fee
          false,  // is_coinbase
          null,  // hex
        ]);

        continue;  // Skip UTXO processing for fully shielded
      }

      let inputTotal = BigInt(0);
      const MAX_BIGINT = BigInt('9223372036854775807');
      const seenInputs = new Set<string>(); // Track duplicate inputs

      const txTotalsForTx =
        txAddressTotals.get(prep.tx.txid) || new Map<string, { received: bigint; sent: bigint }>();

      for (const ref of prep.inputs) {
        const key = `${ref.txid}:${ref.vout}`;

        // Detect duplicate inputs
        if (seenInputs.has(key)) {
          logger.error('DUPLICATE INPUT DETECTED', {
            txid: prep.tx.txid,
            duplicateInput: key,
            blockHeight: block.height,
          });
          continue; // Skip duplicate
        }
        seenInputs.add(key);

        const info = utxoInfoMap.get(key);
        if (info && info.value !== undefined) {
          const newInputTotal = inputTotal + info.value;

          // Check for inputTotal overflow
          if (newInputTotal > MAX_BIGINT || newInputTotal < inputTotal) {
            logger.error('Transaction inputTotal overflow detected', {
              txid: prep.tx.txid,
              currentTotal: inputTotal.toString(),
              addingValue: info.value.toString(),
              wouldBe: newInputTotal.toString(),
              blockHeight: block.height,
            });
            inputTotal = MAX_BIGINT; // Clamp to max
          } else {
            inputTotal = newInputTotal;
          }
          if (info.address) {
            const existing = txTotalsForTx.get(info.address) || { received: BigInt(0), sent: BigInt(0) };
            const newSent = existing.sent + info.value;

            // Check for overflow (if result is less than either operand, we overflowed)
            const MAX_BIGINT = BigInt('9223372036854775807');
            if (newSent > MAX_BIGINT || newSent < existing.sent) {
              logger.error('Address sent value overflow detected, clamping', {
                address: info.address,
                txid: prep.tx.txid,
                existingSent: existing.sent.toString(),
                addingValue: info.value.toString(),
                wouldBe: newSent.toString(),
                blockHeight: block.height,
              });
              existing.sent = MAX_BIGINT;  // Clamp to max
            } else {
              existing.sent = newSent;
            }

            txTotalsForTx.set(info.address, existing);
            txParticipants.get(prep.tx.txid)!.inputs.add(info.address);
          }
        } else {
          logger.warn('Missing input value during transaction indexing', {
            txid: prep.tx.txid,
            inputTxid: ref.txid,
            vout: ref.vout,
          });
        }
      }

      for (const output of prep.tx.vout) {
        const value = this.toSatoshis(output.value);
        if (value === null) continue;

        const address = output.scriptPubKey?.addresses?.[0] || null;
        if (address) {
          const existing = txTotalsForTx.get(address) || { received: BigInt(0), sent: BigInt(0) };
          const newReceived = existing.received + value;

          // Check for overflow
          const MAX_BIGINT = BigInt('9223372036854775807');
          if (newReceived > MAX_BIGINT || newReceived < existing.received) {
            logger.error('Address received value overflow detected, clamping', {
              address,
              txid: prep.tx.txid,
              existingReceived: existing.received.toString(),
              addingValue: value.toString(),
              wouldBe: newReceived.toString(),
              blockHeight: block.height,
            });
            existing.received = MAX_BIGINT;  // Clamp to max
          } else {
            existing.received = newReceived;
          }

          txTotalsForTx.set(address, existing);
          txParticipants.get(prep.tx.txid)!.outputs.add(address);
        }
      }

      if (txTotalsForTx.size > 0) {
        txAddressTotals.set(prep.tx.txid, txTotalsForTx);
      }

      // Don't store hex during sync - saves ~30GB storage
      // Hex is fetched on-demand via API and cached when viewed
      const rawHex = null;
      // Use tx.hex temporarily for size calculation, but don't store it
      const txHexForSize = prep.tx.hex || '';
      const computedSize = prep.tx.size ?? (txHexForSize ? Math.floor(txHexForSize.length / 2) : 0);
      const computedVSize = prep.tx.vsize ?? computedSize;

      // Calculate fee correctly for shielded transactions
      // For shielded transactions: fee = inputs - outputs + shieldedPoolChange
      // where shieldedPoolChange accounts for coins moving between transparent and shielded pools
      let shieldedPoolChange = BigInt(0);

      // V4 Sapling transactions with valueBalance - use parsed data
      const parsedValueBalance = parsedValueBalanceMap.get(prep.tx.txid);
      if (prep.tx.version === 4 && parsedValueBalance !== undefined) {
        // Parsed valueBalance is already in satoshis (bigint)
        // Positive valueBalance = value leaving shielded pool (entering transparent)
        // Negative valueBalance = value entering shielded pool (leaving transparent)
        // For fee calculation: we ADD valueBalance because it represents net flow OUT of shielded pool
        shieldedPoolChange = parsedValueBalance;
      } else if (prep.tx.version === 4 && prep.tx.valueBalance !== undefined) {
        // Fallback to RPC data if parsing failed (valueBalance is in FLUX, convert to satoshis)
        shieldedPoolChange = BigInt(Math.round(prep.tx.valueBalance * 1e8));
      }

      // V2/V4 transactions with JoinSplits (Sprout) - use parsed data
      const parsedJoinSplits = parsedJoinSplitsMap.get(prep.tx.txid);
      if (parsedJoinSplits && parsedJoinSplits.length > 0) {
        // Use parsed JoinSplits (already in satoshis as bigint)
        let joinSplitChange = BigInt(0);
        for (const joinSplit of parsedJoinSplits) {
          // vpub_old = value leaving shielded pool, vpub_new = value entering shielded pool
          joinSplitChange += joinSplit.vpub_old - joinSplit.vpub_new;
        }
        shieldedPoolChange += joinSplitChange;
      } else if ((prep.tx.version === 2 || prep.tx.version === 4) && prep.tx.vjoinsplit && Array.isArray(prep.tx.vjoinsplit)) {
        // Fallback to RPC data if parsing failed (values are in FLUX, convert to satoshis)
        let joinSplitChange = BigInt(0);
        for (const joinSplit of prep.tx.vjoinsplit) {
          const vpubOld = BigInt(Math.round((joinSplit.vpub_old || 0) * 1e8));
          const vpubNew = BigInt(Math.round((joinSplit.vpub_new || 0) * 1e8));
          joinSplitChange += vpubOld - vpubNew;
        }
        shieldedPoolChange += joinSplitChange;
      }

      // Correct fee formula: fee = inputs - outputs - shieldedPoolChange
      // We SUBTRACT shieldedPoolChange because:
      // - When shielding: vpub_old - vpub_new is negative, subtracting it adds to fee (cancels out the shielded amount)
      // - When deshielding: vpub_old - vpub_new is positive, subtracting it reduces fee (cancels out the deshielded amount)
      const fee = prep.isCoinbase ? BigInt(0) : inputTotal - prep.outputTotal - shieldedPoolChange;
      const safeFee = !prep.isCoinbase && fee < BigInt(0) ? BigInt(0) : fee;

      // OPTIMIZED: Removed block.hash - JOIN from blocks table when needed
      txValues.push([
        prep.tx.txid,
        block.height,
        block.time,
        prep.tx.version,
        prep.tx.locktime,
        computedSize,
        computedVSize,
        prep.tx.vin.length,
        prep.tx.vout.length,
        this.safeBigIntToString(inputTotal, `tx:${prep.tx.txid}:inputTotal`),
        this.safeBigIntToString(prep.outputTotal, `tx:${prep.tx.txid}:outputTotal`),
        this.safeBigIntToString(safeFee, `tx:${prep.tx.txid}:fee`),
        prep.isCoinbase,
        rawHex,
      ]);
    }

    // 1. Batch insert all transactions
    // PHASE 3: txid is BYTEA - use decode() for first parameter
    if (txValues.length > 0) {
      const placeholders = txValues.map((_, i) => {
        const offset = i * 14;  // OPTIMIZED: 14 columns (was 15, removed block_hash)
        // PHASE 3: decode() for txid (BYTEA)
        return `(decode($${offset + 1}, 'hex'), $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, $${offset + 14})`;
      }).join(', ');

      // OPTIMIZED: Removed block_hash column - JOIN from blocks table when needed
      const txQuery = `
        INSERT INTO transactions (
          txid, block_height, timestamp, version, locktime,
          size, vsize, input_count, output_count, input_total, output_total,
          fee, is_coinbase, hex
        ) VALUES ${placeholders}
        ON CONFLICT (block_height, txid) DO UPDATE SET
          timestamp = EXCLUDED.timestamp
      `;

      await client.query(txQuery, txValues.flat());

      // Also insert into tx_lookup for fast txid -> block_height lookups on compressed data
      // txid stored as BYTEA (decode from hex) to save ~50% space
      const lookupPlaceholders = txValues.map((_: any, i: number) => {
        const offset = i * 2;
        return `(decode($${offset + 1}, 'hex'), $${offset + 2})`;
      }).join(', ');

      const lookupValues = txValues.map((txVals: any[]) => [txVals[0], txVals[1]]); // txid, block_height
      await client.query(
        `INSERT INTO tx_lookup (txid, block_height) VALUES ${lookupPlaceholders} ON CONFLICT (txid) DO NOTHING`,
        lookupValues.flat()
      );
    }

    // 2. Prepare temp table for spent UTXOs (create and populate BEFORE creating new UTXOs)
    // This fixes same-block create-and-spend bug: UTXOs created and spent in the same block
    // must be created first, then marked as spent.
    // PHASE 3: Uses BYTEA for txid columns
    if (utxosToSpend.length > 0) {
      // Drop and recreate to ensure PRIMARY KEY is always present
      await client.query('DROP TABLE IF EXISTS temp_spent_utxos');
      await client.query('CREATE TEMP TABLE temp_spent_utxos (txid BYTEA, vout INT, spent_txid BYTEA, spent_block_height INT, PRIMARY KEY (txid, vout)) ON COMMIT DROP');

      // PostgreSQL has a parameter limit of 32767 (int16 max), and each spent UTXO has 4 fields
      // To be safe, chunk at 8000 UTXOs per query (8000 * 4 = 32000 parameters < 32767 limit)
      const spendChunkSize = 8000;

      for (let i = 0; i < utxosToSpend.length; i += spendChunkSize) {
        const chunk = utxosToSpend.slice(i, i + spendChunkSize);
        const flatValues = chunk.flat();

        // PHASE 3: Use decode() for BYTEA txid columns
        const spendPlaceholders = chunk.map((_, idx) => {
          const offset = idx * 4;
          return `(decode($${offset + 1}, 'hex'), $${offset + 2}, decode($${offset + 3}, 'hex'), $${offset + 4})`;
        }).join(', ');

        await client.query(`INSERT INTO temp_spent_utxos (txid, vout, spent_txid, spent_block_height) VALUES ${spendPlaceholders} ON CONFLICT (txid, vout) DO UPDATE SET spent_txid = EXCLUDED.spent_txid, spent_block_height = EXCLUDED.spent_block_height`, flatValues);
      }
    }

    // 3. Batch insert new UTXOs BEFORE marking as spent (fixes same-block create-and-spend bug)
    // PHASE 3: Get address IDs for all addresses in this batch
    if (utxosToCreate.length > 0) {
      // Extract unique addresses from UTXOs to create
      const uniqueAddresses = [...new Set(utxosToCreate.map(utxo => utxo[2] as string))];

      // Bulk create addresses and get ID map
      const addressIdMap = await bulkCreateAddresses(client, uniqueAddresses, block.height);

      // PostgreSQL has a parameter limit of 32767 (int16 max), and each UTXO has 7 fields
      // To be safe, chunk at 4500 UTXOs per query (4500 * 7 = 31500 parameters < 32767 limit)
      const utxoChunkSize = 4500;

      for (let i = 0; i < utxosToCreate.length; i += utxoChunkSize) {
        const chunk = utxosToCreate.slice(i, i + utxoChunkSize);

        // PHASE 3: Build values with address_id instead of address, txid as hex string
        const chunkValues: any[] = [];
        for (const utxo of chunk) {
          const addressId = addressIdMap.get(utxo[2] as string);
          if (!addressId) {
            throw new SyncError(`Address ID not found for address: ${utxo[2]}`, { blockHeight: block.height });
          }
          // [txid, vout, address_id, value, script_pubkey, script_type, block_height]
          chunkValues.push([
            utxo[0],      // txid (hex string - will be decoded in query)
            utxo[1],      // vout
            addressId,    // address_id (INTEGER)
            utxo[3],      // value
            utxo[4],      // script_pubkey
            utxo[5],      // script_type
            utxo[6],      // block_height
          ]);
        }

        // PHASE 3: Use decode() for BYTEA txid, address_id instead of address
        const utxoPlaceholders = chunkValues.map((_, idx) => {
          const offset = idx * 7;
          return `(decode($${offset + 1}, 'hex'), $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7})`;
        }).join(', ');

        // Insert into utxos_unspent (split table for unspent UTXOs)
        // PHASE 3: address_id (INTEGER) instead of address (TEXT)
        const utxoQuery = `
          INSERT INTO utxos_unspent (
            txid, vout, address_id, value, script_pubkey, script_type, block_height
          ) VALUES ${utxoPlaceholders}
          ON CONFLICT (txid, vout) DO UPDATE SET
            address_id = EXCLUDED.address_id,
            value = EXCLUDED.value,
            script_pubkey = EXCLUDED.script_pubkey,
            script_type = EXCLUDED.script_type,
            block_height = EXCLUDED.block_height
        `;

        await client.query(utxoQuery, chunkValues.flat());
      }
    }

    // 4. NOW mark UTXOs as spent (after they've been created in step 3)
    // Move from utxos_unspent to utxos_spent
    // PHASE 3: Uses address_id and BYTEA txid (both tables already have matching types)
    if (utxosToSpend.length > 0) {
      // Insert into utxos_spent from utxos_unspent + temp spend info
      // PHASE 3: Uses address_id instead of address
      await client.query(`
        INSERT INTO utxos_spent (
          txid, vout, address_id, value, script_pubkey, script_type, block_height,
          spent_txid, spent_block_height, created_at, spent_at
        )
        SELECT
          u.txid, u.vout, u.address_id, u.value, u.script_pubkey, u.script_type, u.block_height,
          temp.spent_txid, temp.spent_block_height, u.created_at, NOW()
        FROM utxos_unspent u
        INNER JOIN temp_spent_utxos temp ON u.txid = temp.txid AND u.vout = temp.vout
        ON CONFLICT (spent_block_height, txid, vout) DO NOTHING
      `);

      // Delete from utxos_unspent
      await client.query(`
        DELETE FROM utxos_unspent u
        USING temp_spent_utxos temp
        WHERE u.txid = temp.txid AND u.vout = temp.vout
      `);
    }

    // 5. Refresh address transaction cache with correct net flows
    await this.updateAddressTransactionsCache(client, block, txAddressTotals);

    // 6. Batch process FluxNode transactions (if any)
    if (fluxnodeTransactions.length > 0) {
      await this.indexFluxNodeTransactionsBatch(client, block, fluxnodeTransactions);

      logger.info('Indexed block with FluxNode transactions', {
        height: block.height,
        totalTxs: transactions.length,
        fluxnodeTxs: fluxnodeCount
      });
    }

    // Track totals for memory profiling
    totalTxProcessed += transactions.length;
    totalUtxosCreated += utxosToCreate.length;

    // 7. Explicitly clear all intermediate structures to help GC
    // This prevents memory buildup from large transactions/blocks
    utxoInfoMap.clear();
    txAddressTotals.clear();
    txParticipants.clear();
    txMap.clear();
    parsedJoinSplitsMap.clear();
    parsedValueBalanceMap.clear();
    txValues.length = 0;
    utxosToCreate.length = 0;
    utxosToSpend.length = 0;
    txPreparations.length = 0;
    fluxnodeTransactions.length = 0;
  }

  /**
   * Batch index FluxNode transactions (OPTIMIZED)
   * This method processes multiple FluxNode transactions efficiently by:
   * 1. Batching RPC calls for verbose transaction data
   * 2. Fetching raw block hex only once (not per transaction)
   * 3. Batching collateral UTXO lookups
   * 4. Batching database inserts
   */
  private async indexFluxNodeTransactionsBatch(
    client: PoolClient,
    block: Block,
    fluxnodeTxs: Transaction[]
  ): Promise<void> {
    if (fluxnodeTxs.length === 0) return;

    const { parseFluxNodeTransaction } = await import('../parsers/fluxnode-parser');

    // OPTIMIZATION: With txindex=0, fetching raw block hex and parsing is faster than batch RPC
    // Skip batch RPC entirely and go straight to hex-based parsing

    // Step 1: Get raw block hex once for all FluxNode transactions
    let rawBlockHex: string | null = null;

    try {
      rawBlockHex = await this.rpc.getBlock(block.hash, 0) as unknown as string;
    } catch (error: any) {
      logger.error('Failed to fetch raw block hex for FluxNode extraction', {
        block: block.hash,
        height: block.height,
        error: error.message
      });
      // Fatal error - can't parse FluxNode transactions without block hex
      throw error;
    }

    // Step 2: Extract and parse all FluxNode transactions from block hex
    for (const tx of fluxnodeTxs) {
      const txAny = tx as any;

      // Extract hex from block if missing
      if (!tx.hex) {
        tx.hex = extractFluxNodeTransaction(rawBlockHex, tx.txid, block.height) || undefined;

        if (!tx.hex) {
          logger.warn('Failed to extract FluxNode transaction from block hex', {
            txid: tx.txid,
            blockHeight: block.height,
            blockHash: block.hash
          });
          continue;
        }
      }

      // Parse FluxNode fields from hex
      if (txAny.nType === undefined) {
        const parsedData = parseFluxNodeTransaction(tx.hex);
        if (parsedData) {
          txAny.nType = parsedData.type;
          txAny.collateralOutputHash = parsedData.collateralHash;
          txAny.collateralOutputIndex = parsedData.collateralIndex;
          txAny.ip = parsedData.ipAddress;
          txAny.zelnodePubKey = parsedData.publicKey;
          txAny.sig = parsedData.signature;
          txAny.benchmarkTier = parsedData.benchmarkTier;
        } else {
          logger.warn('Failed to parse FluxNode data from hex', {
            txid: tx.txid,
            blockHeight: block.height
          });
        }
      }
    }

    // Step 3: Batch lookup collateral UTXOs for tier determination
    const collateralRefs: Array<{ txid: string; vout: number; fluxnodeTxid: string }> = [];

    for (const tx of fluxnodeTxs) {
      const txAny = tx as any;
      if (!txAny.benchmarkTier && txAny.collateralOutputHash && txAny.collateralOutputIndex !== undefined) {
        collateralRefs.push({
          txid: txAny.collateralOutputHash,
          vout: txAny.collateralOutputIndex,
          fluxnodeTxid: tx.txid
        });
      }
    }

    const tierMap = new Map<string, string>();

    if (collateralRefs.length > 0) {
      // Batch query for all collateral UTXOs
      const params: Array<string | number> = [];
      const placeholders = collateralRefs.map((ref, idx) => {
        params.push(ref.txid, ref.vout);
        const base = idx * 2;
        return `($${base + 1}, $${base + 2})`;
      }).join(', ');

      const utxoResult = await client.query(
        `SELECT txid, vout, value FROM utxos WHERE (txid, vout) IN (${placeholders})`,
        params
      );

      // Create map of collateral -> tier
      for (const row of utxoResult.rows) {
        const collateralAmount = BigInt(row.value);
        const tier = determineFluxNodeTier(collateralAmount);
        tierMap.set(`${row.txid}:${row.vout}`, tier);
      }
    }

    // Step 4: Prepare batch insert data
    const fluxnodeTxValues: any[][] = [];
    const regularTxValues: any[][] = [];

    for (const tx of fluxnodeTxs) {
      const txAny = tx as any;
      // Use hex for size calculation but don't store it (saves ~30GB)
      const txHexForSize = tx.hex || '';

      // Determine tier from map or existing value
      let benchmarkTier: string | null = txAny.benchmarkTier || null;
      if (!benchmarkTier && txAny.collateralOutputHash && txAny.collateralOutputIndex !== undefined) {
        benchmarkTier = tierMap.get(`${txAny.collateralOutputHash}:${txAny.collateralOutputIndex}`) || null;
      }

      // FluxNode transactions table
      // OPTIMIZED: Removed block.hash - JOIN from blocks table when needed
      fluxnodeTxValues.push([
        tx.txid,
        block.height,
        new Date(block.time * 1000),
        tx.version,
        txAny.nType ?? null,
        txAny.collateralOutputHash || null,
        txAny.collateralOutputIndex ?? null,
        txAny.ip || null,
        txAny.zelnodePubKey || txAny.fluxnodePubKey || null,
        txAny.sig || null,
        txAny.redeemScript || null,
        benchmarkTier,
        JSON.stringify({
          sigTime: txAny.sigTime,
          benchmarkSigTime: txAny.benchmarkSigTime,
          updateType: txAny.updateType,
          nFluxNodeTxVersion: txAny.nFluxNodeTxVersion
        }),
      ]);

      // Regular transactions table - store null for hex (fetched on-demand via API)
      // OPTIMIZED: Removed block.hash - JOIN from blocks table when needed
      regularTxValues.push([
        tx.txid,
        block.height,
        block.time,
        tx.version,
        tx.locktime || 0,
        tx.size || (txHexForSize ? txHexForSize.length / 2 : 0),
        tx.vsize || (txHexForSize ? txHexForSize.length / 2 : 0),
        0, // input_count
        0, // output_count
        '0', // input_total
        '0', // output_total
        '0', // fee
        false, // is_coinbase
        null, // hex - not stored during sync, fetched on-demand via API
        true, // is_fluxnode_tx
        txAny.nType ?? null,
      ]);
    }

    // Step 5: Batch insert into fluxnode_transactions table
    // OPTIMIZED: Removed block_hash column - JOIN from blocks table when needed
    // PHASE 3: txid is BYTEA - use decode() for first parameter
    if (fluxnodeTxValues.length > 0) {
      const placeholders = fluxnodeTxValues.map((_, i) => {
        const offset = i * 13;  // OPTIMIZED: 13 columns (was 14, removed block_hash)
        // PHASE 3: decode() for txid (BYTEA)
        return `(decode($${offset + 1}, 'hex'), $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13})`;
      }).join(', ');

      await client.query(
        `INSERT INTO fluxnode_transactions (
          txid, block_height, block_time, version, type,
          collateral_hash, collateral_index, ip_address, public_key, signature,
          p2sh_address, benchmark_tier, extra_data
        ) VALUES ${placeholders}
        ON CONFLICT (block_height, txid) DO UPDATE SET
          block_time = EXCLUDED.block_time,
          version = EXCLUDED.version,
          type = EXCLUDED.type,
          collateral_hash = EXCLUDED.collateral_hash,
          collateral_index = EXCLUDED.collateral_index,
          ip_address = EXCLUDED.ip_address,
          public_key = EXCLUDED.public_key,
          signature = EXCLUDED.signature,
          p2sh_address = EXCLUDED.p2sh_address,
          benchmark_tier = EXCLUDED.benchmark_tier,
          extra_data = EXCLUDED.extra_data`,
        fluxnodeTxValues.flat()
      );
    }

    // Step 6: Batch insert into transactions table
    // OPTIMIZED: Removed block_hash column - JOIN from blocks table when needed
    // PHASE 3: txid is BYTEA - use decode() for first parameter
    if (regularTxValues.length > 0) {
      const placeholders = regularTxValues.map((_, i) => {
        const offset = i * 16;  // OPTIMIZED: 16 columns (was 17, removed block_hash)
        // PHASE 3: decode() for txid (BYTEA)
        return `(decode($${offset + 1}, 'hex'), $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, $${offset + 14}, $${offset + 15}, $${offset + 16})`;
      }).join(', ');

      await client.query(
        `INSERT INTO transactions (
          txid, block_height, timestamp, version, locktime,
          size, vsize, input_count, output_count, input_total, output_total,
          fee, is_coinbase, hex, is_fluxnode_tx, fluxnode_type
        ) VALUES ${placeholders}
        ON CONFLICT (block_height, txid) DO UPDATE SET
          is_fluxnode_tx = TRUE,
          fluxnode_type = EXCLUDED.fluxnode_type,
          hex = EXCLUDED.hex`,
        regularTxValues.flat()
      );

      // Also insert into tx_lookup for fast txid -> block_height lookups on compressed data
      // txid stored as BYTEA (decode from hex) to save ~50% space
      const lookupPlaceholders = regularTxValues.map((_: any, i: number) => {
        const offset = i * 2;
        return `(decode($${offset + 1}, 'hex'), $${offset + 2})`;
      }).join(', ');

      const lookupValues = regularTxValues.map((txVals: any[]) => [txVals[0], txVals[1]]); // txid, block_height
      await client.query(
        `INSERT INTO tx_lookup (txid, block_height) VALUES ${lookupPlaceholders} ON CONFLICT (txid) DO NOTHING`,
        lookupValues.flat()
      );
    }

    logger.debug('Batch indexed FluxNode transactions', {
      count: fluxnodeTxs.length,
      blockHeight: block.height
    });
  }

  /**
   * Refresh the address_transactions cache for the provided txids using net values.
   */
  private async updateAddressTransactionsCache(
    client: PoolClient,
    block: Block,
    addressTotals: Map<string, Map<string, { received: bigint; sent: bigint }>>
  ): Promise<void> {
    if (addressTotals.size === 0) return;

    const entries: Array<{
      txid: string;
      address: string;
      received: bigint;
      sent: bigint;
    }> = [];

    for (const [txid, totalsMap] of addressTotals.entries()) {
      for (const [address, totals] of totalsMap.entries()) {
        if (totals.received === BigInt(0) && totals.sent === BigInt(0)) continue;
        entries.push({ txid, address, received: totals.received, sent: totals.sent });
      }
    }

    if (entries.length === 0) return;

    // PHASE 3: Get address IDs for all addresses in this batch
    const uniqueAddresses = [...new Set(entries.map(e => e.address))];
    const addressIdMap = await bulkCreateAddresses(client, uniqueAddresses, block.height);

    // PostgreSQL has a parameter limit of 32767 (int16 max), and each entry has 7 fields
    // To be safe, chunk at 4000 entries per query (4000 * 7 = 28000 parameters < 32767 limit)
    const chunkSize = 4000;

    for (let i = 0; i < entries.length; i += chunkSize) {
      const chunk = entries.slice(i, i + chunkSize);

      const params: Array<string | number> = [];
      const MAX_BIGINT = BigInt('9223372036854775807');
      const WARN_THRESHOLD = MAX_BIGINT / BigInt(2); // Warn if over half of max

      // PHASE 3: 7 columns - address_id (INTEGER) instead of address (TEXT), txid as BYTEA
      const placeholders = chunk.map((entry, index) => {
        const base = index * 7;
        const direction = entry.received >= entry.sent ? 'received' : 'sent';

        // Get address_id for this entry
        const addressId = addressIdMap.get(entry.address);
        if (!addressId) {
          throw new SyncError(`Address ID not found for address: ${entry.address}`, { blockHeight: block.height });
        }

        // Clamp values to PostgreSQL bigint max before inserting
        let clampedReceived = entry.received;
        let clampedSent = entry.sent;

        // Log suspiciously large values (even if under max)
        if (entry.received > WARN_THRESHOLD || entry.sent > WARN_THRESHOLD) {
          logger.warn('Large address transaction value detected', {
            address: entry.address,
            txid: entry.txid,
            received: entry.received.toString(),
            sent: entry.sent.toString(),
            receivedFlux: (Number(entry.received) / 1e8).toFixed(2),
            sentFlux: (Number(entry.sent) / 1e8).toFixed(2),
            blockHeight: block.height,
          });
        }

        if (entry.received > MAX_BIGINT) {
          logger.error('Clamping address received value before database insert', {
            address: entry.address,
            txid: entry.txid,
            received: entry.received.toString(),
            clampedTo: MAX_BIGINT.toString(),
            blockHeight: block.height,
          });
          clampedReceived = MAX_BIGINT;
        }

        if (entry.sent > MAX_BIGINT) {
          logger.error('Clamping address sent value before database insert', {
            address: entry.address,
            txid: entry.txid,
            sent: entry.sent.toString(),
            clampedTo: MAX_BIGINT.toString(),
            blockHeight: block.height,
          });
          clampedSent = MAX_BIGINT;
        }

        // PHASE 3: address_id (INTEGER) instead of address (TEXT), txid will be decoded
        params.push(
          addressId,
          entry.txid,
          block.height,
          block.time,
          direction,
          clampedReceived.toString(),
          clampedSent.toString()
        );
        // PHASE 3: decode() for txid (BYTEA)
        return `($${base + 1}, decode($${base + 2}, 'hex'), $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7})`;
      }).join(', ');

      // PHASE 3: address_id (INTEGER) instead of address (TEXT), txid (BYTEA)
      const sql = `
        INSERT INTO address_transactions (
          address_id,
          txid,
          block_height,
          timestamp,
          direction,
          received_value,
          sent_value
        ) VALUES ${placeholders}
        ON CONFLICT (block_height, address_id, txid) DO UPDATE SET
          timestamp      = EXCLUDED.timestamp,
          direction      = EXCLUDED.direction,
          received_value = EXCLUDED.received_value,
          sent_value     = EXCLUDED.sent_value;
      `;

      await client.query(sql, params);
    }
  }

  /**
   * Update FluxNode producer statistics
   */
  private async updateProducerStats(client: PoolClient, block: Block): Promise<void> {
    if (!block.producer) return;

    const reward = block.producerReward ? BigInt(Math.floor(block.producerReward * 1e8)) : BigInt(0);

    const query = `
      INSERT INTO producers (
        fluxnode, blocks_produced, first_block, last_block, total_rewards, updated_at
      ) VALUES ($1, 1, $2, $2, $3, NOW())
      ON CONFLICT (fluxnode) DO UPDATE SET
        blocks_produced = producers.blocks_produced + 1,
        last_block = $2,
        total_rewards = producers.total_rewards + $3,
        updated_at = NOW()
    `;

    await client.query(query, [block.producer, block.height, reward.toString()]);
  }

  /**
   * Update sync state
   */
  private async updateSyncState(client: PoolClient, height: number, hash: string): Promise<void> {
    const query = `
      UPDATE sync_state
      SET current_height = $1,
          last_block_hash = $2,
          last_sync_time = NOW()
      WHERE id = 1
    `;

    await client.query(query, [height, hash]);
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
    const result = await this.db.query(`
      SELECT current_height, chain_height, last_block_hash, is_syncing
      FROM sync_state
      WHERE id = 1
    `);

    const row = result.rows[0];
    return {
      currentHeight: row?.current_height || 0,
      chainHeight: row?.chain_height || 0,
      lastBlockHash: row?.last_block_hash || null,
      isSyncing: row?.is_syncing || false,
    };
  }

  /**
   * Set syncing status
   */
  async setSyncingStatus(isSyncing: boolean, chainHeight?: number): Promise<void> {
    const query = chainHeight !== undefined
      ? 'UPDATE sync_state SET is_syncing = $1, chain_height = $2 WHERE id = 1'
      : 'UPDATE sync_state SET is_syncing = $1 WHERE id = 1';

    const params = chainHeight !== undefined ? [isSyncing, chainHeight] : [isSyncing];
    await this.db.query(query, params);
  }

  /**
   * Calculate and store shielded pool statistics for a block
   */
  private async updateSupplyStats(
    client: PoolClient,
    blockHeight: number,
    transactions: Transaction[]
  ): Promise<void> {
    let shieldedPoolChange = BigInt(0);

    // Fetch raw block hex if we have any v2 or v4 transactions that need parsing
    // This is necessary because getBlock with verbosity 2 doesn't include tx.hex
    const needsRawBlock = transactions.some(tx => tx.version === 2 || tx.version === 4);
    let rawBlockHex: string | null = null;

    if (needsRawBlock) {
      try {
        // Get block hash from first transaction's blockhash, or query from DB
        const blockHashQuery = await client.query('SELECT hash FROM blocks WHERE height = $1', [blockHeight]);
        const blockHash = blockHashQuery.rows[0]?.hash;

        if (blockHash) {
          rawBlockHex = await this.rpc.getBlock(blockHash, 0) as unknown as string;
        }
      } catch (error) {
        logger.warn('Failed to fetch raw block hex for shielded pool calculation', {
          blockHeight,
          error: (error as Error).message,
        });
      }
    }

    // Calculate shielded pool changes from this block's transactions
    for (const tx of transactions) {
      // Check if we need to parse shielded data from hex
      let parsedVpubs: Array<{ vpub_old: bigint; vpub_new: bigint }> | undefined;
      let parsedValueBalance: bigint | undefined;

      // Always parse from hex to maintain satoshi-level precision
      // RPC returns float values which lose precision when converted to/from satoshis
      if ((tx.version === 2 || tx.version === 4)) {
        try {
          const { parseTransactionShieldedData, extractTransactionFromBlock } = await import('../parsers/block-parser');

          // Get transaction hex - either from tx.hex or extract from raw block
          let txHex: string | undefined = tx.hex;
          if (!txHex && rawBlockHex) {
            const extracted = extractTransactionFromBlock(rawBlockHex, tx.txid, blockHeight);
            txHex = extracted || undefined;
          }

          if (txHex) {
            const shieldedData = parseTransactionShieldedData(txHex);
            parsedVpubs = shieldedData.vjoinsplit;
            parsedValueBalance = shieldedData.valueBalance;
          }
        } catch (error) {
          logger.warn('Failed to parse shielded data from transaction hex', {
            txid: tx.txid,
            blockHeight,
            error: (error as Error).message,
          });
        }
      }

      // V2 and V4 transactions with JoinSplits (Sprout shielded operations)
      // V4 transactions can contain BOTH JoinSplits (Sprout) AND valueBalance (Sapling)
      if (tx.version === 2 || tx.version === 4) {
        // CRITICAL: Prefer parsed hex data over RPC data
        // Parsed hex gives satoshi-level precision; RPC returns floats which lose precision
        // This must match the fast sync logic (indexBlocksBatch) for consistent supply calculation
        let joinsplits: any[] | undefined = parsedVpubs as any[] | undefined;
        let usingParsedData = joinsplits && joinsplits.length > 0;

        // Fall back to RPC data if parsing didn't provide JoinSplit data
        if ((!joinsplits || joinsplits.length === 0) && tx.vjoinsplit && tx.vjoinsplit.length > 0) {
          joinsplits = tx.vjoinsplit;
          usingParsedData = false;
        }

        if (joinsplits && Array.isArray(joinsplits)) {
          let txJoinSplitChange = BigInt(0);
          let hasInsaneValue = false;

          for (let jsIndex = 0; jsIndex < joinsplits.length; jsIndex++) {
            const joinSplit = joinsplits[jsIndex];
            // vpub_old: value entering shielded pool (from transparent)
            // vpub_new: value exiting shielded pool (to transparent)
            // Handle both bigint (from parser) and number (from RPC) types
            const vpubOld: bigint = typeof joinSplit.vpub_old === 'bigint'
              ? joinSplit.vpub_old
              : BigInt(Math.round((joinSplit.vpub_old || 0) * 1e8));
            const vpubNew: bigint = typeof joinSplit.vpub_new === 'bigint'
              ? joinSplit.vpub_new
              : BigInt(Math.round((joinSplit.vpub_new || 0) * 1e8));

            // SANITY CHECK: Prevent bogus parser values in JoinSplits
            // Maximum theoretical Flux supply is ~1 billion FLUX = 1e17 satoshis
            // Any vpub value larger than this is a parser error
            const MAX_REASONABLE_VALUE = BigInt(1_000_000_000) * BigInt(100_000_000); // 1B FLUX in satoshis
            const absVpubOld = vpubOld < BigInt(0) ? -vpubOld : vpubOld;
            const absVpubNew = vpubNew < BigInt(0) ? -vpubNew : vpubNew;

            if (absVpubOld > MAX_REASONABLE_VALUE || absVpubNew > MAX_REASONABLE_VALUE) {
              if (jsIndex === 0) {
                // Only log once per transaction to avoid spam
                logger.error('Hex parser produced insane JoinSplit values - discarding ALL parsed JoinSplits for this tx', {
                  txid: tx.txid,
                  blockHeight,
                  firstBadVpubOld: vpubOld.toString(),
                  firstBadVpubNew: vpubNew.toString(),
                  usingParsedData,
                  hasRPCData: tx.vjoinsplit !== undefined && tx.vjoinsplit.length > 0,
                  totalJoinSplits: joinsplits.length,
                });
              }
              hasInsaneValue = true;
              break; // Stop processing - all JoinSplits from this parse are suspect
            }

            txJoinSplitChange += vpubOld - vpubNew;
          }

          // Only apply JoinSplit changes if they passed sanity check
          if (!hasInsaneValue) {
            shieldedPoolChange += txJoinSplitChange;
          } else if (usingParsedData) {
            // Parser produced garbage - treat transaction as having no JoinSplits
            logger.warn('Ignoring parsed JoinSplits due to insane values - transaction treated as no JoinSplits', {
              txid: tx.txid,
              blockHeight,
              parsedJoinSplitCount: joinsplits.length,
            });
          }
        }
      }

      // V4 Sapling transactions with valueBalance - prefer parsed data (matches fast sync logic)
      if (tx.version === 4) {
        // CRITICAL: Prefer parsed hex data over RPC data for consistency with fast sync
        const vBalance = parsedValueBalance ?? tx.valueBalance;
        if (vBalance !== undefined) {
          // Standard Zcash/Flux Sapling semantics:
          // Positive valueBalance = value leaving shielded pool (pool decreases)
          // Negative valueBalance = value entering shielded pool (pool increases)
          // So we SUBTRACT valueBalance from pool change
          //
          // Handle both bigint (from parser) and number (from RPC) types
          const valueBalanceSatoshis = typeof vBalance === 'bigint'
            ? vBalance
            : BigInt(Math.round(vBalance * 1e8));

          // SANITY CHECK: Prevent bogus parser values from corrupting shielded pool
          // Maximum theoretical Flux supply is ~1 billion FLUX = 1e17 satoshis
          // Any valueBalance larger than this is a parser error
          const MAX_REASONABLE_VALUE = BigInt(1_000_000_000) * BigInt(100_000_000); // 1B FLUX in satoshis
          const absValue = valueBalanceSatoshis < BigInt(0) ? -valueBalanceSatoshis : valueBalanceSatoshis;

          if (absValue > MAX_REASONABLE_VALUE) {
            logger.error('FOUND IT! Insane valueBalance detected from parser, skipping', {
              txid: tx.txid,
              blockHeight,
              valueBalanceSatoshis: valueBalanceSatoshis.toString(),
              valueBalanceFlux: (Number(valueBalanceSatoshis) / 1e8).toFixed(2),
              parsedFromHex: parsedValueBalance !== undefined,
              fromRPC: tx.valueBalance !== undefined,
            });
            // Skip this insane value - don't apply it to shielded pool
            continue;
          }

          // SUBTRACT valueBalance (standard Zcash semantics)
          shieldedPoolChange -= valueBalanceSatoshis;
        }
      }
    }

    // Calculate coinbase reward from transactions (must match fast sync logic)
    let coinbaseReward = BigInt(0);
    for (const tx of transactions) {
      const isCoinbase = tx.vin && tx.vin.length > 0 && !!tx.vin[0].coinbase;
      if (isCoinbase && tx.vout) {
        for (const output of tx.vout) {
          const satoshis = this.toSatoshis(output.value);
          if (satoshis !== null) {
            coinbaseReward += satoshis;
          }
        }
        break; // Only one coinbase per block
      }
    }

    // Get previous supply state (must match fast sync query)
    const prevQuery = `
      SELECT transparent_supply, shielded_pool
      FROM supply_stats
      WHERE block_height < $1
      ORDER BY block_height DESC
      LIMIT 1
    `;
    const prevResult = await client.query(prevQuery, [blockHeight]);
    let transparentSupply = prevResult.rows[0]?.transparent_supply
      ? BigInt(prevResult.rows[0].transparent_supply)
      : BigInt(0);
    let shieldedPool = prevResult.rows[0]?.shielded_pool
      ? BigInt(prevResult.rows[0].shielded_pool)
      : BigInt(0);

    // Calculate new supply values (must match fast sync formulas exactly)
    // transparent_supply += coinbaseReward - shieldedPoolChange
    // shielded_pool += shieldedPoolChange
    transparentSupply += coinbaseReward - shieldedPoolChange;
    shieldedPool += shieldedPoolChange;
    const totalSupply = transparentSupply + shieldedPool;

    // Insert supply stats for this block (must match fast sync insert)
    const insertQuery = `
      INSERT INTO supply_stats (
        block_height, transparent_supply, shielded_pool, total_supply, updated_at
      ) VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (block_height) DO UPDATE SET
        transparent_supply = EXCLUDED.transparent_supply,
        shielded_pool = EXCLUDED.shielded_pool,
        total_supply = EXCLUDED.total_supply,
        updated_at = NOW()
    `;

    await client.query(insertQuery, [
      blockHeight,
      this.safeBigIntToString(transparentSupply, 'transparent_supply'),
      this.safeBigIntToString(shieldedPool, 'shielded_pool'),
      this.safeBigIntToString(totalSupply, 'total_supply'),
    ]);

    // Log significant shielded pool changes
    if (shieldedPoolChange !== BigInt(0)) {
      logger.debug('Shielded pool change detected', {
        blockHeight,
        change: shieldedPoolChange.toString(),
        newTotal: shieldedPool.toString(),
      });
    }
  }
}
