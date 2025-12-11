/**
 * Flux Blockchain API Client
 * Type-safe client for interacting with the Flux blockchain via FluxIndexer API.
 */

import { FluxIndexerAPI, FluxIndexerAPIError } from "./fluxindexer-client";
import type {
  Block,
  BlockSummary,
  Transaction,
  AddressInfo,
  NetworkStatus,
  SyncStatus,
  BlockchainStats,
  DashboardStats,
  AddressTransactionsPage,
} from "@/types/flux-api";

/**
 * Custom error class for Flux API errors
 */
export class FluxAPIError extends FluxIndexerAPIError {
  constructor(
    message: string,
    public statusCode?: number,
    public response?: unknown
  ) {
    super(message, statusCode, response);
    this.name = "FluxAPIError";
  }
}

/**
 * Flux API Client
 * Main interface for blockchain data access
 */
export class FluxAPI {
  /**
   * Fetch a block by hash or height
   * @param hashOrHeight - Block hash (string) or height (number)
   * @returns Block data
   */
  static async getBlock(hashOrHeight: string | number): Promise<Block> {
    return FluxIndexerAPI.getBlock(hashOrHeight);
  }

  /**
   * Fetch a block with full transaction details included
   * More efficient than fetching block + each transaction separately
   * @param hashOrHeight - Block hash (string) or height (number)
   * @returns Block and transactions
   */
  static async getBlockWithTransactions(hashOrHeight: string | number) {
    return FluxIndexerAPI.getBlockWithTransactions(hashOrHeight);
  }

  /**
   * Fetch raw block data
   * @param hashOrHeight - Block hash (string) or height (number)
   * @returns Raw block hex string
   */
  static async getRawBlock(_hashOrHeight: string | number): Promise<{ rawblock: string }> {
    // FluxIndexer doesn't support raw blocks, return empty
    return { rawblock: '' };
  }

  /**
   * Fetch block index/summary
   * @param height - Block height
   * @returns Block summary
   */
  static async getBlockIndex(height: number): Promise<BlockSummary> {
    return FluxIndexerAPI.getBlockIndex(height);
  }

  /**
   * Get block hash by height
   * @param height - Block height
   * @returns Block hash
   */
  static async getBlockHash(height: number): Promise<string> {
    return FluxIndexerAPI.getBlockHash(height);
  }

  /**
   * Fetch latest blocks
   * @param limit - Number of blocks to fetch (default: 10)
   * @returns Array of block summaries
   */
  static async getLatestBlocks(limit: number = 10): Promise<BlockSummary[]> {
    return FluxIndexerAPI.getLatestBlocks(limit);
  }

  /**
   * Fetch a transaction by ID
   * @param txid - Transaction ID
   * @returns Transaction data
   */
  static async getTransaction(txid: string): Promise<Transaction> {
    return FluxIndexerAPI.getTransaction(txid);
  }

  /**
   * Fetch multiple transactions in a single request (batch endpoint)
   * More efficient than fetching each transaction individually.
   * @param txids - Array of transaction IDs
   * @param blockHeight - Optional block height hint for optimization
   * @returns Array of transactions in same order as input txids
   */
  static async getTransactionsBatch(txids: string[], blockHeight?: number): Promise<Transaction[]> {
    return FluxIndexerAPI.getTransactionsBatch(txids, blockHeight);
  }

  /**
   * Fetch raw transaction data
   * @param txid - Transaction ID
   * @returns Raw transaction hex string
   */
  static async getRawTransaction(txid: string): Promise<{ rawtx: string }> {
    return FluxIndexerAPI.getRawTransaction(txid);
  }

  /**
   * Fetch address information
   * @param address - Flux address
   * @returns Address information including balance and transaction count
   */
  static async getAddress(address: string): Promise<AddressInfo> {
    return FluxIndexerAPI.getAddress(address);
  }

  /**
   * Fetch address balance
   * @param address - Flux address
   * @returns Balance in FLUX
   */
  static async getAddressBalance(address: string): Promise<number> {
    return FluxIndexerAPI.getAddressBalance(address);
  }

  /**
   * Fetch address total received
   * @param address - Flux address
   * @returns Total received in FLUX
   */
  static async getAddressTotalReceived(address: string): Promise<number> {
    return FluxIndexerAPI.getAddressTotalReceived(address);
  }

  /**
   * Fetch address total sent
   * @param address - Flux address
   * @returns Total sent in FLUX
   */
  static async getAddressTotalSent(address: string): Promise<number> {
    return FluxIndexerAPI.getAddressTotalSent(address);
  }

  /**
   * Fetch address unconfirmed balance
   * @param address - Flux address
   * @returns Unconfirmed balance in FLUX
   */
  static async getAddressUnconfirmedBalance(address: string): Promise<number> {
    return FluxIndexerAPI.getAddressUnconfirmedBalance(address);
  }

  /**
   * Fetch address UTXOs (Unspent Transaction Outputs)
   * @param address - Flux address
   * @returns Array of UTXOs
   */
  static async getAddressUtxos(address: string): Promise<Array<{
    txid: string;
    vout: number;
    value: string;
    height?: number;
    confirmations?: number;
  }>> {
    return FluxIndexerAPI.getAddressUtxos(address);
  }

  /**
   * Fetch transactions for multiple addresses
   * @param addresses - Array of Flux addresses
   * @param params - Query parameters:
   *   - from: Starting transaction index (for pagination)
   *   - to: Ending transaction index (for pagination)
   *   - fromBlock: Starting block height (for date filtering)
   *   - toBlock: Ending block height (for date filtering)
   * @returns Paginated transaction list
   */
  static async getAddressTransactions(
    addresses: string[],
    params?: { from?: number; to?: number; fromBlock?: number; toBlock?: number; fromTimestamp?: number; toTimestamp?: number }
  ): Promise<AddressTransactionsPage> {
    return FluxIndexerAPI.getAddressTransactions(addresses, params);
  }

  /**
   * Fetch aggregated dashboard stats
   */
  static async getDashboardStats(): Promise<DashboardStats> {
    return FluxIndexerAPI.getDashboardStats();
  }

  /**
   * Fetch network status
   * @returns Network status and info
   */
  static async getStatus(): Promise<NetworkStatus> {
    return FluxIndexerAPI.getStatus();
  }

  /**
   * Fetch sync status
   * @returns Sync status
   */
  static async getSyncStatus(): Promise<SyncStatus> {
    const result = await FluxIndexerAPI.getSyncStatus();
    return {
      status: result.status as "syncing" | "synced" | "error",
      blockChainHeight: result.blockChainHeight,
      syncPercentage: result.syncPercentage,
      height: result.height,
      type: result.type,
    };
  }

  /**
   * Fetch blockchain statistics
   * @param days - Number of days to get stats for
   * @returns Blockchain statistics
   */
  static async getStats(_days?: number): Promise<BlockchainStats> {
    // FluxIndexer doesn't provide stats yet, return empty values
    return {
      avgBlockSize: 0,
      avgTransactionPerBlock: 0,
      avgTransactionValue: 0,
      blocks: 0,
      height: 0,
      totalFees: 0,
      totalTransactions: 0,
      totalVolume: 0,
    };
  }

  /**
   * Fetch current supply
   * @returns Current FLUX supply
   */
  static async getSupply(): Promise<number> {
    return FluxIndexerAPI.getSupply();
  }

  /**
   * Estimate fee for transaction
   * @param nbBlocks - Number of blocks
   * @returns Estimated fee per KB
   */
  static async estimateFee(nbBlocks: number = 2): Promise<number> {
    return FluxIndexerAPI.estimateFee(nbBlocks);
  }
}
