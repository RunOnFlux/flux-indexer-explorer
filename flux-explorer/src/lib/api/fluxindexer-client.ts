/**
 * FluxIndexer API Client
 *
 * Client for interacting with FluxIndexer API v1
 * Custom implementation for Flux blockchain indexing
 */

import ky from "ky";
import type {
  Block,
  BlockSummary,
  Transaction,
  AddressInfo,
  NetworkStatus,
  DashboardStats,
  AddressTransactionSummary,
  AddressTransactionsPage,
} from "@/types/flux-api";
import {
  convertFluxIndexerTransaction,
  convertFluxIndexerBlock,
  convertFluxIndexerBlockSummary,
  convertFluxIndexerAddress,
  satoshisToFlux,
} from "./fluxindexer-utils";
import { parseFluxNodeTransaction, isFluxNodeTransaction, getTierFromCollateral } from "@/lib/flux-tx-parser";
import { getApiConfig } from "./config";

// FluxIndexer API response types
interface FluxIndexerApiResponse {
  name?: string;
  version?: string;
  network?: string;
  consensus?: string;
  indexer: {
    syncing: boolean;
    synced: boolean;
    currentHeight: number;
    chainHeight: number;
    progress: string;
    blocksIndexed?: number;
    transactionsIndexed?: number;
    addressesIndexed?: number;
    percentage?: number;
    lastSyncTime: string | null;
  };
  daemon?: {
    version: string;
    protocolVersion: number;
    blocks: number;
    headers: number;
    bestBlockHash: string;
    difficulty: number;
    chainwork: string;
    consensus: string;
    connections: number;
  } | {
    status: string;
    version: string;
    consensus: string;
  };
  timestamp?: string;
  uptime?: number;
}

interface FluxIndexerBlockResponse {
  hash: string;
  size?: number;
  height: number;
  version?: number;
  merkleRoot?: string;
  txs?: Array<FluxIndexerTransactionResponse>; // Can include full transaction objects
  time?: number;
  nonce?: string;
  bits?: string;
  difficulty: string;
  chainWork?: string;
  confirmations?: number;
  previousBlockHash?: string;
  nextBlockHash?: string;
  reward?: string;
  txCount?: number;
  txDetails?: FluxIndexerTransactionDetailResponse[];
  txSummary?: FluxIndexerBlockTxSummaryResponse;
}

interface FluxIndexerTransactionDetailResponse {
  txid: string;
  order: number;
  kind: 'coinbase' | 'transfer' | 'fluxnode_start' | 'fluxnode_confirm' | 'fluxnode_other';
  isCoinbase: boolean;
  fluxnodeType?: number | null;
  fluxnodeTier?: string | null;
  fluxnodeIp?: string | null;
  fluxnodePubKey?: string | null;
  fluxnodeSignature?: string | null;
  valueSat?: number;
  value?: number;
  feeSat?: number;
  fee?: number;
  size?: number;
}

interface FluxIndexerBlockTxSummaryResponse {
  total: number;
  regular: number;
  coinbase: number;
  transfers: number;
  fluxnodeStart: number;
  fluxnodeConfirm: number;
  fluxnodeOther: number;
  fluxnodeTotal: number;
  tierCounts: {
    cumulus: number;
    nimbus: number;
    stratus: number;
    starting: number;
    unknown: number;
  };
}

interface FluxIndexerTransactionResponse {
  txid: string;
  version?: number;
  lockTime?: number;
  vin?: Array<{
    txid?: string;
    vout?: number;
    sequence?: number;
    n?: number;
    scriptSig?: { hex: string; asm: string };
    addresses?: string[];
    value?: string;
    coinbase?: string;
  }>;
  vout?: Array<{
    value?: string;
    n: number;
    hex?: string;
    asm?: string;
    addresses?: string[];
    scriptPubKey?: {
      hex?: string;
      asm?: string;
      addresses?: string[];
      type?: string;
      opReturnHex?: string | null;
      opReturnText?: string | null;
    };
    spentTxId?: string;
    spentIndex?: number;
    spentHeight?: number;
  }>;
  blockHash?: string;
  blockHeight?: number;
  confirmations?: number;
  blockTime?: number;
  value?: string;
  size?: number;
  vsize?: number;
  valueIn?: string;
  fees?: string;
  hex?: string;
  receivedValue?: string;
  sentValue?: string;
}

interface FluxIndexerAddressResponse {
  address: string;
  balance?: string;
  totalReceived?: string;
  totalSent?: string;
  unconfirmedBalance?: string;
  unconfirmedTxs?: number;
  txs?: number;
  transactions?: FluxIndexerTransactionResponse[];
}

interface FluxIndexerAddressTransactionsResponse {
  address: string;
  transactions: Array<{
    txid: string;
    blockHeight: number;
    timestamp: number;
    blockHash?: string;
    direction?: string;
    value?: string;
    receivedValue?: string;
    sentValue?: string;
    fromAddresses?: string[];
    fromAddressCount?: number;
    toAddresses?: string[];
    toAddressCount?: number;
    selfTransfer?: boolean;
    feeValue?: string;
    changeValue?: string;
    toOthersValue?: string;
    confirmations?: number;
    isCoinbase?: boolean;
  }>;
  total: number;
  filteredTotal?: number;
  limit: number;
  offset: number;
}

interface FluxIndexerUtxoResponse {
  txid: string;
  vout: number;
  value: string;
  height?: number;
  confirmations?: number;
}

interface FluxIndexerLatestBlockSummaryTierCounts {
  cumulus?: number;
  nimbus?: number;
  stratus?: number;
  starting?: number;
  unknown?: number;
}

interface FluxIndexerLatestBlockSummary {
  height: number;
  hash: string;
  time?: number;
  timestamp?: number;
  txCount?: number;
  tx_count?: number;
  txlength?: number;
  size?: number;
  regularTxCount?: number;
  regular_tx_count?: number;
  nodeConfirmationCount?: number;
  node_confirmation_count?: number;
  tierCounts?: FluxIndexerLatestBlockSummaryTierCounts;
  tier_counts?: FluxIndexerLatestBlockSummaryTierCounts;
}

interface FluxIndexerLatestBlocksResponse {
  blocks: FluxIndexerLatestBlockSummary[];
}

// Support both server-side and client-side API URL configuration
// Server-side (API routes): SERVER_API_URL (Docker internal) or NEXT_PUBLIC_API_URL
// Client-side (browser): NEXT_PUBLIC_API_URL (browser-accessible)
function getApiBaseUrl(): string {
  // Check if we're running on the server or client
  const isServer = typeof window === 'undefined';

  if (isServer) {
    // Server-side: Use SERVER_API_URL (Docker internal) or fallback to NEXT_PUBLIC_API_URL
    const apiUrl = process.env.SERVER_API_URL || process.env.NEXT_PUBLIC_API_URL;
    return apiUrl || "http://localhost:3002";
  } else {
    // Client-side: Auto-detect from browser URL for FluxOS multi-node deployment
    if (process.env.NEXT_PUBLIC_API_URL === 'AUTO' || !process.env.NEXT_PUBLIC_API_URL) {
      // Auto-detect: Use same host as the explorer, port 3002
      const protocol = window.location.protocol;
      const hostname = window.location.hostname;
      return `${protocol}//${hostname}:3002`;
    }
    // Use explicit URL if provided
    return process.env.NEXT_PUBLIC_API_URL;
  }
}

// Store current API base URL - will update dynamically for server vs client
let currentApiBaseUrl = getApiBaseUrl();

/**
 * Create API client with dynamic configuration
 * Always gets fresh base URL to ensure correct server/client URL
 */
function createApiClient() {
  const config = getApiConfig();
  // Update currentApiBaseUrl to ensure we're using the right URL for current context
  currentApiBaseUrl = getApiBaseUrl();

  return ky.create({
    prefixUrl: currentApiBaseUrl,
    timeout: config.timeout,
    retry: {
      limit: config.retryLimit,
      methods: ["get"],
      statusCodes: [408, 413, 429, 500, 502, 503, 504],
    },
  });
}

// Initialize API client - will be recreated as needed
let apiClient = createApiClient();

/**
 * Recreate API client with updated configuration
 * Called when API mode changes
 */
export function recreateApiClient(): void {
  apiClient = createApiClient();
  console.log('[FluxIndexer Client] API client recreated with base URL:', currentApiBaseUrl);
}

/**
 * Update the API base URL and rebuild the client if it changed
 */
export function setApiBaseUrl(newBaseUrl: string | undefined | null): void {
  if (!newBaseUrl || newBaseUrl === currentApiBaseUrl) {
    return;
  }

  currentApiBaseUrl = newBaseUrl;
  recreateApiClient();
}

// Listen for API endpoint changes (from health monitor)
if (typeof window !== 'undefined') {
  window.addEventListener('api-endpoint-changed', (event) => {
    const detail = (event as CustomEvent<{ endpoint?: { url?: string } }>).detail;
    if (detail?.endpoint?.url) {
      setApiBaseUrl(detail.endpoint.url);
    } else {
      recreateApiClient();
    }
  });
}

/**
 * Custom error class for FluxIndexer API errors
 */
export class FluxIndexerAPIError extends Error {
  constructor(
    message: string,
    public statusCode?: number,
    public originalError?: unknown
  ) {
    super(message);
    this.name = "FluxIndexerAPIError";
  }
}

function getStatusCode(error: unknown): number | undefined {
  if (error && typeof error === 'object' && 'response' in error) {
    const response = (error as { response?: { status?: number } }).response;
    return response?.status;
  }
  return undefined;
}

/**
 * FluxIndexer API Client Class
 */
export class FluxIndexerAPI {
  /**
   * Fetch network status and info
   */
  static async getStatus(): Promise<NetworkStatus> {
    try {
      const response = await apiClient.get("api/v1/status").json<FluxIndexerApiResponse>();

      const daemonInfo = response.daemon && 'blocks' in response.daemon ? response.daemon : null;

      return {
        info: {
          version: daemonInfo?.protocolVersion || 0,
          protocolversion: daemonInfo?.protocolVersion || 0,
          blocks: daemonInfo?.blocks || response.indexer.currentHeight,
          timeoffset: 0,
          connections: daemonInfo?.connections || 0,
          proxy: "",
          difficulty: daemonInfo?.difficulty || 0,
          testnet: false,
          relayfee: 0.00001,
          errors: "",
          network: "livenet",
        },
      };
    } catch (error) {
      throw new FluxIndexerAPIError(
        "Failed to fetch network status",
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch a block by hash or height
   */
  static async getBlock(hashOrHeight: string | number): Promise<Block> {
    try {
      const response = await apiClient.get(`api/v1/blocks/${hashOrHeight}`).json<FluxIndexerBlockResponse>();
      return convertFluxIndexerBlock(response);
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch block ${hashOrHeight}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch a block with full transaction details included
   * This is more efficient than fetching block + each transaction separately
   */
  static async getBlockWithTransactions(hashOrHeight: string | number): Promise<{
    block: Block;
    transactions: FluxIndexerTransactionResponse[];
  }> {
    try {
      // Try to fetch block with transaction details
      const response = await apiClient.get(`api/v1/blocks/${hashOrHeight}`).json<FluxIndexerBlockResponse>();

      return {
        block: convertFluxIndexerBlock(response),
        transactions: response.txs || [],
      };
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch block with transactions ${hashOrHeight}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch block index/summary by height
   */
  static async getBlockIndex(height: number): Promise<BlockSummary> {
    try {
      const response = await apiClient.get(`api/v1/blocks/${height}`).json<FluxIndexerBlockResponse>();
      return convertFluxIndexerBlockSummary(response);
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch block index ${height}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Get block hash by height
   */
  static async getBlockHash(height: number): Promise<string> {
    try {
      const response = await apiClient.get(`api/v1/blocks/${height}`).json<FluxIndexerBlockResponse>();
      return response.hash;
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch block hash for height ${height}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch latest blocks with FluxNode aggregation when available.
   * Falls back to legacy per-block fetching if the optimized endpoint is missing.
   */
  static async getLatestBlocks(limit: number = 10): Promise<BlockSummary[]> {
    try {
      const response = await apiClient.get("api/v1/blocks/latest", {
        searchParams: { limit: Math.max(1, Math.min(limit, 50)).toString() },
      }).json<FluxIndexerLatestBlocksResponse>();

      if (!Array.isArray(response.blocks)) {
        return this.fetchLatestBlocksLegacy(limit);
      }

      return response.blocks.map((block) => {
        const txCount =
          block.txCount ??
          block.tx_count ??
          block.txlength ??
          0;
        const nodeCount =
          block.nodeConfirmationCount ??
          block.node_confirmation_count ??
          0;
        const regularCount =
          block.regularTxCount ??
          block.regular_tx_count ??
          Math.max(0, txCount - nodeCount);
        const tierCountsSource =
          block.tierCounts ??
          block.tier_counts ??
          {};

        return {
          hash: block.hash,
          height: block.height,
          time: block.time ?? block.timestamp ?? 0,
          txlength: txCount,
          size: block.size ?? 0,
          regularTxCount: regularCount,
          nodeConfirmationCount: nodeCount,
          tierCounts: {
            cumulus: tierCountsSource.cumulus ?? 0,
            nimbus: tierCountsSource.nimbus ?? 0,
            stratus: tierCountsSource.stratus ?? 0,
            starting: tierCountsSource.starting ?? 0,
            unknown: tierCountsSource.unknown ?? 0,
          },
        };
      });
    } catch (error) {
      const statusCode = getStatusCode(error);
      if (statusCode === 404) {
        return this.fetchLatestBlocksLegacy(limit);
      }

      throw new FluxIndexerAPIError(
        "Failed to fetch latest blocks",
        statusCode,
        error
      );
    }
  }

  /**
   * Legacy latest blocks fetcher that walks heights individually.
   * Used only when the optimized API endpoint is unavailable.
   */
  private static async fetchLatestBlocksLegacy(limit: number): Promise<BlockSummary[]> {
    const config = getApiConfig();

    // Get current height first
    const statusResponse = await apiClient.get("api/v1/status").json<FluxIndexerApiResponse>();
    const currentHeight = statusResponse.indexer.currentHeight;

    // Fetch blocks starting from current height
    const blocks: BlockSummary[] = [];
    const batchSize = Math.min(config.batchSize, 20); // Use dynamic batch size, cap at 20

    for (let i = 0; i < limit; i += batchSize) {
      const startHeight = currentHeight - i;
      const endHeight = Math.max(currentHeight - i - batchSize + 1, 0);

      // Fetch individual blocks
      const blockPromises = [];
      for (let h = startHeight; h >= endHeight && blocks.length < limit; h--) {
        blockPromises.push(
          apiClient
            .get(`api/v1/blocks/${h}`)
            .json<FluxIndexerBlockResponse>()
            .then(convertFluxIndexerBlockSummary)
        );
      }

      const fetchedBlocks = await Promise.all(blockPromises);
      blocks.push(...fetchedBlocks);

      // Add throttle delay between batches (except for the last batch)
      if (blocks.length < limit && i + batchSize < limit) {
        await new Promise(resolve => setTimeout(resolve, config.throttleDelay));
      }

      if (blocks.length >= limit) break;
    }

    return blocks.slice(0, limit);
  }

  /**
   * Fetch a transaction by ID
   *
   * Parses FluxNode-specific data from raw hex for confirmation/update transactions
   */
  static async getTransaction(txid: string): Promise<Transaction> {
    try {
      const response = await apiClient.get(`api/v1/transactions/${txid}`).json<FluxIndexerTransactionResponse>();
      const tx = convertFluxIndexerTransaction(response);

      // Check if this is a FluxNode transaction (0 inputs, 0 outputs)
      if (isFluxNodeTransaction(tx) && response.hex) {
        // Parse FluxNode data from raw hex
        const fluxNodeData = parseFluxNodeTransaction(response.hex);

        if (fluxNodeData && fluxNodeData.collateralOutputHash) {
          // Fetch the collateral transaction to determine tier
          try {
            const collateralTx = await apiClient
              .get(`api/v1/transactions/${fluxNodeData.collateralOutputHash}`)
              .json<FluxIndexerTransactionResponse>();

            // Get the output at the specified index
            const collateralOutput =
              collateralTx.vout?.[fluxNodeData.collateralOutputIndex ?? 0];

            if (collateralOutput?.value) {
              // Convert satoshis to FLUX and determine tier
              const collateralAmount = satoshisToFlux(
                parseInt(collateralOutput.value)
              );
              fluxNodeData.benchmarkTier = getTierFromCollateral(collateralAmount);
            }
          } catch (collateralError) {
            console.error("Failed to fetch collateral transaction:", collateralError);
            // Continue with undefined tier if we can't fetch collateral
          }

          // Merge FluxNode-specific fields
          return {
            ...tx,
            ...fluxNodeData,
          };
        }
      }

      return tx;
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch transaction ${txid}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch raw transaction hex data
   */
  static async getRawTransaction(txid: string): Promise<{ rawtx: string }> {
    try {
      const response = await apiClient.get(`api/v1/transactions/${txid}`).json<{ hex?: string }>();
      // FluxIndexer returns raw hex in the "hex" field
      return {
        rawtx: response.hex || "",
      };
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch raw transaction ${txid}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch address information
   */
  static async getAddress(address: string): Promise<AddressInfo> {
    try {
      const response = await apiClient
        .get(`api/v1/addresses/${address}`, {
          searchParams: {
            details: "txs",
            page: 1,
            pageSize: 1000,
          },
        })
        .json<FluxIndexerAddressResponse>();

      const converted = convertFluxIndexerAddress(response);

      // Note: converted.transactions already contains transaction IDs from convertFluxIndexerAddress
      // The response.transactions contains full transaction objects which we don't need here

      return converted;
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch address ${address}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch address balance
   */
  static async getAddressBalance(address: string): Promise<number> {
    try {
      const response = await apiClient.get(`api/v1/addresses/${address}`).json<FluxIndexerAddressResponse>();
      return satoshisToFlux(response.balance || '0');
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch balance for ${address}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch address total received
   */
  static async getAddressTotalReceived(address: string): Promise<number> {
    try {
      const response = await apiClient.get(`api/v1/addresses/${address}`).json<FluxIndexerAddressResponse>();
      return satoshisToFlux(response.totalReceived || '0');
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch total received for ${address}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch address total sent
   */
  static async getAddressTotalSent(address: string): Promise<number> {
    try {
      const response = await apiClient.get(`api/v1/addresses/${address}`).json<FluxIndexerAddressResponse>();
      return satoshisToFlux(response.totalSent || '0');
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch total sent for ${address}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch address unconfirmed balance
   */
  static async getAddressUnconfirmedBalance(address: string): Promise<number> {
    try {
      const response = await apiClient.get(`api/v1/addresses/${address}`).json<FluxIndexerAddressResponse>();
      return satoshisToFlux(response.unconfirmedBalance || '0');
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch unconfirmed balance for ${address}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch address UTXOs
   */
  static async getAddressUtxos(address: string): Promise<FluxIndexerUtxoResponse[]> {
    try {
      const response = await apiClient.get(`api/v1/addresses/${address}/utxos`).json<FluxIndexerUtxoResponse[]>();
      return response || [];
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch UTXOs for ${address}`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch transactions for addresses with pagination and optional block height filtering
   *
   * @param addresses Array of addresses (only first is used)
   * @param params Pagination and filtering parameters:
   *   - from: Starting transaction index (for pagination)
   *   - to: Ending transaction index (for pagination)
   *   - fromBlock: Starting block height (for date filtering)
   *   - toBlock: Ending block height (for date filtering)
   */
  static async getAddressTransactions(
    addresses: string[],
    params?: { from?: number; to?: number; fromBlock?: number; toBlock?: number; fromTimestamp?: number; toTimestamp?: number }
  ): Promise<AddressTransactionsPage> {
    try {
      // FluxIndexer API uses single address with pagination
      const address = addresses[0]; // Take first address

      // Calculate page from params
      const from = params?.from || 0;
      const to = params?.to || 25;
      const pageSize = Math.max(1, to - from);

      // Build search params for new API format
      const searchParams: Record<string, string> = {
        limit: pageSize.toString(),
        offset: from.toString(),
      };

      if (params?.fromBlock !== undefined) {
        searchParams.fromBlock = params.fromBlock.toString();
      }
      if (params?.toBlock !== undefined) {
        searchParams.toBlock = params.toBlock.toString();
      }
      if (params?.fromTimestamp !== undefined) {
        searchParams.fromTimestamp = params.fromTimestamp.toString();
      }
      if (params?.toTimestamp !== undefined) {
        searchParams.toTimestamp = params.toTimestamp.toString();
      }

      const response = await apiClient
        .get(`api/v1/addresses/${address}/transactions`, { searchParams })
        .json<FluxIndexerAddressTransactionsResponse>();

      const filteredTotal = response.filteredTotal ?? response.total ?? 0;

      const items: AddressTransactionSummary[] = (response.transactions || []).map((tx) => {
        const valueSat = tx.value ?? '0';
        const receivedSat = tx.receivedValue ?? '0';
        const sentSat = tx.sentValue ?? '0';
        const feeSat = tx.feeValue ?? (BigInt(sentSat) > BigInt(receivedSat)
          ? (BigInt(sentSat) - BigInt(receivedSat)).toString()
          : '0');
        const changeSat = tx.changeValue ?? '0';
        const toOthersSat = tx.toOthersValue ?? '0';
        return {
          txid: tx.txid,
          blockHeight: tx.blockHeight,
          timestamp: tx.timestamp,
          blockHash: tx.blockHash,
          direction: tx.direction === 'sent' ? 'sent' : 'received',
          valueSat,
          value: satoshisToFlux(valueSat),
          receivedValueSat: receivedSat,
          receivedValue: satoshisToFlux(receivedSat),
          sentValueSat: sentSat,
          sentValue: satoshisToFlux(sentSat),
          feeValueSat: feeSat,
          feeValue: satoshisToFlux(feeSat),
          changeValueSat: changeSat,
          changeValue: satoshisToFlux(changeSat),
          toOthersValueSat: toOthersSat,
          toOthersValue: satoshisToFlux(toOthersSat),
          fromAddresses: tx.fromAddresses ?? [],
          fromAddressCount: tx.fromAddressCount ?? (tx.fromAddresses?.length ?? 0),
          toAddresses: tx.toAddresses ?? [],
          toAddressCount: tx.toAddressCount ?? (tx.toAddresses?.length ?? 0),
          selfTransfer: tx.selfTransfer ?? (BigInt(receivedSat) > BigInt(0) && BigInt(sentSat) > BigInt(0)),
           confirmations: tx.confirmations ?? 0,
           isCoinbase: tx.isCoinbase ?? false,
        };
      });

      return {
        totalItems: response.total ?? filteredTotal,
        filteredTotal,
        from,
        to: Math.min(from + pageSize, filteredTotal),
        limit: pageSize,
        offset: from,
        pagesTotal: pageSize > 0 ? Math.max(1, Math.ceil(filteredTotal / pageSize)) : 1,
        items,
      };
    } catch (error) {
      throw new FluxIndexerAPIError(
        `Failed to fetch transactions for addresses`,
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch aggregated dashboard stats
   */
  static async getDashboardStats(): Promise<DashboardStats> {
    try {
      return await apiClient.get("api/v1/stats/dashboard").json<DashboardStats>();
    } catch (error) {
      throw new FluxIndexerAPIError(
        "Failed to fetch dashboard stats",
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch sync status
   */
  static async getSyncStatus(): Promise<{ status: string; blockChainHeight: number; syncPercentage: number; height: number; type: string }> {
    try {
      const response = await apiClient.get("api/v1/sync").json<FluxIndexerApiResponse>();

      const chainHeight = response.indexer.chainHeight || 0;
      const currentHeight = response.indexer.currentHeight || 0;
      const synced = response.indexer.synced;
      const percentage = response.indexer.percentage !== undefined
        ? response.indexer.percentage
        : (chainHeight > 0 ? (currentHeight / chainHeight) * 100 : 0);

      return {
        status: synced ? "synced" : "syncing",
        blockChainHeight: chainHeight,
        syncPercentage: synced ? 100 : percentage,
        height: currentHeight,
        type: "fluxindexer",
      };
    } catch (error) {
      throw new FluxIndexerAPIError(
        "Failed to fetch sync status",
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Fetch current FLUX supply
   */
  static async getSupply(): Promise<number> {
    try {
      // FluxIndexer doesn't provide supply directly
      // Would need to calculate from blocks or use different source
      return 0;
    } catch (error) {
      throw new FluxIndexerAPIError(
        "Failed to fetch supply",
        getStatusCode(error),
        error
      );
    }
  }

  /**
   * Estimate transaction fee
   */
  static async estimateFee(_nbBlocks: number = 2): Promise<number> {
    try {
      // FluxIndexer doesn't provide fee estimation
      // Return a default value
      return 0.00001;
    } catch (error) {
      throw new FluxIndexerAPIError(
        "Failed to estimate fee",
        getStatusCode(error),
        error
      );
    }
  }
}
