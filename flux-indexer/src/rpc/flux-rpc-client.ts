/**
 * Flux RPC Client
 *
 * Handles communication with Flux daemon v9.0.0+ via JSON-RPC
 */

import fetch from 'node-fetch';
import {
  FluxRPCConfig,
  RPCRequest,
  RPCResponse,
  Block,
  BlockHeader,
  Transaction,
  RPCError,
} from '../types';
import { logger } from '../utils/logger';

export class FluxRPCClient {
  private url: string;
  private auth: string | null = null;
  private timeout: number;
  private requestId = 0;

  constructor(config: FluxRPCConfig) {
    this.url = config.url;
    this.timeout = config.timeout || 30000;

    if (config.username && config.password) {
      this.auth = Buffer.from(`${config.username}:${config.password}`).toString('base64');
    }
  }

  /**
   * Make RPC call to Flux daemon
   */
  private nextRequestId(): number {
    return ++this.requestId;
  }

  private buildRequest(method: string, params: any[] = []): RPCRequest {
    return {
      jsonrpc: '2.0',
      id: this.nextRequestId(),
      method,
      params,
    };
  }

  private async call<T = any>(method: string, params: any[] = []): Promise<T> {
    const request = this.buildRequest(method, params);

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (this.auth) {
      headers['Authorization'] = `Basic ${this.auth}`;
    }

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);

      const response = await fetch(this.url, {
        method: 'POST',
        headers,
        body: JSON.stringify(request),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new RPCError(
          `HTTP ${response.status}: ${response.statusText}`,
          response.status
        );
      }

      const data = await response.json() as RPCResponse<T>;

      if (data.error) {
        throw new RPCError(
          data.error.message,
          data.error.code,
          { method, params }
        );
      }

      return data.result;
    } catch (error: any) {
      if (error.name === 'AbortError') {
        throw new RPCError(`RPC timeout after ${this.timeout}ms`, -1, { method, params });
      }

      if (error instanceof RPCError) {
        throw error;
      }

      throw new RPCError(
        `RPC call failed: ${error.message}`,
        -1,
        { method, params, error: error.message }
      );
    }
  }

  async batchCall<T = any>(requests: Array<{ method: string; params?: any[] }>): Promise<T[]> {
    if (requests.length === 0) {
      return [];
    }

    if (requests.length === 1) {
      const result = await this.call<T>(requests[0].method, requests[0].params ?? []);
      return [result];
    }

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (this.auth) {
      headers['Authorization'] = `Basic ${this.auth}`;
    }

    const payload = requests.map((req) => this.buildRequest(req.method, req.params ?? []));
    const idMap = new Map<number, number>();
    payload.forEach((req, index) => idMap.set(Number(req.id), index));

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);

      const response = await fetch(this.url, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new RPCError(
          `HTTP ${response.status}: ${response.statusText}`,
          response.status
        );
      }

      const data = await response.json();

      if (!Array.isArray(data)) {
        throw new RPCError('Invalid batch response from RPC server', -1, { data });
      }

      const results: T[] = new Array(requests.length);

      for (const item of data) {
        if (item.error) {
          throw new RPCError(item.error.message, item.error.code, item);
        }
        const index = idMap.get(Number(item.id));
        if (index === undefined) {
          logger.warn('Received RPC response with unknown id', { id: item.id });
          continue;
        }
        results[index] = item.result;
      }

      // Ensure all results are filled
      for (let i = 0; i < results.length; i++) {
        if (results[i] === undefined) {
          throw new RPCError('Missing RPC batch result', -1, { request: requests[i] });
        }
      }

      return results;
    } catch (error) {
      logger.warn('Batch RPC call failed, falling back to individual requests', { error });
      const results: T[] = [];
      for (const req of requests) {
        results.push(await this.call<T>(req.method, req.params ?? []));
      }
      return results;
    }
  }

  /**
   * Get blockchain info
   */
  async getBlockchainInfo(): Promise<{
    chain: string;
    blocks: number;
    headers: number;
    bestblockhash: string;
    difficulty: number;
    mediantime: number;
    verificationprogress: number;
    chainwork: string;
    pruned: boolean;
    softforks: any[];
    valuePools?: Array<{
      id: string;
      chainValue: number;
      chainValueZat: number;
    }>;
  }> {
    return this.call('getblockchaininfo');
  }

  /**
   * Get current block count
   */
  async getBlockCount(): Promise<number> {
    return this.call('getblockcount');
  }

  /**
   * Get block hash by height
   */
  async getBlockHash(height: number): Promise<string> {
    return this.call('getblockhash', [height]);
  }

  /**
   * Get block by hash or height
   * @param hashOrHeight - Block hash or height
   * @param verbosity - 0 = hex, 1 = json, 2 = json with tx details
   */
  async getBlock(hashOrHeight: string | number, verbosity: 0 | 1 | 2 = 2): Promise<Block> {
    let hash: string;

    if (typeof hashOrHeight === 'number') {
      hash = await this.getBlockHash(hashOrHeight);
    } else {
      hash = hashOrHeight;
    }

    return this.call('getblock', [hash, verbosity]);
  }

  /**
   * Get block header
   */
  async getBlockHeader(hash: string, verbose: boolean = true): Promise<BlockHeader | string> {
    return this.call('getblockheader', [hash, verbose]);
  }

  /**
   * Get raw transaction
   * @param txid - Transaction ID
   * @param verbose - If true, returns JSON; if false, returns hex
   * @param blockhash - Optional block hash to help locate the transaction
   */
  async getRawTransaction(txid: string, verbose: boolean = true, blockhash?: string): Promise<Transaction | string> {
    const params = blockhash ? [txid, verbose, blockhash] : [txid, verbose];
    return this.call('getrawtransaction', params);
  }

  /**
   * Get raw mempool
   * @param verbose - If true, returns detailed info; if false, returns txids
   */
  async getRawMempool(verbose: boolean = false): Promise<string[] | Record<string, any>> {
    return this.call('getrawmempool', [verbose]);
  }

  /**
   * Get mempool info
   */
  async getMempoolInfo(): Promise<{
    size: number;
    bytes: number;
    usage: number;
    maxmempool: number;
    mempoolminfee: number;
  }> {
    return this.call('getmempoolinfo');
  }

  /**
   * Get address balance (requires addressindex)
   */
  async getAddressBalance(addresses: string[]): Promise<{
    balance: number;
    received: number;
  }> {
    return this.call('getaddressbalance', [{ addresses }]);
  }

  /**
   * Get address UTXOs (requires addressindex)
   */
  async getAddressUtxos(addresses: string[]): Promise<Array<{
    address: string;
    txid: string;
    outputIndex: number;
    script: string;
    satoshis: number;
    height: number;
  }>> {
    return this.call('getaddressutxos', [{ addresses }]);
  }

  /**
   * Get address transaction IDs (requires addressindex)
   */
  async getAddressTxids(addresses: string[], start?: number, end?: number): Promise<string[]> {
    const params: any = { addresses };
    if (start !== undefined) params.start = start;
    if (end !== undefined) params.end = end;
    return this.call('getaddresstxids', [params]);
  }

  /**
   * Get address deltas (requires addressindex)
   */
  async getAddressDeltas(addresses: string[], start?: number, end?: number): Promise<Array<{
    satoshis: number;
    txid: string;
    index: number;
    blockindex: number;
    height: number;
    address: string;
  }>> {
    const params: any = { addresses };
    if (start !== undefined) params.start = start;
    if (end !== undefined) params.end = end;
    return this.call('getaddressdeltas', [params]);
  }

  /**
   * Get network info
   */
  async getNetworkInfo(): Promise<{
    version: number;
    subversion: string;
    protocolversion: number;
    localservices: string;
    connections: number;
    networks: any[];
    relayfee: number;
  }> {
    return this.call('getnetworkinfo');
  }

  /**
   * Get FluxNode list (PoN specific)
   */
  async getFluxNodeList(): Promise<any> {
    try {
      return await this.call('listfluxnodes');
    } catch (error) {
      logger.warn('getFluxNodeList failed, method may not be available', { error });
      return [];
    }
  }

  /**
   * Get FluxNode status (PoN specific)
   */
  async getFluxNodeStatus(ip?: string): Promise<any> {
    try {
      const params = ip ? [ip] : [];
      return await this.call('getfluxnodestatus', params);
    } catch (error) {
      logger.warn('getFluxNodeStatus failed, method may not be available', { error });
      return null;
    }
  }

  /**
   * Batch get blocks
   * @param heights - Array of block heights to fetch
   * @param includeRawHex - If true, also fetch raw block hex and attach as rawHex property
   */
  async batchGetBlocks(heights: number[], includeRawHex: boolean = false): Promise<Block[]> {
    if (heights.length === 0) {
      return [];
    }

    // First resolve all hashes in a batch
    const hashRequests = heights.map((height) => ({ method: 'getblockhash', params: [height] }));
    const hashes = await this.batchCall<string>(hashRequests);

    // Fetch blocks with verbosity 2 (full transaction data)
    const blockRequests = hashes.map((hash) => ({ method: 'getblock', params: [hash, 2] }));

    // Optionally also fetch raw hex (verbosity 0) for shielded transaction parsing
    let rawHexPromise: Promise<string[]> | null = null;
    if (includeRawHex) {
      const hexRequests = hashes.map((hash) => ({ method: 'getblock', params: [hash, 0] }));
      rawHexPromise = this.batchCall<string>(hexRequests);
    }

    try {
      const [blocks, rawHexes] = await Promise.all([
        this.batchCall<Block>(blockRequests),
        rawHexPromise || Promise.resolve([])
      ]);

      // Attach raw hex to blocks if requested
      if (includeRawHex && rawHexes.length > 0) {
        for (let i = 0; i < blocks.length; i++) {
          (blocks[i] as any).rawHex = rawHexes[i];
        }
      }

      return blocks;
    } catch (error: any) {
      // If batch call fails (e.g., HTTP 500 for FluxNode blocks), fetch individually
      // and fall back to verbosity 1 on error
      logger.warn('Batch block fetch failed, fetching blocks individually with fallback', {
        heights: heights.length,
        error: error.message
      });

      const blocks: Block[] = [];
      for (let i = 0; i < hashes.length; i++) {
        const hash = hashes[i];
        const height = heights[i];

        try {
          // Try verbosity 2 first
          const block = await this.call<Block>('getblock', [hash, 2]);

          // Also fetch raw hex if requested
          if (includeRawHex) {
            try {
              const rawHex = await this.call<string>('getblock', [hash, 0]);
              (block as any).rawHex = rawHex;
            } catch {
              // Continue without raw hex
            }
          }

          blocks.push(block);
        } catch (error500: any) {
          // If HTTP 500 (daemon can't process FluxNode transactions), fall back to verbosity 1
          if (error500 instanceof RPCError && error500.message.includes('500')) {
            logger.debug('Falling back to verbosity 1 for block with FluxNode transactions', {
              height,
              hash
            });

            try {
              const block = await this.call<Block>('getblock', [hash, 1]);
              blocks.push(block);
            } catch (fallbackError: any) {
              logger.error('Failed to fetch block even with verbosity 1', {
                height,
                hash,
                error: fallbackError.message
              });
              throw fallbackError;
            }
          } else {
            throw error500;
          }
        }
      }

      return blocks;
    }
  }

  async batchGetRawTransactions(
    txids: string[],
    verbose: boolean = true,
    blockhash?: string | (string | undefined)[]
  ): Promise<Array<Transaction | string>> {
    if (txids.length === 0) {
      return [];
    }

    const requests = txids.map((txid, index) => {
      const params: any[] = [txid, verbose];
      const hash = Array.isArray(blockhash) ? blockhash[index] : blockhash;
      if (hash) {
        params.push(hash);
      }
      return { method: 'getrawtransaction', params };
    });
    return this.batchCall<Transaction | string>(requests);
  }

  /**
   * Test RPC connection
   */
  async testConnection(): Promise<boolean> {
    try {
      await this.getBlockCount();
      return true;
    } catch (error) {
      logger.error('RPC connection test failed', { error });
      return false;
    }
  }

  /**
   * Get best block hash
   */
  async getBestBlockHash(): Promise<string> {
    return this.call('getbestblockhash');
  }

  /**
   * Get chain tips (for reorg detection)
   */
  async getChainTips(): Promise<Array<{
    height: number;
    hash: string;
    branchlen: number;
    status: string;
  }>> {
    return this.call('getchaintips');
  }

  /**
   * Validate address
   */
  async validateAddress(address: string): Promise<{
    isvalid: boolean;
    address?: string;
    scriptPubKey?: string;
    ismine?: boolean;
    iswatchonly?: boolean;
  }> {
    return this.call('validateaddress', [address]);
  }

  /**
   * Get difficulty
   */
  async getDifficulty(): Promise<number> {
    return this.call('getdifficulty');
  }

  /**
   * Estimate fee
   */
  async estimateFee(nblocks: number = 6): Promise<number> {
    try {
      return await this.call('estimatefee', [nblocks]);
    } catch (error) {
      logger.warn('estimatefee not available, returning default', { error });
      return 0.0001; // Default fee
    }
  }
}
