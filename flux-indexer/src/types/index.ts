/**
 * FluxIndexer Type Definitions
 *
 * Types for Flux v9.0.0+ blockchain data structures
 */

// ============================================================================
// RPC Types (Flux Daemon v9.0.0+)
// ============================================================================

export interface FluxRPCConfig {
  url: string;
  username?: string;
  password?: string;
  timeout?: number;
}

export interface RPCRequest {
  jsonrpc: string;
  id: string | number;
  method: string;
  params: any[];
}

export interface RPCResponse<T = any> {
  result: T;
  error: {
    code: number;
    message: string;
  } | null;
  id: string | number;
}

// ============================================================================
// Block Types
// ============================================================================

export interface Block {
  hash: string;
  confirmations: number;
  size: number;
  height: number;
  version: number;
  versionHex: string;
  merkleroot: string;
  tx: string[] | Transaction[];  // Can be txids or full tx objects
  time: number;
  mediantime: number;
  nonce: number;
  bits: string;
  difficulty: number;
  chainwork: string;
  previousblockhash?: string;
  nextblockhash?: string;
  producer?: string;  // FluxNode IP/ID (PoN)
  producerReward?: number;
}

export interface BlockHeader {
  hash: string;
  confirmations: number;
  height: number;
  version: number;
  versionHex: string;
  merkleroot: string;
  time: number;
  mediantime: number;
  nonce: number;
  bits: string;
  difficulty: number;
  chainwork: string;
  previousblockhash?: string;
  nextblockhash?: string;
}

// ============================================================================
// Transaction Types
// ============================================================================

export interface Transaction {
  txid: string;
  hash: string;
  version: number;
  size: number;
  vsize: number;
  locktime: number;
  vin: TransactionInput[];
  vout: TransactionOutput[];
  hex?: string;
  blockhash?: string;
  confirmations?: number;
  time?: number;
  blocktime?: number;
  is_shielded?: boolean;  // Fully shielded transaction (no transparent components)
  // Shielded transaction components (Zcash Sapling)
  // Note: Different RPC versions may use different field names
  vShieldedOutput?: Array<{
    cv: string;
    cmu: string;
    ephemeralKey: string;
    encCiphertext: string;
    outCiphertext: string;
    proof: string;
  }>;
  vShieldedOutput2?: Array<{
    cv: string;
    cmu: string;
    ephemeralKey: string;
    encCiphertext: string;
    outCiphertext: string;
    proof: string;
  }>;
  vShieldedSpend?: Array<{
    cv: string;
    anchor: string;
    nullifier: string;
    rk: string;
    proof: string;
    spendAuthSig: string;
  }>;
  vShieldedSpend2?: Array<{
    cv: string;
    anchor: string;
    nullifier: string;
    rk: string;
    proof: string;
    spendAuthSig: string;
  }>;
  // Legacy shielded (JoinSplit - Sprout)
  vjoinsplit?: Array<{
    vpub_old: number;
    vpub_new: number;
    anchor: string;
    nullifiers: string[];
    commitments: string[];
    onetimePubKey: string;
    randomSeed: string;
    macs: string[];
    proof: string;
    ciphertexts: string[];
  }>;
  valueBalance?: number; // Net value transferred from/to shielded pool
  bindingSig?: string;   // Binding signature for shielded transactions
}

export interface TransactionInput {
  txid?: string;
  vout?: number;
  scriptSig?: {
    asm: string;
    hex: string;
  };
  sequence: number;
  coinbase?: string;
}

export interface TransactionOutput {
  value: number;
  n: number;
  scriptPubKey: ScriptPubKey;
}

export interface ScriptPubKey {
  asm: string;
  hex: string;
  reqSigs?: number;
  type: string;
  addresses?: string[];
}

// ============================================================================
// UTXO Types
// ============================================================================

export interface UTXO {
  txid: string;
  vout: number;
  address: string;
  value: bigint;
  scriptPubKey: string;
  scriptType: string;
  blockHeight: number;
  spent: boolean;
  spentTxid?: string;
  spentBlockHeight?: number;
}

export interface AddressUTXO {
  txid: string;
  vout: number;
  value: string;
  height: number;
  confirmations: number;
}

// ============================================================================
// Address Types
// ============================================================================

export interface AddressInfo {
  address: string;
  balance: string;
  totalReceived: string;
  totalSent: string;
  txCount: number;
  unconfirmedBalance: string;
  unconfirmedTxCount: number;
  firstSeen?: number;
  lastActivity?: number;
}

export interface AddressBalance {
  balance: number;
  received: number;
}

export interface AddressTransaction {
  txid: string;
  vin: TransactionInput[];
  vout: TransactionOutput[];
  blockHash: string;
  blockHeight: number;
  confirmations: number;
  blockTime: number;
  value: string;
  fee?: string;
  addresses: string[];
}

// ============================================================================
// FluxNode Producer Types (PoN)
// ============================================================================

export interface Producer {
  fluxnode: string;
  blocksProduced: number;
  firstBlock: number;
  lastBlock: number;
  totalRewards: bigint;
  averageBlockTime: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface ProducerStats {
  fluxnode: string;
  blocksProduced: number;
  totalRewards: string;
  firstBlock: number;
  lastBlock: number;
  averageBlockTime: number;
  rewardPerBlock: string;
  percentageOfBlocks: number;
}

// ============================================================================
// Database Types
// ============================================================================

export interface DBBlock {
  height: number;
  hash: string;
  prev_hash: string | null;
  merkle_root: string;
  timestamp: number;
  bits: string;
  nonce: bigint;
  version: number;
  size: number;
  tx_count: number;
  producer: string | null;
  producer_reward: bigint | null;
  difficulty: number;
  chainwork: string;
  created_at: Date;
}

export interface DBTransaction {
  txid: string;
  block_height: number;
  block_hash: string;
  timestamp: number;
  version: number;
  locktime: bigint;
  size: number;
  vsize: number;
  input_count: number;
  output_count: number;
  input_total: bigint;
  output_total: bigint;
  fee: bigint;
  is_coinbase: boolean;
  hex: string | null;
  created_at: Date;
}

export interface DBUTXO {
  txid: string;
  vout: number;
  address: string;
  value: bigint;
  script_pubkey: string;
  script_type: string;
  block_height: number;
  spent: boolean;
  spent_txid: string | null;
  spent_block_height: number | null;
  created_at: Date;
  spent_at: Date | null;
}

export interface DBAddressSummary {
  address: string;
  balance: bigint;
  tx_count: number;
  received_total: bigint;
  sent_total: bigint;
  unspent_count: number;
  first_seen: number | null;
  last_activity: number | null;
  updated_at: Date;
}

export interface DBProducer {
  fluxnode: string;
  blocks_produced: number;
  first_block: number | null;
  last_block: number | null;
  total_rewards: bigint;
  avg_block_time: number | null;
  created_at: Date;
  updated_at: Date;
}

export interface DBSyncState {
  id: number;
  current_height: number;
  chain_height: number;
  sync_percentage: number;
  last_block_hash: string | null;
  last_sync_time: Date | null;
  is_syncing: boolean;
  sync_start_time: Date | null;
  blocks_per_second: number | null;
  estimated_completion: Date | null;
}

export interface DBReorg {
  id: number;
  from_height: number;
  to_height: number;
  common_ancestor: number;
  old_hash: string | null;
  new_hash: string | null;
  blocks_affected: number;
  occurred_at: Date;
}

// ============================================================================
// API Response Types
// ============================================================================

export interface APIStatus {
  name: string;
  version: string;
  network: string;
  consensus: string;
  indexer: {
    syncing: boolean;
    synced: boolean;
    currentHeight: number;
    chainHeight: number;
    progress: string;
    blocksIndexed: number;
    transactionsIndexed: number;
    addressesIndexed: number;
    lastSyncTime: string | null;
  };
  daemon: {
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
  timestamp: string;
  uptime: number;
}

export interface APIPagination {
  page: number;
  totalPages: number;
  itemsOnPage: number;
  totalItems?: number;
}

// ============================================================================
// Indexer Types
// ============================================================================

export interface IndexerConfig {
  rpc: FluxRPCConfig;
  clickhouse: {
    host: string;
    port: number;
    database: string;
    username: string;
    password: string;
    requestTimeout?: number;
    maxOpenConnections?: number;
  };
  indexer: {
    batchSize: number;
    pollingInterval: number;
    startHeight?: number;
    enableReorgHandling: boolean;
    maxReorgDepth: number;
  };
  api: {
    port: number;
    host: string;
    corsEnabled: boolean;
  };
}

export interface SyncProgress {
  currentHeight: number;
  chainHeight: number;
  percentage: number;
  blocksPerSecond: number;
  estimatedCompletion: Date | null;
  isSyncing: boolean;
}

export interface IndexStats {
  totalBlocks: number;
  totalTransactions: number;
  totalUTXOs: number;
  totalAddresses: number;
  totalProducers: number;
  databaseSize: string;
  indexerUptime: number;
  syncProgress: SyncProgress;
}

// ============================================================================
// Error Types
// ============================================================================

export class IndexerError extends Error {
  constructor(
    message: string,
    public code: string,
    public details?: any
  ) {
    super(message);
    this.name = 'IndexerError';
  }
}

export class RPCError extends IndexerError {
  constructor(message: string, public rpcCode: number, details?: any) {
    super(message, 'RPC_ERROR', details);
    this.name = 'RPCError';
  }
}

export class DatabaseError extends IndexerError {
  constructor(message: string, details?: any) {
    super(message, 'DATABASE_ERROR', details);
    this.name = 'DatabaseError';
  }
}

export class SyncError extends IndexerError {
  constructor(message: string, details?: any) {
    super(message, 'SYNC_ERROR', details);
    this.name = 'SyncError';
  }
}
