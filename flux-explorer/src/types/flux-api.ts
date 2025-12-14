/**
 * FluxIndexer API Type Definitions
 * These types match the responses from the bundled Flux indexer service
 */

// ============================================================================
// Block Types
// ============================================================================

export interface Block {
  hash: string;
  size: number;
  height: number;
  version: number;
  merkleroot: string;
  tx: string[]; // Array of transaction IDs
  txDetails?: BlockTransactionDetail[];
  txSummary?: BlockTransactionSummary;
  time: number;
  nonce: string;
  bits: string;
  difficulty: number;
  chainwork: string;
  confirmations: number;
  previousblockhash?: string;
  nextblockhash?: string;
  reward: number;
  isMainChain: boolean;
  poolInfo?: {
    poolName: string;
    url: string;
  };
  // FluxNode miner information (PoUW)
  miner?: string; // Wallet address of the miner
  nodeTier?: "CUMULUS" | "NIMBUS" | "STRATUS"; // FluxNode tier
}

export interface BlockTransactionDetail {
  txid: string;
  order: number;
  kind: "coinbase" | "transfer" | "fluxnode_start" | "fluxnode_confirm" | "fluxnode_other";
  isCoinbase: boolean;
  fluxnodeType?: number | null;
  fluxnodeTier?: string | null;
  fluxnodeIp?: string | null;
  fluxnodePubKey?: string | null;
  fluxnodeSignature?: string | null;
  valueSat?: number;
  value?: number;
  valueInSat?: number;
  valueIn?: number;
  feeSat?: number;
  fee?: number;
  size?: number;
  version?: number;
  isShielded?: boolean;
  // From/to addresses for transfer display (avoids separate batch API call)
  fromAddr?: string | null;
  toAddr?: string | null;
}

export interface BlockTransactionSummary {
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

export interface BlockSummary {
  hash: string;
  height: number;
  time: number;
  txlength: number;
  size: number;
  regularTxCount?: number;
  nodeConfirmationCount?: number;
  tierCounts?: {
    cumulus: number;
    nimbus: number;
    stratus: number;
    starting: number;
    unknown: number;
  };
}

export interface DashboardStats {
  latestBlock: {
    height: number;
    hash: string | null;
    timestamp: number | null;
  };
  averages: {
    blockTimeSeconds: number;
  };
  transactions24h: number;
  latestRewards: Array<{
    height: number;
    hash: string;
    timestamp: number;
    txid: string;
    totalRewardSat: number;
    totalReward: number;
    outputs: Array<{
      address: string | null;
      valueSat: number;
      value: number;
    }>;
  }>;
  generatedAt: string;
}

// ============================================================================
// Transaction Types
// ============================================================================

export interface TransactionInput {
  txid: string;
  vout: number;
  sequence: number;
  n: number;
  scriptSig: {
    hex: string;
    asm: string;
  };
  addr?: string;
  valueSat: number;
  value: number;
  doubleSpentTxID?: string;
  coinbase?: string; // Present on coinbase transactions
}

export interface TransactionOutput {
  value: string;
  n: number;
  scriptPubKey: {
    hex: string;
    asm: string;
    addresses?: string[];
    type: string;
    opReturnHex?: string | null;
    opReturnText?: string | null;
  };
  spentTxId?: string;
  spentIndex?: number;
  spentHeight?: number;
}

export interface Transaction {
  txid: string;
  version: number;
  locktime: number;
  vin: TransactionInput[];
  vout: TransactionOutput[];
  blockhash?: string;
  blockheight?: number;
  confirmations: number;
  time?: number;
  blocktime?: number;
  valueOut: number;
  size: number;
  vsize?: number;
  valueIn: number;
  fees: number;
  // FluxNode transaction fields (version 5 & 6)
  nType?: number | null; // 2 = START, 4 = CONFIRM
  benchmarkTier?: string | null; // "CUMULUS", "NIMBUS", "STRATUS"
  ip?: string | null;
  zelnodePubKey?: string | null;
  fluxnodePubKey?: string | null;
  sig?: string | null;
  collateralOutputHash?: string | null;
  collateralOutputIndex?: number | null;
  p2shAddress?: string | null;
}

// ============================================================================
// Address Types
// ============================================================================

export interface AddressTransaction {
  txid: string;
  version: number;
  locktime: number;
  vin: TransactionInput[];
  vout: TransactionOutput[];
  blockhash: string;
  blockheight: number;
  confirmations: number;
  time: number;
  blocktime: number;
  valueOut: number;
  size: number;
  valueIn: number;
  fees: number;
}

export interface AddressInfo {
  addrStr: string;
  balance: number;
  balanceSat: number;
  totalReceived: number;
  totalReceivedSat: number;
  totalSent: number;
  totalSentSat: number;
  unconfirmedBalance: number;
  unconfirmedBalanceSat: number;
  unconfirmedTxApperances: number;
  txApperances: number;
  transactions: string[]; // Array of transaction IDs
  cumulusCount?: number;
  nimbusCount?: number;
  stratusCount?: number;
  fluxnodeLastSync?: string | null;
}

export interface AddressUTXO {
  address: string;
  txid: string;
  vout: number;
  scriptPubKey: string;
  amount: number;
  satoshis: number;
  height: number;
  confirmations: number;
}

// ============================================================================
// Network/Status Types
// ============================================================================

export interface NetworkStatus {
  info: {
    version: number;
    protocolversion: number;
    blocks: number;
    timeoffset: number;
    connections: number;
    proxy: string;
    difficulty: number;
    testnet: boolean;
    relayfee: number;
    errors: string;
    network: string;
  };
}

export interface SyncStatus {
  status: "syncing" | "synced" | "error";
  blockChainHeight: number;
  syncPercentage: number;
  height: number;
  error?: string;
  type: string;
}

// ============================================================================
// Statistics Types
// ============================================================================

export interface BlockchainStats {
  avgBlockSize: number;
  avgTransactionPerBlock: number;
  avgTransactionValue: number;
  blocks: number;
  height: number;
  totalFees: number;
  totalTransactions: number;
  totalVolume: number;
}

export interface Supply {
  supply: number;
}

// ============================================================================
// API Error Types
// ============================================================================

export interface ApiError {
  message: string;
  code?: number;
  details?: unknown;
}

// ============================================================================
// Pagination Types
// ============================================================================

export interface PaginatedResponse<T> {
  pagesTotal: number;
  items: T[];
}

export interface QueryParams {
  from?: number;
  to?: number;
  limit?: number;
  offset?: number;
  cursorHeight?: number;
  cursorTxid?: string;
}

export interface AddressTransactionSummary {
  txid: string;
  blockHeight: number;
  timestamp: number;
  blockHash?: string;
  direction: "received" | "sent";
  valueSat: string;
  value: number;
  receivedValueSat: string;
  receivedValue: number;
  sentValueSat: string;
  sentValue: number;
  feeValueSat: string;
  feeValue: number;
  changeValueSat: string;
  changeValue: number;
  toOthersValueSat: string;
  toOthersValue: number;
  fromAddresses: string[];
  fromAddressCount: number;
  toAddresses: string[];
  toAddressCount: number;
  selfTransfer: boolean;
  confirmations: number;
  isCoinbase: boolean;
}

export interface AddressTransactionsPage extends PaginatedResponse<AddressTransactionSummary> {
  totalItems: number;
  filteredTotal: number;
  from: number;
  to: number;
  limit: number;
  offset: number;
  nextCursor?: {
    height: number;
    txid: string;
  };
}
