export interface InsightBlockRow {
  hash: string;
  height: number;
  size: number;
  version: number;
  merkle_root: string;
  timestamp: number;
  bits: string;
  difficulty: number | string;
  chainwork: string;
  prev_hash?: string | null;
  producer_reward?: string | number | null;
  producer?: string | null;
  tx_count?: number;
  nonce?: string | number | null;
}

export interface InsightTxRow {
  txid: string;
  version: number;
  locktime: string | number;
  block_height: number;
  timestamp: number;
  input_total: string | number;
  output_total: string | number;
  fee: string | number;
  size: number;
  is_coinbase: number;
  is_fluxnode_tx: number;
  fluxnode_type?: number | null;
}

export interface InsightInputRow {
  txid: string;
  vout: number;
  address: string;
  value: string | number;
  script_type?: string;
}

export interface InsightOutputRow {
  vout: number;
  address?: string | null;
  value: string | number;
  script_pubkey: string;
  script_type: string;
  spent: number;
  spent_txid?: string | null;
  // Derived by the service layer; not a raw utxos table column.
  spent_index?: number | null;
  spent_block_height?: number | null;
}

export interface InsightAddressSummaryRow {
  balance: string | number;
  received_total: string | number;
  sent_total: string | number;
  tx_count: string | number;
}

export interface InsightUtxoRow {
  address: string;
  txid: string;
  vout: number;
  script_pubkey: string;
  value: string | number;
  block_height?: number;
  timestamp?: number;
  confirmations: number;
}
