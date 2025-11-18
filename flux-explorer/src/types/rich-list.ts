/**
 * Rich List Data Types
 *
 * Shared types for rich list data structure used by both scanner and explorer
 */

import type { RichListCategory } from "@/data/rich-list-labels";

export interface RichListAddress {
  rank: number;
  address: string;
  balance: number;
  percentage: number;
  txCount: number;
  cumulusCount?: number;
  nimbusCount?: number;
  stratusCount?: number;
  label?: string;
  category?: RichListCategory;
  note?: string;
  locked?: boolean;
}

export interface RichListData {
  lastUpdate: string; // ISO 8601 timestamp
  lastBlockHeight: number;
  totalSupply: number;
  transparentSupply?: number; // Transparent UTXO supply
  shieldedPool?: number; // Shielded pool balance
  totalAddresses: number;
  addresses: RichListAddress[];
}

export interface RichListMetadata {
  lastUpdate: string;
  lastBlockHeight: number;
  totalSupply: number;
  totalAddresses: number;
  scanDuration: number; // milliseconds
}
