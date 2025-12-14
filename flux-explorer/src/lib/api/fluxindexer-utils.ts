/**
 * FluxIndexer API Utilities
 *
 * Conversion utilities for FluxIndexer API responses
 */

import type { Transaction, Block, BlockSummary, AddressInfo } from "@/types/flux-api";

// FluxIndexer response types
interface FluxIndexerTransactionVin {
  txid?: string;
  vout?: number;
  sequence?: number;
  n?: number;
  scriptSig?: { hex: string; asm: string };
  addresses?: string[];
  addr?: string;  // Batch endpoint returns addr directly
  value?: string;
  coinbase?: string;
}

interface FluxIndexerTransactionVout {
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
}

interface FluxIndexerTransaction {
  txid: string;
  version?: number;
  lockTime?: number;
  vin?: FluxIndexerTransactionVin[];
  vout?: FluxIndexerTransactionVout[];
  blockHash?: string;
  blockHeight?: number;
  block_height?: number;  // Batch endpoint uses snake_case
  confirmations?: number;
  blockTime?: number;
  time?: number;  // Batch endpoint returns time directly
  timestamp?: number;  // Batch endpoint can return timestamp
  value?: string;
  valueOut?: string;  // Batch endpoint returns valueOut directly
  size?: number;
  vsize?: number;
  valueIn?: string;
  fees?: string;
  hex?: string;
}

interface FluxIndexerBlock {
  hash: string;
  size?: number;
  height: number;
  version?: number;
  merkleRoot?: string;
  txs?: Array<{ txid: string; vout?: FluxIndexerTransactionVout[] }>;
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
  txDetails?: Array<{
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
    feeSat?: number;
    fee?: number;
    size?: number;
    fromAddr?: string | null;
    toAddr?: string | null;
  }>;
  txSummary?: {
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
  };
}

interface FluxIndexerAddress {
  address: string;
  balance?: string;
  totalReceived?: string;
  totalSent?: string;
  unconfirmedBalance?: string;
  unconfirmedTxs?: number;
  txs?: number;
  transactions?: Array<{ txid: string }>;
  cumulusCount?: number;
  nimbusCount?: number;
  stratusCount?: number;
  fluxnodeLastSync?: string | null;
}

/**
 * Convert satoshis (as string) to FLUX
 * FluxIndexer returns values as satoshi strings
 */
export function satoshisToFlux(satoshis: string | number): number {
  const value = typeof satoshis === 'string' ? parseInt(satoshis) : satoshis;
  return value / 100000000;
}

/**
 * Convert FLUX to satoshis (as string)
 */
export function fluxToSatoshis(flux: number): string {
  return Math.floor(flux * 100000000).toString();
}

/**
 * Parse difficulty from string to number
 * FluxIndexer returns difficulty as string
 */
export function parseDifficulty(difficulty: string): number {
  return parseFloat(difficulty);
}

/**
 * Convert FluxIndexer transaction to our Transaction type
 */
export function convertFluxIndexerTransaction(bbTx: FluxIndexerTransaction): Transaction {
  const hex = bbTx.hex || '';
  const computedSize = bbTx.size && bbTx.size > 0
    ? bbTx.size
    : hex
      ? Math.floor(hex.length / 2)
      : 0;
  const computedVSize = bbTx.vsize && bbTx.vsize > 0
    ? bbTx.vsize
    : computedSize;

  return {
    txid: bbTx.txid,
    version: bbTx.version || 0,
    locktime: bbTx.lockTime || 0,
    vin: bbTx.vin?.map((input: FluxIndexerTransactionVin) => ({
      txid: input.txid || '',
      vout: input.vout || 0,
      sequence: input.sequence || 0,
      n: input.n || 0,
      scriptSig: input.scriptSig || { hex: '', asm: '' },
      addr: input.addr || input.addresses?.[0],  // Batch endpoint returns addr directly
      valueSat: input.value ? parseInt(input.value) : 0,
      value: input.value ? satoshisToFlux(input.value) : 0,
      coinbase: input.coinbase,
    })) || [],
    vout: bbTx.vout?.map((output: FluxIndexerTransactionVout) => {
      const scriptHex = output.scriptPubKey?.hex ?? output.hex ?? '';
      const scriptAsm = output.scriptPubKey?.asm ?? output.asm ?? '';
      const scriptAddresses = output.scriptPubKey?.addresses ?? output.addresses;
      const scriptType = output.scriptPubKey?.type ?? 'unknown';
      const opReturnHex = output.scriptPubKey?.opReturnHex ?? null;
      const opReturnText = output.scriptPubKey?.opReturnText ?? null;

      return {
        value: (output.value ? satoshisToFlux(output.value) : 0).toString(),
        n: output.n,
        scriptPubKey: {
          hex: scriptHex,
          asm: scriptAsm,
          addresses: scriptAddresses,
          type: scriptType,
          opReturnHex,
          opReturnText,
        },
        spentTxId: output.spentTxId,
        spentIndex: output.spentIndex,
        spentHeight: output.spentHeight,
      };
    }) || [],
    blockhash: bbTx.blockHash,
    blockheight: bbTx.blockHeight ?? bbTx.block_height,
    confirmations: bbTx.confirmations || 0,
    time: bbTx.blockTime ?? bbTx.time ?? bbTx.timestamp ?? 0,
    blocktime: bbTx.blockTime ?? bbTx.time ?? bbTx.timestamp ?? 0,
    // Batch endpoint returns valueOut directly, regular endpoint uses value
    valueOut: (bbTx.valueOut ?? bbTx.value) ? satoshisToFlux(bbTx.valueOut ?? bbTx.value ?? '0') : 0,
    size: computedSize,
    vsize: computedVSize,
    valueIn: bbTx.valueIn ? satoshisToFlux(bbTx.valueIn) : 0,
    fees: bbTx.fees ? satoshisToFlux(bbTx.fees) : 0,
  };
}

/**
 * Convert FluxIndexer block to our Block type
 */
export function convertFluxIndexerBlock(bbBlock: FluxIndexerBlock): Block {
  const txDetails = bbBlock.txDetails?.map((detail) => ({
    txid: detail.txid,
    order: detail.order,
    kind: detail.kind,
    isCoinbase: detail.isCoinbase,
    fluxnodeType: detail.fluxnodeType ?? null,
    fluxnodeTier: detail.fluxnodeTier ?? null,
    fluxnodeIp: detail.fluxnodeIp ?? null,
    fluxnodePubKey: detail.fluxnodePubKey ?? null,
    fluxnodeSignature: detail.fluxnodeSignature ?? null,
    valueSat: detail.valueSat ?? 0,
    value: detail.value ?? 0,
    feeSat: detail.feeSat ?? 0,
    fee: detail.fee ?? 0,
    size: detail.size ?? 0,
    fromAddr: detail.fromAddr ?? null,
    toAddr: detail.toAddr ?? null,
  })) || [];

  const txSummary = bbBlock.txSummary
    ? {
        total: bbBlock.txSummary.total,
        regular: bbBlock.txSummary.regular,
        coinbase: bbBlock.txSummary.coinbase,
        transfers: bbBlock.txSummary.transfers,
        fluxnodeStart: bbBlock.txSummary.fluxnodeStart,
        fluxnodeConfirm: bbBlock.txSummary.fluxnodeConfirm,
        fluxnodeOther: bbBlock.txSummary.fluxnodeOther,
        fluxnodeTotal: bbBlock.txSummary.fluxnodeTotal,
        tierCounts: {
          cumulus: bbBlock.txSummary.tierCounts.cumulus,
          nimbus: bbBlock.txSummary.tierCounts.nimbus,
          stratus: bbBlock.txSummary.tierCounts.stratus,
          starting: bbBlock.txSummary.tierCounts.starting,
          unknown: bbBlock.txSummary.tierCounts.unknown,
        },
      }
    : undefined;

  const txIds = txDetails.length > 0
    ? txDetails.map((detail) => detail.txid)
    : (bbBlock.txs?.map((tx: { txid: string }) => tx.txid) || []);

  return {
    hash: bbBlock.hash,
    size: bbBlock.size || 0,
    height: bbBlock.height,
    version: bbBlock.version || 0,
    merkleroot: bbBlock.merkleRoot || '',
    tx: txIds,
    txDetails,
    txSummary,
    time: bbBlock.time || 0,
    nonce: bbBlock.nonce || '0',
    bits: bbBlock.bits || '',
    difficulty: parseDifficulty(bbBlock.difficulty),
    chainwork: bbBlock.chainWork || '',
    confirmations: bbBlock.confirmations || 0,
    previousblockhash: bbBlock.previousBlockHash,
    nextblockhash: bbBlock.nextBlockHash,
    reward: bbBlock.reward ? satoshisToFlux(bbBlock.reward) : 0,
    isMainChain: true,
  };
}

/**
 * Convert FluxIndexer block summary to our BlockSummary type
 */
export function convertFluxIndexerBlockSummary(bbBlock: FluxIndexerBlock): BlockSummary {
  return {
    height: bbBlock.height,
    hash: bbBlock.hash,
    time: bbBlock.time || 0,
    txlength: bbBlock.txCount || 0,
    size: bbBlock.size || 0,
  };
}

/**
 * Convert FluxIndexer address to our Address type
 */
export function convertFluxIndexerAddress(bbAddr: FluxIndexerAddress): AddressInfo {
  return {
    addrStr: bbAddr.address,
    balance: satoshisToFlux(bbAddr.balance || '0'),
    balanceSat: parseInt(bbAddr.balance || '0'),
    totalReceived: satoshisToFlux(bbAddr.totalReceived || '0'),
    totalReceivedSat: parseInt(bbAddr.totalReceived || '0'),
    totalSent: satoshisToFlux(bbAddr.totalSent || '0'),
    totalSentSat: parseInt(bbAddr.totalSent || '0'),
    unconfirmedBalance: satoshisToFlux(bbAddr.unconfirmedBalance || '0'),
    unconfirmedBalanceSat: parseInt(bbAddr.unconfirmedBalance || '0'),
    unconfirmedTxApperances: bbAddr.unconfirmedTxs || 0,
    txApperances: bbAddr.txs || 0,
    transactions: bbAddr.transactions?.map((tx: { txid: string }) => tx.txid) || [],
    cumulusCount: bbAddr.cumulusCount,
    nimbusCount: bbAddr.nimbusCount,
    stratusCount: bbAddr.stratusCount,
    fluxnodeLastSync: bbAddr.fluxnodeLastSync,
  };
}
