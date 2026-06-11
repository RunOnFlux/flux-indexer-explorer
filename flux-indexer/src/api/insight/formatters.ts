import type {
  InsightAddressSummaryRow,
  InsightBlockRow,
  InsightFluxnodeTransactionRow,
  InsightInputRow,
  InsightOutputRow,
  InsightTxRow,
  InsightUtxoRow,
} from './types';
import { toSafeInteger, zatoshisToFlux, zatoshisToFluxString, zatoshisToSafeNumber } from './utils';

export interface FormatBlockInput {
  block: InsightBlockRow;
  txids: string[];
  confirmations: number;
  nextBlockHash?: string | null;
}

export interface FormatTransactionInput {
  tx: InsightTxRow;
  blockHash?: string | null;
  confirmations: number;
  inputs: InsightInputRow[];
  outputs: InsightOutputRow[];
  fluxnode?: InsightFluxnodeTransactionRow | null;
  coinbaseScript?: string | null;
}

export interface FormatAddressSummaryInput {
  address: string;
  summary?: InsightAddressSummaryRow | null;
  mempool?: {
    balanceDelta: bigint | string | number;
    txCount: number;
  };
  transactions?: string[];
}

export type FormatSupplyInput = bigint | string | number;
export type SupplyObjectKey = 'supply' | 'circulatingSupply';

export function formatBlock({
  block,
  txids,
  confirmations,
  nextBlockHash,
}: FormatBlockInput) {
  const producer = normalizeProducer(block.producer);

  return {
    hash: block.hash,
    size: block.size,
    height: block.height,
    version: block.version,
    merkleroot: block.merkle_root,
    tx: txids,
    txlength: block.tx_count ?? txids.length,
    time: block.timestamp,
    nonce: block.nonce ?? null,
    bits: block.bits,
    difficulty: Number(block.difficulty),
    chainwork: block.chainwork,
    confirmations,
    previousblockhash: formatPreviousBlockHash(block.prev_hash),
    nextblockhash: nextBlockHash ?? undefined,
    reward: zatoshisToFlux(block.producer_reward ?? 0),
    isMainChain: true,
    minedBy: producer,
    poolInfo: producer === null ? {} : {
      poolName: producer,
      url: null,
    },
  };
}

export function formatTransaction({
  tx,
  blockHash,
  confirmations,
  inputs,
  outputs,
  fluxnode,
  coinbaseScript,
}: FormatTransactionInput) {
  const isCoinbase = tx.is_coinbase === 1;

  return {
    txid: tx.txid,
    version: tx.version,
    locktime: toSafeInteger(tx.locktime, 'locktime'),
    vin: isCoinbase ? [formatCoinbaseInput(coinbaseScript)] : inputs.map(formatInput),
    vout: outputs.map(formatOutput),
    blockhash: blockHash ?? undefined,
    blockheight: tx.block_height,
    confirmations,
    time: tx.timestamp,
    blocktime: tx.timestamp,
    valueOut: zatoshisToFlux(tx.output_total),
    // Legacy Insight omits valueIn and fees on coinbase transactions. The
    // indexer stores the block's total collected fees on the coinbase row,
    // which would otherwise display as the coinbase tx's own fee.
    ...(isCoinbase ? {} : {
      valueIn: zatoshisToFlux(tx.input_total),
      fees: zatoshisToFlux(tx.fee),
    }),
    size: tx.size,
    isCoinBase: isCoinbase,
    isFluxnodeTx: tx.is_fluxnode_tx === 1,
    fluxnodeType: tx.fluxnode_type ?? null,
    ...(fluxnode ? { fluxnode: formatFluxnode(fluxnode) } : {}),
  };
}

export function formatAddressSummary({
  address,
  summary,
  mempool,
  transactions,
}: FormatAddressSummaryInput) {
  const balance = summary?.balance ?? 0n;
  const receivedTotal = summary?.received_total ?? 0n;
  const sentTotal = summary?.sent_total ?? 0n;
  const txCount = summary ? toSafeInteger(summary.tx_count, 'tx_count') : 0;
  const balanceSat = zatoshisToSafeNumber(balance);
  const receivedSat = zatoshisToSafeNumber(receivedTotal);
  const sentSat = zatoshisToSafeNumber(sentTotal);
  const unconfirmedBalance = mempool?.balanceDelta ?? 0n;
  const unconfirmedBalanceSat = zatoshisToSafeNumber(unconfirmedBalance);

  return {
    addrStr: address,
    balance: zatoshisToFlux(balance),
    balanceSat,
    totalReceived: zatoshisToFlux(receivedTotal),
    totalReceivedSat: receivedSat,
    totalSent: zatoshisToFlux(sentTotal),
    totalSentSat: sentSat,
    unconfirmedBalance: zatoshisToFlux(unconfirmedBalance),
    unconfirmedBalanceSat,
    unconfirmedTxApperances: mempool?.txCount ?? 0,
    txApperances: txCount,
    transactions: transactions ?? [],
  };
}

export function formatUtxo(row: InsightUtxoRow) {
  return {
    address: row.address,
    txid: row.txid,
    vout: row.vout,
    scriptPubKey: row.script_pubkey,
    amount: zatoshisToFlux(row.value),
    satoshis: zatoshisToSafeNumber(row.value),
    confirmations: row.confirmations,
    height: row.block_height,
    ts: row.timestamp,
  };
}

export function formatSupply(input: FormatSupplyInput): string;
export function formatSupply<K extends SupplyObjectKey>(input: FormatSupplyInput, objectKey: K): Record<K, string>;
export function formatSupply<K extends SupplyObjectKey>(
  input: FormatSupplyInput,
  objectKey?: K
): string | Record<K, string> {
  const textAmount = formatFluxText(input);

  if (objectKey) {
    return { [objectKey]: textAmount } as Record<K, string>;
  }

  return textAmount;
}

function formatInput(input: InsightInputRow, index: number) {
  return {
    txid: input.txid,
    vout: input.vout,
    sequence: input.sequence ?? 0xffffffff,
    n: index,
    scriptSig: {
      hex: input.script_sig?.hex ?? '',
      asm: input.script_sig?.asm ?? '',
    },
    addr: input.address,
    valueSat: zatoshisToSafeNumber(input.value),
    value: zatoshisToFlux(input.value),
    scriptType: input.script_type,
  };
}

// Field names mirror the /api/v1 transaction response so legacy consumers
// find familiar FluxNode metadata keys.
function formatFluxnode(row: InsightFluxnodeTransactionRow) {
  const collateralHash = row.collateral_hash?.trim() || '';

  return {
    nType: typeof row.type === 'number' ? row.type : null,
    ...(collateralHash ? {
      collateralOutputHash: collateralHash,
      collateralOutputIndex: Number(row.collateral_index ?? 0),
    } : {}),
    benchmarkTier: row.benchmark_tier?.trim() || null,
    ip: row.ip_address?.trim() || null,
    fluxnodePubKey: row.public_key?.trim() || null,
    sig: row.signature?.trim() || null,
    p2shAddress: row.p2sh_address?.trim() || null,
  };
}

function formatCoinbaseInput(coinbaseScript?: string | null) {
  return {
    coinbase: coinbaseScript ?? '',
    sequence: 0xffffffff,
    n: 0,
  };
}

function formatOutput(output: InsightOutputRow) {
  return {
    value: zatoshisToFluxString(output.value),
    n: output.vout,
    scriptPubKey: {
      hex: output.script_pubkey,
      asm: output.script_pubkey,
      addresses: formatOutputAddresses(output.address),
      type: output.script_type,
    },
    spentTxId: output.spent ? output.spent_txid ?? null : null,
    spentIndex: output.spent ? output.spent_index ?? null : null,
    spentHeight: output.spent ? output.spent_block_height ?? null : null,
  };
}

function formatFluxText(value: bigint | string | number): string {
  return zatoshisToFluxString(value).replace(/\.?0+$/, '');
}

function formatPreviousBlockHash(prevHash: string | null | undefined): string | null {
  const trimmedHash = prevHash?.trim() ?? '';

  if (trimmedHash.length === 0 || /^0+$/.test(trimmedHash)) {
    return null;
  }

  return trimmedHash;
}

function normalizeProducer(producer: string | null | undefined): string | null {
  const trimmedProducer = producer?.trim() ?? '';
  return trimmedProducer.length === 0 ? null : trimmedProducer;
}

function formatOutputAddresses(address: string | null | undefined): string[] {
  if (address === undefined || address === null) {
    return [];
  }

  const trimmedAddress = address.trim();
  if (
    trimmedAddress.length === 0
    || trimmedAddress === 'SHIELDED_OR_NONSTANDARD'
    || trimmedAddress === 'UNKNOWN'
  ) {
    return [];
  }

  return [trimmedAddress];
}
