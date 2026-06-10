import express, { type NextFunction, type Request, type Response, type Router } from 'express';
import {
  formatAddressSummary,
  formatBlock,
  formatSupply,
  formatTransaction,
  formatUtxo,
} from './formatters';
import type {
  InsightAddressSummaryServiceResult,
  InsightBlockServiceResult,
  InsightListBlocksServiceResult,
  InsightStatisticSeriesKind,
  InsightTransactionServiceResult,
} from './service';
import type { InsightUtxoRow } from './types';
import {
  type InsightRange,
  parseAddressList,
  parseRange,
  sendBadRequest,
  sendNotFound,
} from './utils';

export interface InsightAddressTransactionsResult {
  totalItems: number;
  items: unknown[];
}

export interface InsightAddressBalanceSumResult {
  balance: string | number;
  unconfirmedBalance: string | number;
  immature: string | number;
}

export interface InsightRouterService {
  getBlock(heightOrHash: string | number): Promise<InsightBlockServiceResult | null>;
  getBlockHashByHeight(height: number): Promise<string | null>;
  getRawBlock(heightOrHash: string | number): Promise<string | null>;
  listBlocks(query: Record<string, unknown>): Promise<InsightListBlocksServiceResult>;
  getTransaction(txid: string): Promise<InsightTransactionServiceResult | null>;
  getRawTransaction(txid: string): Promise<string | null>;
  getTransactionsByBlock?(blockHash: string): Promise<unknown[]>;
  getTransactionsByAddress?(address: string): Promise<unknown[]>;
  getAddressSummary(address: string, noTxList: boolean): Promise<InsightAddressSummaryServiceResult>;
  getAddressUtxos(addresses: string[], queryMempool: boolean): Promise<InsightUtxoRow[]>;
  getAddressTransactions?(addresses: string[], range: InsightRange): Promise<InsightAddressTransactionsResult>;
  getAddressBalanceSum?(addresses: string[]): Promise<InsightAddressBalanceSumResult>;
  sendRawTransaction(rawtx: string): Promise<string>;
  getStatus?(query: string | undefined): Promise<unknown>;
  getSync?(): Promise<unknown>;
  getPeer?(): unknown | Promise<unknown>;
  getVersion?(): Promise<unknown>;
  estimateFees?(targets: number[]): Promise<Record<number, number>>;
  verifyMessage?(address: string, signature: string, message: string): Promise<boolean>;
  listFluxNodes?(filter?: string): Promise<unknown>;
  getSupply?(): Promise<string>;
  getCurrency?(): unknown | Promise<unknown>;
  getMarketsInfo?(): unknown | Promise<unknown>;
  dosList?(): Promise<unknown>;
  startList?(): Promise<unknown>;
  getStatisticSeries?(kind: InsightStatisticSeriesKind, rawDays?: string): Promise<unknown>;
  getStatisticsTotal?(): Promise<unknown>;
  getPools?(dateRaw?: string): Promise<unknown>;
  getPoolsLastHour?(): Promise<unknown>;
  getBalanceIntervals?(): Promise<unknown>;
  getRicherThan?(): Promise<unknown>;
  getRichestAddressesList?(): Promise<unknown>;
}

type AsyncRouteHandler = (req: Request, res: Response) => Promise<void>;

const FLUXNODE_COLLATERAL_ZATOSHIS = new Set([
  1000n * 100000000n,
  10000n * 100000000n,
  12500n * 100000000n,
  25000n * 100000000n,
  40000n * 100000000n,
  100000n * 100000000n,
]);
const MAX_FEE_TARGETS = 20;
const MAX_FEE_TARGET_BLOCKS = 1008;

export function createInsightCompatibilityRouter(service: InsightRouterService): Router {
  const router = express.Router();
  router.use(express.urlencoded({ extended: false, limit: '2mb' }));

  router.get('/block/:blockHash', asyncHandler(async (req, res) => {
    const result = await service.getBlock(req.params.blockHash);
    if (result === null) {
      sendNotFound(res, req.originalUrl);
      return;
    }

    res.json(formatBlock(result));
  }));

  router.get('/block-index/:height', asyncHandler(async (req, res) => {
    const height = parseNonNegativeSafeInteger(req.params.height);
    if (height === null) {
      sendBadRequest(res, 'Invalid block height');
      return;
    }

    const blockHash = await service.getBlockHashByHeight(height);
    if (blockHash === null) {
      sendNotFound(res, req.originalUrl);
      return;
    }

    res.json({ blockHash });
  }));

  router.get('/rawblock/:blockHashOrHeight', asyncHandler(async (req, res) => {
    const rawblock = await service.getRawBlock(req.params.blockHashOrHeight);
    if (rawblock === null) {
      sendNotFound(res, req.originalUrl);
      return;
    }

    res.json({ rawblock });
  }));

  router.get('/blocks', asyncHandler(async (req, res) => {
    let result: InsightListBlocksServiceResult;
    try {
      result = await service.listBlocks(toRecord(req.query));
    } catch (error) {
      const message = errorMessage(error, 'Invalid blocks query');
      if (isBlockDateValidationError(message)) {
        sendBadRequest(res, message);
        return;
      }

      throw error;
    }

    const blocks = result.blocks.map(formatBlockListItem);
    const blockDate = result.blockDate;

    res.json({
      blocks,
      length: blocks.length,
      pagination: blockDate
        ? {
          next: blockDate.next,
          prev: blockDate.prev,
          currentTs: blockDate.end,
          current: blockDate.current,
          isToday: blockDate.current === new Date().toISOString().slice(0, 10),
          more: blocks.length > 0,
          moreTs: blockDate.end + 1,
        }
        : {
          next: null,
          prev: null,
          currentTs: blocks[0]?.time ?? null,
          current: null,
          isToday: true,
          more: blocks.length > 0,
          moreTs: blocks[0]?.time ?? null,
        },
    });
  }));

  router.get('/tx/:txid', asyncHandler(async (req, res) => {
    const result = await service.getTransaction(req.params.txid);
    if (result === null) {
      sendNotFound(res, req.originalUrl);
      return;
    }

    res.json(formatTransaction(result));
  }));

  router.get('/rawtx/:txid', asyncHandler(async (req, res) => {
    const rawtx = await service.getRawTransaction(req.params.txid);
    if (rawtx === null) {
      sendNotFound(res, req.originalUrl);
      return;
    }

    res.json({ rawtx });
  }));

  router.get('/txs', asyncHandler(async (req, res) => {
    if (typeof req.query.block === 'string') {
      if (!service.getTransactionsByBlock) {
        sendNotImplemented(res, 'Block transaction lookup is not implemented');
        return;
      }

      const txs = await service.getTransactionsByBlock(req.query.block);
      res.json({ pagesTotal: txs.length > 0 ? 1 : 0, txs: txs.map(formatTransactionListItem) });
      return;
    }

    if (typeof req.query.address === 'string') {
      if (!service.getTransactionsByAddress) {
        sendNotImplemented(res, 'Address transaction lookup is not implemented');
        return;
      }

      const txs = await service.getTransactionsByAddress(req.query.address);
      res.json({ pagesTotal: txs.length > 0 ? 1 : 0, txs: txs.map(formatTransactionListItem) });
      return;
    }

    res.json({ pagesTotal: 0, txs: [] });
  }));

  router.post('/tx/send', asyncHandler(async (req, res) => {
    const rawtx = req.body?.rawtx;
    if (typeof rawtx !== 'string' || rawtx.trim() === '') {
      sendBadRequest(res, 'Missing rawtx');
      return;
    }

    let txid: string;
    try {
      txid = await service.sendRawTransaction(rawtx.trim());
    } catch (error) {
      sendBadRequest(res, errorMessage(error, 'Transaction broadcast failed'));
      return;
    }

    res.json({ txid });
  }));

  router.get('/addr/:addr', asyncHandler(async (req, res) => {
    const result = await service.getAddressSummary(req.params.addr, isNoTxList(req.query.noTxList));
    res.json(formatAddressSummary({
      address: req.params.addr,
      summary: result.summary,
      mempool: result.mempool,
      transactions: result.transactions,
    }));
  }));

  router.get('/addr/:addr/balance', asyncHandler(async (req, res) => {
    const result = await service.getAddressSummary(req.params.addr, true);
    sendPlainText(res, satoshiText(result.summary?.balance));
  }));

  router.get('/addr/:addr/totalReceived', asyncHandler(async (req, res) => {
    const result = await service.getAddressSummary(req.params.addr, true);
    sendPlainText(res, satoshiText(result.summary?.received_total));
  }));

  router.get('/addr/:addr/totalSent', asyncHandler(async (req, res) => {
    const result = await service.getAddressSummary(req.params.addr, true);
    sendPlainText(res, satoshiText(result.summary?.sent_total));
  }));

  router.get('/addr/:addr/unconfirmedBalance', asyncHandler(async (req, res) => {
    const result = await service.getAddressSummary(req.params.addr, true);
    sendPlainText(res, satoshiText(result.mempool?.balanceDelta));
  }));

  router.get('/addr/:addr/utxo', asyncHandler(async (req, res) => {
    const rows = await service.getAddressUtxos([req.params.addr], true);
    res.json(rows.map(formatUtxo));
  }));

  router.get('/addrs/:addrs/utxo', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    const rows = await service.getAddressUtxos(addresses, true);
    res.json(rows.map(formatUtxo));
  }));

  router.get('/addrs/:addrs/unspent', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    const rows = await service.getAddressUtxos(addresses, false);
    res.json(rows.map(formatUtxo));
  }));

  router.post('/addrs/utxo', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(undefined, req.body);
    const rows = await service.getAddressUtxos(addresses, true);
    res.json(rows.map(formatUtxo));
  }));

  router.get('/addrs/:addrs/txs', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    const range = parseRange(toRecord(req.query));
    if (!service.getAddressTransactions) {
      sendNotImplemented(res, 'Address transaction lookup is not implemented');
      return;
    }

    const result = await service.getAddressTransactions(addresses, range);

    res.json(formatAddressTransactions(result, range));
  }));

  router.post('/addrs/txs', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(undefined, req.body);
    const range = parseRange({ ...toRecord(req.query), ...toRecord(req.body) });
    if (!service.getAddressTransactions) {
      sendNotImplemented(res, 'Address transaction lookup is not implemented');
      return;
    }

    const result = await service.getAddressTransactions(addresses, range);

    res.json(formatAddressTransactions(result, range));
  }));

  router.get('/addrs/:addrs/balance', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    if (!service.getAddressBalanceSum) {
      sendNotImplemented(res, 'Address balance lookup is not implemented');
      return;
    }

    const result = await service.getAddressBalanceSum(addresses);

    res.json(result);
  }));

  router.get('/status', asyncHandler(async (req, res) => {
    if (!service.getStatus) {
      sendNotImplemented(res, 'Status lookup is not implemented');
      return;
    }

    res.json(await service.getStatus(firstString(req.query.q)));
  }));

  router.get('/sync', asyncHandler(async (_req, res) => {
    if (!service.getSync) {
      sendNotImplemented(res, 'Sync status lookup is not implemented');
      return;
    }

    res.json(await service.getSync());
  }));

  router.get('/peer', asyncHandler(async (_req, res) => {
    if (!service.getPeer) {
      sendNotImplemented(res, 'Peer lookup is not implemented');
      return;
    }

    res.json(await service.getPeer());
  }));

  router.get('/version', asyncHandler(async (_req, res) => {
    if (!service.getVersion) {
      sendNotImplemented(res, 'Version lookup is not implemented');
      return;
    }

    res.json(await service.getVersion());
  }));

  router.get('/utils/estimatefee', asyncHandler(async (req, res) => {
    if (!service.estimateFees) {
      sendNotImplemented(res, 'Fee estimation is not implemented');
      return;
    }

    const targets = parseFeeTargets(req.query.nbBlocks);
    if (targets === null) {
      sendBadRequest(res, 'Invalid nbBlocks');
      return;
    }

    res.json(await service.estimateFees(targets));
  }));

  router.all('/messages/verify', asyncHandler(async (req, res) => {
    const address = trimmedString(req.body?.address) ?? trimmedString(req.query.address);
    const signature = trimmedString(req.body?.signature) ?? trimmedString(req.query.signature);
    const message = rawString(req.body?.message) ?? rawString(req.query.message);
    if (!address || !signature || message === undefined || message.trim().length === 0) {
      sendBadRequest(res, 'Missing address, signature, or message');
      return;
    }

    if (!service.verifyMessage) {
      sendNotImplemented(res, 'Message verification is not implemented');
      return;
    }

    try {
      res.json({ result: await service.verifyMessage(address, signature, message) });
    } catch (error) {
      if (!isMessageVerificationInputError(error)) {
        throw error;
      }

      sendBadRequest(res, errorMessage(error, 'Message verification failed'));
    }
  }));

  const listFluxNodesHandler = asyncHandler(async (req, res) => {
    if (!service.listFluxNodes) {
      sendNotImplemented(res, 'FluxNode list lookup is not implemented');
      return;
    }

    res.json(await service.listFluxNodes(fluxNodeFilter(req)));
  });
  router.get('/fluxnode/listfluxnodes', listFluxNodesHandler);
  router.get('/fluxnode/listfluxnodes/:filter', listFluxNodesHandler);
  router.post('/fluxnode/listfluxnodes', listFluxNodesHandler);
  router.post('/fluxnode/listfluxnodes/:filter', listFluxNodesHandler);
  router.get('/zelnode/listfluxnodes', listFluxNodesHandler);
  router.get('/zelnode/listfluxnodes/:filter', listFluxNodesHandler);
  router.post('/zelnode/listfluxnodes', listFluxNodesHandler);
  router.post('/zelnode/listfluxnodes/:filter', listFluxNodesHandler);

  router.get('/fluxnode/addrs/:addrs/utxo', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    const rows = await service.getAddressUtxos(addresses, true);
    res.json(rows.filter(isFluxNodeCollateralUtxo).map(formatUtxo));
  }));

  router.post('/fluxnode/addrs/utxo', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(undefined, req.body);
    const rows = await service.getAddressUtxos(addresses, true);
    res.json(rows.filter(isFluxNodeCollateralUtxo).map(formatUtxo));
  }));

  router.get('/fluxnode/doslist', asyncHandler(async (_req, res) => {
    if (!service.dosList) {
      sendNotImplemented(res, 'FluxNode DoS list lookup is not implemented');
      return;
    }

    res.json(await service.dosList());
  }));

  router.get('/fluxnode/startlist', asyncHandler(async (_req, res) => {
    if (!service.startList) {
      sendNotImplemented(res, 'FluxNode start list lookup is not implemented');
      return;
    }

    res.json(await service.startList());
  }));

  const supplyHandler = asyncHandler(async (req, res) => {
    if (!service.getSupply) {
      sendNotImplemented(res, 'Supply lookup is not implemented');
      return;
    }

    const supply = await service.getSupply();
    if (firstString(req.query.format)?.toLowerCase() === 'object') {
      res.json(formatSupply(supply, supplyObjectKey(req.path)));
      return;
    }

    sendPlainText(res, formatSupply(supply));
  });
  router.get('/supply', supplyHandler);
  router.get('/total-supply', supplyHandler);
  router.get('/statistics/total-supply', supplyHandler);

  const circulatingSupplyHandler = (_req: Request, res: Response) => {
    sendNotImplemented(res, 'Circulating supply lookup is not implemented');
  };
  router.get('/circulating-supply', circulatingSupplyHandler);
  router.get('/circulation', circulatingSupplyHandler);
  router.get('/statistics/circulating-supply', circulatingSupplyHandler);
  router.get('/statistics/main-chain-circulating-locked', circulatingSupplyHandler);

  const statisticSeriesHandler = (kind: InsightStatisticSeriesKind) => asyncHandler(async (req, res) => {
    if (!service.getStatisticSeries) {
      sendNotImplemented(res, 'Statistic series lookup is not implemented');
      return;
    }

    res.json(await service.getStatisticSeries(kind, firstString(req.query.days)));
  });
  router.get('/statistics/supply', statisticSeriesHandler('supply'));
  router.get('/statistics/fees', statisticSeriesHandler('fees'));
  router.get('/statistics/network-hash', (_req, res) => {
    sendNotImplemented(res, 'Network hash statistics lookup is not implemented');
  });
  router.get('/statistics/transactions', statisticSeriesHandler('transactions'));
  router.get('/statistics/outputs', statisticSeriesHandler('outputs'));
  router.get('/statistics/difficulty', statisticSeriesHandler('difficulty'));
  router.get('/statistics/active-addresses', statisticSeriesHandler('active-addresses'));

  router.get('/statistics/pools', asyncHandler(async (req, res) => {
    if (!service.getPools) {
      sendNotImplemented(res, 'Pool statistics lookup is not implemented');
      return;
    }

    try {
      res.json(await service.getPools(firstString(req.query.date) ?? firstString(req.query.blockDate)));
    } catch (error) {
      const message = errorMessage(error, 'Invalid pool statistics query');
      if (isBlockDateValidationError(message)) {
        sendBadRequest(res, message);
        return;
      }

      throw error;
    }
  }));

  router.get('/statistics/pools-last-hour', asyncHandler(async (_req, res) => {
    if (!service.getPoolsLastHour) {
      sendNotImplemented(res, 'Pool statistics lookup is not implemented');
      return;
    }

    res.json(await service.getPoolsLastHour());
  }));

  router.get('/statistics/total', asyncHandler(async (_req, res) => {
    if (!service.getStatisticsTotal) {
      sendNotImplemented(res, 'Total statistics lookup is not implemented');
      return;
    }

    res.json(await service.getStatisticsTotal());
  }));

  router.get('/statistics/balance-intervals', asyncHandler(async (_req, res) => {
    if (!service.getBalanceIntervals) {
      sendNotImplemented(res, 'Balance interval lookup is not implemented');
      return;
    }

    res.json(await service.getBalanceIntervals());
  }));

  router.get('/statistics/richer-than', asyncHandler(async (_req, res) => {
    if (!service.getRicherThan) {
      sendNotImplemented(res, 'Richer-than statistics lookup is not implemented');
      return;
    }

    res.json(await service.getRicherThan());
  }));

  router.get('/statistics/richest-addresses-list', asyncHandler(async (_req, res) => {
    if (!service.getRichestAddressesList) {
      sendNotImplemented(res, 'Richest addresses lookup is not implemented');
      return;
    }

    res.json(await service.getRichestAddressesList());
  }));

  router.get('/currency', asyncHandler(async (_req, res) => {
    if (!service.getCurrency) {
      sendNotImplemented(res, 'Currency lookup is not implemented');
      return;
    }

    res.json(await service.getCurrency());
  }));

  router.get('/markets/info', asyncHandler(async (_req, res) => {
    if (!service.getMarketsInfo) {
      sendNotImplemented(res, 'Markets info lookup is not implemented');
      return;
    }

    res.json(await service.getMarketsInfo());
  }));

  router.use((req, res) => {
    sendNotFound(res, req.originalUrl);
  });

  return router;
}

function asyncHandler(fn: AsyncRouteHandler) {
  return (req: Request, res: Response, next: NextFunction) => {
    fn(req, res).catch(next);
  };
}

function formatBlockListItem(block: InsightListBlocksServiceResult['blocks'][number]) {
  const producer = typeof block.producer === 'string' && block.producer.trim().length > 0
    ? block.producer.trim()
    : null;

  return {
    height: block.height,
    size: block.size,
    hash: block.hash,
    time: block.timestamp,
    txlength: block.tx_count ?? 0,
    poolInfo: producer === null ? {} : {
      poolName: producer,
      url: null,
    },
  };
}

function formatAddressTransactions(result: InsightAddressTransactionsResult, range: InsightRange) {
  const totalItems = normalizeTotalItems(result.totalItems, result.items.length);

  return {
    totalItems,
    from: range.from,
    to: Math.min(range.to, totalItems),
    items: result.items.map(formatTransactionListItem),
  };
}

function formatTransactionListItem(item: unknown): unknown {
  return isTransactionServiceResult(item) ? formatTransaction(item) : item;
}

function isTransactionServiceResult(value: unknown): value is InsightTransactionServiceResult {
  return isRecord(value)
    && isRecord(value.tx)
    && Array.isArray(value.inputs)
    && Array.isArray(value.outputs)
    && typeof value.confirmations === 'number';
}

function normalizeTotalItems(totalItems: unknown, fallback: number): number {
  if (typeof totalItems === 'number' && Number.isSafeInteger(totalItems) && totalItems >= 0) {
    return totalItems;
  }

  if (typeof totalItems === 'string' && /^\d+$/.test(totalItems.trim())) {
    const parsed = Number(totalItems);
    if (Number.isSafeInteger(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function parseNonNegativeSafeInteger(raw: string): number | null {
  const trimmed = raw.trim();
  if (!/^\d+$/.test(trimmed)) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function parseFeeTargets(raw: unknown): number[] | null {
  const parts = stringParamParts(raw);
  const rawTargets = (parts.length > 0 ? parts : ['2'])
    .flatMap((part) => part.split(','))
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  if (rawTargets.length === 0) {
    return [2];
  }

  const targets: number[] = [];
  const seen = new Set<number>();
  for (const rawTarget of rawTargets) {
    const target = parseNonNegativeSafeInteger(rawTarget);
    if (target === null || target <= 0 || target > MAX_FEE_TARGET_BLOCKS) {
      return null;
    }

    if (seen.has(target)) {
      continue;
    }

    seen.add(target);
    targets.push(target);
    if (targets.length > MAX_FEE_TARGETS) {
      return null;
    }
  }

  return targets;
}

function stringParamParts(raw: unknown): string[] {
  if (raw === undefined || raw === null) {
    return [];
  }

  if (Array.isArray(raw)) {
    return raw.flatMap(stringParamParts);
  }

  return [typeof raw === 'string' ? raw : String(raw)];
}

function fluxNodeFilter(req: Request): string | undefined {
  return firstString(req.params.filter)
    ?? firstString(req.body?.filter)
    ?? firstString(req.body?.filters)
    ?? firstString(req.body?.node)
    ?? firstString(req.query.filter);
}

function isFluxNodeCollateralUtxo(row: InsightUtxoRow): boolean {
  const value = zatoshiBigInt(row.value);
  return value !== null && FLUXNODE_COLLATERAL_ZATOSHIS.has(value);
}

function zatoshiBigInt(value: string | number): bigint | null {
  if (typeof value === 'number') {
    return Number.isSafeInteger(value) && value >= 0 ? BigInt(value) : null;
  }

  const trimmed = value.trim();
  if (!/^\d+$/.test(trimmed)) {
    return null;
  }

  return BigInt(trimmed);
}

function supplyObjectKey(path: string): 'supply' | 'circulatingSupply' {
  return path.includes('total-supply') ? 'supply' : 'circulatingSupply';
}

function satoshiText(value: unknown): string {
  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) ? String(value) : '0';
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : '0';
  }

  return '0';
}

function sendPlainText(res: Response, value: string): void {
  res.type('text/plain').send(value);
}

function sendNotImplemented(res: Response, message: string): void {
  res.status(501).json({ message, code: 1 });
}

function errorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  if (isRecord(error) && typeof error.message === 'string' && error.message.length > 0) {
    return error.message;
  }

  return fallback;
}

function isMessageVerificationInputError(error: unknown): boolean {
  const message = errorMessage(error, '').toLowerCase();
  if (
    message.includes('invalid address')
    || message.includes('invalid signature')
    || message.includes('malformed')
    || message.includes('bad signature')
  ) {
    return true;
  }

  const code = errorCode(error);
  return code === -32602 || code === -5;
}

function errorCode(error: unknown): number | null {
  if (!isRecord(error)) {
    return null;
  }

  if (typeof error.rpcCode === 'number' && Number.isFinite(error.rpcCode)) {
    return error.rpcCode;
  }

  return typeof error.code === 'number' && Number.isFinite(error.code) ? error.code : null;
}

function isBlockDateValidationError(message: string): boolean {
  return message.includes('Invalid blockDate');
}

function isNoTxList(value: unknown): boolean {
  const raw = firstString(value)?.trim().toLowerCase();
  return raw === '1' || raw === 'true';
}

function firstString(value: unknown): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }

  if (Array.isArray(value)) {
    return firstString(value[0]);
  }

  return typeof value === 'string' ? value : String(value);
}

function trimmedString(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function rawString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function toRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
