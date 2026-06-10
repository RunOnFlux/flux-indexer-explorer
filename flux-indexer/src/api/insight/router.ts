import express, { type NextFunction, type Request, type Response, type Router } from 'express';
import {
  formatAddressSummary,
  formatBlock,
  formatTransaction,
  formatUtxo,
} from './formatters';
import type {
  InsightAddressSummaryServiceResult,
  InsightBlockServiceResult,
  InsightListBlocksServiceResult,
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
}

type AsyncRouteHandler = (req: Request, res: Response) => Promise<void>;

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

function toRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
