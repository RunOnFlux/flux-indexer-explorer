import express from 'express';
import { RPCError } from '../../../types';
import { ClickHouseAPIServer } from '../../server';
import { createInsightCompatibilityRouter, type InsightRouterService } from '../router';
import { InsightValidationError } from '../utils';
import { readJson, withTestServer } from './http-test-utils';

type MockInsightRouterService = {
  [K in keyof InsightRouterService]: InsightRouterService[K] extends (...args: infer Args) => infer Result
    ? jest.Mock<Result, Args>
    : InsightRouterService[K];
};

function createApp(serviceOverrides: Partial<MockInsightRouterService> & Record<string, any> = {}) {
  const app = express();
  app.use(express.json());

  const service = {
    getBlock: jest.fn(),
    getBlockHashByHeight: jest.fn(),
    getRawBlock: jest.fn(),
    listBlocks: jest.fn(),
    getTransaction: jest.fn(),
    getRawTransaction: jest.fn(),
    getTransactionsByBlock: jest.fn(),
    getAddressSummary: jest.fn(),
    getAddressUtxos: jest.fn(),
    getAddressTransactions: jest.fn(),
    getAddressBalanceSum: jest.fn(),
    sendRawTransaction: jest.fn(),
    getStatisticSeries: jest.fn(),
    getStatisticsTotal: jest.fn(),
    getPools: jest.fn(),
    getPoolsLastHour: jest.fn(),
    getBalanceIntervals: jest.fn(),
    getRicherThan: jest.fn(),
    getRichestAddressesList: jest.fn(),
    ...serviceOverrides,
  } as MockInsightRouterService & Record<string, any>;

  app.use('/insight-api', createInsightCompatibilityRouter(service));
  return { app, service };
}

describe('Insight server mount', () => {
  function createServerApp(rpcOverrides: Record<string, jest.Mock> = {}) {
    const ch = {
      query: jest.fn(),
      queryOne: jest.fn(),
      queryCount: jest.fn(),
    } as unknown as ConstructorParameters<typeof ClickHouseAPIServer>[0];
    const rpc = rpcOverrides as unknown as ConstructorParameters<typeof ClickHouseAPIServer>[1];
    const syncEngine = {} as unknown as ConstructorParameters<typeof ClickHouseAPIServer>[2];
    const server = new ClickHouseAPIServer(ch, rpc, syncEngine, 0);
    return server.getApp();
  }

  test('returns legacy 404 for unmatched Insight GET paths', async () => {
    const app = createServerApp();
    await withTestServer(app, async (baseUrl) => {
      const insightResponse = await fetch(`${baseUrl}/insight-api/not-real`);

      expect(insightResponse.status).toBe(404);
      await expect(readJson(insightResponse)).resolves.toEqual({
        status: 404,
        url: '/insight-api/not-real',
        error: 'Not found',
      });
    });
  });

  test('returns legacy 404 for unmatched Insight POST paths', async () => {
    const app = createServerApp();
    await withTestServer(app, async (baseUrl) => {
      const insightResponse = await fetch(`${baseUrl}/insight-api/not-real`, { method: 'POST' });

      expect(insightResponse.status).toBe(404);
      await expect(readJson(insightResponse)).resolves.toEqual({
        status: 404,
        url: '/insight-api/not-real',
        error: 'Not found',
      });
    });
  });

  test('returns legacy 404 for exact Insight mount path', async () => {
    const app = createServerApp();
    await withTestServer(app, async (baseUrl) => {
      const insightResponse = await fetch(`${baseUrl}/insight-api`);

      expect(insightResponse.status).toBe(404);
      await expect(readJson(insightResponse)).resolves.toEqual({
        status: 404,
        url: '/insight-api',
        error: 'Not found',
      });
    });
  });

  test('preserves API 404 body', async () => {
    const app = createServerApp();
    await withTestServer(app, async (baseUrl) => {
      const apiResponse = await fetch(`${baseUrl}/api/not-real`);

      expect(apiResponse.status).toBe(404);
      await expect(readJson(apiResponse)).resolves.toEqual({ error: 'Not found' });
    });
  });

  test('POST /tx/send broadcasts JSON rawtx larger than 100KB', async () => {
    const rawtx = 'ab'.repeat(75000);
    const sendRawTransaction = jest.fn().mockResolvedValue('large-json-txid');
    const app = createServerApp({ sendRawTransaction });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ rawtx }),
      });

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ txid: 'large-json-txid' });
      expect(sendRawTransaction).toHaveBeenCalledWith(rawtx);
    });
  });

  test('POST /tx/send returns Insight 413 for JSON bodies above the router limit', async () => {
    const sendRawTransaction = jest.fn();
    const app = createServerApp({ sendRawTransaction });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ rawtx: 'ab'.repeat(1200000) }),
      });

      expect(response.status).toBe(413);
      await expect(readJson(response)).resolves.toEqual({
        message: 'request entity too large',
        code: 1,
      });
      expect(sendRawTransaction).not.toHaveBeenCalled();
    });
  });

  test('POST /tx/send returns Insight 400 for malformed JSON bodies', async () => {
    const sendRawTransaction = jest.fn();
    const app = createServerApp({ sendRawTransaction });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{"rawtx":',
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: expect.any(String),
        code: 1,
      });
      expect(sendRawTransaction).not.toHaveBeenCalled();
    });
  });

  test('API error handler honors body-parser client error statuses', async () => {
    const app = createServerApp();

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/api/v1/transactions/batch`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{"txids":',
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({ error: expect.any(String) });
    });
  });

  test('GET /currency returns the legacy currency shape', async () => {
    const app = createServerApp();

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/currency`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({
        status: 200,
        data: { rate: null, short: 'FLUX' },
      });
    });
  });
});

describe('Insight core routes', () => {
  test('GET /block/:hash returns legacy block shape', async () => {
    const { app, service } = createApp({
      getBlock: jest.fn().mockResolvedValue({
        block: {
          hash: 'hash',
          height: 1,
          size: 10,
          version: 1,
          merkle_root: 'merk',
          timestamp: 100,
          bits: 'bits',
          difficulty: 1,
          chainwork: 'work',
          prev_hash: 'prev',
          producer_reward: '5000000000',
          producer: 'producer',
        },
        txids: ['tx'],
        confirmations: 2,
        nextBlockHash: null,
      }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/block/hash`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toMatchObject({
        hash: 'hash',
        merkleroot: 'merk',
        tx: ['tx'],
        reward: 50,
      });
      expect(service.getBlock).toHaveBeenCalledWith('hash');
    });
  });

  test('GET /block-index/:height returns blockHash', async () => {
    const { app, service } = createApp({
      getBlockHashByHeight: jest.fn().mockResolvedValue('blockhash'),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/block-index/9`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ blockHash: 'blockhash' });
      expect(service.getBlockHashByHeight).toHaveBeenCalledWith(9);
    });
  });

  test('GET /rawblock/:hash returns rawblock wrapper', async () => {
    const { app, service } = createApp({
      getRawBlock: jest.fn().mockResolvedValue('hex'),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/rawblock/abc`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ rawblock: 'hex' });
      expect(service.getRawBlock).toHaveBeenCalledWith('abc');
    });
  });

  test('GET /blocks builds date pagination from truncation and the oldest block', async () => {
    const listBlocks = jest.fn().mockResolvedValue({
      blocks: [
        { height: 102, hash: 'c'.repeat(64), timestamp: 1577900000, size: 200, tx_count: 2, producer: 'pool' },
        { height: 101, hash: 'b'.repeat(64), timestamp: 1577890000, size: 100, tx_count: 1, producer: '' },
      ],
      blockDate: {
        start: 1577836800,
        end: 1577923199,
        current: '2020-01-01',
        next: '2020-01-02',
        prev: '2019-12-31',
      },
      more: true,
    });
    const { app } = createApp({ listBlocks });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(
        `${baseUrl}/insight-api/blocks?blockDate=2020-01-01&startTimestamp=1577910000`
      );

      expect(response.status).toBe(200);
      const body = await readJson(response) as { length: number; pagination: unknown };
      expect(body.length).toBe(2);
      expect(body.pagination).toEqual({
        next: '2020-01-02',
        prev: '2019-12-31',
        currentTs: 1577923199,
        current: '2020-01-01',
        isToday: false,
        more: true,
        moreTs: 1577889999,
      });
      expect(listBlocks).toHaveBeenCalledWith(
        expect.objectContaining({ blockDate: '2020-01-01', startTimestamp: '1577910000' })
      );
    });
  });

  test('GET /blocks reports no further pages when the date window is exhausted', async () => {
    const listBlocks = jest.fn().mockResolvedValue({
      blocks: [],
      blockDate: {
        start: 1577836800,
        end: 1577923199,
        current: '2020-01-01',
        next: '2020-01-02',
        prev: '2019-12-31',
      },
      more: false,
    });
    const { app } = createApp({ listBlocks });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/blocks?blockDate=2020-01-01`);

      expect(response.status).toBe(200);
      const body = await readJson(response) as { pagination: { more: boolean; moreTs: number } };
      expect(body.pagination.more).toBe(false);
      expect(body.pagination.moreTs).toBe(1577923199);
    });
  });

  test('GET /blocks without blockDate keeps the recent-blocks pagination shape', async () => {
    const listBlocks = jest.fn().mockResolvedValue({
      blocks: [
        { height: 102, hash: 'c'.repeat(64), timestamp: 2000, size: 200, tx_count: 2, producer: '' },
        { height: 101, hash: 'b'.repeat(64), timestamp: 1500, size: 100, tx_count: 1, producer: '' },
      ],
      blockDate: null,
      more: true,
    });
    const { app } = createApp({ listBlocks });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/blocks`);

      expect(response.status).toBe(200);
      const body = await readJson(response) as { pagination: unknown };
      expect(body.pagination).toEqual({
        next: null,
        prev: null,
        currentTs: 2000,
        current: null,
        isToday: true,
        more: true,
        moreTs: 2000,
      });
    });
  });

  test('GET /blocks returns bad request when query parsing fails', async () => {
    const { app } = createApp({
      listBlocks: jest.fn().mockRejectedValue(new InsightValidationError('Invalid blockDate (expected YYYY-MM-DD)')),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/blocks?blockDate=invalid`);

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Invalid blockDate (expected YYYY-MM-DD)',
        code: 1,
      });
    });
  });

  test('GET /blocks treats non-validation errors mentioning blockDate as server errors', async () => {
    const { app } = createApp({
      listBlocks: jest.fn().mockRejectedValue(new Error('Invalid blockDate (expected YYYY-MM-DD)')),
    });
    const errorHandler: express.ErrorRequestHandler = (error, _req, res, _next) => {
      res.status(500).json({ error: error.message });
    };
    app.use(errorHandler);

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/blocks?blockDate=invalid`);

      expect(response.status).toBe(500);
      await expect(readJson(response)).resolves.toEqual({
        error: 'Invalid blockDate (expected YYYY-MM-DD)',
      });
    });
  });

  test('GET /blocks lets operational list errors reach error handling', async () => {
    const { app } = createApp({
      listBlocks: jest.fn().mockRejectedValue(new Error('ClickHouse unavailable')),
    });
    const errorHandler: express.ErrorRequestHandler = (error, _req, res, _next) => {
      res.status(500).json({ error: error.message });
    };
    app.use(errorHandler);

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/blocks`);

      expect(response.status).toBe(500);
      const body = await readJson(response);
      expect(body).toEqual({ error: 'ClickHouse unavailable' });
      expect(body).not.toEqual({ message: 'ClickHouse unavailable', code: 1 });
    });
  });

  test('GET /tx/:txid returns formatted transaction', async () => {
    const { app, service } = createApp({
      getTransaction: jest.fn().mockResolvedValue({
        tx: {
          txid: 'tx',
          version: 1,
          locktime: 0,
          block_height: 1,
          timestamp: 100,
          input_total: '2',
          output_total: '1',
          fee: '1',
          size: 1,
          is_coinbase: 0,
          is_fluxnode_tx: 0,
        },
        blockHash: 'block',
        confirmations: 1,
        inputs: [],
        outputs: [],
      }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/tx`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toMatchObject({
        txid: 'tx',
        blockhash: 'block',
        blockheight: 1,
      });
      expect(service.getTransaction).toHaveBeenCalledWith('tx');
    });
  });

  test('GET /txs?block returns not implemented when block transaction hook is missing', async () => {
    const { app } = createApp({
      getTransactionsByBlock: undefined,
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/txs?block=blockhash`);

      expect(response.status).toBe(501);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Block transaction lookup is not implemented',
        code: 1,
      });
    });
  });

  test('GET /txs?block returns paged block transactions from service hook', async () => {
    const { app, service } = createApp({
      getTransactionsByBlock: jest.fn().mockResolvedValue({
        pagesTotal: 3,
        txs: [{ txid: 'tx1' }, { txid: 'tx2' }],
      }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/txs?block=blockhash`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({
        pagesTotal: 3,
        txs: [{ txid: 'tx1' }, { txid: 'tx2' }],
      });
      expect(service.getTransactionsByBlock).toHaveBeenCalledWith('blockhash', 0);
    });
  });

  test('GET /txs?block forwards the requested pageNum', async () => {
    const { app, service } = createApp({
      getTransactionsByBlock: jest.fn().mockResolvedValue({ pagesTotal: 3, txs: [] }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/txs?block=blockhash&pageNum=2`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ pagesTotal: 3, txs: [] });
      expect(service.getTransactionsByBlock).toHaveBeenCalledWith('blockhash', 2);
    });
  });

  test('GET /txs rejects invalid pageNum values', async () => {
    const { app, service } = createApp({
      getTransactionsByBlock: jest.fn().mockResolvedValue({ pagesTotal: 1, txs: [] }),
    });

    await withTestServer(app, async (baseUrl) => {
      for (const pageNum of ['-1', 'abc', '1.5']) {
        const response = await fetch(`${baseUrl}/insight-api/txs?block=blockhash&pageNum=${pageNum}`);

        expect(response.status).toBe(400);
        await expect(readJson(response)).resolves.toEqual({ message: 'Invalid pageNum', code: 1 });
      }

      expect(service.getTransactionsByBlock).not.toHaveBeenCalled();
    });
  });

  test('GET /txs?address pages address transactions through the range hook', async () => {
    const { app, service } = createApp({
      getAddressTransactions: jest.fn().mockResolvedValue({
        totalItems: 25,
        items: [{ txid: 'tx1' }],
      }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/txs?address=addr1`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({
        pagesTotal: 3,
        txs: [{ txid: 'tx1' }],
      });
      expect(service.getAddressTransactions).toHaveBeenCalledWith(
        ['addr1'],
        { from: 0, to: 10, limit: 10 }
      );
    });
  });

  test('GET /txs?address forwards the requested pageNum window', async () => {
    const { app, service } = createApp({
      getAddressTransactions: jest.fn().mockResolvedValue({ totalItems: 25, items: [] }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/txs?address=addr1&pageNum=2`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ pagesTotal: 3, txs: [] });
      expect(service.getAddressTransactions).toHaveBeenCalledWith(
        ['addr1'],
        { from: 20, to: 30, limit: 10 }
      );
    });
  });

  test('GET /txs?address returns not implemented when address transaction hook is missing', async () => {
    const { app } = createApp({
      getAddressTransactions: undefined,
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/txs?address=addr1`);

      expect(response.status).toBe(501);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Address transaction lookup is not implemented',
        code: 1,
      });
    });
  });

  test('GET /addr/:addr honors noTxList', async () => {
    const { app, service } = createApp({
      getAddressSummary: jest.fn().mockResolvedValue({
        summary: { balance: '0', received_total: '0', sent_total: '0', tx_count: 0 },
        mempool: { balanceDelta: 0n, txCount: 0 },
        transactions: [],
      }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addr/addr?noTxList=1`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toMatchObject({
        addrStr: 'addr',
        transactions: [],
      });
      expect(service.getAddressSummary).toHaveBeenCalledWith('addr', true);
    });
  });

  test('POST /tx/send accepts JSON rawtx', async () => {
    const { app, service } = createApp({
      sendRawTransaction: jest.fn().mockResolvedValue('txid'),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ rawtx: 'abcd' }),
      });

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ txid: 'txid' });
      expect(service.sendRawTransaction).toHaveBeenCalledWith('abcd');
    });
  });

  test('POST /tx/send returns bad request when broadcast rejects', async () => {
    const { app } = createApp({
      sendRawTransaction: jest.fn().mockRejectedValue(new Error('bad-txns-inputs-missingorspent')),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ rawtx: 'abcd' }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: 'bad-txns-inputs-missingorspent',
        code: 1,
      });
    });
  });

  test('POST /tx/send rejects non-string JSON rawtx', async () => {
    const { app, service } = createApp({
      sendRawTransaction: jest.fn().mockResolvedValue('txid'),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ rawtx: {} }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({ message: 'Missing rawtx', code: 1 });
      expect(service.sendRawTransaction).not.toHaveBeenCalled();
    });
  });

  test('POST /tx/send accepts legacy form-encoded rawtx', async () => {
    const { app, service } = createApp({
      sendRawTransaction: jest.fn().mockResolvedValue('txid'),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ rawtx: '01000000' }),
      });

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ txid: 'txid' });
      expect(service.sendRawTransaction).toHaveBeenCalledWith('01000000');
    });
  });

  test('POST /tx/send accepts legacy form-encoded rawtx larger than 100KB', async () => {
    const rawtx = 'ab'.repeat(60000);
    const { app, service } = createApp({
      sendRawTransaction: jest.fn().mockResolvedValue('large-txid'),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ rawtx }),
      });

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ txid: 'large-txid' });
      expect(service.sendRawTransaction).toHaveBeenCalledWith(rawtx);
    });
  });

  test('GET /addrs/:addrs/txs returns not implemented when address transaction hook is missing', async () => {
    const { app } = createApp({
      getAddressTransactions: undefined,
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/addr/txs`);

      expect(response.status).toBe(501);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Address transaction lookup is not implemented',
        code: 1,
      });
    });
  });

  test('GET /addrs/:addrs/txs returns address transactions from service hook', async () => {
    const { app, service } = createApp({
      getAddressTransactions: jest.fn().mockResolvedValue({
        totalItems: 3,
        items: [{ txid: 'tx2' }, { txid: 'tx1' }],
      }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/addr1,addr2/txs?from=1&to=3`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({
        totalItems: 3,
        from: 1,
        to: 3,
        items: [{ txid: 'tx2' }, { txid: 'tx1' }],
      });
      expect(service.getAddressTransactions).toHaveBeenCalledWith(
        ['addr1', 'addr2'],
        { from: 1, to: 3, limit: 2 }
      );
    });
  });

  test('GET /addrs/:addrs/txs rejects from beyond the UInt32 limit', async () => {
    const { app, service } = createApp({
      getAddressTransactions: jest.fn().mockResolvedValue({ totalItems: 0, items: [] }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/addr1/txs?from=4294967296`);

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Invalid from (must be an integer between 0 and 4294967295)',
        code: 1,
      });
      expect(service.getAddressTransactions).not.toHaveBeenCalled();
    });
  });

  test('POST /addrs/txs rejects to beyond the UInt32 limit', async () => {
    const { app, service } = createApp({
      getAddressTransactions: jest.fn().mockResolvedValue({ totalItems: 0, items: [] }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/txs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ addrs: 'addr1', from: 0, to: 9007199254740991 }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Invalid to (must be an integer between 0 and 4294967295)',
        code: 1,
      });
      expect(service.getAddressTransactions).not.toHaveBeenCalled();
    });
  });

  test('POST /addrs/txs queries every address from a JSON array body', async () => {
    const { app, service } = createApp({
      getAddressTransactions: jest.fn().mockResolvedValue({ totalItems: 0, items: [] }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/txs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ addrs: ['addr1', 'addr2'] }),
      });

      expect(response.status).toBe(200);
      expect(service.getAddressTransactions).toHaveBeenCalledWith(
        ['addr1', 'addr2'],
        { from: 0, to: 10, limit: 10 }
      );
    });
  });

  test('POST /addrs/utxo queries every address from a JSON array body', async () => {
    const { app, service } = createApp({
      getAddressUtxos: jest.fn().mockResolvedValue([]),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/utxo`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ addrs: ['addr1', 'addr2'] }),
      });

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual([]);
      expect(service.getAddressUtxos).toHaveBeenCalledWith(['addr1', 'addr2'], true);
    });
  });

  test('POST /addrs/utxo rejects array bodies with non-string entries', async () => {
    const { app, service } = createApp({
      getAddressUtxos: jest.fn().mockResolvedValue([]),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/utxo`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ addrs: ['addr1', 5] }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Invalid address list (expected an array of address strings)',
        code: 1,
      });
      expect(service.getAddressUtxos).not.toHaveBeenCalled();
    });
  });

  test('GET /addrs/:addrs/balance returns summed balance from service hook', async () => {
    const { app, service } = createApp({
      getAddressBalanceSum: jest.fn().mockResolvedValue({
        balance: '9007199254740997',
        unconfirmedBalance: 5,
        immature: 0,
      }),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/addr1,addr2/balance`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({
        balance: '9007199254740997',
        unconfirmedBalance: 5,
        immature: 0,
      });
      expect(service.getAddressBalanceSum).toHaveBeenCalledWith(['addr1', 'addr2']);
    });
  });

  test('GET /addrs/:addrs/balance returns not implemented when balance hook is missing', async () => {
    const { app } = createApp({
      getAddressBalanceSum: undefined,
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/addrs/addr/balance`);

      expect(response.status).toBe(501);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Address balance lookup is not implemented',
        code: 1,
      });
    });
  });

  test('GET /status?q=getDifficulty returns service status wrapper', async () => {
    const getStatus = jest.fn().mockResolvedValue({ difficulty: 123.456 });
    const { app } = createApp({ getStatus });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/status?q=getDifficulty`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ difficulty: 123.456 });
      expect(getStatus).toHaveBeenCalledWith('getDifficulty');
    });
  });

  test('GET /sync returns legacy sync fields', async () => {
    const legacySync = {
      status: 'syncing',
      blockChainHeight: 100,
      syncPercentage: 99.5,
      height: 99,
      error: null,
      type: 'bitcore node',
    };
    const getSync = jest.fn().mockResolvedValue(legacySync);
    const { app } = createApp({ getSync });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/sync`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual(legacySync);
      expect(getSync).toHaveBeenCalledWith();
    });
  });

  test('GET /utils/estimatefee supports multiple targets', async () => {
    const estimateFees = jest.fn().mockResolvedValue({ 2: 0.002, 6: 0.006 });
    const { app } = createApp({ estimateFees });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/utils/estimatefee?nbBlocks=2,6`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ 2: 0.002, 6: 0.006 });
      expect(estimateFees).toHaveBeenCalledWith([2, 6]);
    });
  });

  test('GET /utils/estimatefee deduplicates repeated targets', async () => {
    const estimateFees = jest.fn().mockResolvedValue({ 2: 0.002, 6: 0.006 });
    const { app } = createApp({ estimateFees });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/utils/estimatefee?nbBlocks=2,6,2`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ 2: 0.002, 6: 0.006 });
      expect(estimateFees).toHaveBeenCalledWith([2, 6]);
    });
  });

  test('GET /utils/estimatefee rejects too-large targets', async () => {
    const estimateFees = jest.fn().mockResolvedValue({ 1009: 1.009 });
    const { app } = createApp({ estimateFees });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/utils/estimatefee?nbBlocks=1009`);

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({ message: 'Invalid nbBlocks', code: 1 });
      expect(estimateFees).not.toHaveBeenCalled();
    });
  });

  test('POST /messages/verify requires address signature and message', async () => {
    const verifyMessage = jest.fn().mockResolvedValue(true);
    const { app } = createApp({ verifyMessage });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/messages/verify`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ address: 'taddr', message: 'hello' }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Missing address, signature, or message',
        code: 1,
      });
      expect(verifyMessage).not.toHaveBeenCalled();
    });
  });

  test('POST /messages/verify rejects non-string JSON fields', async () => {
    const verifyMessage = jest.fn().mockResolvedValue(true);
    const { app } = createApp({ verifyMessage });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/messages/verify`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ address: 'taddr', signature: { value: 'sig' }, message: 'hello' }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Missing address, signature, or message',
        code: 1,
      });
      expect(verifyMessage).not.toHaveBeenCalled();
    });
  });

  test('POST /messages/verify rejects blank string fields', async () => {
    const verifyMessage = jest.fn().mockResolvedValue(true);
    const { app } = createApp({ verifyMessage });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/messages/verify`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ address: 'taddr', signature: '   ', message: 'hello' }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Missing address, signature, or message',
        code: 1,
      });
      expect(verifyMessage).not.toHaveBeenCalled();
    });
  });

  test('POST /messages/verify returns service verification result', async () => {
    const verifyMessage = jest.fn().mockResolvedValue(true);
    const { app } = createApp({ verifyMessage });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/messages/verify`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ address: ' taddr ', signature: ' sig ', message: ' hello ' }),
      });

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({ result: true });
      expect(verifyMessage).toHaveBeenCalledWith('taddr', 'sig', ' hello ');
    });
  });

  test('POST /messages/verify returns bad request when verification rejects', async () => {
    const verifyMessage = jest.fn().mockRejectedValue(new Error('Invalid address'));
    const { app } = createApp({ verifyMessage });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/messages/verify`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ address: 'taddr', signature: 'sig', message: 'hello' }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({ message: 'Invalid address', code: 1 });
      expect(verifyMessage).toHaveBeenCalledWith('taddr', 'sig', 'hello');
    });
  });

  test('POST /messages/verify returns bad request for invalid-parameter RPC errors', async () => {
    const verifyMessage = jest.fn().mockRejectedValue(new RPCError('Invalid parameter', -32602));
    const { app } = createApp({ verifyMessage });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/messages/verify`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ address: 'taddr', signature: 'sig', message: 'hello' }),
      });

      expect(response.status).toBe(400);
      await expect(readJson(response)).resolves.toEqual({ message: 'Invalid parameter', code: 1 });
      expect(verifyMessage).toHaveBeenCalledWith('taddr', 'sig', 'hello');
    });
  });

  test('POST /messages/verify lets operational verification errors reach error handling', async () => {
    const verifyMessage = jest.fn().mockRejectedValue(new Error('RPC timeout'));
    const { app } = createApp({ verifyMessage });
    const errorHandler: express.ErrorRequestHandler = (error, _req, res, _next) => {
      res.status(500).json({ error: error.message });
    };
    app.use(errorHandler);

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/messages/verify`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ address: 'taddr', signature: 'sig', message: 'hello' }),
      });

      expect(response.status).toBe(500);
      await expect(readJson(response)).resolves.toEqual({ error: 'RPC timeout' });
      expect(verifyMessage).toHaveBeenCalledWith('taddr', 'sig', 'hello');
    });
  });

  test('GET /fluxnode/listfluxnodes/:filter calls service filter hook', async () => {
    const listFluxNodes = jest.fn().mockResolvedValue({
      result: [{ txhash: 'abc', outidx: 1 }],
      error: null,
      id: null,
    });
    const { app } = createApp({ listFluxNodes });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/fluxnode/listfluxnodes/abc-1`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({
        result: [{ txhash: 'abc', outidx: 1 }],
        error: null,
        id: null,
      });
      expect(listFluxNodes).toHaveBeenCalledWith('abc-1');
    });
  });

  test('GET /fluxnode/addrs/:addrs/utxo filters collateral UTXOs and formats them', async () => {
    const getAddressUtxos = jest.fn().mockResolvedValue([
      {
        address: 'addr1',
        txid: '1'.repeat(64),
        vout: 0,
        script_pubkey: '76a914',
        value: '100000000000',
        block_height: 10,
        confirmations: 5,
      },
      {
        address: 'addr1',
        txid: '2'.repeat(64),
        vout: 1,
        script_pubkey: '76a914',
        value: '42',
        block_height: 11,
        confirmations: 4,
      },
      {
        address: 'addr2',
        txid: '3'.repeat(64),
        vout: 2,
        script_pubkey: '76a914',
        value: '4000000000000',
        block_height: 12,
        confirmations: 3,
      },
    ]);
    const { app } = createApp({ getAddressUtxos });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/fluxnode/addrs/addr1,addr2/utxo`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual([
        expect.objectContaining({ txid: '1'.repeat(64), amount: 1000, satoshis: 100000000000 }),
        expect.objectContaining({ txid: '3'.repeat(64), amount: 40000, satoshis: 4000000000000 }),
      ]);
      expect(getAddressUtxos).toHaveBeenCalledWith(['addr1', 'addr2'], true);
    });
  });

  test('GET /supply returns text by default and circulatingSupply object with format=object', async () => {
    const getSupply = jest.fn().mockResolvedValue('100000000');
    const { app } = createApp({ getSupply });

    await withTestServer(app, async (baseUrl) => {
      const textResponse = await fetch(`${baseUrl}/insight-api/supply`);
      expect(textResponse.status).toBe(200);
      expect(textResponse.headers.get('content-type')).toContain('text/plain');
      await expect(textResponse.text()).resolves.toBe('1');

      const objectResponse = await fetch(`${baseUrl}/insight-api/supply?format=object`);
      expect(objectResponse.status).toBe(200);
      await expect(readJson(objectResponse)).resolves.toEqual({ circulatingSupply: '1' });
      expect(getSupply).toHaveBeenCalledTimes(2);
    });
  });

  test('GET /statistics/circulating-supply is explicit not implemented', async () => {
    const getSupply = jest.fn().mockResolvedValue('100000000');
    const { app } = createApp({ getSupply });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/statistics/circulating-supply`);

      expect(response.status).toBe(501);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Circulating supply lookup is not implemented',
        code: 1,
      });
      expect(getSupply).not.toHaveBeenCalled();
    });
  });

  test('GET /statistics/supply returns series data from service hook', async () => {
    const getStatisticSeries = jest.fn().mockResolvedValue([
      { date: '2026-06-10', sum: '1000.00000000' },
    ]);
    const { app } = createApp({ getStatisticSeries });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/statistics/supply?days=30`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual([
        { date: '2026-06-10', sum: '1000.00000000' },
      ]);
      expect(getStatisticSeries).toHaveBeenCalledWith('supply', '30');
    });
  });

  test('GET /statistics/network-hash is explicit not implemented', async () => {
    const getStatisticSeries = jest.fn().mockResolvedValue([{ date: '2026-06-10', sum: 42 }]);
    const { app } = createApp({ getStatisticSeries });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/statistics/network-hash`);

      expect(response.status).toBe(501);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Network hash statistics lookup is not implemented',
        code: 1,
      });
      expect(getStatisticSeries).not.toHaveBeenCalled();
    });
  });

  test('GET /statistics/total returns total statistics from service hook', async () => {
    const getStatisticsTotal = jest.fn().mockResolvedValue({
      n_blocks_mined: 10,
      number_of_transactions: 25,
      blocks_by_pool: [],
    });
    const { app } = createApp({ getStatisticsTotal });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/statistics/total`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({
        n_blocks_mined: 10,
        number_of_transactions: 25,
        blocks_by_pool: [],
      });
      expect(getStatisticsTotal).toHaveBeenCalledWith();
    });
  });

  test('GET /statistics/pools forwards requested date to service hook', async () => {
    const getPools = jest.fn().mockResolvedValue({
      date: '2026-06-10',
      n_blocks_mined: 4,
      blocks_by_pool: [],
      pagination: { current: '2026-06-10', next: '2026-06-11', prev: '2026-06-09' },
    });
    const { app } = createApp({ getPools });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/statistics/pools?date=2026-06-10`);

      expect(response.status).toBe(200);
      await expect(readJson(response)).resolves.toEqual({
        date: '2026-06-10',
        n_blocks_mined: 4,
        blocks_by_pool: [],
        pagination: { current: '2026-06-10', next: '2026-06-11', prev: '2026-06-09' },
      });
      expect(getPools).toHaveBeenCalledWith('2026-06-10');
    });
  });

  test('GET /statistics/fees returns not implemented when statistics hook is missing', async () => {
    const { app } = createApp({
      getStatisticSeries: undefined,
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/statistics/fees`);

      expect(response.status).toBe(501);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Statistic series lookup is not implemented',
        code: 1,
      });
    });
  });

  test('GET /peer returns not implemented when peer hook is missing', async () => {
    const { app } = createApp();

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/peer`);

      expect(response.status).toBe(501);
      await expect(readJson(response)).resolves.toEqual({
        message: 'Peer lookup is not implemented',
        code: 1,
      });
    });
  });

  test('missing core resource returns legacy 404 body', async () => {
    const { app } = createApp({
      getBlock: jest.fn().mockResolvedValue(null),
    });

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/block/missing`);

      expect(response.status).toBe(404);
      await expect(readJson(response)).resolves.toEqual({
        status: 404,
        url: '/insight-api/block/missing',
        error: 'Not found',
      });
    });
  });

  test('unmatched standalone router path returns legacy 404 body', async () => {
    const { app } = createApp();

    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/does-not-exist`);

      expect(response.status).toBe(404);
      await expect(readJson(response)).resolves.toEqual({
        status: 404,
        url: '/insight-api/does-not-exist',
        error: 'Not found',
      });
    });
  });
});
