import express from 'express';
import { createInsightCompatibilityRouter, type InsightRouterService } from '../router';
import { readJson, withTestServer } from './http-test-utils';

type MockInsightRouterService = {
  [K in keyof InsightRouterService]: InsightRouterService[K] extends (...args: infer Args) => infer Result
    ? jest.Mock<Result, Args>
    : InsightRouterService[K];
};

function createApp(serviceOverrides: Partial<MockInsightRouterService> = {}) {
  const app = express();
  app.use(express.json());

  const service: MockInsightRouterService = {
    getBlock: jest.fn(),
    getBlockHashByHeight: jest.fn(),
    getRawBlock: jest.fn(),
    listBlocks: jest.fn(),
    getTransaction: jest.fn(),
    getRawTransaction: jest.fn(),
    getTransactionsByBlock: jest.fn(),
    getTransactionsByAddress: jest.fn(),
    getAddressSummary: jest.fn(),
    getAddressUtxos: jest.fn(),
    getAddressTransactions: jest.fn(),
    getAddressBalanceSum: jest.fn(),
    sendRawTransaction: jest.fn(),
    ...serviceOverrides,
  };

  app.use('/insight-api', createInsightCompatibilityRouter(service));
  return { app, service };
}

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

  test('GET /blocks returns bad request when query parsing fails', async () => {
    const { app } = createApp({
      listBlocks: jest.fn().mockRejectedValue(new Error('Invalid blockDate (expected YYYY-MM-DD)')),
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
});
