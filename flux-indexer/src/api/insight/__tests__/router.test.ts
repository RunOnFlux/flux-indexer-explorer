import express from 'express';
import { RPCError } from '../../../types';
import { ClickHouseAPIServer } from '../../server';
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

describe('Insight server mount', () => {
  test('mounts Insight compatibility routes without changing API 404 behavior', async () => {
    const ch = {
      query: jest.fn(),
      queryOne: jest.fn(),
      queryCount: jest.fn(),
    } as unknown as ConstructorParameters<typeof ClickHouseAPIServer>[0];
    const rpc = {} as unknown as ConstructorParameters<typeof ClickHouseAPIServer>[1];
    const syncEngine = {} as unknown as ConstructorParameters<typeof ClickHouseAPIServer>[2];
    const server = new ClickHouseAPIServer(ch, rpc, syncEngine, 0);
    const app = server.getApp();

    await withTestServer(app, async (baseUrl) => {
      const insightResponse = await fetch(`${baseUrl}/insight-api/not-real`);

      expect(insightResponse.status).toBe(404);
      await expect(readJson(insightResponse)).resolves.toEqual({
        status: 404,
        url: '/insight-api/not-real',
        error: 'Not found',
      });

      const apiResponse = await fetch(`${baseUrl}/api/not-real`);

      expect(apiResponse.status).toBe(404);
      await expect(readJson(apiResponse)).resolves.toEqual({ error: 'Not found' });
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
});
