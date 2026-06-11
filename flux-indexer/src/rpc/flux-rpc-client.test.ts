jest.mock('node-fetch', () => ({ __esModule: true, default: jest.fn() }));

import fetch from 'node-fetch';
import { FluxRPCClient } from './flux-rpc-client';
import { RPCError } from '../types';

const mockedFetch = fetch as unknown as jest.Mock;

function mockedClient(): { rpc: FluxRPCClient; call: jest.Mock } {
  const rpc = new FluxRPCClient({ url: 'http://127.0.0.1:16124' });
  const call = jest.fn();
  (rpc as unknown as { call: jest.Mock }).call = call;
  return { rpc, call };
}

function fetchClient(): FluxRPCClient {
  return new FluxRPCClient({ url: 'http://127.0.0.1:16124' });
}

function httpResponse(options: {
  ok?: boolean;
  status?: number;
  statusText?: string;
  body?: unknown;
  jsonError?: Error;
}): { ok: boolean; status: number; statusText: string; json: jest.Mock } {
  return {
    ok: options.ok ?? false,
    status: options.status ?? 500,
    statusText: options.statusText ?? 'Internal Server Error',
    json: options.jsonError
      ? jest.fn().mockRejectedValue(options.jsonError)
      : jest.fn().mockResolvedValue(options.body),
  };
}

describe('FluxRPCClient legacy Insight helpers', () => {
  test('sendRawTransaction delegates to sendrawtransaction', async () => {
    const { rpc, call } = mockedClient();
    call.mockResolvedValue('txid');
    await expect(rpc.sendRawTransaction('abcd')).resolves.toBe('txid');
    expect(call).toHaveBeenCalledWith('sendrawtransaction', ['abcd']);
  });

  test('verifyMessage delegates to verifymessage', async () => {
    const { rpc, call } = mockedClient();
    call.mockResolvedValue(true);
    await expect(rpc.verifyMessage('addr', 'sig', 'message')).resolves.toBe(true);
    expect(call).toHaveBeenCalledWith('verifymessage', ['addr', 'sig', 'message']);
  });

  test('getInfo falls back to composed network info when getinfo is unavailable', async () => {
    const { rpc, call } = mockedClient();
    const getBlockchainInfo = jest.fn().mockResolvedValue({
      chain: 'main',
      blocks: 123,
      difficulty: 4.5,
    });
    const getNetworkInfo = jest.fn().mockResolvedValue({
      version: 900000,
      protocolversion: 70015,
      connections: 8,
      relayfee: 0.00001,
    });
    (rpc as unknown as { getBlockchainInfo: jest.Mock }).getBlockchainInfo = getBlockchainInfo;
    (rpc as unknown as { getNetworkInfo: jest.Mock }).getNetworkInfo = getNetworkInfo;
    call.mockRejectedValueOnce(new RPCError('Method not found', -32601));

    await expect(rpc.getInfo()).resolves.toEqual({
      version: 900000,
      protocolversion: 70015,
      walletversion: 0,
      blocks: 123,
      timeoffset: 0,
      connections: 8,
      proxy: '',
      difficulty: 4.5,
      testnet: false,
      relayfee: 0.00001,
      errors: '',
      network: 'main',
      reward: 0,
    });
    expect(call).toHaveBeenCalledWith('getinfo');
    expect(getBlockchainInfo).toHaveBeenCalledTimes(1);
    expect(getNetworkInfo).toHaveBeenCalledTimes(1);
  });

  test('getInfo rethrows non-method-not-found RPC errors without fallback calls', async () => {
    const { rpc, call } = mockedClient();
    const error = new RPCError('RPC failed', -1);
    const getBlockchainInfo = jest.fn();
    const getNetworkInfo = jest.fn();
    (rpc as unknown as { getBlockchainInfo: jest.Mock }).getBlockchainInfo = getBlockchainInfo;
    (rpc as unknown as { getNetworkInfo: jest.Mock }).getNetworkInfo = getNetworkInfo;
    call.mockRejectedValueOnce(error);

    await expect(rpc.getInfo()).rejects.toBe(error);
    expect(getBlockchainInfo).not.toHaveBeenCalled();
    expect(getNetworkInfo).not.toHaveBeenCalled();
  });

  test('viewDeterministicFluxNodeList falls back to listfluxnodes when deterministic list is unavailable', async () => {
    const { rpc, call } = mockedClient();
    call.mockRejectedValueOnce(new RPCError('Method not found', -32601)).mockResolvedValueOnce([{ ip: '1.2.3.4' }]);
    await expect(rpc.viewDeterministicFluxNodeList()).resolves.toEqual([{ ip: '1.2.3.4' }]);
    expect(call).toHaveBeenNthCalledWith(1, 'viewdeterministiczelnodelist', []);
    expect(call).toHaveBeenNthCalledWith(2, 'listfluxnodes', []);
  });

  test('getInfo falls back when getinfo is signaled missing via bare HTTP 404', async () => {
    const { rpc, call } = mockedClient();
    const getBlockchainInfo = jest.fn().mockResolvedValue({
      chain: 'main',
      blocks: 123,
      difficulty: 4.5,
    });
    const getNetworkInfo = jest.fn().mockResolvedValue({
      version: 900000,
      protocolversion: 70015,
      connections: 8,
      relayfee: 0.00001,
    });
    (rpc as unknown as { getBlockchainInfo: jest.Mock }).getBlockchainInfo = getBlockchainInfo;
    (rpc as unknown as { getNetworkInfo: jest.Mock }).getNetworkInfo = getNetworkInfo;
    call.mockRejectedValueOnce(new RPCError('HTTP 404: Not Found', 404));

    await expect(rpc.getInfo()).resolves.toMatchObject({ blocks: 123, network: 'main' });
    expect(getBlockchainInfo).toHaveBeenCalledTimes(1);
    expect(getNetworkInfo).toHaveBeenCalledTimes(1);
  });

  test('viewDeterministicFluxNodeList falls back when signaled missing via bare HTTP 404', async () => {
    const { rpc, call } = mockedClient();
    call.mockRejectedValueOnce(new RPCError('HTTP 404: Not Found', 404)).mockResolvedValueOnce([{ ip: '1.2.3.4' }]);
    await expect(rpc.viewDeterministicFluxNodeList()).resolves.toEqual([{ ip: '1.2.3.4' }]);
    expect(call).toHaveBeenNthCalledWith(1, 'viewdeterministiczelnodelist', []);
    expect(call).toHaveBeenNthCalledWith(2, 'listfluxnodes', []);
  });

  test('viewDeterministicFluxNodeList rethrows non-method-not-found RPC errors', async () => {
    const { rpc, call } = mockedClient();
    const error = new RPCError('RPC failed', -1);
    call.mockRejectedValueOnce(error);

    await expect(rpc.viewDeterministicFluxNodeList()).rejects.toBe(error);
    expect(call).toHaveBeenCalledTimes(1);
    expect(call).toHaveBeenCalledWith('viewdeterministiczelnodelist', []);
  });

  test('supports peer, mining, info, version, doslist, and startlist calls', async () => {
    const { rpc, call } = mockedClient();
    call.mockResolvedValue({});
    await rpc.getPeerInfo();
    await rpc.getMiningInfo();
    await rpc.getInfo();
    await rpc.getVersion();
    await rpc.dosList();
    await rpc.startList();
    expect(call.mock.calls.map((entry) => entry[0])).toEqual([
      'getpeerinfo',
      'getmininginfo',
      'getinfo',
      'getnetworkinfo',
      'getdoslist',
      'getstartlist',
    ]);
  });
});

describe('FluxRPCClient HTTP error surfacing', () => {
  test('non-2xx response with a JSON-RPC error body surfaces the daemon code and message', async () => {
    mockedFetch.mockResolvedValue(httpResponse({
      body: { result: null, error: { code: -26, message: '66: insufficient priority' }, id: 1 },
    }));

    const promise = fetchClient().sendRawTransaction('abcd');
    await expect(promise).rejects.toBeInstanceOf(RPCError);
    await expect(promise).rejects.toMatchObject({
      message: '66: insufficient priority',
      rpcCode: -26,
    });
  });

  test('non-2xx response without a JSON body falls back to the HTTP status error', async () => {
    mockedFetch.mockResolvedValue(httpResponse({
      status: 502,
      statusText: 'Bad Gateway',
      jsonError: new Error('invalid json'),
    }));

    await expect(fetchClient().getBlockCount()).rejects.toMatchObject({
      message: 'HTTP 502: Bad Gateway',
      rpcCode: 502,
    });
  });

  test('non-2xx response with JSON lacking an error envelope falls back to the HTTP status error', async () => {
    mockedFetch.mockResolvedValue(httpResponse({
      status: 503,
      statusText: 'Service Unavailable',
      body: { detail: 'overloaded' },
    }));

    await expect(fetchClient().getBlockCount()).rejects.toMatchObject({
      message: 'HTTP 503: Service Unavailable',
      rpcCode: 503,
    });
  });

  test('2xx success responses are returned unchanged', async () => {
    mockedFetch.mockResolvedValue(httpResponse({
      ok: true,
      status: 200,
      statusText: 'OK',
      body: { result: 42, error: null, id: 1 },
    }));

    await expect(fetchClient().getBlockCount()).resolves.toBe(42);
  });

  test('2xx response with a JSON-RPC error still surfaces the daemon code and message', async () => {
    mockedFetch.mockResolvedValue(httpResponse({
      ok: true,
      status: 200,
      statusText: 'OK',
      body: { result: null, error: { code: -5, message: 'Invalid address' }, id: 1 },
    }));

    await expect(fetchClient().verifyMessage('addr', 'sig', 'msg')).rejects.toMatchObject({
      message: 'Invalid address',
      rpcCode: -5,
    });
  });

  test('getInfo composes a fallback when getinfo arrives as bare HTTP 404 without a JSON body', async () => {
    mockedFetch.mockImplementation(async (_url: string, init: { body: string }) => {
      const request = JSON.parse(init.body) as { method: string };
      switch (request.method) {
        case 'getinfo':
          return httpResponse({ status: 404, statusText: 'Not Found', jsonError: new Error('no body') });
        case 'getblockchaininfo':
          return httpResponse({
            ok: true,
            status: 200,
            statusText: 'OK',
            body: { result: { chain: 'main', blocks: 99, difficulty: 7 }, error: null, id: 1 },
          });
        case 'getnetworkinfo':
          return httpResponse({
            ok: true,
            status: 200,
            statusText: 'OK',
            body: {
              result: { version: 900000, protocolversion: 70015, connections: 4, relayfee: 0.00001 },
              error: null,
              id: 2,
            },
          });
        default:
          throw new Error(`unexpected method ${request.method}`);
      }
    });

    await expect(fetchClient().getInfo()).resolves.toMatchObject({
      blocks: 99,
      connections: 4,
      network: 'main',
    });
  });

  test('batchCall surfaces a daemon JSON-RPC error from a non-2xx batch response', async () => {
    mockedFetch.mockResolvedValue(httpResponse({
      body: [{ result: null, error: { code: -8, message: 'Block height out of range' }, id: 1 }],
    }));

    await expect(
      fetchClient().batchCall([
        { method: 'getblockhash', params: [1] },
        { method: 'getblockhash', params: [2] },
      ])
    ).rejects.toMatchObject({
      message: 'Block height out of range',
      rpcCode: -8,
    });
  });
});

describe('FluxRPCClient batchGetBlocks verbosity fallback', () => {
  test('falls back to verbosity 1 when the verbosity 2 fetch fails with a daemon RPC error', async () => {
    const { rpc, call } = mockedClient();
    const batchCall = jest.fn()
      .mockResolvedValueOnce(['hash1'])
      .mockRejectedValueOnce(new RPCError('Batch failed', -1));
    (rpc as unknown as { batchCall: jest.Mock }).batchCall = batchCall;
    call.mockImplementation(async (method: string, params: any[]) => {
      if (method === 'getblock' && params[1] === 2) {
        throw new RPCError('CDataStream::read(): end of data', -32603);
      }
      if (method === 'getblock' && params[1] === 1) {
        return { hash: 'hash1', height: 10, tx: [] };
      }
      throw new Error(`unexpected call ${method}`);
    });

    await expect(rpc.batchGetBlocks([10])).resolves.toEqual([{ hash: 'hash1', height: 10, tx: [] }]);
    expect(call).toHaveBeenCalledWith('getblock', ['hash1', 2]);
    expect(call).toHaveBeenCalledWith('getblock', ['hash1', 1]);
  });

  test('rethrows non-RPC errors from the verbosity 2 fetch without falling back', async () => {
    const { rpc, call } = mockedClient();
    const batchCall = jest.fn()
      .mockResolvedValueOnce(['hash1'])
      .mockRejectedValueOnce(new RPCError('Batch failed', -1));
    (rpc as unknown as { batchCall: jest.Mock }).batchCall = batchCall;
    const failure = new Error('boom');
    call.mockRejectedValue(failure);

    await expect(rpc.batchGetBlocks([10])).rejects.toBe(failure);
    expect(call).toHaveBeenCalledTimes(1);
    expect(call).toHaveBeenCalledWith('getblock', ['hash1', 2]);
  });
});
