import { FluxRPCClient } from './flux-rpc-client';
import { RPCError } from '../types';

function mockedClient(): { rpc: FluxRPCClient; call: jest.Mock } {
  const rpc = new FluxRPCClient({ url: 'http://127.0.0.1:16124' });
  const call = jest.fn();
  (rpc as unknown as { call: jest.Mock }).call = call;
  return { rpc, call };
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
