jest.mock('../../../parsers/block-parser', () => ({
  extractTransactionFromBlock: jest.fn(),
}));

import { extractTransactionFromBlock } from '../../../parsers/block-parser';
import { encodeFluxAddress } from '../../../utils/script-utils';
import { InsightCompatibilityService } from '../service';

const extractTransactionFromBlockMock = extractTransactionFromBlock as jest.MockedFunction<typeof extractTransactionFromBlock>;

const P2PKH_HASH160 = '00112233445566778899aabbccddeeff00112233';
const P2SH_HASH160 = 'ffeeddccbbaa99887766554433221100ffeeddcc';
const P2PKH_ADDRESS = encodeFluxAddress(P2PKH_HASH160, 'p2pkh') as string;
const P2SH_ADDRESS = encodeFluxAddress(P2SH_HASH160, 'p2sh') as string;
const P2PKH_SCRIPT = `76a914${P2PKH_HASH160}88ac`;
const P2SH_SCRIPT = `a914${P2SH_HASH160}87`;

function createService() {
  const ch = {
    query: jest.fn(),
    queryOne: jest.fn(),
  };
  const rpc = {
    getBlock: jest.fn(),
    getRawTransaction: jest.fn(),
    sendRawTransaction: jest.fn(),
    estimateFee: jest.fn(),
  };
  const getMempoolAddressDeltas = jest.fn().mockResolvedValue(new Map());

  return {
    service: new InsightCompatibilityService(ch as any, rpc as any, getMempoolAddressDeltas),
    ch,
    rpc,
    getMempoolAddressDeltas,
  };
}

describe('InsightCompatibilityService', () => {
  test('gets a block by height and transaction ids', async () => {
    const { service, ch } = createService();
    const block = {
      hash: 'a'.repeat(64),
      height: 100,
      size: 1234,
      version: 4,
      merkle_root: 'b'.repeat(64),
      timestamp: 1700000000,
      bits: '1d00ffff',
      difficulty: 12.5,
      chainwork: '00ff',
      prev_hash: 'c'.repeat(64),
      producer_reward: '5000000000',
      producer: 'producer',
      tx_count: 2,
      is_valid: 1,
    };
    const nextHash = 'd'.repeat(64);

    ch.queryOne.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('FROM blocks') && params?.height === 100) {
        return block;
      }

      if (sql.includes('FROM sync_state')) {
        return { chain_height: 105, current_height: 105 };
      }

      if (sql.includes('max(height)')) {
        return { height: 105 };
      }

      if (sql.includes('FROM blocks') && params?.height === 101) {
        return { hash: nextHash };
      }

      throw new Error(`Unexpected queryOne: ${sql}`);
    });
    ch.query.mockResolvedValue([
      { txid: '1'.repeat(64) },
      { txid: '2'.repeat(64) },
    ]);

    await expect(service.getBlock(100)).resolves.toEqual({
      block,
      txids: ['1'.repeat(64), '2'.repeat(64)],
      confirmations: 6,
      nextBlockHash: nextHash,
    });
  });

  test('uses sync_state chain height for block confirmations', async () => {
    const { service, ch } = createService();
    const block = {
      hash: 'a'.repeat(64),
      height: 100,
      size: 1234,
      version: 4,
      merkle_root: 'b'.repeat(64),
      timestamp: 1700000000,
      bits: '1d00ffff',
      difficulty: 12.5,
      chainwork: '00ff',
      is_valid: 1,
    };

    ch.queryOne.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('FROM blocks') && params?.height === 100) {
        return block;
      }

      if (sql.includes('FROM sync_state')) {
        return { chain_height: 105, current_height: 104 };
      }

      if (sql.includes('FROM blocks') && params?.height === 101) {
        return null;
      }

      throw new Error(`Unexpected queryOne: ${sql}`);
    });
    ch.query.mockResolvedValue([]);

    await expect(service.getBlock(100)).resolves.toMatchObject({
      confirmations: 6,
    });
    expect(ch.queryOne).toHaveBeenCalledWith(expect.stringContaining('FROM sync_state'));
  });

  test('returns null for invalidated blocks', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue({
      hash: 'a'.repeat(64),
      height: 100,
      is_valid: 0,
    });

    await expect(service.getBlock('a'.repeat(64))).resolves.toBeNull();
    expect(ch.query).not.toHaveBeenCalled();
  });

  test('looks up all-digit 64-character block hashes as hashes when not valid heights', async () => {
    const { service, ch } = createService();
    const numericHash = '9'.repeat(64);

    ch.queryOne.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('WHERE hash = {hash:FixedString(64)}')) {
        expect(params).toEqual({ hash: numericHash });
        return null;
      }

      throw new Error(`Unexpected queryOne: ${sql}`);
    });

    await expect(service.getBlock(numericHash)).resolves.toBeNull();
    expect(ch.queryOne).toHaveBeenCalledTimes(1);
    expect(ch.query).not.toHaveBeenCalled();
  });

  test('gets raw block hex by height through RPC', async () => {
    const { service, rpc } = createService();
    rpc.getBlock.mockResolvedValue('deadbeef');

    await expect(service.getRawBlock(123)).resolves.toBe('deadbeef');
    expect(rpc.getBlock).toHaveBeenCalledWith(123, 0);
  });

  test('returns null when raw block RPC lookup fails', async () => {
    const { service, rpc } = createService();
    rpc.getBlock.mockRejectedValue(new Error('Block not found'));

    await expect(service.getRawBlock(999999999)).resolves.toBeNull();
    expect(rpc.getBlock).toHaveBeenCalledWith(999999999, 0);
  });

  test('falls back to extracting raw transaction hex from raw block', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);
    const blockHash = 'b'.repeat(64);
    const rawBlockHex = '00ff';
    const extractedHex = '01000000';

    rpc.getRawTransaction.mockRejectedValue(new Error('No such mempool or blockchain transaction'));
    rpc.getBlock.mockResolvedValue(rawBlockHex);
    extractTransactionFromBlockMock.mockReturnValue(extractedHex);
    ch.queryOne.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('FROM transactions')) {
        return { txid, block_height: 250000, is_valid: 1 };
      }

      if (sql.includes('FROM blocks') && params?.height === 250000) {
        return { hash: blockHash, height: 250000, is_valid: 1 };
      }

      throw new Error(`Unexpected queryOne: ${sql}`);
    });

    await expect(service.getRawTransaction(txid)).resolves.toBe(extractedHex);
    expect(rpc.getRawTransaction).toHaveBeenCalledWith(txid, false);
    expect(rpc.getBlock).toHaveBeenCalledWith(blockHash, 0);
    expect(extractTransactionFromBlockMock).toHaveBeenCalledWith(rawBlockHex, txid, 250000);
  });

  test('lists recent blocks without a blockDate timestamp filter', async () => {
    const { service, ch } = createService();
    const blocks = [
      { hash: 'c'.repeat(64), height: 102, is_valid: 1 },
      { hash: 'b'.repeat(64), height: 101, is_valid: 1 },
    ];
    ch.queryOne.mockResolvedValue({ chain_height: 1000, current_height: 1000 });
    ch.query.mockResolvedValue(blocks);

    await expect(service.listBlocks({ limit: '2' })).resolves.toEqual({
      blocks,
      blockDate: null,
    });

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('FROM blocks');
    expect(sql).not.toContain('timestamp >=');
    expect(sql).not.toContain('timestamp <=');
    expect(sql).toContain('height <= {maxHeight:UInt32}');
    expect(sql).toContain('height > {minHeight:UInt32}');
    expect(params).toEqual({ limit: 2, maxHeight: 1000, minHeight: 748 });
    expect(ch.queryOne).toHaveBeenCalledWith(expect.stringContaining('FROM sync_state'));
  });

  test('lists recent blocks from indexed current height when chain height is ahead', async () => {
    const { service, ch } = createService();
    const blocks = [
      { hash: 'c'.repeat(64), height: 500, is_valid: 1 },
      { hash: 'b'.repeat(64), height: 499, is_valid: 1 },
    ];
    ch.queryOne.mockResolvedValue({ chain_height: 1000, current_height: 500 });
    ch.query.mockResolvedValue(blocks);

    await expect(service.listBlocks({ limit: '2' })).resolves.toEqual({
      blocks,
      blockDate: null,
    });

    const [, params] = ch.query.mock.calls[0];
    expect(params).toEqual({ limit: 2, maxHeight: 500, minHeight: 248 });
  });

  test('lists blocks with parsed blockDate metadata when date filtered', async () => {
    const { service, ch } = createService();
    const blocks = [
      { hash: 'b'.repeat(64), height: 101, is_valid: 1 },
    ];
    ch.query.mockResolvedValue(blocks);

    await expect(service.listBlocks({ blockDate: '2026-06-10', limit: '1' })).resolves.toEqual({
      blocks,
      blockDate: {
        start: 1781049600,
        end: 1781135999,
        current: '2026-06-10',
        next: '2026-06-11',
        prev: '2026-06-09',
      },
    });
  });

  test('orders transaction inputs by decoded RPC vin order', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);
    const firstPrev = '1'.repeat(64);
    const secondPrev = '2'.repeat(64);
    const inputs = [
      { txid: firstPrev, vout: 0, address: 'first', value: '100', script_type: 'pubkeyhash' },
      { txid: secondPrev, vout: 1, address: 'second', value: '200', script_type: 'pubkeyhash' },
    ];

    mockTransactionLookup(ch, txid, inputs);
    rpc.getRawTransaction.mockResolvedValue({
      txid,
      vin: [
        { txid: secondPrev, vout: 1, sequence: 0xffffffff },
        { txid: firstPrev, vout: 0, sequence: 0xffffffff },
      ],
    });

    const result = await service.getTransaction(txid);

    expect(result?.inputs.map((input) => input.address)).toEqual(['second', 'first']);
    expect(rpc.getRawTransaction).toHaveBeenCalledWith(txid, true);
  });

  test('keeps ClickHouse input order when decoded RPC lookup fails', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);
    const inputs = [
      { txid: '1'.repeat(64), vout: 0, address: 'first', value: '100', script_type: 'pubkeyhash' },
      { txid: '2'.repeat(64), vout: 1, address: 'second', value: '200', script_type: 'pubkeyhash' },
    ];

    mockTransactionLookup(ch, txid, inputs);
    rpc.getRawTransaction.mockRejectedValue(new Error('RPC unavailable'));

    const result = await service.getTransaction(txid);

    expect(result?.inputs).toEqual(inputs);
    expect(rpc.getRawTransaction).toHaveBeenCalledWith(txid, true);
  });

  test('reconstructs blank standard transaction output scripts', async () => {
    const { service, ch } = createService();
    const txid = 'a'.repeat(64);
    const storedScript = '6a04deadbeef';

    ch.queryOne.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('FROM transactions')) {
        return {
          txid,
          version: 1,
          locktime: 0,
          block_height: 100,
          timestamp: 1700000000,
          input_total: '0',
          output_total: '300',
          fee: '0',
          size: 225,
          is_coinbase: 0,
          is_fluxnode_tx: 0,
          is_valid: 1,
        };
      }

      if (sql.includes('FROM blocks') && params?.height === 100) {
        return { hash: 'b'.repeat(64), height: 100, is_valid: 1 };
      }

      if (sql.includes('FROM sync_state')) {
        return { chain_height: 105, current_height: 105 };
      }

      throw new Error(`Unexpected queryOne: ${sql}`);
    });
    ch.query.mockImplementation(async (sql: string) => {
      if (sql.includes('WHERE txid =')) {
        return [
          {
            vout: 0,
            address: P2PKH_ADDRESS,
            value: '100',
            script_pubkey: '',
            script_type: 'pubkeyhash',
            spent: 0,
          },
          {
            vout: 1,
            address: P2SH_ADDRESS,
            value: '200',
            script_pubkey: '',
            script_type: 'scripthash',
            spent: 0,
          },
          {
            vout: 2,
            address: 'not-a-flux-address',
            value: '0',
            script_pubkey: storedScript,
            script_type: 'nulldata',
            spent: 0,
          },
        ];
      }

      if (sql.includes('WHERE spent_txid')) {
        return [];
      }

      throw new Error(`Unexpected query: ${sql}`);
    });

    const result = await service.getTransaction(txid);

    expect(result?.outputs.map((output) => output.script_pubkey)).toEqual([
      P2PKH_SCRIPT,
      P2SH_SCRIPT,
      storedScript,
    ]);
  });

  test('gets address summary with noTxList support', async () => {
    const { service, ch, getMempoolAddressDeltas } = createService();
    const summary = {
      balance: '250000000',
      received_total: '500000000',
      sent_total: '250000000',
      tx_count: '2',
    };
    const mempool = { balanceDelta: 1000n, txCount: 1 };

    ch.queryOne.mockResolvedValue(summary);
    ch.query.mockResolvedValue([
      { txid: '1'.repeat(64) },
      { txid: '2'.repeat(64) },
    ]);
    getMempoolAddressDeltas.mockResolvedValue(new Map([['taddr', mempool]]));

    await expect(service.getAddressSummary('taddr', false)).resolves.toEqual({
      summary,
      transactions: ['1'.repeat(64), '2'.repeat(64)],
      mempool,
    });

    ch.query.mockClear();

    await expect(service.getAddressSummary('taddr', true)).resolves.toEqual({
      summary,
      transactions: [],
      mempool,
    });
    expect(ch.query).not.toHaveBeenCalled();
  });

  test('caps address utxo inputs and applies row limit', async () => {
    const { service, ch } = createService();
    const addresses = Array.from({ length: 150 }, (_, index) => ` taddr${index} `);
    ch.query.mockResolvedValue([]);
    ch.queryOne.mockResolvedValue({ chain_height: 200, current_height: 200 });

    await expect(service.getAddressUtxos(addresses, true)).resolves.toEqual([]);

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('LIMIT {limit:UInt32}');
    expect(params.addresses).toHaveLength(100);
    expect(params.addresses[0]).toBe('taddr0');
    expect(params.addresses[99]).toBe('taddr99');
    expect(params.limit).toBe(5000);
  });

  test('reconstructs blank standard address utxo scripts and preserves stored scripts', async () => {
    const { service, ch } = createService();
    const storedScript = '76a90088ac';
    ch.queryOne.mockResolvedValue({ chain_height: 200, current_height: 200 });
    ch.query.mockResolvedValue([
      {
        address: P2PKH_ADDRESS,
        txid: '1'.repeat(64),
        vout: 0,
        script_pubkey: '',
        script_type: 'pubkeyhash',
        value: '100',
        block_height: 198,
      },
      {
        address: P2SH_ADDRESS,
        txid: '2'.repeat(64),
        vout: 1,
        script_pubkey: '',
        script_type: 'scripthash',
        value: '200',
        block_height: 199,
      },
      {
        address: 'not-a-flux-address',
        txid: '3'.repeat(64),
        vout: 2,
        script_pubkey: storedScript,
        script_type: 'nonstandard',
        value: '300',
        block_height: 200,
      },
    ]);

    const result = await service.getAddressUtxos([P2PKH_ADDRESS, P2SH_ADDRESS, 'not-a-flux-address'], false);
    const [sql] = ch.query.mock.calls[0];

    expect(sql).toContain('script_type');
    expect(result.map((utxo) => utxo.script_pubkey)).toEqual([
      P2PKH_SCRIPT,
      P2SH_SCRIPT,
      storedScript,
    ]);
  });

  test('broadcasts raw transaction and returns txid', async () => {
    const { service, rpc } = createService();
    rpc.sendRawTransaction.mockResolvedValue('f'.repeat(64));

    await expect(service.sendRawTransaction('01000000')).resolves.toBe('f'.repeat(64));
    expect(rpc.sendRawTransaction).toHaveBeenCalledWith('01000000');
  });

  test('estimates multiple fee targets', async () => {
    const { service, rpc } = createService();
    rpc.estimateFee.mockImplementation(async (target: number) => target / 1000);

    await expect(service.estimateFees([2, 6, 24])).resolves.toEqual({
      2: 0.002,
      6: 0.006,
      24: 0.024,
    });
    expect(rpc.estimateFee).toHaveBeenCalledTimes(3);
    expect(rpc.estimateFee).toHaveBeenNthCalledWith(1, 2);
    expect(rpc.estimateFee).toHaveBeenNthCalledWith(2, 6);
    expect(rpc.estimateFee).toHaveBeenNthCalledWith(3, 24);
  });
});

function mockTransactionLookup(
  ch: ReturnType<typeof createService>['ch'],
  txid: string,
  inputs: Array<{ txid: string; vout: number; address: string; value: string; script_type: string }>
) {
  ch.queryOne.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
    if (sql.includes('FROM transactions')) {
      return {
        txid,
        version: 1,
        locktime: 0,
        block_height: 100,
        timestamp: 1700000000,
        input_total: '300',
        output_total: '250',
        fee: '50',
        size: 225,
        is_coinbase: 0,
        is_fluxnode_tx: 0,
        is_valid: 1,
      };
    }

    if (sql.includes('FROM blocks') && params?.height === 100) {
      return { hash: 'b'.repeat(64), height: 100, is_valid: 1 };
    }

    if (sql.includes('FROM sync_state')) {
      return { chain_height: 105, current_height: 105 };
    }

    if (sql.includes('max(height)')) {
      return { height: 105 };
    }

    throw new Error(`Unexpected queryOne: ${sql}`);
  });
  ch.query.mockImplementation(async (sql: string) => {
    if (sql.includes('WHERE txid =')) {
      return [];
    }

    if (sql.includes('WHERE spent_txid')) {
      return inputs;
    }

    throw new Error(`Unexpected query: ${sql}`);
  });
}
