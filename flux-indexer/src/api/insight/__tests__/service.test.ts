jest.mock('../../../parsers/block-parser', () => ({
  extractTransactionFromBlock: jest.fn(),
}));

import { extractTransactionFromBlock } from '../../../parsers/block-parser';
import { RPCError } from '../../../types';
import { encodeFluxAddress } from '../../../utils/script-utils';
import { InsightCompatibilityService } from '../service';
import { InsightValidationError } from '../utils';

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
    getDifficulty: jest.fn(),
    getBestBlockHash: jest.fn(),
    getMiningInfo: jest.fn(),
    getPeerInfo: jest.fn(),
    getInfo: jest.fn(),
    getVersion: jest.fn(),
    verifyMessage: jest.fn(),
    viewDeterministicFluxNodeList: jest.fn(),
    dosList: jest.fn(),
    startList: jest.fn(),
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
      more: false,
    });

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('FROM blocks');
    expect(sql).not.toContain('timestamp >=');
    expect(sql).not.toContain('timestamp <=');
    expect(sql).toContain('height <= {maxHeight:UInt32}');
    expect(sql).toContain('height >= {minHeight:UInt32}');
    expect(params).toEqual({ limit: 3, maxHeight: 1000, minHeight: 748 });
    expect(ch.queryOne).toHaveBeenCalledWith(expect.stringContaining('FROM sync_state'));
  });

  test('reports more recent blocks when the page is truncated', async () => {
    const { service, ch } = createService();
    const blocks = [
      { hash: 'd'.repeat(64), height: 103, is_valid: 1 },
      { hash: 'c'.repeat(64), height: 102, is_valid: 1 },
      { hash: 'b'.repeat(64), height: 101, is_valid: 1 },
    ];
    ch.queryOne.mockResolvedValue({ chain_height: 1000, current_height: 1000 });
    ch.query.mockResolvedValue(blocks);

    await expect(service.listBlocks({ limit: '2' })).resolves.toEqual({
      blocks: blocks.slice(0, 2),
      blockDate: null,
      more: true,
    });
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
      more: false,
    });

    const [, params] = ch.query.mock.calls[0];
    expect(params).toEqual({ limit: 3, maxHeight: 500, minHeight: 248 });
  });

  test('includes the genesis block when the recent window reaches height zero', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue({ chain_height: 100, current_height: 100 });
    ch.query.mockResolvedValue([]);

    await service.listBlocks({ limit: '2' });

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('height >= {minHeight:UInt32}');
    expect(params).toMatchObject({ maxHeight: 100, minHeight: 0 });
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
      more: false,
    });
  });

  test('caps the blockDate window with startTimestamp and reports truncation', async () => {
    const { service, ch } = createService();
    const rows = [
      { hash: 'c'.repeat(64), height: 102, timestamp: 1781100000, is_valid: 1 },
      { hash: 'b'.repeat(64), height: 101, timestamp: 1781099000, is_valid: 1 },
    ];
    ch.query.mockResolvedValue(rows);

    const result = await service.listBlocks({
      blockDate: '2026-06-10',
      startTimestamp: '1781100500',
      limit: '1',
    });

    expect(result.blocks).toEqual([rows[0]]);
    expect(result.more).toBe(true);

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('timestamp >= {start:UInt32}');
    expect(sql).toContain('timestamp <= {end:UInt32}');
    expect(sql).toContain('ORDER BY timestamp DESC, height DESC');
    expect(params).toEqual({ start: 1781049600, end: 1781100500, limit: 2 });
  });

  test('clamps startTimestamp to the end of the requested blockDate', async () => {
    const { service, ch } = createService();
    ch.query.mockResolvedValue([]);

    await expect(service.listBlocks({
      blockDate: '2026-06-10',
      startTimestamp: '4294967295',
      limit: '1',
    })).resolves.toEqual({
      blocks: [],
      blockDate: expect.objectContaining({ end: 1781135999 }),
      more: false,
    });

    const [, params] = ch.query.mock.calls[0];
    expect(params).toEqual({ start: 1781049600, end: 1781135999, limit: 2 });
  });

  test('rejects invalid startTimestamp values', async () => {
    const { service } = createService();

    await expect(service.listBlocks({ blockDate: '2026-06-10', startTimestamp: 'abc' }))
      .rejects.toBeInstanceOf(InsightValidationError);
    await expect(service.listBlocks({ blockDate: '2026-06-10', startTimestamp: 'abc' }))
      .rejects.toThrow('Invalid startTimestamp (must be an integer between 1 and 4294967295)');
    await expect(service.listBlocks({ blockDate: '2026-06-10', startTimestamp: '0' }))
      .rejects.toThrow('Invalid startTimestamp (must be an integer between 1 and 4294967295)');
    await expect(service.listBlocks({ blockDate: '2026-06-10', startTimestamp: '4294967296' }))
      .rejects.toThrow('Invalid startTimestamp (must be an integer between 1 and 4294967295)');
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

  test('decorates inputs with decoded scriptSig and sequence even for single-input txs', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);
    const prev = '1'.repeat(64);
    const inputs = [
      { txid: prev, vout: 0, address: 'first', value: '100', script_type: 'pubkeyhash' },
    ];

    mockTransactionLookup(ch, txid, inputs);
    rpc.getRawTransaction.mockResolvedValue({
      txid,
      vin: [
        { txid: prev, vout: 0, sequence: 0, scriptSig: { hex: '47abcd', asm: '47abcd[ALL]' } },
      ],
    });

    const result = await service.getTransaction(txid);

    expect(rpc.getRawTransaction).toHaveBeenCalledWith(txid, true);
    expect(result?.inputs).toEqual([
      {
        ...inputs[0],
        sequence: 0,
        script_sig: { hex: '47abcd', asm: '47abcd[ALL]' },
      },
    ]);
    expect(result?.coinbaseScript).toBeNull();
  });

  test('captures the real coinbase script from the decoded transaction', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);

    mockTransactionLookup(ch, txid, [], { is_coinbase: 1 });
    rpc.getRawTransaction.mockResolvedValue({
      txid,
      vin: [{ coinbase: '0341e21f0102', sequence: 4294967295 }],
    });

    const result = await service.getTransaction(txid);

    expect(rpc.getRawTransaction).toHaveBeenCalledWith(txid, true);
    expect(result?.coinbaseScript).toBe('0341e21f0102');
    expect(result?.inputs).toEqual([]);
  });

  test('falls back to a null coinbase script when the decoded lookup fails', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);

    mockTransactionLookup(ch, txid, [], { is_coinbase: 1 });
    rpc.getRawTransaction.mockRejectedValue(new Error('RPC unavailable'));

    const result = await service.getTransaction(txid);

    expect(result?.coinbaseScript).toBeNull();
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

  test('serves mempool transactions from the daemon when ClickHouse has no row', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);
    const confirmedParent = '1'.repeat(64);
    const mempoolParent = '2'.repeat(64);

    ch.queryOne.mockResolvedValue(null);
    ch.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('(txid, vout) IN')) {
        expect(params?.outpoints).toEqual([[confirmedParent, 0], [mempoolParent, 1]]);
        return [
          { txid: confirmedParent, vout: 0, address: 't1Confirmed', value: '150000000', script_type: 'pubkeyhash' },
        ];
      }

      throw new Error(`Unexpected query: ${sql}`);
    });
    rpc.getRawTransaction.mockImplementation(async (requested: string) => {
      if (requested === txid) {
        return {
          txid,
          version: 4,
          locktime: 0,
          size: 250,
          vin: [
            { txid: confirmedParent, vout: 0, sequence: 4294967294, scriptSig: { hex: 'aa', asm: 'aa-asm' } },
            { txid: mempoolParent, vout: 1, sequence: 4294967295, scriptSig: { hex: 'bb', asm: 'bb-asm' } },
          ],
          vout: [
            {
              value: 2.4,
              n: 0,
              scriptPubKey: { hex: '76a914ff', asm: 'OP_DUP', type: 'pubkeyhash', addresses: ['t1Recipient'] },
            },
          ],
        };
      }

      if (requested === mempoolParent) {
        return {
          txid: mempoolParent,
          vout: [
            { value: 1, n: 1, scriptPubKey: { hex: '76a9', type: 'pubkeyhash', addresses: ['t1MempoolParent'] } },
          ],
        };
      }

      throw new Error(`Unexpected getRawTransaction: ${requested}`);
    });

    const result = await service.getTransaction(txid);

    expect(rpc.getRawTransaction).toHaveBeenCalledWith(txid, true);
    expect(result?.tx).toMatchObject({
      txid,
      version: 4,
      block_height: -1,
      input_total: '250000000',
      output_total: '240000000',
      fee: '10000000',
      size: 250,
      is_coinbase: 0,
      is_valid: 1,
    });
    expect(typeof result?.tx.timestamp).toBe('number');
    expect(result?.confirmations).toBe(0);
    expect(result?.blockHash).toBeNull();
    expect(result?.inputs).toEqual([
      {
        txid: confirmedParent,
        vout: 0,
        address: 't1Confirmed',
        value: '150000000',
        script_type: 'pubkeyhash',
        sequence: 4294967294,
        script_sig: { hex: 'aa', asm: 'aa-asm' },
      },
      {
        txid: mempoolParent,
        vout: 1,
        address: 't1MempoolParent',
        value: '100000000',
        script_type: 'pubkeyhash',
        sequence: 4294967295,
        script_sig: { hex: 'bb', asm: 'bb-asm' },
      },
    ]);
    expect(result?.outputs).toEqual([
      {
        vout: 0,
        address: 't1Recipient',
        value: '240000000',
        script_pubkey: '76a914ff',
        script_type: 'pubkeyhash',
        spent: 0,
        spent_txid: null,
        spent_index: null,
        spent_block_height: null,
      },
    ]);
  });

  test('omits the mempool fee when a parent outpoint cannot be resolved', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);
    const unknownParent = '3'.repeat(64);

    ch.queryOne.mockResolvedValue(null);
    ch.query.mockResolvedValue([]);
    rpc.getRawTransaction.mockImplementation(async (requested: string) => {
      if (requested === txid) {
        return {
          txid,
          version: 4,
          locktime: 0,
          size: 200,
          vin: [{ txid: unknownParent, vout: 0, sequence: 4294967295 }],
          vout: [
            { value: 0.9, n: 0, scriptPubKey: { hex: '76a9', type: 'pubkeyhash', addresses: ['t1Out'] } },
          ],
        };
      }

      throw new RPCError('No information available about transaction', -5);
    });

    const result = await service.getTransaction(txid);

    expect(result?.tx.fee).toBeNull();
    expect(result?.inputs).toEqual([
      {
        txid: unknownParent,
        vout: 0,
        address: '',
        value: '0',
        script_type: undefined,
        sequence: 4294967295,
        script_sig: undefined,
      },
    ]);
  });

  test('returns null when the daemon does not know the transaction', async () => {
    const { service, ch, rpc } = createService();

    ch.queryOne.mockResolvedValue(null);
    rpc.getRawTransaction.mockRejectedValue(
      new RPCError('No information available about transaction', -5)
    );

    await expect(service.getTransaction('a'.repeat(64))).resolves.toBeNull();
    expect(rpc.getRawTransaction).toHaveBeenCalledWith('a'.repeat(64), true);
  });

  test('rethrows daemon errors other than not-found for unindexed transactions', async () => {
    const { service, ch, rpc } = createService();

    ch.queryOne.mockResolvedValue(null);
    rpc.getRawTransaction.mockRejectedValue(new RPCError('Work queue depth exceeded', 500));

    await expect(service.getTransaction('a'.repeat(64)))
      .rejects.toThrow('Work queue depth exceeded');
  });

  test('reports daemon block data when the daemon already sees the tx as mined', async () => {
    const { service, ch, rpc } = createService();
    const txid = 'a'.repeat(64);
    const blockHash = 'b'.repeat(64);

    ch.queryOne.mockResolvedValue(null);
    rpc.getRawTransaction.mockResolvedValue({
      txid,
      version: 4,
      locktime: 0,
      size: 120,
      confirmations: 3,
      blockhash: blockHash,
      height: 9000,
      time: 1700000123,
      blocktime: 1700000123,
      vin: [{ coinbase: '0328e80b00', sequence: 4294967295 }],
      vout: [
        { value: 5, n: 0, scriptPubKey: { hex: '76a9', type: 'pubkeyhash', addresses: ['t1Miner'] } },
      ],
    });

    const result = await service.getTransaction(txid);

    expect(result?.tx).toMatchObject({
      block_height: 9000,
      timestamp: 1700000123,
      is_coinbase: 1,
      fee: null,
    });
    expect(result?.confirmations).toBe(3);
    expect(result?.blockHash).toBe(blockHash);
    expect(result?.coinbaseScript).toBe('0328e80b00');
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

    const [txidSql, txidParams] = ch.query.mock.calls[0];
    expect(txidSql).toContain('LIMIT {limit:UInt32}');
    expect(txidSql).toContain('OFFSET {offset:UInt32}');
    expect(txidParams).toEqual({ address: 'taddr', limit: 1000, offset: 0 });

    ch.query.mockClear();

    await expect(service.getAddressSummary('taddr', true)).resolves.toEqual({
      summary,
      transactions: [],
      mempool,
    });
    expect(ch.query).not.toHaveBeenCalled();
  });

  test('windows address summary transaction ids with from/to', async () => {
    const { service, ch, getMempoolAddressDeltas } = createService();
    const summary = {
      balance: '250000000',
      received_total: '500000000',
      sent_total: '250000000',
      tx_count: '5000',
    };

    ch.queryOne.mockResolvedValue(summary);
    ch.query.mockResolvedValue([{ txid: '3'.repeat(64) }]);
    getMempoolAddressDeltas.mockResolvedValue(new Map());

    const result = await service.getAddressSummary('taddr', false, { from: 1200, to: 1202 });

    expect(result.transactions).toEqual(['3'.repeat(64)]);
    const [, txidParams] = ch.query.mock.calls[0];
    expect(txidParams).toEqual({ address: 'taddr', limit: 2, offset: 1200 });
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

  test('calculates address utxo confirmations from current height', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue({ chain_height: 105, current_height: 105 });
    ch.query.mockResolvedValue([
      {
        address: P2PKH_ADDRESS,
        txid: '1'.repeat(64),
        vout: 0,
        script_pubkey: '',
        script_type: 'pubkeyhash',
        value: '100',
        block_height: 100,
      },
      {
        address: P2SH_ADDRESS,
        txid: '2'.repeat(64),
        vout: 1,
        script_pubkey: '',
        script_type: 'scripthash',
        value: '200',
        block_height: 105,
      },
    ]);

    const result = await service.getAddressUtxos([P2PKH_ADDRESS, P2SH_ADDRESS], false);

    expect(result.map((utxo) => utxo.confirmations)).toEqual([6, 1]);
    expect(ch.queryOne).toHaveBeenCalledWith(expect.stringContaining('FROM sync_state'));
  });

  test('filters mempool-spent utxos and appends mempool-created utxos when querying mempool', async () => {
    const { service, ch, getMempoolAddressDeltas } = createService();
    const spentTxid = '1'.repeat(64);
    const keptTxid = '2'.repeat(64);
    const createdTxid = 'a'.repeat(64);
    ch.queryOne.mockResolvedValue({ chain_height: 105, current_height: 105 });
    ch.query.mockResolvedValue([
      {
        address: P2PKH_ADDRESS,
        txid: spentTxid,
        vout: 0,
        script_pubkey: '',
        script_type: 'pubkeyhash',
        value: '100',
        block_height: 100,
      },
      {
        address: P2PKH_ADDRESS,
        txid: keptTxid,
        vout: 1,
        script_pubkey: '',
        script_type: 'pubkeyhash',
        value: '200',
        block_height: 105,
      },
    ]);
    getMempoolAddressDeltas.mockResolvedValue(new Map([
      [P2PKH_ADDRESS, {
        balanceDelta: 0n,
        txCount: 2,
        spentOutpoints: new Set([`${spentTxid}:0`, `${createdTxid}:1`]),
        createdUtxos: [
          { txid: createdTxid, vout: 0, value: 300n, scriptPubkey: '51' },
          // Re-spent within the mempool, so it must not be listed.
          { txid: createdTxid, vout: 1, value: 400n, scriptPubkey: '52' },
        ],
      }],
    ]));

    const result = await service.getAddressUtxos([P2PKH_ADDRESS], true);

    expect(result).toEqual([
      expect.objectContaining({ txid: keptTxid, vout: 1, value: '200', confirmations: 1 }),
      {
        address: P2PKH_ADDRESS,
        txid: createdTxid,
        vout: 0,
        script_pubkey: '51',
        value: '300',
        confirmations: 0,
      },
    ]);
  });

  test('returns confirmed-only utxos without touching the mempool provider when queryMempool is false', async () => {
    const { service, ch, getMempoolAddressDeltas } = createService();
    const spentTxid = '1'.repeat(64);
    ch.queryOne.mockResolvedValue({ chain_height: 105, current_height: 105 });
    ch.query.mockResolvedValue([
      {
        address: P2PKH_ADDRESS,
        txid: spentTxid,
        vout: 0,
        script_pubkey: '',
        script_type: 'pubkeyhash',
        value: '100',
        block_height: 100,
      },
    ]);
    getMempoolAddressDeltas.mockResolvedValue(new Map([
      [P2PKH_ADDRESS, {
        balanceDelta: 0n,
        txCount: 1,
        spentOutpoints: new Set([`${spentTxid}:0`]),
        createdUtxos: [{ txid: 'a'.repeat(64), vout: 0, value: 300n, scriptPubkey: '51' }],
      }],
    ]));

    const result = await service.getAddressUtxos([P2PKH_ADDRESS], false);

    expect(result).toEqual([
      expect.objectContaining({ txid: spentTxid, vout: 0, value: '100', confirmations: 6 }),
    ]);
    expect(getMempoolAddressDeltas).not.toHaveBeenCalled();
  });

  test('pushes the collateral value filter into sql so capped addresses keep collateral utxos', async () => {
    const { service, ch } = createService();
    const collateral = 1000n * 100000000n;
    const collateralTxid = 'c'.repeat(64);
    ch.queryOne.mockResolvedValue({ chain_height: 200, current_height: 200 });
    ch.query.mockImplementation(async (_sql: string, params?: Record<string, unknown>) => {
      // Simulate an address whose 5000 newest utxos are all non-collateral:
      // the old collateral row only comes back when the filter reaches SQL.
      if (!Array.isArray(params?.values)) {
        return [];
      }
      return [
        {
          address: P2PKH_ADDRESS,
          txid: collateralTxid,
          vout: 0,
          script_pubkey: '',
          script_type: 'pubkeyhash',
          value: collateral.toString(),
          block_height: 1,
        },
      ];
    });

    const result = await service.getAddressUtxos([P2PKH_ADDRESS], true, [collateral]);

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('AND value IN {values:Array(UInt64)}');
    expect(sql).toContain('LIMIT {limit:UInt32}');
    expect(params).toEqual({
      addresses: [P2PKH_ADDRESS],
      values: ['100000000000'],
      limit: 5000,
    });
    expect(result).toEqual([
      expect.objectContaining({ txid: collateralTxid, value: '100000000000', confirmations: 200 }),
    ]);
  });

  test('applies the collateral value filter to mempool-created utxos', async () => {
    const { service, ch, getMempoolAddressDeltas } = createService();
    const collateral = 1000n * 100000000n;
    ch.queryOne.mockResolvedValue({ chain_height: 200, current_height: 200 });
    ch.query.mockResolvedValue([]);
    getMempoolAddressDeltas.mockResolvedValue(new Map([
      [P2PKH_ADDRESS, {
        balanceDelta: 0n,
        txCount: 1,
        spentOutpoints: new Set<string>(),
        createdUtxos: [
          { txid: 'a'.repeat(64), vout: 0, value: collateral, scriptPubkey: '51' },
          { txid: 'a'.repeat(64), vout: 1, value: 42n, scriptPubkey: '52' },
        ],
      }],
    ]));

    const result = await service.getAddressUtxos([P2PKH_ADDRESS], true, [collateral]);

    expect(result).toEqual([
      expect.objectContaining({ txid: 'a'.repeat(64), vout: 0, value: '100000000000', confirmations: 0 }),
    ]);
  });

  test('gets multi-address transactions with deduped capped addresses and full tx details in order', async () => {
    const { service, ch } = createService();
    const firstTxid = 'a'.repeat(64);
    const secondTxid = 'b'.repeat(64);
    const addresses = [
      ' alpha ',
      'beta',
      'alpha',
      '',
      ...Array.from({ length: 150 }, (_, index) => `addr${index}`),
    ];
    let txLookupParams: Record<string, unknown> | undefined;
    let countParams: Record<string, unknown> | undefined;

    ch.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('FROM address_transactions')) {
        txLookupParams = params;
        return [
          { txid: secondTxid },
          { txid: firstTxid },
        ];
      }

      if (sql.includes('WHERE txid =')) {
        return [];
      }

      if (sql.includes('WHERE spent_txid')) {
        return [];
      }

      throw new Error(`Unexpected query: ${sql}`);
    });
    ch.queryOne.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('FROM address_transactions') && sql.includes('uniqExact(txid)')) {
        countParams = params;
        return { totalItems: '4' };
      }

      if (sql.includes('FROM transactions') && typeof params?.txid === 'string') {
        return {
          txid: params.txid,
          version: 1,
          locktime: 0,
          block_height: params.txid === secondTxid ? 200 : 199,
          timestamp: params.txid === secondTxid ? 1700000200 : 1700000100,
          input_total: '0',
          output_total: '0',
          fee: '0',
          size: 100,
          is_coinbase: 0,
          is_fluxnode_tx: 0,
          is_valid: 1,
        };
      }

      if (sql.includes('FROM blocks') && typeof params?.height === 'number') {
        return { hash: `${params.height}`.padStart(64, '0'), height: params.height, is_valid: 1 };
      }

      if (sql.includes('FROM sync_state')) {
        return { chain_height: 200, current_height: 200 };
      }

      throw new Error(`Unexpected queryOne: ${sql}`);
    });

    const insight = service as unknown as {
      getAddressTransactions(
        addresses: string[],
        range: { from: number; to: number; limit: number }
      ): Promise<{ totalItems: number; items: Array<{ tx: { txid: string } }> }>;
    };

    const result = await insight.getAddressTransactions(addresses, { from: 2, to: 4, limit: 2 });

    expect(result.totalItems).toBe(4);
    expect(result.items.map((item) => item.tx.txid)).toEqual([secondTxid, firstTxid]);
    expect(txLookupParams).toEqual({
      addresses: expect.arrayContaining(['alpha', 'beta']),
      limit: 2,
      offset: 2,
    });
    expect((txLookupParams?.addresses as string[])).toHaveLength(100);
    expect((countParams?.addresses as string[])).toHaveLength(100);
    expect(ch.query.mock.calls[0][0]).toContain('address IN {addresses:Array(String)}');
    expect(ch.query.mock.calls[0][0]).toContain('LIMIT {limit:UInt32}');
    expect(ch.query.mock.calls[0][0]).toContain('OFFSET {offset:UInt32}');
  });

  test('caps multi-address transaction range limit before fetching full tx details', async () => {
    const { service, ch } = createService();
    let txLookupParams: Record<string, unknown> | undefined;

    ch.query.mockImplementation(async (sql: string, params?: Record<string, unknown>) => {
      if (sql.includes('FROM address_transactions')) {
        txLookupParams = params;
        return [];
      }

      throw new Error(`Unexpected query: ${sql}`);
    });
    ch.queryOne.mockImplementation(async (sql: string) => {
      if (sql.includes('FROM address_transactions') && sql.includes('uniqExact(txid)')) {
        return { totalItems: '0' };
      }

      throw new Error(`Unexpected queryOne: ${sql}`);
    });

    const insight = service as unknown as {
      getAddressTransactions(
        addresses: string[],
        range: { from: number; to: number; limit: number }
      ): Promise<{ totalItems: number; items: unknown[] }>;
    };

    await expect(insight.getAddressTransactions(['alpha'], { from: 0, to: 500, limit: 500 }))
      .resolves.toEqual({ totalItems: 0, items: [] });
    expect(txLookupParams).toMatchObject({ limit: 50, offset: 0 });
  });

  test('sums multi-address balances with requested mempool deltas using safe zatoshi output', async () => {
    const { service, ch, getMempoolAddressDeltas } = createService();
    ch.queryOne.mockResolvedValue({ balance: '9007199254740992' });
    getMempoolAddressDeltas.mockResolvedValue(new Map([
      ['alpha', { balanceDelta: 2n, txCount: 1 }],
      ['beta', { balanceDelta: 3n, txCount: 1 }],
      ['ignored', { balanceDelta: 1000000000n, txCount: 1 }],
    ]));

    const insight = service as unknown as {
      getAddressBalanceSum(addresses: string[]): Promise<{
        balance: string | number;
        unconfirmedBalance: string | number;
        immature: string | number;
      }>;
    };

    await expect(insight.getAddressBalanceSum([' alpha ', 'beta', 'alpha', '']))
      .resolves.toEqual({
        balance: '9007199254740992',
        unconfirmedBalance: 5,
        immature: 0,
      });

    const [sql, params] = ch.queryOne.mock.calls[0];
    expect(sql).toContain('sumMerge(balance)');
    expect(sql).toContain('address IN {addresses:Array(String)}');
    expect(params).toEqual({ addresses: ['alpha', 'beta'] });
  });

  test('pages block transaction lookups ten at a time with a shared chain height', async () => {
    const { service, ch } = createService();
    const txids = Array.from({ length: 205 }, (_, index) => `${index}`.padStart(64, '0'));
    ch.queryOne.mockResolvedValue({ chain_height: 300, current_height: 300 });
    jest.spyOn(service, 'getBlock').mockResolvedValue({
      block: { hash: 'a'.repeat(64), height: 1 } as any,
      txids,
      confirmations: 1,
      nextBlockHash: null,
    });
    const getTransaction = jest.spyOn(service, 'getTransaction')
      .mockImplementation(async (txid: string) => ({
        tx: { txid } as any,
        outputs: [],
        inputs: [],
        confirmations: 1,
        blockHash: 'a'.repeat(64),
      }));

    const firstPage = await service.getTransactionsByBlock('a'.repeat(64));

    expect(firstPage.pagesTotal).toBe(21);
    expect(firstPage.txs).toHaveLength(10);
    expect(getTransaction).toHaveBeenCalledTimes(10);
    expect(getTransaction).toHaveBeenNthCalledWith(1, txids[0], 300);
    expect(getTransaction).toHaveBeenNthCalledWith(10, txids[9], 300);

    getTransaction.mockClear();

    const lastPage = await service.getTransactionsByBlock('a'.repeat(64), 20);

    expect(lastPage.pagesTotal).toBe(21);
    expect(lastPage.txs).toHaveLength(5);
    expect(getTransaction).toHaveBeenCalledTimes(5);
    expect(getTransaction).toHaveBeenNthCalledWith(1, txids[200], 300);
    expect(getTransaction).toHaveBeenNthCalledWith(5, txids[204], 300);

    getTransaction.mockClear();

    const beyondEnd = await service.getTransactionsByBlock('a'.repeat(64), 21);

    expect(beyondEnd).toEqual({ pagesTotal: 21, txs: [] });
    expect(getTransaction).not.toHaveBeenCalled();
  });

  test('broadcasts raw transaction and returns txid', async () => {
    const { service, rpc } = createService();
    rpc.sendRawTransaction.mockResolvedValue('f'.repeat(64));

    await expect(service.sendRawTransaction('01000000')).resolves.toBe('f'.repeat(64));
    expect(rpc.sendRawTransaction).toHaveBeenCalledWith('01000000');
  });

  test('returns legacy currency compatibility shape with null rate', () => {
    const { service } = createService();

    expect(service.getCurrency()).toEqual({
      status: 200,
      data: { rate: null, short: 'FLUX' },
    });
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

  test('gets status wrappers from RPC helpers', async () => {
    const { service, rpc } = createService();
    rpc.getDifficulty.mockResolvedValue(123.456);
    rpc.getBestBlockHash.mockResolvedValue('besthash');
    rpc.getInfo.mockResolvedValue({ blocks: 100 });

    await expect(service.getStatus('getDifficulty')).resolves.toEqual({ difficulty: 123.456 });
    await expect(service.getStatus('getLastBlockHash')).resolves.toEqual({
      syncTipHash: 'besthash',
      lastblockhash: 'besthash',
    });
    await expect(service.getStatus(undefined)).resolves.toEqual({ info: { blocks: 100 } });
  });

  test('returns sync_state in legacy sync shape with string numeric fields parsed safely', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue({
      current_height: '99',
      chain_height: '100',
      sync_percentage: '99.5',
      is_syncing: '1',
    });

    await expect(service.getSync()).resolves.toEqual({
      status: 'syncing',
      blockChainHeight: 100,
      syncPercentage: 99.5,
      height: 99,
      error: null,
      type: 'bitcore node',
    });
  });

  test('normalizes fresh sync_state seed to nonnegative syncing state', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue({
      current_height: -1,
      chain_height: 0,
      sync_percentage: 0,
      is_syncing: 0,
    });

    await expect(service.getSync()).resolves.toEqual({
      status: 'syncing',
      blockChainHeight: 0,
      syncPercentage: 0,
      height: 0,
      error: null,
      type: 'bitcore node',
    });
  });

  test('reports syncing when progress is incomplete even if sync_state flag is false', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue({
      current_height: 10,
      chain_height: 100,
      sync_percentage: 10,
      is_syncing: 0,
    });

    await expect(service.getSync()).resolves.toMatchObject({
      status: 'syncing',
      blockChainHeight: 100,
      syncPercentage: 10,
      height: 10,
    });
  });

  test('filters deterministic FluxNode list by collateral outpoint while preserving RPC envelope', async () => {
    const { service, rpc } = createService();
    const first = { txhash: 'abc', outidx: 1, ip: '1.2.3.4:16125', status: 'ENABLED' };
    const second = { txhash: 'def', outidx: 0, ip: '5.6.7.8:16125', status: 'ENABLED' };
    rpc.viewDeterministicFluxNodeList.mockResolvedValue({
      result: [first, second],
      error: null,
      id: 'flux',
    });

    await expect(service.listFluxNodes('abc-1')).resolves.toEqual({
      result: [first],
      error: null,
      id: 'flux',
    });
  });

  test('filters deterministic FluxNode list by exact collateral outpoint', async () => {
    const { service, rpc } = createService();
    const txhash = 'a'.repeat(64);
    const first = { txhash, outidx: 1, ip: '1.2.3.4:16125', status: 'ENABLED' };
    const second = { txhash, outidx: 10, ip: '5.6.7.8:16125', status: 'ENABLED' };
    rpc.viewDeterministicFluxNodeList.mockResolvedValue({
      result: [first, second],
      error: null,
      id: 'flux',
    });

    await expect(service.listFluxNodes(`${txhash}-1`)).resolves.toEqual({
      result: [first],
      error: null,
      id: 'flux',
    });
  });

  test('gets latest supply as a zatoshis string using schema-correct ordering', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue({ total_supply: '123456789' });

    await expect(service.getSupply()).resolves.toBe('123456789');

    const [sql] = ch.queryOne.mock.calls[0];
    expect(sql).toContain('FROM supply_stats');
    expect(sql).toContain('ORDER BY block_height DESC, _version DESC');
  });

  test('anchors circulating supply to the indexed total minus the locked parallel-asset delta', async () => {
    const { service, ch } = createService();
    // At height 2676212 the theoretical locked delta is 1455519600085160
    // zatoshis (mainchain 42616569450000000 - all chains 41161049849914840,
    // pinned against the legacy insight-api).
    ch.queryOne.mockResolvedValue({ block_height: 2676212, total_supply: '42000000000000000' });

    await expect(service.getCirculatingSupply()).resolves.toBe('40544480399914840');

    const [sql] = ch.queryOne.mock.calls[0];
    expect(sql).toContain('FROM supply_stats');
    expect(sql).toContain('block_height');
    expect(sql).toContain('ORDER BY block_height DESC, _version DESC');
  });

  test('serves the theoretical main chain supply for circulating-locked lookups', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue({ block_height: 2676212, total_supply: '42000000000000000' });

    await expect(service.getMainChainCirculatingLockedSupply()).resolves.toBe('42616569450000000');
  });

  test('returns zero supplies when no supply stats row exists', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValue(undefined);

    await expect(service.getSupply()).resolves.toBe('0');
    await expect(service.getCirculatingSupply()).resolves.toBe('0');
    await expect(service.getMainChainCirculatingLockedSupply()).resolves.toBe('0');
  });

  test('gets a supply statistic series from daily supply rows', async () => {
    const { service, ch } = createService();
    ch.query.mockResolvedValue([
      { date: '2026-06-09', total_supply: '123456789' },
      { date: '2026-06-10', total_supply: '223456789' },
    ]);

    const insight = service as unknown as {
      getStatisticSeries(kind: string, rawDays?: string): Promise<Array<{ date: string; sum: string }>>;
    };

    await expect(insight.getStatisticSeries('supply', '30')).resolves.toEqual([
      { date: '2026-06-09', sum: '1.23456789' },
      { date: '2026-06-10', sum: '2.23456789' },
    ]);

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('FROM mv_daily_supply');
    expect(sql).toContain('WHERE day >= today() - {days:UInt16}');
    expect(params).toEqual({ days: 30 });
  });

  test('does not alias network hash statistic series to difficulty', async () => {
    const { service, ch } = createService();
    const insight = service as unknown as {
      getStatisticSeries(kind: string, rawDays?: string): Promise<unknown[]>;
    };

    await expect(insight.getStatisticSeries('network-hash', '30'))
      .rejects.toThrow('Network hash statistics are not implemented');
    expect(ch.query).not.toHaveBeenCalled();
  });

  test('gets transaction statistic series from latest valid transaction and block rows', async () => {
    const { service, ch } = createService();
    ch.query.mockResolvedValue([
      { date: '2026-06-10', transaction_count: '7', block_count: '2' },
    ]);
    const insight = service as unknown as {
      getStatisticSeries(
        kind: string,
        rawDays?: string
      ): Promise<Array<{ date: string; transaction_count: number; block_count: number }>>;
    };

    await expect(insight.getStatisticSeries('transactions', '14')).resolves.toEqual([
      { date: '2026-06-10', transaction_count: 7, block_count: 2 },
    ]);

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('FROM transactions');
    expect(sql).toContain('FROM blocks');
    expect(sql).toContain('LIMIT 1 BY txid');
    expect(sql).toContain('LIMIT 1 BY height');
    expect(sql).not.toContain('mv_hourly_tx_count');
    expect(params).toEqual({ days: 14 });
  });

  test('gets total statistics with legacy pool keys and no fake network hash', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockImplementation(async (sql: string) => {
      if (sql.includes('n_blocks_mined')) {
        return {
          n_blocks_mined: '3',
          time_between_blocks: 120,
          mined_currency_amount: '15000000000',
          difficulty: 42,
        };
      }

      if (sql.includes('number_of_transactions')) {
        return {
          number_of_transactions: '9',
          transaction_fees: '123000000',
          outputs_volume: '456000000',
        };
      }

      throw new Error(`Unexpected queryOne: ${sql}`);
    });
    ch.query.mockResolvedValue([
      { producer: 'pool-a', blocks_found: '2' },
      { producer: 'pool-b', blocks_found: '1' },
    ]);

    const insight = service as unknown as {
      getStatisticsTotal(): Promise<{
        network_hash_ps: number;
        blocks_by_pool: Array<{
          address: string;
          poolName: string;
          url: string | null;
          blocks_found: number;
          percent_total: number;
        }>;
      }>;
    };

    await expect(insight.getStatisticsTotal()).resolves.toMatchObject({
      network_hash_ps: 0,
      blocks_by_pool: [
        { address: 'pool-a', poolName: 'pool-a', url: null, blocks_found: 2, percent_total: 66.666667 },
        { address: 'pool-b', poolName: 'pool-b', url: null, blocks_found: 1, percent_total: 33.333333 },
      ],
    });
    const blockSql = ch.queryOne.mock.calls[0][0];
    expect(blockSql).toContain('toString(count()) AS n_blocks_mined');
    expect(blockSql).toContain('avgIf(timestamp - previous_timestamp, previous_timestamp > 0)');
    expect(blockSql).not.toMatch(/WHERE\s+previous_timestamp/i);
    const poolSql = ch.query.mock.calls[0][0];
    expect(poolSql).toContain('count() AS blocks_found_count');
    expect(poolSql).toContain('toString(blocks_found_count) AS blocks_found');
    expect(poolSql).toContain('ORDER BY blocks_found_count DESC, producer ASC');
    expect(poolSql).not.toContain('ORDER BY blocks_found DESC');
  });

  test('gets balance intervals with legacy count and sum fields', async () => {
    const { service, ch } = createService();
    ch.query.mockResolvedValue([
      { bucket: '1-10 FLUX', count: '2', sum: '300000000' },
      { bucket: '10,000+ FLUX', count: 1, sum: '1000000000000' },
    ]);

    const insight = service as unknown as {
      getBalanceIntervals(): Promise<Array<{ min: string; max: string | null; count: number; sum: string | number }>>;
    };

    const result = await insight.getBalanceIntervals();

    expect(result).toEqual(expect.arrayContaining([
      { min: '100000000', max: '1000000000', count: 2, sum: 300000000 },
      { min: '1000000000000', max: null, count: 1, sum: 1000000000000 },
    ]));
    const [sql] = ch.query.mock.calls[0];
    expect(sql).toContain('toString(sum(balance)) AS sum');
    expect(sql).toMatch(/multiIf\([\s\S]+?\)\s+AS bucket,\s*balance\s+FROM \(/);
  });

  test('gets richer-than statistics as a FLUX-denominated array', async () => {
    const { service, ch } = createService();
    ch.query.mockResolvedValue([
      { threshold: '100000000', count: '3' },
      { threshold: '1000000000', count: 2 },
    ]);

    const insight = service as unknown as {
      getRicherThan(): Promise<Array<{ amount_flux: number; count_addresses: number }>>;
    };

    const result = await insight.getRicherThan();

    expect(Array.isArray(result)).toBe(true);
    expect(result).toEqual(expect.arrayContaining([
      { amount_flux: 1, count_addresses: 3 },
      { amount_flux: 10, count_addresses: 2 },
    ]));
  });

  test('gets pool statistics for a validated date with pagination metadata', async () => {
    const { service, ch } = createService();
    ch.query.mockResolvedValue([
      { producer: 'pool-a', blocks_found: '3' },
      { producer: '', blocks_found: 1 },
    ]);

    const insight = service as unknown as {
      getPools(dateRaw?: string): Promise<{
        date: string;
        n_blocks_mined: number;
        blocks_by_pool: Array<{
          address: string;
          poolName: string;
          url: string | null;
          blocks_found: number;
          percent_total: number;
        }>;
        pagination: { current: string; next: string; prev: string };
      }>;
    };

    await expect(insight.getPools('2026-06-10')).resolves.toEqual({
      date: '2026-06-10',
      n_blocks_mined: 4,
      blocks_by_pool: [
        { address: 'pool-a', poolName: 'pool-a', url: null, blocks_found: 3, percent_total: 75 },
        { address: 'Unknown', poolName: 'Unknown', url: null, blocks_found: 1, percent_total: 25 },
      ],
      pagination: {
        current: '2026-06-10',
        next: '2026-06-11',
        prev: '2026-06-09',
      },
    });

    const [sql, params] = ch.query.mock.calls[0];
    expect(sql).toContain('FROM blocks');
    expect(sql).toContain('timestamp >= {start:UInt32}');
    expect(sql).toContain('timestamp <= {end:UInt32}');
    expect(sql).toContain('count() AS blocks_found_count');
    expect(sql).toContain('toString(blocks_found_count) AS blocks_found');
    expect(sql).toContain('ORDER BY blocks_found_count DESC, producer ASC');
    expect(sql).not.toContain('ORDER BY blocks_found DESC');
    expect(params).toEqual({ start: 1781049600, end: 1781135999 });
  });

  test('rejects invalid pool statistic dates', async () => {
    const { service, ch } = createService();
    const insight = service as unknown as {
      getPools(dateRaw?: string): Promise<unknown>;
    };

    await expect(insight.getPools('not-a-date')).rejects.toThrow('Invalid blockDate');
    await expect(insight.getPools('1950-01-01')).rejects.toThrow('Invalid blockDate');
    await expect(insight.getPools('9999-01-01')).rejects.toThrow('Invalid blockDate');
    expect(ch.query).not.toHaveBeenCalled();
  });

  test('delegates message and auxiliary RPC helpers', async () => {
    const { service, rpc } = createService();
    rpc.verifyMessage.mockResolvedValue(true);
    rpc.getVersion.mockResolvedValue({ version: 9000000 });
    rpc.dosList.mockResolvedValue(['banned']);
    rpc.startList.mockResolvedValue(['started']);

    await expect(service.verifyMessage('addr', 'sig', 'msg')).resolves.toBe(true);
    await expect(service.getVersion()).resolves.toEqual({ version: 9000000 });
    await expect(service.dosList()).resolves.toEqual(['banned']);
    await expect(service.startList()).resolves.toEqual(['started']);
    expect(rpc.verifyMessage).toHaveBeenCalledWith('addr', 'sig', 'msg');
  });
});

function mockTransactionLookup(
  ch: ReturnType<typeof createService>['ch'],
  txid: string,
  inputs: Array<{ txid: string; vout: number; address: string; value: string; script_type: string }>,
  txOverrides: Record<string, unknown> = {}
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
        ...txOverrides,
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
