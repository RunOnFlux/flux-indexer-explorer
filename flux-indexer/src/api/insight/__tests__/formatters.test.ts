import {
  formatAddressSummary,
  formatBlock,
  formatSupply,
  formatTransaction,
  formatUtxo,
} from '../formatters';
import {
  createNotFound,
  InsightValidationError,
  normalizeHash,
  parseAddressList,
  parseBlockDate,
  parseLimit,
  parseRange,
  sendBadRequest,
  toZatoshis,
  zatoshisToFlux,
  zatoshisToFluxString,
} from '../utils';

describe('Insight compatibility utilities', () => {
  test('formats satoshis as Flux numbers and strings', () => {
    expect(zatoshisToFlux(123456789n)).toBe(1.23456789);
    expect(zatoshisToFluxString(100000000n)).toBe('1.00000000');
    expect(zatoshisToFluxString(1n)).toBe('0.00000001');
  });

  test('rejects unsafe number zatoshi inputs', () => {
    expect(toZatoshis('9007199254740993')).toBe(9007199254740993n);
    expect(() => toZatoshis(Number.MAX_SAFE_INTEGER + 1)).toThrow(/safe integer/);
    expect(() => zatoshisToFlux(Number.MAX_SAFE_INTEGER + 1)).toThrow(/safe integer/);
  });

  test('parses comma-separated addresses from path and body', () => {
    expect(parseAddressList('a,b,,c')).toEqual(['a', 'b', 'c']);
    expect(parseAddressList(undefined, 'x,y')).toEqual(['x', 'y']);
  });

  test('parses every address from array bodies', () => {
    expect(parseAddressList(undefined, { addrs: ['t1A', 't1B'] })).toEqual(['t1A', 't1B']);
    expect(parseAddressList(undefined, { addresses: ['t1A'] })).toEqual(['t1A']);
    expect(parseAddressList(undefined, ['t1A', 't1B', 't1C'])).toEqual(['t1A', 't1B', 't1C']);
    expect(parseAddressList(undefined, { addrs: [] })).toEqual([]);
  });

  test('rejects address array bodies with non-string entries', () => {
    expect(() => parseAddressList(undefined, { addrs: ['t1A', 5] })).toThrow(InsightValidationError);
    expect(() => parseAddressList(undefined, [{ addr: 't1A' }])).toThrow(/array of address strings/);
  });

  test('parses Insight from/to range with bounded defaults', () => {
    expect(parseRange({ from: '5', to: '9' })).toEqual({ from: 5, to: 9, limit: 4 });
    expect(parseRange({})).toEqual({ from: 0, to: 10, limit: 10 });
  });

  test('defaults invalid Insight ranges and zero limits', () => {
    expect(parseRange({ from: '5', to: '4' })).toEqual({ from: 5, to: 15, limit: 10 });
    expect(parseRange({ from: '5', to: '5' })).toEqual({ from: 5, to: 15, limit: 10 });
    expect(parseLimit('0', 10, 50)).toBe(10);
  });

  test('accepts Insight ranges up to the UInt32 limit', () => {
    expect(parseRange({ from: '4294967295' })).toEqual({
      from: 4294967295,
      to: 4294967305,
      limit: 10,
    });
  });

  test('rejects Insight ranges beyond the UInt32 limit', () => {
    expect(() => parseRange({ from: '4294967296' })).toThrow(InsightValidationError);
    expect(() => parseRange({ from: '4294967296' })).toThrow(/Invalid from/);
    expect(() => parseRange({ to: '4294967296' })).toThrow(/Invalid to/);
    expect(() => parseRange({ from: String(Number.MAX_SAFE_INTEGER) })).toThrow(InsightValidationError);
    expect(() => parseRange({ from: '99999999999999999999' })).toThrow(InsightValidationError);
  });

  test('parses blockDate as UTC day bounds', () => {
    expect(parseBlockDate('2026-06-10')).toEqual({
      start: 1781049600,
      end: 1781135999,
      current: '2026-06-10',
      next: '2026-06-11',
      prev: '2026-06-09',
    });
  });

  test('rejects malformed blockDate values with typed validation errors', () => {
    expect(() => parseBlockDate('not-a-date')).toThrow(InsightValidationError);
    expect(() => parseBlockDate('not-a-date')).toThrow(/expected YYYY-MM-DD/);
    expect(() => parseBlockDate('2026-02-30')).toThrow(InsightValidationError);
    expect(() => parseBlockDate('2026-02-30')).toThrow(/real UTC date/);
  });

  test('rejects blockDate values outside the UInt32 timestamp range', () => {
    expect(() => parseBlockDate('1950-01-01')).toThrow(InsightValidationError);
    expect(() => parseBlockDate('9999-01-01')).toThrow(InsightValidationError);
    expect(() => parseBlockDate('2106-02-07')).toThrow(/Invalid blockDate/);
    expect(parseBlockDate('2106-02-06')).toMatchObject({
      start: 4294857600,
      end: 4294943999,
    });
    expect(parseBlockDate('1970-01-01')).toMatchObject({ start: 0, end: 86399 });
  });

  test('normalizes hashes and rejects invalid hash input', () => {
    expect(normalizeHash('ABC123')).toBe('0000000000000000000000000000000000000000000000000000000000abc123');
    expect(() => normalizeHash('not-a-hex-hash')).toThrow(/hex/);
    expect(() => normalizeHash('a'.repeat(65))).toThrow(/64/);
  });

  test('creates legacy not found response body', () => {
    expect(createNotFound('/insight-api/missing')).toEqual({
      status: 404,
      url: '/insight-api/missing',
      error: 'Not found',
    });
  });

  test('sends legacy bad request response body', () => {
    const response = {
      status: jest.fn().mockReturnThis(),
      json: jest.fn().mockReturnThis(),
    } as unknown as Parameters<typeof sendBadRequest>[0];

    const result = sendBadRequest(response, 'Bad input');

    expect(response.status).toHaveBeenCalledWith(400);
    expect(response.json).toHaveBeenCalledWith({ message: 'Bad input', code: 1 });
    expect(result).toBe(response);
  });
});

describe('Insight compatibility formatters', () => {
  test('formats block response with legacy field names', () => {
    const result = formatBlock({
      block: {
        hash: 'abc',
        height: 10,
        size: 123,
        version: 4,
        merkle_root: 'merk',
        timestamp: 1000,
        bits: '1d00ffff',
        difficulty: 12.5,
        chainwork: 'ff',
        prev_hash: 'prev',
        producer_reward: '5000000000',
        producer: 'node-ip',
        tx_count: 2,
        nonce: '12345',
      },
      txids: ['tx1', 'tx2'],
      confirmations: 3,
      nextBlockHash: 'next',
    });

    expect(result).toMatchObject({
      hash: 'abc',
      height: 10,
      merkleroot: 'merk',
      time: 1000,
      nonce: '12345',
      tx: ['tx1', 'tx2'],
      confirmations: 3,
      previousblockhash: 'prev',
      nextblockhash: 'next',
      reward: 50,
      isMainChain: true,
      minedBy: 'node-ip',
      poolInfo: { poolName: 'node-ip', url: null },
    });
  });

  test('formats genesis previous hash as null', () => {
    const baseBlock = {
      hash: 'genesis',
      height: 0,
      size: 123,
      version: 4,
      merkle_root: 'merk',
      timestamp: 1000,
      bits: '1d00ffff',
      difficulty: 1,
      chainwork: '00',
    };

    expect(formatBlock({
      block: { ...baseBlock, prev_hash: null },
      txids: [],
      confirmations: 100,
    })).toMatchObject({ previousblockhash: null });
    expect(formatBlock({
      block: { ...baseBlock, prev_hash: '' },
      txids: [],
      confirmations: 100,
    })).toMatchObject({ previousblockhash: null });
    expect(formatBlock({
      block: { ...baseBlock, prev_hash: '0'.repeat(64) },
      txids: [],
      confirmations: 100,
    })).toMatchObject({ previousblockhash: null });
  });

  test('normalizes empty block producer fields', () => {
    const result = formatBlock({
      block: {
        hash: 'abc',
        height: 10,
        size: 123,
        version: 4,
        merkle_root: 'merk',
        timestamp: 1000,
        bits: '1d00ffff',
        difficulty: 12.5,
        chainwork: 'ff',
        producer_reward: '5000000000',
        producer: '   ',
      },
      txids: [],
      confirmations: 3,
    });

    expect(result).toMatchObject({
      minedBy: null,
      poolInfo: {},
    });
  });

  test('formats transaction values in legacy Insight shape', () => {
    const result = formatTransaction({
      tx: {
        txid: 'txid',
        version: 1,
        locktime: 0,
        block_height: 20,
        timestamp: 2000,
        input_total: '300000000',
        output_total: '299000000',
        fee: '1000000',
        size: 225,
        is_coinbase: 0,
        is_fluxnode_tx: 0,
      },
      blockHash: 'block',
      confirmations: 6,
      inputs: [{ txid: 'prev', vout: 1, address: 'from', value: '300000000', script_type: 'pubkeyhash' }],
      outputs: [{
        vout: 0,
        address: 'to',
        value: '299000000',
        script_pubkey: '76a9',
        script_type: 'pubkeyhash',
        spent: 1,
        spent_txid: 'spent',
        spent_index: 2,
        spent_block_height: 30,
      }],
    });

    expect(result).toMatchObject({
      txid: 'txid',
      blockhash: 'block',
      blockheight: 20,
      confirmations: 6,
      valueIn: 3,
      valueOut: 2.99,
      fees: 0.01,
      vin: [{ txid: 'prev', vout: 1, addr: 'from', valueSat: 300000000, value: 3 }],
      vout: [{
        value: '2.99000000',
        n: 0,
        scriptPubKey: { addresses: ['to'], type: 'pubkeyhash' },
        spentTxId: 'spent',
        spentIndex: 2,
        spentHeight: 30,
      }],
    });
  });

  test('formats string locktime as a safe number', () => {
    const result = formatTransaction({
      tx: {
        txid: 'locktime',
        version: 1,
        locktime: '4294967295',
        block_height: 20,
        timestamp: 2000,
        input_total: '0',
        output_total: '0',
        fee: '0',
        size: 225,
        is_coinbase: 0,
        is_fluxnode_tx: 0,
      },
      blockHash: 'block',
      confirmations: 6,
      inputs: [],
      outputs: [],
    });

    expect(result.locktime).toBe(4294967295);
  });

  test('formats coinbase transactions with synthetic coinbase vin', () => {
    const tx = {
      txid: 'coinbase',
      version: 1,
      locktime: 0,
      block_height: 21,
      timestamp: 2100,
      input_total: '0',
      output_total: '5000000000',
      // The indexer stores the block's total collected fees on the coinbase
      // row; it must never display as the coinbase tx's own fee.
      fee: '1000000',
      size: 100,
      is_coinbase: 1,
      is_fluxnode_tx: 0,
    };

    const result = formatTransaction({
      tx,
      blockHash: 'block',
      confirmations: 7,
      inputs: [{ txid: 'ignored', vout: 0, address: 'ignored', value: '1', script_type: 'pubkeyhash' }],
      outputs: [],
    });

    expect(result.vin).toEqual([{ coinbase: '', sequence: 0xffffffff, n: 0 }]);
    expect(result).not.toHaveProperty('fees');
    expect(result).not.toHaveProperty('valueIn');
    expect(result.isCoinBase).toBe(true);

    const withScript = formatTransaction({
      tx,
      blockHash: 'block',
      confirmations: 7,
      inputs: [],
      outputs: [],
      coinbaseScript: '0341e21f00',
    });

    expect(withScript.vin).toEqual([{ coinbase: '0341e21f00', sequence: 0xffffffff, n: 0 }]);
  });

  test('emits decoded scriptSig and sequence on transaction inputs', () => {
    const result = formatTransaction({
      tx: {
        txid: 'txid',
        version: 1,
        locktime: 0,
        block_height: 20,
        timestamp: 2000,
        input_total: '300000000',
        output_total: '299000000',
        fee: '1000000',
        size: 225,
        is_coinbase: 0,
        is_fluxnode_tx: 0,
      },
      blockHash: 'block',
      confirmations: 6,
      inputs: [
        {
          txid: 'prev',
          vout: 1,
          address: 'from',
          value: '200000000',
          script_type: 'pubkeyhash',
          sequence: 0,
          script_sig: { hex: '47abcd', asm: '47abcd[ALL]' },
        },
        { txid: 'prev2', vout: 0, address: 'from2', value: '100000000', script_type: 'pubkeyhash' },
      ],
      outputs: [],
    });

    expect(result.vin[0]).toMatchObject({
      sequence: 0,
      scriptSig: { hex: '47abcd', asm: '47abcd[ALL]' },
    });
    // Inputs without decoded vin data fall back to the legacy stub values.
    expect(result.vin[1]).toMatchObject({
      sequence: 0xffffffff,
      scriptSig: { hex: '', asm: '' },
    });
  });

  test('omits block fields and unresolved fees for mempool transactions', () => {
    const result = formatTransaction({
      tx: {
        txid: 'mempool-tx',
        version: 4,
        locktime: 0,
        block_height: -1,
        timestamp: 1750000000,
        input_total: '100',
        output_total: '90',
        fee: null,
        size: 200,
        is_coinbase: 0,
        is_fluxnode_tx: 0,
      },
      blockHash: null,
      confirmations: 0,
      inputs: [],
      outputs: [],
    });

    expect(result).not.toHaveProperty('blockheight');
    expect(result).not.toHaveProperty('blocktime');
    expect(result).not.toHaveProperty('fees');
    expect(result.time).toBe(1750000000);
    expect(result.valueIn).toBe(0.000001);
    expect(result.confirmations).toBe(0);
    // blockhash serializes away entirely for mempool transactions.
    expect(JSON.parse(JSON.stringify(result))).not.toHaveProperty('blockhash');
  });

  test('emits fluxnode metadata with legacy v1 field names', () => {
    const tx = {
      txid: 'fluxnode-tx',
      version: 5,
      locktime: 0,
      block_height: 20,
      timestamp: 2000,
      input_total: '0',
      output_total: '0',
      fee: '0',
      size: 300,
      is_coinbase: 0,
      is_fluxnode_tx: 1,
      fluxnode_type: 6,
    };

    const result = formatTransaction({
      tx,
      blockHash: 'block',
      confirmations: 6,
      inputs: [],
      outputs: [],
      fluxnode: {
        type: 6,
        collateral_hash: 'c'.repeat(64),
        collateral_index: 3,
        ip_address: '1.2.3.4:16125',
        public_key: '04abcd',
        signature: 'sig==',
        p2sh_address: 't3P2SH',
        benchmark_tier: 'CUMULUS',
      },
    });

    expect(result.isFluxnodeTx).toBe(true);
    expect(result.fluxnode).toEqual({
      nType: 6,
      collateralOutputHash: 'c'.repeat(64),
      collateralOutputIndex: 3,
      benchmarkTier: 'CUMULUS',
      ip: '1.2.3.4:16125',
      fluxnodePubKey: '04abcd',
      sig: 'sig==',
      p2shAddress: 't3P2SH',
    });

    const withoutRow = formatTransaction({
      tx,
      blockHash: 'block',
      confirmations: 6,
      inputs: [],
      outputs: [],
      fluxnode: null,
    });

    expect(withoutRow).not.toHaveProperty('fluxnode');
  });

  test('omits empty fluxnode collateral and normalizes blank fields to null', () => {
    const result = formatTransaction({
      tx: {
        txid: 'fluxnode-tx',
        version: 5,
        locktime: 0,
        block_height: 20,
        timestamp: 2000,
        input_total: '0',
        output_total: '0',
        fee: '0',
        size: 300,
        is_coinbase: 0,
        is_fluxnode_tx: 1,
        fluxnode_type: 1,
      },
      blockHash: 'block',
      confirmations: 6,
      inputs: [],
      outputs: [],
      fluxnode: {
        type: 1,
        collateral_hash: '   ',
        collateral_index: 0,
        ip_address: '',
        public_key: '',
        signature: '',
        p2sh_address: '',
        benchmark_tier: '',
      },
    });

    expect(result.fluxnode).toEqual({
      nType: 1,
      benchmarkTier: null,
      ip: null,
      fluxnodePubKey: null,
      sig: null,
      p2shAddress: null,
    });
    expect(result.fluxnode).not.toHaveProperty('collateralOutputHash');
    expect(result.fluxnode).not.toHaveProperty('collateralOutputIndex');
  });

  test('filters sentinel transaction output addresses', () => {
    const result = formatTransaction({
      tx: {
        txid: 'txid',
        version: 1,
        locktime: 0,
        block_height: 20,
        timestamp: 2000,
        input_total: '0',
        output_total: '5',
        fee: '0',
        size: 225,
        is_coinbase: 0,
        is_fluxnode_tx: 0,
      },
      blockHash: 'block',
      confirmations: 6,
      inputs: [],
      outputs: [
        { vout: 0, address: 'SHIELDED_OR_NONSTANDARD', value: '1', script_pubkey: '00', script_type: 'nonstandard', spent: 0 },
        { vout: 1, address: 'UNKNOWN', value: '1', script_pubkey: '00', script_type: 'nonstandard', spent: 0 },
        { vout: 2, address: '', value: '1', script_pubkey: '00', script_type: 'nonstandard', spent: 0 },
        { vout: 3, address: null, value: '1', script_pubkey: '00', script_type: 'nonstandard', spent: 0 },
        { vout: 4, value: '1', script_pubkey: '00', script_type: 'nonstandard', spent: 0 },
      ],
    });

    expect(result.vout.map((output) => output.scriptPubKey.addresses)).toEqual([[], [], [], [], []]);
  });

  test('formats address summary with legacy misspelled fields', () => {
    const result = formatAddressSummary({
      address: 'addr',
      summary: {
        balance: '250000000',
        received_total: '500000000',
        sent_total: '250000000',
        tx_count: 7,
      },
      mempool: { balanceDelta: 100000000n, txCount: 2 },
      transactions: ['tx1', 'tx2'],
    });

    expect(result).toEqual({
      addrStr: 'addr',
      balance: 2.5,
      balanceSat: 250000000,
      totalReceived: 5,
      totalReceivedSat: 500000000,
      totalSent: 2.5,
      totalSentSat: 250000000,
      unconfirmedBalance: 1,
      unconfirmedBalanceSat: 100000000,
      unconfirmedTxApperances: 2,
      txApperances: 7,
      transactions: ['tx1', 'tx2'],
    });
  });

  test('normalizes quoted address summary transaction count', () => {
    const result = formatAddressSummary({
      address: 'addr',
      summary: {
        balance: '0',
        received_total: '0',
        sent_total: '0',
        tx_count: '7',
      },
    });

    expect(result.txApperances).toBe(7);
  });

  test('formats unseen address summary with zero defaults', () => {
    const result = formatAddressSummary({
      address: 'new-address',
      summary: null,
    });

    expect(result).toEqual({
      addrStr: 'new-address',
      balance: 0,
      balanceSat: 0,
      totalReceived: 0,
      totalReceivedSat: 0,
      totalSent: 0,
      totalSentSat: 0,
      unconfirmedBalance: 0,
      unconfirmedBalanceSat: 0,
      unconfirmedTxApperances: 0,
      txApperances: 0,
      transactions: [],
    });
  });

  test('formats UTXO response with Insight field names', () => {
    expect(formatUtxo({
      address: 'addr',
      txid: 'tx',
      vout: 2,
      script_pubkey: '76a9',
      value: '123456789',
      block_height: 99,
      timestamp: 1234,
      confirmations: 5,
    })).toEqual({
      address: 'addr',
      txid: 'tx',
      vout: 2,
      scriptPubKey: '76a9',
      amount: 1.23456789,
      satoshis: 123456789,
      confirmations: 5,
      height: 99,
      ts: 1234,
    });
  });

  test('returns unsafe legacy satoshi fields as decimal strings', () => {
    const unsafe = '9007199254740993';
    const addressSummary = formatAddressSummary({
      address: 'addr',
      summary: {
        balance: unsafe,
        received_total: unsafe,
        sent_total: unsafe,
        tx_count: 1,
      },
      mempool: { balanceDelta: unsafe, txCount: 1 },
    });
    const tx = formatTransaction({
      tx: {
        txid: 'large',
        version: 1,
        locktime: 0,
        block_height: 20,
        timestamp: 2000,
        input_total: unsafe,
        output_total: unsafe,
        fee: '0',
        size: 225,
        is_coinbase: 0,
        is_fluxnode_tx: 0,
      },
      blockHash: 'block',
      confirmations: 6,
      inputs: [{ txid: 'prev', vout: 1, address: 'from', value: unsafe, script_type: 'pubkeyhash' }],
      outputs: [],
    });
    const utxo = formatUtxo({
      address: 'addr',
      txid: 'tx',
      vout: 2,
      script_pubkey: '76a9',
      value: unsafe,
      confirmations: 5,
    });

    expect(addressSummary).toMatchObject({
      balanceSat: unsafe,
      totalReceivedSat: unsafe,
      totalSentSat: unsafe,
      unconfirmedBalanceSat: unsafe,
    });
    expect(tx.vin[0]).toMatchObject({ valueSat: unsafe });
    expect(utxo).toMatchObject({ satoshis: unsafe });
  });

  test('formats supply as text or keyed object', () => {
    expect(formatSupply(100000000n)).toBe('1');
    expect(formatSupply(100000000n, 'supply')).toEqual({ supply: '1' });
    expect(formatSupply(123456789n, 'circulatingSupply')).toEqual({ circulatingSupply: '1.23456789' });
  });
});
