# Insight API Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `/insight-api/*` REST compatibility endpoints to `flux-indexer` so existing Insight API consumers can migrate without route rewrites.

**Architecture:** Add a standalone Insight compatibility router mounted beside the existing `/api/v1` API. Keep legacy response formatting in dedicated formatter and service modules so the current explorer API remains unchanged. Use ClickHouse for indexed reads and Flux RPC for raw hex, broadcast, live node state, message verification, fee estimates, and daemon FluxNode lists.

**Tech Stack:** TypeScript, Express 4, Jest, ClickHouse client wrapper, Flux JSON-RPC client, Node 20 built-in `fetch` for test HTTP calls.

---

## File Structure

- Modify `flux-indexer/package.json`: make `npm test` compile TypeScript before running Jest against `dist`.
- Create `flux-indexer/jest.config.cjs`: configure Jest to discover compiled `*.test.js` files under `dist`.
- Modify `flux-indexer/src/api/server.ts`: mount the Insight router at `/insight-api` before the static frontend fallback and expose the Express app for tests.
- Modify `flux-indexer/src/rpc/flux-rpc-client.ts`: add public methods for legacy RPC-backed endpoints.
- Create `flux-indexer/src/api/insight/types.ts`: define compatibility row, domain, and response types.
- Create `flux-indexer/src/api/insight/utils.ts`: validation, amount conversion, pagination, 404/error helpers, date parsing, and FluxNode filter helpers.
- Create `flux-indexer/src/api/insight/formatters.ts`: pure functions that convert ClickHouse/RPC rows into legacy Insight response shapes.
- Create `flux-indexer/src/api/insight/service.ts`: ClickHouse and RPC-backed compatibility query methods.
- Create `flux-indexer/src/api/insight/router.ts`: Express route registration and request/response handling.
- Create `flux-indexer/src/api/insight/__tests__/http-test-utils.ts`: helper for starting an ephemeral Express test server.
- Create `flux-indexer/src/api/insight/__tests__/formatters.test.ts`: pure response-shape tests.
- Create `flux-indexer/src/api/insight/__tests__/service.test.ts`: mocked ClickHouse/RPC tests for service behavior.
- Create `flux-indexer/src/api/insight/__tests__/router.test.ts`: HTTP route tests against the compatibility router.
- Create `flux-indexer/src/rpc/flux-rpc-client.test.ts`: RPC method parameter tests.

## Implementation Notes

- Do not change existing `/api/v1` response shapes.
- Mount `/insight-api` before `express.static()` and before the catch-all route in `server.ts`.
- The current middleware only enables JSON request bodies. The Insight router must add `express.urlencoded({ extended: false })` so legacy form posts to `/tx/send`, `/addrs/utxo`, `/addrs/txs`, and FluxNode filter routes work.
- Amounts in Insight responses use FLUX numbers or fixed 8-decimal strings depending on the field. Satoshi fields are plain numbers.
- Preserve the legacy misspellings `txApperances` and `unconfirmedTxApperances`.
- Keep raw block and raw transaction hex RPC-backed; do not add raw hex columns to ClickHouse.
- Use latest-row selection with `_version` or `version` ordering instead of broad `FINAL` scans on high-volume tables.

---

### Task 1: Compile-Then-Jest Test Harness

**Files:**
- Modify: `flux-indexer/package.json`
- Create: `flux-indexer/jest.config.cjs`
- Create: `flux-indexer/src/api/insight/__tests__/http-test-utils.ts`

- [ ] **Step 1: Write the failing test harness helper**

Create `flux-indexer/src/api/insight/__tests__/http-test-utils.ts`:

```ts
import type express from 'express';
import type { Server } from 'http';

export async function withTestServer(
  app: express.Application,
  run: (baseUrl: string) => Promise<void>
): Promise<void> {
  const server = await new Promise<Server>((resolve) => {
    const listener = app.listen(0, () => resolve(listener));
  });

  const address = server.address();
  if (!address || typeof address === 'string') {
    server.close();
    throw new Error('Expected an ephemeral TCP port');
  }

  try {
    await run(`http://127.0.0.1:${address.port}`);
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

export async function readJson<T>(response: Response): Promise<T> {
  const text = await response.text();
  return JSON.parse(text) as T;
}
```

- [ ] **Step 2: Run the test command to verify the harness is not wired**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: FAIL because Jest is not yet configured to pass when no compiled tests exist.

- [ ] **Step 3: Configure compiled Jest tests**

Modify `flux-indexer/package.json`:

```json
{
  "scripts": {
    "build": "tsc",
    "start": "node dist/index.js",
    "dev": "tsx watch src/index.ts",
    "test": "npm run build && jest --runInBand --passWithNoTests",
    "debug:block": "tsx src/scripts/debug-block-parser.ts",
    "inspect:block": "tsx src/scripts/inspect-block.ts"
  }
}
```

Create `flux-indexer/jest.config.cjs`:

```js
module.exports = {
  testEnvironment: 'node',
  testMatch: ['<rootDir>/dist/**/*.test.js'],
  testPathIgnorePatterns: ['/node_modules/'],
  clearMocks: true
};
```

- [ ] **Step 4: Run the test command to verify the harness is clean**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS with no tests found is acceptable for this step because the test harness now compiles. If Jest exits with code 1 on no tests, continue after Task 2 adds the first test.

- [ ] **Step 5: Commit**

```bash
git add flux-indexer/package.json flux-indexer/jest.config.cjs flux-indexer/src/api/insight/__tests__/http-test-utils.ts
git commit -m "test: add compiled jest harness"
```

---

### Task 2: Pure Insight Utility and Formatter Functions

**Files:**
- Create: `flux-indexer/src/api/insight/types.ts`
- Create: `flux-indexer/src/api/insight/utils.ts`
- Create: `flux-indexer/src/api/insight/formatters.ts`
- Create: `flux-indexer/src/api/insight/__tests__/formatters.test.ts`

- [ ] **Step 1: Write failing formatter and utility tests**

Create `flux-indexer/src/api/insight/__tests__/formatters.test.ts`:

```ts
import {
  formatAddressSummary,
  formatBlock,
  formatTransaction,
  formatUtxo,
} from '../formatters';
import {
  createNotFound,
  parseAddressList,
  parseBlockDate,
  parseRange,
  zatoshisToFlux,
  zatoshisToFluxString,
} from '../utils';

describe('Insight compatibility utilities', () => {
  test('formats satoshis as Flux numbers and strings', () => {
    expect(zatoshisToFlux(123456789n)).toBe(1.23456789);
    expect(zatoshisToFluxString(100000000n)).toBe('1.00000000');
    expect(zatoshisToFluxString(1n)).toBe('0.00000001');
  });

  test('parses comma-separated addresses from path and body', () => {
    expect(parseAddressList('a,b,,c')).toEqual(['a', 'b', 'c']);
    expect(parseAddressList(undefined, 'x,y')).toEqual(['x', 'y']);
  });

  test('parses Insight from/to range with bounded defaults', () => {
    expect(parseRange({ from: '5', to: '9' })).toEqual({ from: 5, to: 9, limit: 4 });
    expect(parseRange({})).toEqual({ from: 0, to: 10, limit: 10 });
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

  test('creates legacy not found response body', () => {
    expect(createNotFound('/insight-api/missing')).toEqual({
      status: 404,
      url: '/insight-api/missing',
      error: 'Not found',
    });
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
      outputs: [{ vout: 0, address: 'to', value: '299000000', script_pubkey: '76a9', script_type: 'pubkeyhash', spent: 0 }],
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
      vout: [{ value: '2.99000000', n: 0, scriptPubKey: { addresses: ['to'], type: 'pubkeyhash' } }],
    });
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
});
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: FAIL because `../formatters` and `../utils` do not exist.

- [ ] **Step 3: Add types and utility implementations**

Create `flux-indexer/src/api/insight/types.ts` with exported interfaces used by tests and later tasks:

```ts
export interface InsightBlockRow {
  hash: string;
  height: number;
  size: number;
  version: number;
  merkle_root: string;
  timestamp: number;
  bits: string;
  difficulty: number | string;
  chainwork: string;
  prev_hash?: string | null;
  producer_reward?: string | number | null;
  producer?: string | null;
  tx_count?: number;
}

export interface InsightTxRow {
  txid: string;
  version: number;
  locktime: number;
  block_height: number;
  timestamp: number;
  input_total: string | number;
  output_total: string | number;
  fee: string | number;
  size: number;
  is_coinbase: number;
  is_fluxnode_tx: number;
  fluxnode_type?: number | null;
}

export interface InsightInputRow {
  txid: string;
  vout: number;
  address: string;
  value: string | number;
  script_type?: string;
}

export interface InsightOutputRow {
  vout: number;
  address: string;
  value: string | number;
  script_pubkey: string;
  script_type: string;
  spent: number;
  spent_txid?: string | null;
  spent_block_height?: number | null;
}

export interface InsightAddressSummaryRow {
  balance: string | number;
  received_total: string | number;
  sent_total: string | number;
  tx_count: number;
}

export interface InsightUtxoRow {
  address: string;
  txid: string;
  vout: number;
  script_pubkey: string;
  value: string | number;
  block_height?: number;
  timestamp?: number;
  confirmations: number;
}
```

Create `flux-indexer/src/api/insight/utils.ts`:

```ts
import { Request, Response } from 'express';

export const SATOSHIS_PER_FLUX = 100000000n;
export const HASH_REGEX = /^[0-9a-fA-F]{1,64}$/;

export function toBigInt(value: string | number | bigint | null | undefined): bigint {
  if (value === null || value === undefined || value === '') return 0n;
  return typeof value === 'bigint' ? value : BigInt(value);
}

export function zatoshisToFlux(value: string | number | bigint | null | undefined): number {
  const satoshis = toBigInt(value);
  const sign = satoshis < 0n ? -1 : 1;
  const abs = satoshis < 0n ? -satoshis : satoshis;
  return sign * (Number(abs / SATOSHIS_PER_FLUX) + Number(abs % SATOSHIS_PER_FLUX) / 1e8);
}

export function zatoshisToFluxString(value: string | number | bigint | null | undefined): string {
  const satoshis = toBigInt(value);
  const negative = satoshis < 0n;
  const abs = negative ? -satoshis : satoshis;
  const whole = abs / SATOSHIS_PER_FLUX;
  const fractional = (abs % SATOSHIS_PER_FLUX).toString().padStart(8, '0');
  return `${negative ? '-' : ''}${whole.toString()}.${fractional}`;
}

export function parseAddressList(pathValue?: string, bodyValue?: string): string[] {
  const raw = bodyValue || pathValue || '';
  return raw.split(',').map((part) => part.trim()).filter(Boolean);
}

export function parseRange(query: Record<string, unknown>): { from: number; to: number; limit: number } {
  const from = Math.max(0, Number.parseInt(String(query.from ?? '0'), 10) || 0);
  const requestedTo = Number.parseInt(String(query.to ?? ''), 10);
  const to = Number.isFinite(requestedTo) && requestedTo > from ? Math.min(requestedTo, from + 100) : from + 10;
  return { from, to, limit: to - from };
}

export function parseLimit(raw: unknown, defaultValue: number, max: number): number {
  const parsed = Number.parseInt(String(raw ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed < 1) return defaultValue;
  return Math.min(parsed, max);
}

export function normalizeHash(raw: string): string {
  return raw.trim().toLowerCase().padStart(64, '0');
}

export function isValidHash(raw: string): boolean {
  return HASH_REGEX.test(raw.trim());
}

export function parseBlockDate(raw: string): { start: number; end: number; current: string; next: string; prev: string } {
  if (!/^\\d{4}-\\d{2}-\\d{2}$/.test(raw)) {
    throw new Error('Please use yyyy-mm-dd format');
  }
  const startMs = Date.parse(`${raw}T00:00:00.000Z`);
  if (!Number.isFinite(startMs)) {
    throw new Error('Please use yyyy-mm-dd format');
  }
  const dayMs = 24 * 60 * 60 * 1000;
  const currentDate = new Date(startMs);
  const nextDate = new Date(startMs + dayMs);
  const prevDate = new Date(startMs - dayMs);
  return {
    start: Math.floor(startMs / 1000),
    end: Math.floor((startMs + dayMs - 1000) / 1000),
    current: currentDate.toISOString().slice(0, 10),
    next: nextDate.toISOString().slice(0, 10),
    prev: prevDate.toISOString().slice(0, 10),
  };
}

export function createNotFound(url: string): { status: 404; url: string; error: 'Not found' } {
  return { status: 404, url, error: 'Not found' };
}

export function sendNotFound(req: Request, res: Response): void {
  res.status(404).json(createNotFound(req.originalUrl));
}

export function sendBadRequest(res: Response, message: string): void {
  res.status(400).json({ message, code: 1 });
}
```

- [ ] **Step 4: Add formatter implementation**

Create `flux-indexer/src/api/insight/formatters.ts`:

```ts
import {
  InsightAddressSummaryRow,
  InsightBlockRow,
  InsightInputRow,
  InsightOutputRow,
  InsightTxRow,
  InsightUtxoRow,
} from './types';
import { toBigInt, zatoshisToFlux, zatoshisToFluxString } from './utils';

export function formatBlock(input: {
  block: InsightBlockRow;
  txids: string[];
  confirmations: number;
  nextBlockHash?: string | null;
}): Record<string, unknown> {
  const { block, txids, confirmations, nextBlockHash } = input;
  const minedBy = block.producer || null;
  return {
    hash: block.hash,
    size: block.size,
    height: block.height,
    version: block.version,
    merkleroot: block.merkle_root,
    tx: txids,
    time: block.timestamp,
    bits: block.bits,
    difficulty: Number(block.difficulty ?? 0),
    chainwork: block.chainwork,
    confirmations,
    previousblockhash: block.height === 0 ? null : block.prev_hash || null,
    nextblockhash: nextBlockHash || null,
    reward: zatoshisToFlux(block.producer_reward ?? 0),
    isMainChain: confirmations > 0,
    minedBy,
    poolInfo: minedBy ? { poolName: minedBy, url: null } : {},
  };
}

export function formatTransaction(input: {
  tx: InsightTxRow;
  blockHash: string | null;
  confirmations: number;
  inputs: InsightInputRow[];
  outputs: InsightOutputRow[];
  fluxnode?: Record<string, unknown> | null;
}): Record<string, unknown> {
  const { tx, blockHash, confirmations, inputs, outputs, fluxnode } = input;
  const coinbase = tx.is_coinbase === 1;
  const vin = coinbase
    ? [{ coinbase: 'coinbase', sequence: 0xffffffff, n: 0 }]
    : inputs.map((input, index) => ({
        txid: input.txid,
        vout: input.vout,
        sequence: 0xffffffff,
        n: index,
        scriptSig: { hex: '', asm: '' },
        addr: input.address,
        valueSat: Number(input.value),
        value: zatoshisToFlux(input.value),
        doubleSpentTxID: null,
      }));

  const vout = outputs.map((output) => {
    const hasAddress = output.address && output.address !== 'SHIELDED_OR_NONSTANDARD' && output.address !== 'UNKNOWN';
    return {
      value: zatoshisToFluxString(output.value),
      n: output.vout,
      scriptPubKey: {
        hex: output.script_pubkey || '',
        asm: '',
        addresses: hasAddress ? [output.address] : [],
        type: output.script_type || 'nonstandard',
      },
      spentTxId: output.spent === 1 ? output.spent_txid || null : null,
      spentIndex: null,
      spentHeight: output.spent === 1 ? output.spent_block_height || null : null,
    };
  });

  return {
    txid: tx.txid,
    version: tx.version,
    locktime: tx.locktime,
    vin,
    vout,
    blockhash: blockHash,
    blockheight: tx.block_height,
    confirmations,
    time: tx.timestamp,
    blocktime: confirmations > 0 ? tx.timestamp : undefined,
    valueOut: zatoshisToFlux(tx.output_total),
    valueIn: coinbase ? undefined : zatoshisToFlux(tx.input_total),
    fees: coinbase ? undefined : zatoshisToFlux(tx.fee),
    size: tx.size,
    isCoinBase: coinbase || undefined,
    ...(fluxnode || {}),
  };
}

export function formatAddressSummary(input: {
  address: string;
  summary: InsightAddressSummaryRow | null;
  mempool: { balanceDelta: bigint; txCount: number };
  transactions: string[];
}): Record<string, unknown> {
  const summary = input.summary || { balance: 0, received_total: 0, sent_total: 0, tx_count: 0 };
  const balanceSat = Number(summary.balance);
  const receivedSat = Number(summary.received_total);
  const sentSat = Number(summary.sent_total);
  const unconfirmedSat = Number(input.mempool.balanceDelta);
  return {
    addrStr: input.address,
    balance: zatoshisToFlux(balanceSat),
    balanceSat,
    totalReceived: zatoshisToFlux(receivedSat),
    totalReceivedSat: receivedSat,
    totalSent: zatoshisToFlux(sentSat),
    totalSentSat: sentSat,
    unconfirmedBalance: zatoshisToFlux(input.mempool.balanceDelta),
    unconfirmedBalanceSat: unconfirmedSat,
    unconfirmedTxApperances: input.mempool.txCount,
    txApperances: Number(summary.tx_count || 0),
    transactions: input.transactions,
  };
}

export function formatUtxo(row: InsightUtxoRow): Record<string, unknown> {
  const result: Record<string, unknown> = {
    address: row.address,
    txid: row.txid,
    vout: row.vout,
    scriptPubKey: row.script_pubkey || '',
    amount: zatoshisToFlux(row.value),
    satoshis: Number(row.value),
    confirmations: row.confirmations,
  };
  if (row.block_height !== undefined) result.height = row.block_height;
  if (row.timestamp !== undefined) result.ts = row.timestamp;
  return result;
}

export function formatSupply(value: string | number | bigint, objectKey?: 'supply' | 'circulatingSupply'): string | Record<string, string> {
  const flux = zatoshisToFlux(toBigInt(value));
  const text = Number.isInteger(flux) ? String(flux) : flux.toFixed(8).replace(/0+$/, '').replace(/\\.$/, '');
  return objectKey ? { [objectKey]: text } : text;
}
```

- [ ] **Step 5: Run tests and verify green**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS for `formatters.test`.

- [ ] **Step 6: Commit**

```bash
git add flux-indexer/src/api/insight/types.ts flux-indexer/src/api/insight/utils.ts flux-indexer/src/api/insight/formatters.ts flux-indexer/src/api/insight/__tests__/formatters.test.ts
git commit -m "feat: add insight compatibility formatters"
```

---

### Task 3: Legacy RPC Client Methods

**Files:**
- Modify: `flux-indexer/src/rpc/flux-rpc-client.ts`
- Create: `flux-indexer/src/rpc/flux-rpc-client.test.ts`

- [ ] **Step 1: Write failing RPC method tests**

Create `flux-indexer/src/rpc/flux-rpc-client.test.ts`:

```ts
import { FluxRPCClient } from './flux-rpc-client';

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

  test('viewDeterministicFluxNodeList falls back to listfluxnodes', async () => {
    const { rpc, call } = mockedClient();
    call.mockRejectedValueOnce(new Error('missing')).mockResolvedValueOnce([{ ip: '1.2.3.4' }]);
    await expect(rpc.viewDeterministicFluxNodeList()).resolves.toEqual([{ ip: '1.2.3.4' }]);
    expect(call).toHaveBeenNthCalledWith(1, 'viewdeterministiczelnodelist', []);
    expect(call).toHaveBeenNthCalledWith(2, 'listfluxnodes', []);
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
      'doslist',
      'startlist',
    ]);
  });
});
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: FAIL because the new public methods are missing on `FluxRPCClient`.

- [ ] **Step 3: Add RPC methods**

Append these methods inside the `FluxRPCClient` class in `flux-indexer/src/rpc/flux-rpc-client.ts`:

```ts
  async sendRawTransaction(rawtx: string): Promise<string> {
    return this.call('sendrawtransaction', [rawtx]);
  }

  async verifyMessage(address: string, signature: string, message: string): Promise<boolean> {
    return this.call('verifymessage', [address, signature, message]);
  }

  async getPeerInfo(): Promise<any[]> {
    return this.call('getpeerinfo');
  }

  async getMiningInfo(): Promise<any> {
    return this.call('getmininginfo');
  }

  async getInfo(): Promise<any> {
    try {
      return await this.call('getinfo');
    } catch {
      const [chain, network] = await Promise.all([
        this.getBlockchainInfo(),
        this.getNetworkInfo(),
      ]);
      return {
        version: network.version,
        protocolversion: network.protocolversion,
        walletversion: 0,
        blocks: chain.blocks,
        timeoffset: 0,
        connections: network.connections,
        proxy: '',
        difficulty: chain.difficulty,
        testnet: chain.chain !== 'main',
        relayfee: network.relayfee,
        errors: '',
        network: chain.chain,
        reward: 0,
      };
    }
  }

  async getVersion(): Promise<any> {
    return this.call('getnetworkinfo');
  }

  async viewDeterministicFluxNodeList(): Promise<any> {
    try {
      return await this.call('viewdeterministiczelnodelist');
    } catch {
      return this.call('listfluxnodes');
    }
  }

  async dosList(): Promise<any> {
    return this.call('doslist');
  }

  async startList(): Promise<any> {
    return this.call('startlist');
  }
```

- [ ] **Step 4: Run tests and verify green**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS for formatter and RPC tests.

- [ ] **Step 5: Commit**

```bash
git add flux-indexer/src/rpc/flux-rpc-client.ts flux-indexer/src/rpc/flux-rpc-client.test.ts
git commit -m "feat: add insight rpc helpers"
```

---

### Task 4: ClickHouse/RPC Compatibility Service

**Files:**
- Create: `flux-indexer/src/api/insight/service.ts`
- Create: `flux-indexer/src/api/insight/__tests__/service.test.ts`

- [ ] **Step 1: Write failing service tests**

Create `flux-indexer/src/api/insight/__tests__/service.test.ts`:

```ts
import { InsightCompatibilityService } from '../service';

function createService() {
  const ch = {
    query: jest.fn(),
    queryOne: jest.fn(),
  };
  const rpc = {
    getBlock: jest.fn(),
    getRawTransaction: jest.fn(),
    sendRawTransaction: jest.fn(),
    verifyMessage: jest.fn(),
    estimateFee: jest.fn(),
    getInfo: jest.fn(),
    getDifficulty: jest.fn(),
    getBestBlockHash: jest.fn(),
    getMiningInfo: jest.fn(),
    getPeerInfo: jest.fn(),
    viewDeterministicFluxNodeList: jest.fn(),
    dosList: jest.fn(),
    startList: jest.fn(),
  };
  const service = new InsightCompatibilityService(ch as any, rpc as any, async () => new Map());
  return { service, ch, rpc };
}

describe('InsightCompatibilityService', () => {
  test('gets a block by height and transaction ids', async () => {
    const { service, ch } = createService();
    ch.queryOne
      .mockResolvedValueOnce({ hash: 'block', height: 2, is_valid: 1 })
      .mockResolvedValueOnce({ h: 4 })
      .mockResolvedValueOnce({ hash: 'next' });
    ch.query.mockResolvedValueOnce([{ txid: 'tx1' }, { txid: 'tx2' }]);

    await expect(service.getBlock('2')).resolves.toMatchObject({
      block: { hash: 'block' },
      txids: ['tx1', 'tx2'],
      confirmations: 3,
      nextBlockHash: 'next',
    });
  });

  test('returns null for invalidated blocks', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValueOnce({ hash: 'block', height: 2, is_valid: 0 });
    await expect(service.getBlock('2')).resolves.toBeNull();
  });

  test('gets raw block hex by height through RPC', async () => {
    const { service, rpc } = createService();
    rpc.getBlock.mockResolvedValue('hex');
    await expect(service.getRawBlock('12')).resolves.toBe('hex');
    expect(rpc.getBlock).toHaveBeenCalledWith(12, 0);
  });

  test('gets address summary with noTxList support', async () => {
    const { service, ch } = createService();
    ch.queryOne.mockResolvedValueOnce({
      balance: '1',
      received_total: '2',
      sent_total: '1',
      tx_count: 1,
    });
    ch.query.mockResolvedValueOnce([{ txid: 'tx1' }]);
    const result = await service.getAddressSummary('addr', false);
    expect(result.transactions).toEqual(['tx1']);

    const resultNoTxList = await service.getAddressSummary('addr', true);
    expect(resultNoTxList.transactions).toEqual([]);
  });

  test('broadcasts raw transaction and returns txid', async () => {
    const { service, rpc } = createService();
    rpc.sendRawTransaction.mockResolvedValue('txid');
    await expect(service.sendRawTransaction('abcd')).resolves.toBe('txid');
  });

  test('estimates multiple fee targets', async () => {
    const { service, rpc } = createService();
    rpc.estimateFee.mockResolvedValueOnce(0.1).mockResolvedValueOnce(0.2);
    await expect(service.estimateFees([2, 6])).resolves.toEqual({ '2': 0.1, '6': 0.2 });
  });
});
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: FAIL because `InsightCompatibilityService` is missing.

- [ ] **Step 3: Add service implementation**

Create `flux-indexer/src/api/insight/service.ts` with this class shape and implement each method using the SQL patterns shown:

```ts
import { ClickHouseConnection } from '../../database/connection';
import { FluxRPCClient } from '../../rpc/flux-rpc-client';
import { isValidHash, normalizeHash, parseBlockDate, parseLimit, parseRange } from './utils';

export class InsightCompatibilityService {
  constructor(
    private ch: Pick<ClickHouseConnection, 'query' | 'queryOne'>,
    private rpc: FluxRPCClient,
    private getMempoolAddressDeltas: () => Promise<Map<string, { balanceDelta: bigint; txCount: number }>>
  ) {}

  async getBlock(heightOrHash: string): Promise<any | null> {
    const isHeight = /^\\d+$/.test(heightOrHash);
    const block = isHeight
      ? await this.ch.queryOne<any>(`
          SELECT *
          FROM blocks
          WHERE height = {height:UInt32}
          ORDER BY _version DESC
          LIMIT 1
        `, { height: Number(heightOrHash) })
      : isValidHash(heightOrHash)
        ? await this.ch.queryOne<any>(`
            SELECT *
            FROM blocks
            WHERE hash = {hash:FixedString(64)}
            ORDER BY _version DESC
            LIMIT 1
          `, { hash: normalizeHash(heightOrHash) })
        : null;

    if (!block || block.is_valid !== 1) return null;

    const [txRows, tip, nextBlock] = await Promise.all([
      this.ch.query<{ txid: string }>(`
        SELECT txid
        FROM (
          SELECT txid, tx_index, is_valid
          FROM transactions
          WHERE block_height = {height:UInt32}
          ORDER BY txid, _version DESC
          LIMIT 1 BY txid
        )
        WHERE is_valid = 1
        ORDER BY tx_index ASC
      `, { height: block.height }),
      this.ch.queryOne<{ h: number }>('SELECT max(height) as h FROM blocks WHERE is_valid = 1'),
      this.ch.queryOne<{ hash: string }>(`
        SELECT hash
        FROM (
          SELECT hash, is_valid
          FROM blocks
          WHERE height = {height:UInt32}
          ORDER BY _version DESC
          LIMIT 1
        )
        WHERE is_valid = 1
      `, { height: block.height + 1 }),
    ]);

    return {
      block,
      txids: txRows.map((row) => row.txid),
      confirmations: Number(tip?.h ?? block.height) - Number(block.height) + 1,
      nextBlockHash: nextBlock?.hash ?? null,
    };
  }

  async getBlockHashByHeight(height: number): Promise<string | null> {
    const row = await this.ch.queryOne<{ hash: string; is_valid: number }>(`
      SELECT hash, is_valid
      FROM blocks
      WHERE height = {height:UInt32}
      ORDER BY _version DESC
      LIMIT 1
    `, { height });
    return row && row.is_valid === 1 ? row.hash : null;
  }

  async getRawBlock(heightOrHash: string): Promise<string> {
    if (/^\\d+$/.test(heightOrHash)) return this.rpc.getBlock(Number(heightOrHash), 0) as unknown as Promise<string>;
    return this.rpc.getBlock(normalizeHash(heightOrHash), 0) as unknown as Promise<string>;
  }

  async listBlocks(query: Record<string, unknown>): Promise<any> {
    const limit = parseLimit(query.limit, 10, 200);
    const blockDate = typeof query.blockDate === 'string' ? parseBlockDate(query.blockDate) : null;
    const params: Record<string, unknown> = { limit };
    const where = blockDate
      ? 'timestamp >= {start:UInt32} AND timestamp <= {end:UInt32}'
      : '1';
    if (blockDate) {
      params.start = blockDate.start;
      params.end = blockDate.end;
    }

    const blocks = await this.ch.query<any>(`
      SELECT height, size, hash, timestamp, tx_count, producer
      FROM (
        SELECT height, size, hash, timestamp, tx_count, producer, is_valid
        FROM blocks
        WHERE ${where}
        ORDER BY height DESC, _version DESC
        LIMIT 1 BY height
      )
      WHERE is_valid = 1
      ORDER BY height DESC
      LIMIT ${limit}
    `, params);

    return { blocks, blockDate };
  }

  async getTransaction(txid: string): Promise<any | null> {
    const normalized = normalizeHash(txid);
    const tx = await this.ch.queryOne<any>(`
      SELECT *
      FROM transactions
      WHERE txid = {txid:FixedString(64)}
      ORDER BY _version DESC
      LIMIT 1
    `, { txid: normalized });
    if (!tx || tx.is_valid !== 1) return null;

    const [outputs, inputs, block, tip, fluxnode] = await Promise.all([
      this.ch.query<any>(`
        SELECT vout, address, value, script_type, script_pubkey, spent, spent_txid, spent_block_height
        FROM (
          SELECT vout, address, value, script_type, script_pubkey, spent, spent_txid, spent_block_height
          FROM utxos
          WHERE txid = {txid:FixedString(64)}
          ORDER BY vout, version DESC
          LIMIT 1 BY vout
        )
        ORDER BY vout
      `, { txid: normalized }),
      this.ch.query<any>(`
        SELECT txid, vout, address, value, script_type
        FROM (
          SELECT txid, vout, address, value, script_type
          FROM utxos
          WHERE spent_txid = {txid:FixedString(64)}
          ORDER BY txid, vout, version DESC
          LIMIT 1 BY txid, vout
        )
        ORDER BY vout
      `, { txid: normalized }),
      this.ch.queryOne<any>(`
        SELECT hash
        FROM (
          SELECT hash, is_valid
          FROM blocks
          WHERE height = {height:UInt32}
          ORDER BY _version DESC
          LIMIT 1
        )
        WHERE is_valid = 1
      `, { height: tx.block_height }),
      this.ch.queryOne<{ h: number }>('SELECT max(height) as h FROM blocks WHERE is_valid = 1'),
      this.ch.queryOne<any>(`
        SELECT type, collateral_hash, collateral_index, ip_address, public_key, signature, p2sh_address, benchmark_tier
        FROM (
          SELECT type, collateral_hash, collateral_index, ip_address, public_key, signature, p2sh_address, benchmark_tier, is_valid
          FROM fluxnode_transactions
          WHERE txid = {txid:FixedString(64)}
          ORDER BY _version DESC
          LIMIT 1
        )
        WHERE is_valid = 1
      `, { txid: normalized }),
    ]);

    return {
      tx,
      outputs,
      inputs,
      blockHash: block?.hash ?? null,
      confirmations: Number(tip?.h ?? tx.block_height) - Number(tx.block_height) + 1,
      fluxnode,
    };
  }

  async getRawTransaction(txid: string): Promise<string> {
    const raw = await this.rpc.getRawTransaction(normalizeHash(txid), false);
    return typeof raw === 'string' ? raw : '';
  }

  async getAddressSummary(address: string, noTxList: boolean): Promise<any> {
    const [summary, txRows, mempool] = await Promise.all([
      this.ch.queryOne<any>(`
        SELECT
          sumMerge(balance) AS balance,
          sumMerge(received_total) AS received_total,
          sumMerge(sent_total) AS sent_total,
          sumMerge(tx_count) AS tx_count
        FROM address_summary_agg
        WHERE address = {address:String}
        GROUP BY address
      `, { address }),
      noTxList ? Promise.resolve([]) : this.ch.query<{ txid: string }>(`
        SELECT txid
        FROM (
          SELECT txid, block_height, tx_index, is_valid
          FROM address_transactions
          WHERE address = {address:String}
          ORDER BY txid, _version DESC
          LIMIT 1 BY txid
        )
        WHERE is_valid = 1
        ORDER BY block_height DESC, tx_index ASC
        LIMIT 1000
      `, { address }),
      this.getMempoolAddressDeltas(),
    ]);
    return { summary, transactions: txRows.map((row) => row.txid), mempool: mempool.get(address) ?? { balanceDelta: 0n, txCount: 0 } };
  }

  async getAddressUtxos(addresses: string[], queryMempool: boolean): Promise<any[]> {
    const tip = await this.ch.queryOne<{ h: number }>('SELECT max(height) as h FROM blocks WHERE is_valid = 1');
    const currentHeight = Number(tip?.h ?? 0);
    const rows = await this.ch.query<any>(`
      SELECT txid, vout, address, value, script_pubkey, block_height
      FROM (
        SELECT txid, vout, address, value, script_pubkey, block_height, spent
        FROM utxos
        WHERE address IN {addresses:Array(String)}
        ORDER BY txid, vout, version DESC
        LIMIT 1 BY txid, vout
      )
      WHERE spent = 0
      ORDER BY block_height DESC
      LIMIT 5000
    `, { addresses });
    return rows.map((row) => ({ ...row, confirmations: row.block_height > 0 ? currentHeight - row.block_height + 1 : 0 }));
  }

  async sendRawTransaction(rawtx: string): Promise<string> {
    return this.rpc.sendRawTransaction(rawtx);
  }

  async estimateFees(targets: number[]): Promise<Record<string, number>> {
    const result: Record<string, number> = {};
    for (const target of targets) result[String(target)] = await this.rpc.estimateFee(target);
    return result;
  }
}
```

- [ ] **Step 4: Run tests and verify green**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS for formatter, RPC, and service tests.

- [ ] **Step 5: Commit**

```bash
git add flux-indexer/src/api/insight/service.ts flux-indexer/src/api/insight/__tests__/service.test.ts
git commit -m "feat: add insight compatibility service"
```

---

### Task 5: Core Block, Transaction, and Address Routes

**Files:**
- Create: `flux-indexer/src/api/insight/router.ts`
- Create: `flux-indexer/src/api/insight/__tests__/router.test.ts`

- [ ] **Step 1: Write failing router tests for core routes**

Create `flux-indexer/src/api/insight/__tests__/router.test.ts`:

```ts
import express from 'express';
import { createInsightCompatibilityRouter } from '../router';
import { withTestServer, readJson } from './http-test-utils';

function createApp(serviceOverrides: Record<string, jest.Mock>) {
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
    getTransactionsByAddress: jest.fn(),
    getAddressSummary: jest.fn(),
    getAddressUtxos: jest.fn(),
    getAddressTransactions: jest.fn(),
    getAddressBalanceSum: jest.fn(),
    sendRawTransaction: jest.fn(),
    ...serviceOverrides,
  };
  app.use('/insight-api', createInsightCompatibilityRouter(service as any));
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
      const body = await readJson<any>(response);
      expect(body).toMatchObject({ hash: 'hash', merkleroot: 'merk', tx: ['tx'], reward: 50 });
      expect(service.getBlock).toHaveBeenCalledWith('hash');
    });
  });

  test('GET /block-index/:height returns blockHash', async () => {
    const { app } = createApp({ getBlockHashByHeight: jest.fn().mockResolvedValue('blockhash') });
    await withTestServer(app, async (baseUrl) => {
      const body = await readJson<any>(await fetch(`${baseUrl}/insight-api/block-index/9`));
      expect(body).toEqual({ blockHash: 'blockhash' });
    });
  });

  test('GET /rawblock/:hash returns rawblock wrapper', async () => {
    const { app } = createApp({ getRawBlock: jest.fn().mockResolvedValue('hex') });
    await withTestServer(app, async (baseUrl) => {
      const body = await readJson<any>(await fetch(`${baseUrl}/insight-api/rawblock/abc`));
      expect(body).toEqual({ rawblock: 'hex' });
    });
  });

  test('GET /tx/:txid returns formatted transaction', async () => {
    const { app } = createApp({
      getTransaction: jest.fn().mockResolvedValue({
        tx: { txid: 'tx', version: 1, locktime: 0, block_height: 1, timestamp: 100, input_total: '2', output_total: '1', fee: '1', size: 1, is_coinbase: 0, is_fluxnode_tx: 0 },
        blockHash: 'block',
        confirmations: 1,
        inputs: [],
        outputs: [],
      }),
    });
    await withTestServer(app, async (baseUrl) => {
      const body = await readJson<any>(await fetch(`${baseUrl}/insight-api/tx/tx`));
      expect(body).toMatchObject({ txid: 'tx', blockhash: 'block', blockheight: 1 });
    });
  });

  test('GET /addr/:addr honors noTxList', async () => {
    const { app, service } = createApp({
      getAddressSummary: jest.fn().mockResolvedValue({
        address: 'addr',
        summary: { balance: '0', received_total: '0', sent_total: '0', tx_count: 0 },
        mempool: { balanceDelta: 0n, txCount: 0 },
        transactions: [],
      }),
    });
    await withTestServer(app, async (baseUrl) => {
      const body = await readJson<any>(await fetch(`${baseUrl}/insight-api/addr/addr?noTxList=1`));
      expect(body.addrStr).toBe('addr');
      expect(body.transactions).toEqual([]);
      expect(service.getAddressSummary).toHaveBeenCalledWith('addr', true);
    });
  });

  test('POST /tx/send accepts JSON rawtx', async () => {
    const { app } = createApp({ sendRawTransaction: jest.fn().mockResolvedValue('txid') });
    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/tx/send`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ rawtx: 'abcd' }),
      });
      expect(await readJson<any>(response)).toEqual({ txid: 'txid' });
    });
  });

  test('missing core resource returns legacy 404 body', async () => {
    const { app } = createApp({ getBlock: jest.fn().mockResolvedValue(null) });
    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/block/missing`);
      expect(response.status).toBe(404);
      expect(await readJson<any>(response)).toEqual({
        status: 404,
        url: '/insight-api/block/missing',
        error: 'Not found',
      });
    });
  });
});
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: FAIL because `createInsightCompatibilityRouter` is missing.

- [ ] **Step 3: Implement core router routes**

Create `flux-indexer/src/api/insight/router.ts` with these imports and route handlers:

```ts
import express, { Request, Response, Router } from 'express';
import { formatAddressSummary, formatBlock, formatTransaction, formatUtxo } from './formatters';
import { parseAddressList, parseRange, sendBadRequest, sendNotFound } from './utils';

export interface InsightRouterService {
  getBlock(heightOrHash: string): Promise<any | null>;
  getBlockHashByHeight(height: number): Promise<string | null>;
  getRawBlock(heightOrHash: string): Promise<string>;
  listBlocks(query: Record<string, unknown>): Promise<any>;
  getTransaction(txid: string): Promise<any | null>;
  getRawTransaction(txid: string): Promise<string>;
  getTransactionsByBlock?(blockHash: string): Promise<any[]>;
  getTransactionsByAddress?(address: string): Promise<any[]>;
  getAddressSummary(address: string, noTxList: boolean): Promise<any>;
  getAddressUtxos(addresses: string[], queryMempool: boolean): Promise<any[]>;
  getAddressTransactions?(addresses: string[], range: { from: number; to: number; limit: number }): Promise<{ totalItems: number; items: any[] }>;
  getAddressBalanceSum?(addresses: string[]): Promise<{ balance: number; unconfirmedBalance: number; immature: number }>;
  sendRawTransaction(rawtx: string): Promise<string>;
}

function asyncHandler(fn: (req: Request, res: Response) => Promise<void>) {
  return (req: Request, res: Response, next: express.NextFunction) => {
    fn(req, res).catch(next);
  };
}

export function createInsightCompatibilityRouter(service: InsightRouterService): Router {
  const router = express.Router();
  router.use(express.urlencoded({ extended: false }));

  router.get('/block/:blockHash', asyncHandler(async (req, res) => {
    const block = await service.getBlock(req.params.blockHash);
    if (!block) return sendNotFound(req, res);
    res.json(formatBlock(block));
  }));

  router.get('/block-index/:height', asyncHandler(async (req, res) => {
    const height = Number.parseInt(req.params.height, 10);
    if (!Number.isInteger(height) || height < 0) return sendBadRequest(res, 'Invalid block height');
    const blockHash = await service.getBlockHashByHeight(height);
    if (!blockHash) return sendNotFound(req, res);
    res.json({ blockHash });
  }));

  router.get('/rawblock/:blockHashOrHeight', asyncHandler(async (req, res) => {
    res.json({ rawblock: await service.getRawBlock(req.params.blockHashOrHeight) });
  }));

  router.get('/blocks', asyncHandler(async (req, res) => {
    const result = await service.listBlocks(req.query);
    const blocks = result.blocks.map((block: any) => ({
      height: block.height,
      size: block.size,
      hash: block.hash,
      time: block.timestamp,
      txlength: block.tx_count,
      poolInfo: block.producer ? { poolName: block.producer, url: null } : {},
    }));
    const date = result.blockDate;
    res.json({
      blocks,
      length: blocks.length,
      pagination: date ? {
        next: date.next,
        prev: date.prev,
        currentTs: date.end,
        current: date.current,
        isToday: date.current === new Date().toISOString().slice(0, 10),
        more: blocks.length > 0,
        moreTs: date.end + 1,
      } : {
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
    const tx = await service.getTransaction(req.params.txid);
    if (!tx) return sendNotFound(req, res);
    res.json(formatTransaction(tx));
  }));

  router.get('/rawtx/:txid', asyncHandler(async (req, res) => {
    res.json({ rawtx: await service.getRawTransaction(req.params.txid) });
  }));

  router.get('/txs', asyncHandler(async (req, res) => {
    if (typeof req.query.block === 'string' && service.getTransactionsByBlock) {
      res.json({ pagesTotal: 1, txs: await service.getTransactionsByBlock(req.query.block) });
      return;
    }
    if (typeof req.query.address === 'string' && service.getTransactionsByAddress) {
      res.json({ pagesTotal: 1, txs: await service.getTransactionsByAddress(req.query.address) });
      return;
    }
    res.json({ pagesTotal: 0, txs: [] });
  }));

  router.post('/tx/send', asyncHandler(async (req, res) => {
    const rawtx = req.body?.rawtx;
    if (typeof rawtx !== 'string' || rawtx.trim() === '') return sendBadRequest(res, 'Missing rawtx');
    res.json({ txid: await service.sendRawTransaction(rawtx.trim()) });
  }));

  router.get('/addr/:addr', asyncHandler(async (req, res) => {
    const noTxList = Number.parseInt(String(req.query.noTxList ?? '0'), 10) === 1;
    const summary = await service.getAddressSummary(req.params.addr, noTxList);
    res.json(formatAddressSummary({ address: req.params.addr, ...summary }));
  }));

  router.get('/addr/:addr/balance', asyncHandler(async (req, res) => {
    const summary = await service.getAddressSummary(req.params.addr, true);
    res.send(String(Number(summary.summary?.balance ?? 0)));
  }));

  router.get('/addr/:addr/totalReceived', asyncHandler(async (req, res) => {
    const summary = await service.getAddressSummary(req.params.addr, true);
    res.send(String(Number(summary.summary?.received_total ?? 0)));
  }));

  router.get('/addr/:addr/totalSent', asyncHandler(async (req, res) => {
    const summary = await service.getAddressSummary(req.params.addr, true);
    res.send(String(Number(summary.summary?.sent_total ?? 0)));
  }));

  router.get('/addr/:addr/unconfirmedBalance', asyncHandler(async (req, res) => {
    const summary = await service.getAddressSummary(req.params.addr, true);
    res.send(String(Number(summary.mempool?.balanceDelta ?? 0n)));
  }));

  router.get('/addr/:addr/utxo', asyncHandler(async (req, res) => {
    const rows = await service.getAddressUtxos([req.params.addr], true);
    res.json(rows.map(formatUtxo));
  }));

  router.get('/addrs/:addrs/utxo', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    res.json((await service.getAddressUtxos(addresses, true)).map(formatUtxo));
  }));

  router.get('/addrs/:addrs/unspent', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    res.json((await service.getAddressUtxos(addresses, false)).map(formatUtxo));
  }));

  router.post('/addrs/utxo', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(undefined, req.body?.addrs);
    res.json((await service.getAddressUtxos(addresses, true)).map(formatUtxo));
  }));

  router.get('/addrs/:addrs/txs', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    const range = parseRange(req.query);
    const result = service.getAddressTransactions
      ? await service.getAddressTransactions(addresses, range)
      : { totalItems: 0, items: [] };
    res.json({ totalItems: result.totalItems, from: range.from, to: Math.min(range.to, result.totalItems), items: result.items });
  }));

  router.post('/addrs/txs', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(undefined, req.body?.addrs);
    const range = parseRange({ ...req.query, ...req.body });
    const result = service.getAddressTransactions
      ? await service.getAddressTransactions(addresses, range)
      : { totalItems: 0, items: [] };
    res.json({ totalItems: result.totalItems, from: range.from, to: Math.min(range.to, result.totalItems), items: result.items });
  }));

  router.get('/addrs/:addrs/balance', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    const result = service.getAddressBalanceSum
      ? await service.getAddressBalanceSum(addresses)
      : { balance: 0, unconfirmedBalance: 0, immature: 0 };
    res.json(result);
  }));

  return router;
}
```

- [ ] **Step 4: Run tests and verify green**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS for formatter, RPC, service, and router core tests.

- [ ] **Step 5: Commit**

```bash
git add flux-indexer/src/api/insight/router.ts flux-indexer/src/api/insight/__tests__/router.test.ts
git commit -m "feat: add core insight compatibility routes"
```

---

### Task 6: Status, Utility, Messages, FluxNode, Supply, Markets, and Statistics Routes

**Files:**
- Modify: `flux-indexer/src/api/insight/router.ts`
- Modify: `flux-indexer/src/api/insight/service.ts`
- Modify: `flux-indexer/src/api/insight/__tests__/router.test.ts`
- Modify: `flux-indexer/src/api/insight/__tests__/service.test.ts`

- [ ] **Step 1: Add failing route tests for non-core API families**

Append tests to `flux-indexer/src/api/insight/__tests__/router.test.ts`:

```ts
describe('Insight status and auxiliary routes', () => {
  test('GET /status?q=getDifficulty returns wrapper object', async () => {
    const { app } = createApp({ getStatus: jest.fn().mockResolvedValue({ difficulty: 9 }) });
    await withTestServer(app, async (baseUrl) => {
      const body = await readJson<any>(await fetch(`${baseUrl}/insight-api/status?q=getDifficulty`));
      expect(body).toEqual({ difficulty: 9 });
    });
  });

  test('GET /sync returns legacy sync fields', async () => {
    const { app } = createApp({ getSync: jest.fn().mockResolvedValue({ status: 'finished', blockChainHeight: 10, syncPercentage: 100, height: 10, error: null, type: 'bitcore node' }) });
    await withTestServer(app, async (baseUrl) => {
      const body = await readJson<any>(await fetch(`${baseUrl}/insight-api/sync`));
      expect(body).toMatchObject({ status: 'finished', height: 10, type: 'bitcore node' });
    });
  });

  test('GET /utils/estimatefee supports multiple nbBlocks targets', async () => {
    const { app } = createApp({ estimateFees: jest.fn().mockResolvedValue({ '2': 0.1, '6': 0.2 }) });
    await withTestServer(app, async (baseUrl) => {
      const body = await readJson<any>(await fetch(`${baseUrl}/insight-api/utils/estimatefee?nbBlocks=2,6`));
      expect(body).toEqual({ '2': 0.1, '6': 0.2 });
    });
  });

  test('POST /messages/verify requires address signature and message', async () => {
    const { app } = createApp({});
    await withTestServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/insight-api/messages/verify`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' });
      expect(response.status).toBe(400);
    });
  });

  test('GET /supply returns plain text by default and object with format=object', async () => {
    const { app } = createApp({ getSupply: jest.fn().mockResolvedValue('10000000000000000') });
    await withTestServer(app, async (baseUrl) => {
      const plain = await fetch(`${baseUrl}/insight-api/supply`);
      expect(await plain.text()).toBe('100000000');
      const object = await readJson<any>(await fetch(`${baseUrl}/insight-api/supply?format=object`));
      expect(object).toEqual({ circulatingSupply: '100000000' });
    });
  });
});
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: FAIL because service and router do not yet implement these route methods.

- [ ] **Step 3: Add service methods**

Add these methods to `InsightCompatibilityService`:

```ts
  async getStatus(query: string | undefined): Promise<any> {
    switch (query) {
      case 'getDifficulty':
        return { difficulty: await this.rpc.getDifficulty() };
      case 'getBestBlockHash':
        return { bestblockhash: await this.rpc.getBestBlockHash() };
      case 'getLastBlockHash': {
        const hash = await this.rpc.getBestBlockHash();
        return { syncTipHash: hash, lastblockhash: hash };
      }
      case 'getMiningInfo':
        return { miningInfo: await this.rpc.getMiningInfo() };
      case 'getPeerInfo':
        return { peerInfo: await this.rpc.getPeerInfo() };
      case 'getFluxNodes':
        return { fluxNodes: await this.rpc.viewDeterministicFluxNodeList() };
      case 'getZelNodes':
        return { zelNodes: await this.rpc.viewDeterministicFluxNodeList() };
      case 'getInfo':
      default:
        return { info: await this.rpc.getInfo() };
    }
  }

  async getSync(): Promise<any> {
    const sync = await this.ch.queryOne<any>(`
      SELECT
        argMax(current_height, updated_at) as current_height,
        argMax(chain_height, updated_at) as chain_height,
        argMax(sync_percentage, updated_at) as sync_percentage,
        argMax(is_syncing, updated_at) as is_syncing
      FROM sync_state
      WHERE id = 1
    `);
    const height = Number(sync?.current_height ?? 0);
    const chainHeight = Number(sync?.chain_height ?? height);
    return {
      status: sync?.is_syncing === 1 ? 'syncing' : 'finished',
      blockChainHeight: chainHeight,
      syncPercentage: Math.round(Number(sync?.sync_percentage ?? (chainHeight > 0 ? height / chainHeight * 100 : 0))),
      height,
      error: null,
      type: 'bitcore node',
    };
  }

  getPeer(): any {
    return { connected: true, host: '127.0.0.1', port: null };
  }

  async verifyMessage(address: string, signature: string, message: string): Promise<boolean> {
    return this.rpc.verifyMessage(address, signature, message);
  }

  async listFluxNodes(filter?: string): Promise<any> {
    const response = await this.rpc.viewDeterministicFluxNodeList();
    const list = Array.isArray(response) ? response : response?.result || [];
    if (!filter) return response;
    const filters = filter.split(',').map((part) => part.trim()).filter(Boolean);
    const result = list.filter((node: any) => filters.some((entry) => {
      if (entry.includes('-')) {
        const [txhash, outidx] = entry.split('-');
        return node.txhash === txhash && String(node.outidx) === String(outidx);
      }
      if (entry.split('.').length === 4) return node.ip === entry;
      return JSON.stringify(node).includes(entry);
    }));
    return { result, error: response?.error ?? null, id: response?.id ?? null };
  }

  async getSupply(): Promise<string> {
    const row = await this.ch.queryOne<{ total_supply: string }>(`
      SELECT toString(total_supply) as total_supply
      FROM supply_stats
      ORDER BY block_height DESC, _version DESC
      LIMIT 1
    `);
    return row?.total_supply ?? '0';
  }

  getCurrency(): any {
    return { status: 'ok', data: null, timestamp: new Date().toISOString() };
  }

  getMarketsInfo(): any {
    return { rate: null, currency: 'USD', source: null, timestamp: new Date().toISOString() };
  }
```

- [ ] **Step 4: Add router routes**

Extend `InsightRouterService` with the new optional methods and register these routes before the final fallback:

```ts
  router.get('/status', asyncHandler(async (req, res) => {
    if (!service.getStatus) return res.json({ info: {} });
    res.json(await service.getStatus(typeof req.query.q === 'string' ? req.query.q : undefined));
  }));

  router.get('/sync', asyncHandler(async (_req, res) => {
    res.json(service.getSync ? await service.getSync() : { status: 'finished', blockChainHeight: 0, syncPercentage: 100, height: 0, error: null, type: 'bitcore node' });
  }));

  router.get('/peer', (_req, res) => {
    res.json(service.getPeer ? service.getPeer() : { connected: true, host: '127.0.0.1', port: null });
  });

  router.get('/version', asyncHandler(async (_req, res) => {
    res.json(service.getVersion ? await service.getVersion() : { version: '0' });
  }));

  router.get('/utils/estimatefee', asyncHandler(async (req, res) => {
    const targets = String(req.query.nbBlocks || '2').split(',').map((raw) => Number.parseInt(raw, 10)).filter((value) => Number.isInteger(value) && value > 0);
    res.json(service.estimateFees ? await service.estimateFees(targets.length ? targets : [2]) : {});
  }));

  router.all('/messages/verify', asyncHandler(async (req, res) => {
    const address = req.body?.address || req.query.address;
    const signature = req.body?.signature || req.query.signature;
    const message = req.body?.message || req.query.message;
    if (typeof address !== 'string' || typeof signature !== 'string' || typeof message !== 'string') {
      return sendBadRequest(res, 'Missing parameters (expected "address", "signature" and "message")');
    }
    res.json({ result: service.verifyMessage ? await service.verifyMessage(address, signature, message) : false });
  }));

  const listFluxNodesHandler = asyncHandler(async (req, res) => {
    const filter = req.body?.filter || req.params.filter || req.query.filter;
    res.json(service.listFluxNodes ? await service.listFluxNodes(typeof filter === 'string' ? filter : undefined) : { result: [], error: null, id: null });
  });
  router.get('/fluxnode/listfluxnodes', listFluxNodesHandler);
  router.get('/fluxnode/listfluxnodes/:filter', listFluxNodesHandler);
  router.post('/fluxnode/listfluxnodes', listFluxNodesHandler);
  router.get('/zelnode/listfluxnodes', listFluxNodesHandler);
  router.get('/zelnode/listfluxnodes/:filter', listFluxNodesHandler);
  router.post('/zelnode/listfluxnodes', listFluxNodesHandler);

  router.get('/fluxnode/addrs/:addrs/utxo', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(req.params.addrs);
    const rows = await service.getAddressUtxos(addresses, true);
    const collateralValues = new Set([1000e8, 12500e8, 40000e8, 10000e8, 25000e8, 100000e8]);
    res.json(rows.filter((row: any) => collateralValues.has(Number(row.value))).map(formatUtxo));
  }));

  router.post('/fluxnode/addrs/utxo', asyncHandler(async (req, res) => {
    const addresses = parseAddressList(undefined, req.body?.addrs);
    const rows = await service.getAddressUtxos(addresses, true);
    const collateralValues = new Set([1000e8, 12500e8, 40000e8, 10000e8, 25000e8, 100000e8]);
    res.json(rows.filter((row: any) => collateralValues.has(Number(row.value))).map(formatUtxo));
  }));

  router.get('/fluxnode/doslist', asyncHandler(async (_req, res) => {
    res.json(service.dosList ? await service.dosList() : { result: [], error: null, id: null });
  }));

  router.get('/fluxnode/startlist', asyncHandler(async (_req, res) => {
    res.json(service.startList ? await service.startList() : { result: [], error: null, id: null });
  }));

  const supplyHandler = asyncHandler(async (req, res) => {
    const value = service.getSupply ? await service.getSupply() : '0';
    const { formatSupply } = await import('./formatters');
    const key = req.path.includes('total-supply') || req.path === '/supply' ? 'supply' : 'circulatingSupply';
    const formatted = formatSupply(value, req.query.format === 'object' ? key as 'supply' | 'circulatingSupply' : undefined);
    if (typeof formatted === 'string') res.send(formatted);
    else res.json(formatted);
  });
  router.get('/supply', supplyHandler);
  router.get('/total-supply', supplyHandler);
  router.get('/circulating-supply', supplyHandler);
  router.get('/circulation', supplyHandler);
  router.get('/statistics/total-supply', supplyHandler);
  router.get('/statistics/circulating-supply', supplyHandler);
  router.get('/statistics/main-chain-circulating-locked', supplyHandler);

  router.get('/currency', (_req, res) => {
    res.json(service.getCurrency ? service.getCurrency() : { status: 'ok', data: null, timestamp: new Date().toISOString() });
  });

  router.get('/markets/info', (_req, res) => {
    res.json(service.getMarketsInfo ? service.getMarketsInfo() : { rate: null, currency: 'USD', source: null, timestamp: new Date().toISOString() });
  });
```

- [ ] **Step 5: Run tests and verify green**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS for all currently added tests.

- [ ] **Step 6: Commit**

```bash
git add flux-indexer/src/api/insight/router.ts flux-indexer/src/api/insight/service.ts flux-indexer/src/api/insight/__tests__/router.test.ts flux-indexer/src/api/insight/__tests__/service.test.ts
git commit -m "feat: add auxiliary insight routes"
```

---

### Task 7: Mount Router in API Server

**Files:**
- Modify: `flux-indexer/src/api/server.ts`
- Modify: `flux-indexer/src/api/insight/__tests__/router.test.ts`

- [ ] **Step 1: Write failing integration-style mount test**

Append to `flux-indexer/src/api/insight/__tests__/router.test.ts`:

```ts
import { ClickHouseAPIServer } from '../../server';

describe('ClickHouseAPIServer Insight mount', () => {
  test('mounts /insight-api before static fallback and leaves /api/v1 missing routes unchanged', async () => {
    const ch = { query: jest.fn(), queryOne: jest.fn(), queryCount: jest.fn() };
    const rpc = {
      getBlockchainInfo: jest.fn(),
      getNetworkInfo: jest.fn(),
      getRawMempool: jest.fn().mockResolvedValue([]),
      getBlock: jest.fn().mockResolvedValue('hex'),
    };
    const syncEngine = {};
    const server = new ClickHouseAPIServer(ch as any, rpc as any, syncEngine as any, 0);
    const app = server.getApp();

    await withTestServer(app, async (baseUrl) => {
      const insightMissing = await fetch(`${baseUrl}/insight-api/not-real`);
      expect(insightMissing.status).toBe(404);
      expect(await readJson<any>(insightMissing)).toEqual({
        status: 404,
        url: '/insight-api/not-real',
        error: 'Not found',
      });

      const apiMissing = await fetch(`${baseUrl}/api/not-real`);
      expect(apiMissing.status).toBe(404);
      expect(await readJson<any>(apiMissing)).toEqual({ error: 'Not found' });
    });
  });
});
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: FAIL because `ClickHouseAPIServer.getApp()` does not exist and `/insight-api` is not mounted.

- [ ] **Step 3: Mount the router**

Modify imports at the top of `flux-indexer/src/api/server.ts`:

```ts
import { createInsightCompatibilityRouter } from './insight/router';
import { InsightCompatibilityService } from './insight/service';
```

Inside `setupRoutes()`, before the existing status endpoints, add:

```ts
    const insightService = new InsightCompatibilityService(
      this.ch,
      this.rpc,
      this.getMempoolAddressDeltas.bind(this)
    );
    this.app.use('/insight-api', createInsightCompatibilityRouter(insightService));
```

Add this public method to `ClickHouseAPIServer`:

```ts
  getApp(): express.Application {
    return this.app;
  }
```

Update the catch-all condition so unmatched `/insight-api/*` routes are not served as frontend HTML:

```ts
      if (req.path.startsWith('/api/') || req.path.startsWith('/insight-api/') || req.path === '/health') {
        return res.status(404).json({ error: 'Not found' });
      }
```

The router-level fallback in Task 8 will return the legacy 404 body for `/insight-api/*`; this catch-all remains a second guard.

- [ ] **Step 4: Run tests and verify green**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS for all tests.

- [ ] **Step 5: Commit**

```bash
git add flux-indexer/src/api/server.ts flux-indexer/src/api/insight/__tests__/router.test.ts
git commit -m "feat: mount insight compatibility api"
```

---

### Task 8: Remaining Service Methods, Statistics, and Router Fallback

**Files:**
- Modify: `flux-indexer/src/api/insight/service.ts`
- Modify: `flux-indexer/src/api/insight/router.ts`
- Modify: `flux-indexer/src/api/insight/__tests__/service.test.ts`
- Modify: `flux-indexer/src/api/insight/__tests__/router.test.ts`

- [ ] **Step 1: Write failing tests for remaining endpoints**

Add service tests for:

```ts
test('getAddressUtxos calculates confirmations from current height', async () => {
  const { service, ch } = createService();
  ch.queryOne.mockResolvedValueOnce({ h: 100 });
  ch.query.mockResolvedValueOnce([{ address: 'addr', txid: 'tx', vout: 0, value: '1', script_pubkey: 'hex', block_height: 90 }]);
  await expect(service.getAddressUtxos(['addr'], true)).resolves.toMatchObject([{ confirmations: 11 }]);
});

test('getSync composes legacy sync response from sync_state', async () => {
  const { service, ch } = createService();
  ch.queryOne.mockResolvedValueOnce({ current_height: 10, chain_height: 20, sync_percentage: 50, is_syncing: 1 });
  await expect(service.getSync()).resolves.toEqual({ status: 'syncing', blockChainHeight: 20, syncPercentage: 50, height: 10, error: null, type: 'bitcore node' });
});
```

Add router tests for:

```ts
test('unmatched /insight-api route returns legacy 404', async () => {
  const { app } = createApp({});
  await withTestServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/insight-api/does-not-exist`);
    expect(response.status).toBe(404);
    expect(await readJson<any>(response)).toEqual({ status: 404, url: '/insight-api/does-not-exist', error: 'Not found' });
  });
});
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: FAIL on methods or fallback behavior not yet complete.

- [ ] **Step 3: Complete service methods for transactions, address history, balances, and stats**

Add these methods to `InsightCompatibilityService`:

```ts
  async getTransactionsByBlock(blockHash: string): Promise<any[]> {
    const block = await this.getBlock(blockHash);
    if (!block) return [];
    const items = [];
    for (const txid of block.txids) {
      const tx = await this.getTransaction(txid);
      if (tx) items.push(tx);
    }
    return items;
  }

  async getTransactionsByAddress(address: string): Promise<any[]> {
    const result = await this.getAddressTransactions([address], { from: 0, to: 10, limit: 10 });
    return result.items;
  }

  async getAddressTransactions(addresses: string[], range: { from: number; to: number; limit: number }): Promise<{ totalItems: number; items: any[] }> {
    const rows = await this.ch.query<{ txid: string }>(`
      SELECT txid
      FROM (
        SELECT txid, block_height, tx_index, is_valid
        FROM address_transactions
        WHERE address IN {addresses:Array(String)}
        ORDER BY txid, _version DESC
        LIMIT 1 BY txid
      )
      WHERE is_valid = 1
      ORDER BY block_height DESC, tx_index ASC
      LIMIT ${range.limit}
      OFFSET ${range.from}
    `, { addresses });
    const countRow = await this.ch.queryOne<{ count: number }>(`
      SELECT count() as count
      FROM (
        SELECT txid
        FROM (
          SELECT txid, is_valid
          FROM address_transactions
          WHERE address IN {addresses:Array(String)}
          ORDER BY txid, _version DESC
          LIMIT 1 BY txid
        )
        WHERE is_valid = 1
      )
    `, { addresses });
    const items = [];
    for (const row of rows) {
      const tx = await this.getTransaction(row.txid);
      if (tx) items.push(tx);
    }
    return { totalItems: Number(countRow?.count ?? rows.length), items };
  }

  async getAddressBalanceSum(addresses: string[]): Promise<{ balance: number; unconfirmedBalance: number; immature: number }> {
    const rows = await this.ch.query<any>(`
      SELECT
        address,
        sumMerge(balance) AS balance
      FROM address_summary_agg
      WHERE address IN {addresses:Array(String)}
      GROUP BY address
    `, { addresses });
    const mempool = await this.getMempoolAddressDeltas();
    const balance = rows.reduce((sum, row) => sum + Number(row.balance ?? 0), 0);
    const unconfirmedBalance = addresses.reduce((sum, address) => sum + Number(mempool.get(address)?.balanceDelta ?? 0n), 0);
    return { balance, unconfirmedBalance, immature: 0 };
  }

  async dosList(): Promise<any> {
    return this.rpc.dosList();
  }

  async startList(): Promise<any> {
    return this.rpc.startList();
  }

  private getStatisticsDays(raw: unknown): number {
    if (raw === 'all') return 730;
    const parsed = Number.parseInt(String(raw ?? '365'), 10);
    if (!Number.isFinite(parsed) || parsed < 1) return 365;
    return Math.min(parsed, 730);
  }

  async getStatisticSeries(kind: 'supply' | 'fees' | 'network-hash' | 'transactions' | 'outputs' | 'difficulty' | 'active-addresses', rawDays: unknown): Promise<any[]> {
    const days = this.getStatisticsDays(rawDays);
    if (kind === 'supply') {
      return this.ch.query<any>(`
        SELECT
          toString(day) as date,
          toString(total_supply) as total_supply,
          toString(transparent_supply) as transparent_supply,
          toString(shielded_pool) as shielded_pool
        FROM mv_daily_supply
        WHERE day >= today() - {days:UInt16}
        ORDER BY day ASC
      `, { days });
    }
    if (kind === 'fees') {
      return this.ch.query<any>(`
        SELECT toString(toDate(toDateTime(timestamp))) as date, sum(fee) / 100000000 as fees
        FROM transactions
        WHERE timestamp >= toUnixTimestamp(now() - INTERVAL {days:UInt16} DAY) AND is_valid = 1
        GROUP BY date
        ORDER BY date ASC
      `, { days });
    }
    if (kind === 'network-hash') {
      return this.ch.query<any>(`
        SELECT toString(toDate(toDateTime(timestamp))) as date, avg(difficulty) as network_hash
        FROM blocks
        WHERE timestamp >= toUnixTimestamp(now() - INTERVAL {days:UInt16} DAY) AND is_valid = 1
        GROUP BY date
        ORDER BY date ASC
      `, { days });
    }
    if (kind === 'transactions') {
      return this.ch.query<any>(`
        SELECT toString(toDate(toDateTime(timestamp))) as date, count() as transaction_count
        FROM transactions
        WHERE timestamp >= toUnixTimestamp(now() - INTERVAL {days:UInt16} DAY) AND is_valid = 1
        GROUP BY date
        ORDER BY date ASC
      `, { days });
    }
    if (kind === 'outputs') {
      return this.ch.query<any>(`
        SELECT toString(toDate(toDateTime(timestamp))) as date, sum(output_total) / 100000000 as output_total
        FROM transactions
        WHERE timestamp >= toUnixTimestamp(now() - INTERVAL {days:UInt16} DAY) AND is_valid = 1
        GROUP BY date
        ORDER BY date ASC
      `, { days });
    }
    if (kind === 'difficulty') {
      return this.ch.query<any>(`
        SELECT toString(toDate(toDateTime(timestamp))) as date, avg(difficulty) as difficulty
        FROM blocks
        WHERE timestamp >= toUnixTimestamp(now() - INTERVAL {days:UInt16} DAY) AND is_valid = 1
        GROUP BY date
        ORDER BY date ASC
      `, { days });
    }
    return this.ch.query<any>(`
      SELECT toString(toDate(toDateTime(timestamp))) as date, uniqExact(address) as active_addresses
      FROM address_transactions
      WHERE timestamp >= toUnixTimestamp(now() - INTERVAL {days:UInt16} DAY) AND is_valid = 1
      GROUP BY date
      ORDER BY date ASC
    `, { days });
  }

  async getStatisticsTotal(): Promise<any> {
    const [blocks, transactions, addresses] = await Promise.all([
      this.ch.queryOne<{ count: number }>('SELECT uniqExact(height) as count FROM blocks WHERE is_valid = 1'),
      this.ch.queryOne<{ count: number }>('SELECT uniqExact(txid, block_height) as count FROM transactions WHERE is_valid = 1'),
      this.ch.queryOne<{ count: number }>('SELECT uniqExact(address) as count FROM address_summary_agg'),
    ]);
    return {
      blocks: Number(blocks?.count ?? 0),
      txs: Number(transactions?.count ?? 0),
      addresses: Number(addresses?.count ?? 0),
    };
  }

  async getPools(dateRaw?: string): Promise<any> {
    const date = dateRaw && /^\\d{4}-\\d{2}-\\d{2}$/.test(dateRaw) ? dateRaw : new Date().toISOString().slice(0, 10);
    const parsed = parseBlockDate(date);
    const rows = await this.ch.query<any>(`
      SELECT producer as poolName, count() as blocks
      FROM blocks
      WHERE timestamp >= {start:UInt32} AND timestamp <= {end:UInt32} AND is_valid = 1
      GROUP BY producer
      ORDER BY blocks DESC
      LIMIT 200
    `, { start: parsed.start, end: parsed.end });
    const total = rows.reduce((sum, row) => sum + Number(row.blocks ?? 0), 0);
    return {
      date,
      n_blocks_mined: total,
      blocks_by_pool: rows.map((row) => ({ poolName: row.poolName || 'Unknown', blocks: Number(row.blocks ?? 0) })),
      pagination: { next: parsed.next, prev: parsed.prev, currentTs: parsed.end, current: parsed.current, isToday: parsed.current === new Date().toISOString().slice(0, 10) },
    };
  }

  async getPoolsLastHour(): Promise<any[]> {
    return this.ch.query<any>(`
      SELECT producer as poolName, count() as blocks
      FROM blocks
      WHERE timestamp >= toUnixTimestamp(now() - INTERVAL 1 HOUR) AND is_valid = 1
      GROUP BY producer
      ORDER BY blocks DESC
      LIMIT 200
    `);
  }

  async getBalanceIntervals(): Promise<any[]> {
    return this.ch.query<any>(`
      SELECT
        multiIf(balance < 100000000, '0-1', balance < 10000000000, '1-100', balance < 1000000000000, '100-10000', '10000+') as interval,
        count() as addresses,
        sum(balance) / 100000000 as coins
      FROM (
        SELECT sumMerge(balance) as balance
        FROM address_summary_agg
        GROUP BY address
      )
      GROUP BY interval
      ORDER BY interval ASC
    `);
  }

  async getRicherThan(): Promise<any[]> {
    return this.ch.query<any>(`
      SELECT threshold, countIf(balance > threshold) as addresses
      FROM (
        SELECT arrayJoin([100000000, 10000000000, 1000000000000]) as threshold
      )
      CROSS JOIN (
        SELECT sumMerge(balance) as balance
        FROM address_summary_agg
        GROUP BY address
      )
      GROUP BY threshold
      ORDER BY threshold ASC
    `);
  }

  async getRichestAddressesList(): Promise<any[]> {
    return this.ch.query<any>(`
      SELECT
        address,
        sumMerge(balance) / 100000000 as balance,
        0 as blocks_mined
      FROM address_summary_agg
      GROUP BY address
      ORDER BY sumMerge(balance) DESC
      LIMIT 200
    `);
  }
```

Add route methods for statistics endpoints in `router.ts`:

```ts
  router.get('/statistics/supply', asyncHandler(async (req, res) => {
    res.json(service.getStatisticSeries ? await service.getStatisticSeries('supply', req.query.days) : []);
  }));
  router.get('/statistics/fees', asyncHandler(async (req, res) => {
    res.json(service.getStatisticSeries ? await service.getStatisticSeries('fees', req.query.days) : []);
  }));
  router.get('/statistics/network-hash', asyncHandler(async (req, res) => {
    res.json(service.getStatisticSeries ? await service.getStatisticSeries('network-hash', req.query.days) : []);
  }));
  router.get('/statistics/pools', asyncHandler(async (req, res) => {
    res.json(service.getPools ? await service.getPools(typeof req.query.date === 'string' ? req.query.date : undefined) : { date: new Date().toISOString().slice(0, 10), n_blocks_mined: 0, blocks_by_pool: [], pagination: null });
  }));
  router.get('/statistics/pools-last-hour', asyncHandler(async (_req, res) => {
    res.json(service.getPoolsLastHour ? await service.getPoolsLastHour() : []);
  }));
  router.get('/statistics/transactions', asyncHandler(async (req, res) => {
    res.json(service.getStatisticSeries ? await service.getStatisticSeries('transactions', req.query.days) : []);
  }));
  router.get('/statistics/outputs', asyncHandler(async (req, res) => {
    res.json(service.getStatisticSeries ? await service.getStatisticSeries('outputs', req.query.days) : []);
  }));
  router.get('/statistics/difficulty', asyncHandler(async (req, res) => {
    res.json(service.getStatisticSeries ? await service.getStatisticSeries('difficulty', req.query.days) : []);
  }));
  router.get('/statistics/total', asyncHandler(async (_req, res) => {
    res.json(service.getStatisticsTotal ? await service.getStatisticsTotal() : { txs: 0, blocks: 0, addresses: 0 });
  }));
  router.get('/statistics/balance-intervals', asyncHandler(async (_req, res) => {
    res.json(service.getBalanceIntervals ? await service.getBalanceIntervals() : []);
  }));
  router.get('/statistics/richer-than', asyncHandler(async (_req, res) => {
    res.json(service.getRicherThan ? await service.getRicherThan() : []);
  }));
  router.get('/statistics/richest-addresses-list', asyncHandler(async (_req, res) => {
    res.json(service.getRichestAddressesList ? await service.getRichestAddressesList() : []);
  }));
  router.get('/statistics/active-addresses', asyncHandler(async (req, res) => {
    res.json(service.getStatisticSeries ? await service.getStatisticSeries('active-addresses', req.query.days) : []);
  }));

  router.use((req, res) => sendNotFound(req, res));
```

- [ ] **Step 4: Run tests and verify green**

Run:

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS for all tests.

- [ ] **Step 5: Build separately**

Run:

```bash
cd flux-indexer
npm run build
```

Expected: PASS with TypeScript emitting `dist`.

- [ ] **Step 6: Commit**

```bash
git add flux-indexer/src/api/insight/service.ts flux-indexer/src/api/insight/router.ts flux-indexer/src/api/insight/__tests__/service.test.ts flux-indexer/src/api/insight/__tests__/router.test.ts
git commit -m "feat: complete insight compatibility surface"
```

---

### Task 9: README API Compatibility Documentation

**Files:**
- Modify: `README.md`
- Modify: `flux-indexer/README.md`

- [ ] **Step 1: Add failing documentation check**

Run:

```bash
rg "/insight-api" README.md flux-indexer/README.md
```

Expected: FAIL because the compatibility API is not documented.

- [ ] **Step 2: Update root README API reference**

Add this under the API Reference section in `README.md`:

```markdown
### Insight Compatibility

Legacy Insight-compatible REST endpoints are available under `/insight-api` on the indexer API port. This compatibility layer is API-only and does not include the old Insight UI or websocket event API.

Common routes:
- `GET /insight-api/block/:hash`
- `GET /insight-api/block-index/:height`
- `GET /insight-api/rawblock/:hashOrHeight`
- `GET /insight-api/tx/:txid`
- `GET /insight-api/rawtx/:txid`
- `POST /insight-api/tx/send`
- `GET /insight-api/addr/:address`
- `GET /insight-api/addr/:address/utxo`
- `GET /insight-api/addrs/:addresses/txs`
- `GET /insight-api/status?q=getInfo`
- `GET /insight-api/sync`
- `GET /insight-api/utils/estimatefee`
- `GET /insight-api/supply`
```

- [ ] **Step 3: Update indexer README**

Add the same API-only compatibility paragraph to `flux-indexer/README.md` under its API section. If there is no API section, add a new `## Insight Compatibility API` section near the existing endpoint documentation.

- [ ] **Step 4: Verify documentation**

Run:

```bash
rg "/insight-api" README.md flux-indexer/README.md
```

Expected: PASS with matches in both files.

- [ ] **Step 5: Commit**

```bash
git add README.md flux-indexer/README.md
git commit -m "docs: document insight compatibility api"
```

---

### Task 10: Final Verification

**Files:**
- No planned edits.

- [ ] **Step 1: Run all indexer tests**

```bash
cd flux-indexer
npm test -- --runInBand
```

Expected: PASS.

- [ ] **Step 2: Run TypeScript build**

```bash
cd flux-indexer
npm run build
```

Expected: PASS.

- [ ] **Step 3: Inspect final diff**

```bash
git status --short
git log --oneline -10
```

Expected: `git status --short` has no uncommitted implementation changes. The recent log includes the commits from this plan.

- [ ] **Step 4: Manual smoke commands with a running local indexer**

If a local indexer is running on port `42067`, run:

```bash
curl -s http://127.0.0.1:42067/insight-api/sync
curl -s http://127.0.0.1:42067/insight-api/status?q=getInfo
curl -s http://127.0.0.1:42067/insight-api/utils/estimatefee?nbBlocks=2,6
```

Expected: each command returns JSON with legacy Insight-compatible keys.

---

## Self-Review Notes

- Spec coverage: the plan covers blocks, transactions, addresses, status, node state, utilities, messages, FluxNode routes, supply, markets, statistics, router mounting, error behavior, tests, and documentation.
- Scope check: this is one subsystem, the REST compatibility API. UI and websocket compatibility remain out of scope.
- Type consistency: the plan consistently uses `InsightCompatibilityService`, `createInsightCompatibilityRouter`, `formatBlock`, `formatTransaction`, `formatAddressSummary`, `formatUtxo`, and `FluxRPCClient` method names across tests and implementation steps.
- Test strategy: every production change has a preceding failing test command, except documentation where the red check is an `rg` documentation check.
