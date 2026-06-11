# Insight API Compatibility Design

## Goal

Add a REST-only Insight compatibility API to the new Flux indexer so existing wallets, services, and integrations can keep using legacy Insight endpoints without depending on the old `insight-api` and `insight-ui` stack.

The compatibility API will live in `flux-indexer` and preserve the existing `/api/v1` explorer API. No UI, Angular routes, static frontend changes, or websocket publish events are included in this design.

## Context

The legacy API in `/Users/vasilismagkoutis/repos/insight-api` exposes routes under the configurable route prefix, normally `/insight-api`. The legacy UI in `/Users/vasilismagkoutis/repos/insight-ui` consumes these routes, but this project only needs REST API compatibility for external consumers.

The new repository already has:

- `flux-indexer/src/api/server.ts` with `/api/v1` routes for status, blocks, transactions, addresses, rich list, supply, producers, nodes, network, mempool, and analytics.
- ClickHouse tables for blocks, transactions, UTXOs, address summaries, address transaction history, supply history, FluxNode transactions, live FluxNode counts, producers, and sync state.
- `flux-indexer/src/rpc/flux-rpc-client.ts` for Flux daemon RPC calls.

## Recommended Architecture

Create a dedicated Insight compatibility router mounted at `/insight-api` inside the `flux-indexer` service.

The router should be implemented separately from the current `/api/v1` handlers so the new explorer API remains stable and the legacy response shapes can be preserved without leaking compatibility quirks into modern endpoints. The router can share focused query helpers with the existing API where behavior matches, but response mapping should remain Insight-specific.

## Components

### Compatibility Router

Add an Express router responsible for registering legacy routes under `/insight-api`.

Responsibilities:

- Parse legacy query and body parameters.
- Validate common parameters such as txids, block hashes, block heights, addresses, limits, and pagination ranges.
- Dispatch read-heavy endpoints to ClickHouse query helpers.
- Dispatch live daemon endpoints to `FluxRPCClient`.
- Return legacy Insight response shapes and status behavior.

### Query Helpers

Add focused helper methods or a small service module for compatibility reads.

Responsibilities:

- Fetch latest valid block by hash or height.
- Fetch transaction summary and transaction details by txid.
- Fetch raw transaction or raw block hex using RPC, with existing block extraction fallback where needed.
- Fetch address summary, address transaction history, and UTXOs.
- Fetch multi-address UTXOs and transactions.
- Fetch historical statistics from available ClickHouse aggregates.

### RPC Additions

Extend `FluxRPCClient` with methods needed by legacy endpoints.

Expected methods:

- `sendRawTransaction(rawtx)`
- `verifyMessage(address, signature, message)`
- `getPeerInfo()`
- `getMiningInfo()`
- `getInfo()`; if the daemon does not expose it, compose the legacy `info` object from `getBlockchainInfo()` and `getNetworkInfo()` with missing wallet-only fields set to neutral values
- `estimateFee(nblocks)`
- `viewDeterministicFluxNodeList()` with fallback to the existing FluxNode list method if the daemon method name differs
- `dosList()` and `startList()` for legacy FluxNode endpoints when supported

RPC-only endpoints should fail with a structured Insight-style error if the daemon does not support the requested method.

## Route Scope

### Blocks

Implement:

- `GET /insight-api/block/:blockHash`
- `GET /insight-api/block-index/:height`
- `GET /insight-api/rawblock/:blockHashOrHeight`
- `GET /insight-api/blocks`

Response requirements:

- `/block/:blockHash` returns legacy fields such as `hash`, `size`, `height`, `version`, `merkleroot`, `tx`, `time`, `bits`, `difficulty`, `chainwork`, `confirmations`, `previousblockhash`, `nextblockhash`, `reward`, `isMainChain`, `minedBy`, and `poolInfo`.
- `/block-index/:height` returns `{ "blockHash": "<hash>" }`.
- `/rawblock/:blockHashOrHeight` returns `{ "rawblock": "<hex>" }`.
- `/blocks` returns `{ blocks, length, pagination }`. When `blockDate=YYYY-MM-DD` is present, select blocks whose timestamps fall inside that UTC day. If `blockDate` is absent, return the most recent blocks.

### Transactions

Implement:

- `GET /insight-api/tx/:txid`
- `GET /insight-api/rawtx/:txid`
- `GET /insight-api/txs?block=<hash>`
- `GET /insight-api/txs?address=<address>`
- `GET /insight-api/txs`
- `POST /insight-api/tx/send`

Response requirements:

- `/tx/:txid` returns legacy transaction fields: `txid`, `version`, `locktime`, `vin`, `vout`, `blockhash`, `blockheight`, `confirmations`, `time`, `blocktime`, `valueOut`, `valueIn`, `fees`, `size`, shielded fields when available, and FluxNode metadata when available.
- `vin` values use `addr`, `valueSat`, `value`, `scriptSig`, `sequence`, and `n` where data exists.
- `vout` values use string FLUX amount in `value`, numeric `n`, and `scriptPubKey` with `hex`, `asm`, `addresses`, and `type`.
- `/rawtx/:txid` returns `{ "rawtx": "<hex>" }`.
- `/txs?block=<hash>` returns transactions for the block in legacy list shape.
- `/txs?address=<address>` returns transactions for the address in legacy list shape.
- `/tx/send` accepts `rawtx` from JSON or form body and returns `{ "txid": "<txid>" }`.

### Addresses

Implement:

- `GET /insight-api/addr/:addr`
- `GET /insight-api/addr/:addr/balance`
- `GET /insight-api/addr/:addr/totalReceived`
- `GET /insight-api/addr/:addr/totalSent`
- `GET /insight-api/addr/:addr/unconfirmedBalance`
- `GET /insight-api/addr/:addr/utxo`
- `GET /insight-api/addrs/:addrs/utxo`
- `POST /insight-api/addrs/utxo`
- `GET /insight-api/addrs/:addrs/unspent`
- `GET /insight-api/addrs/:addrs/txs`
- `POST /insight-api/addrs/txs`
- `GET /insight-api/addrs/:addrs/balance`

Response requirements:

- `/addr/:addr` returns `addrStr`, `balance`, `balanceSat`, `totalReceived`, `totalReceivedSat`, `totalSent`, `totalSentSat`, `unconfirmedBalance`, `unconfirmedBalanceSat`, `unconfirmedTxApperances`, `txApperances`, and `transactions`.
- `noTxList=1` omits or empties `transactions` while keeping summary values.
- Address property endpoints return plain satoshi numbers.
- UTXO endpoints return arrays with `address`, `txid`, `vout`, `scriptPubKey`, `amount`, `satoshis`, `confirmations`, `height`, and `ts` when available.
- Multi-address transaction endpoints return `{ totalItems, from, to, items }` when paginated by `from` and `to`.

### Status and Node State

Implement:

- `GET /insight-api/status`
- `GET /insight-api/status?q=getInfo`
- `GET /insight-api/status?q=getDifficulty`
- `GET /insight-api/status?q=getBestBlockHash`
- `GET /insight-api/status?q=getLastBlockHash`
- `GET /insight-api/status?q=getMiningInfo`
- `GET /insight-api/status?q=getPeerInfo`
- `GET /insight-api/status?q=getFluxNodes`
- `GET /insight-api/status?q=getZelNodes`
- `GET /insight-api/sync`
- `GET /insight-api/peer`
- `GET /insight-api/version`

Response requirements:

- Default `/status` and `q=getInfo` return an object with the top-level key `info` and legacy daemon fields inside that object.
- Difficulty, best block hash, last block hash, mining info, peer info, and FluxNode calls preserve the wrapper keys used by legacy Insight.
- `/sync` returns `status`, `blockChainHeight`, `syncPercentage`, `height`, `error`, and `type`.
- `/peer` returns the legacy fixed connectivity object unless a more accurate RPC-backed value is available.

### Utility, Messages, FluxNode, Supply, Markets, and Statistics

Implement:

- `GET /insight-api/utils/estimatefee?nbBlocks=2,6`
- `GET /insight-api/messages/verify`
- `POST /insight-api/messages/verify`
- `GET /insight-api/fluxnode/listfluxnodes`
- `GET /insight-api/fluxnode/listfluxnodes/:filter`
- `POST /insight-api/fluxnode/listfluxnodes`
- `GET /insight-api/zelnode/listfluxnodes`
- `GET /insight-api/zelnode/listfluxnodes/:filter`
- `POST /insight-api/zelnode/listfluxnodes`
- `GET /insight-api/fluxnode/addrs/:addrs/utxo`
- `POST /insight-api/fluxnode/addrs/utxo`
- `GET /insight-api/fluxnode/doslist`
- `GET /insight-api/fluxnode/startlist`
- `GET /insight-api/supply`
- `GET /insight-api/total-supply`
- `GET /insight-api/circulating-supply`
- `GET /insight-api/circulation`
- `GET /insight-api/statistics/total-supply`
- `GET /insight-api/statistics/circulating-supply`
- `GET /insight-api/statistics/main-chain-circulating-locked`
- `GET /insight-api/statistics/supply`
- `GET /insight-api/statistics/fees`
- `GET /insight-api/statistics/network-hash`
- `GET /insight-api/statistics/pools`
- `GET /insight-api/statistics/pools-last-hour`
- `GET /insight-api/statistics/transactions`
- `GET /insight-api/statistics/outputs`
- `GET /insight-api/statistics/difficulty`
- `GET /insight-api/statistics/total`
- `GET /insight-api/statistics/balance-intervals`
- `GET /insight-api/statistics/richer-than`
- `GET /insight-api/statistics/richest-addresses-list`
- `GET /insight-api/statistics/active-addresses`
- `GET /insight-api/currency`
- `GET /insight-api/markets/info`

Response requirements:

- `estimatefee` returns an object keyed by requested block counts.
- `messages/verify` returns `{ "result": true|false }` and errors when `address`, `signature`, or `message` is missing.
- FluxNode filtered list behavior should match legacy filtering by comma-separated collateral, IP, or substring.
- Supply endpoints return plain text by default and object form when `format=object` is present.
- Statistics endpoints use existing ClickHouse tables with deterministic mappings: supply from `supply_stats` and `mv_daily_supply`, fees and outputs from `transactions`, network hash and difficulty from `blocks`, pools from `blocks.producer`, total and active address counts from `transactions` and `address_transactions`, balance intervals and richest-address lists from `address_summary_agg` plus `live_fluxnodes`.
- `/currency` and `/markets/info` do not add a new external market dependency. They return a compatibility object with `null` price fields and the current timestamp unless the indexer already has configured local market data.

## Data Mapping

Use ClickHouse for indexed chain data:

- `blocks` for block metadata and block date queries.
- `transactions` for transaction metadata and block transaction lists.
- `utxos` for outputs, spent state, and UTXO lists.
- `address_summary_agg` for address balances and totals.
- `address_transactions` for address history.
- `supply_stats` and `mv_daily_supply` for supply endpoints.
- `mv_hourly_tx_count` and block/transaction aggregates for statistics.
- `live_fluxnodes` and `fluxnode_transactions` for FluxNode-specific address and transaction data.

Use Flux RPC for live node data and write operations:

- Raw block and raw transaction hex where ClickHouse does not store raw hex.
- Transaction broadcast.
- Message verification.
- Peer info, mining info, node version, best block hash, and daemon-only status.
- FluxNode daemon lists when exact deterministic list output is required.

## Error Handling

Maintain legacy-style errors for compatibility:

- Missing resources and unmatched `/insight-api/*` routes return HTTP 404 with `{ status: 404, url, error: "Not found" }`.
- Invalid addresses or malformed inputs return HTTP 400 with a clear `message` and `code: 1`.
- Unsupported daemon methods return HTTP 502 with a compatibility error object that includes the RPC method name and keeps the endpoint response deterministic.
- Existing `/api/v1` error behavior must not change.

## Performance

The compatibility API must avoid unbounded ClickHouse scans.

Rules:

- Enforce limits on list endpoints.
- Preserve existing bounded defaults from `/api/v1` where legacy Insight did not define a safe limit.
- Use latest-row selection by `_version` or `version` instead of broad `FINAL` scans for high-volume tables.
- Reuse existing mempool address delta cache for unconfirmed address balances.
- Keep raw hex and live daemon endpoints RPC-backed and avoid storing raw blocks or raw transactions as part of this feature.

## Testing

Add Jest tests around the compatibility router and helper functions before implementation.

Test categories:

- Route registration and prefix isolation: `/insight-api/*` works and `/api/v1/*` remains unchanged.
- Response shape tests for block, block-index, rawblock, tx, rawtx, address summary, UTXO, multi-address UTXO, tx send, status, sync, estimatefee, message verify, FluxNode list, and supply endpoints.
- Legacy edge cases: `noTxList=1`, `from` and `to` pagination, comma-separated addresses, `format=object`, multiple `nbBlocks`, missing message verification fields, malformed txids, missing blocks, missing transactions, and unsupported RPC methods.
- Query helper tests using mocked ClickHouse and RPC clients so tests do not require a live daemon or ClickHouse instance.

## Out of Scope

- Any `insight-ui` page, Angular service, static asset, or frontend route.
- Websocket publish events from the old Insight service.
- Running the old `insight-api` or `insight-ui` services beside the new stack.
- Changing existing `/api/v1` response shapes.
- Storing raw block or raw transaction hex in ClickHouse.

## Acceptance Criteria

- Legacy REST consumers can point their base URL at the new indexer and call `/insight-api/*` endpoints without changing route names.
- Core wallet/service calls for blocks, transactions, addresses, UTXOs, broadcast, status, fee estimates, message verification, FluxNode lists, supply, and common statistics are implemented.
- Existing `/api/v1` explorer endpoints continue to build and behave as before.
- Tests cover the compatibility response shapes and the compatibility router can be exercised without a live ClickHouse or Flux daemon.
