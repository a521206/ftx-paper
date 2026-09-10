# FTX Paper Extraction Tasks

## Foundation

- [x] Standalone package metadata and console entry points
- [x] Broker-neutral market and order contracts
- [x] Runtime SQLite boundary
- [x] Versioned backend API skeleton
- [x] Separate UI server
- [x] Runtime session and execution-ledger boundary

## Core engine

- [x] Define production strategy interface
- [x] Copy and simplify live strategy configuration
- [x] Implement live feature calculator
- [x] Implement setup detection and decision policy
- [x] Implement risk, sizing, and drawdown guards
- [x] Implement exit state machine
- [x] Add deterministic market replay tests

## Broker and runtime

- [x] Implement Zerodha authentication API routes
- [x] Implement Zerodha WebSocket feed and reconnect policy
- [x] Implement order acknowledgement and fill polling
- [x] Add serialized runtime lifecycle
- [x] Add crash recovery and idempotency keys
- [x] Persist complete decision/order/fill/position audit records

## API and UI

- [x] Add API authentication and authorization
- [x] Add API schemas and OpenAPI documentation
- [x] Extract dashboard pages into the UI package
- [x] Add runtime controls and event polling
- [x] Add browser-level smoke tests

## Cutover

- [ ] Run old/new deterministic replay comparison
- [x] Switch `scripts.ftx.run_ftx_web` to `ftx-paper`
- [ ] Remove live implementation from NiftyZoning
- [x] Add standalone deployment and upgrade documentation
# Tasks

- [x] In-process push-feed runtime session
- [x] Serialized start/stop/restart lifecycle
- [x] Remove subprocess worker and command queue
