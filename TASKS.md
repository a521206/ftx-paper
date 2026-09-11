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

## Bit-for-bit canonical FTX parity

### Track P0 — Parity contract

- [x] Define the canonical parity contract: inputs, defaults, timestamps, ordering, missing-data behavior, and output fields
- [x] Document the independent `ftx-paper` mapping to each canonical component
- [x] Define the zero-difference acceptance criteria and the explicitly allowed exceptions
- [ ] Approve [PARITY_CONTRACT.md](PARITY_CONTRACT.md) as the working specification

### Track P1 — Decision-clock parity

- [x] Feed the same completed futures-bar prefix into paper decisions as the canonical pipeline
- [x] Match warmup, duplicate-minute rejection, late-bar handling, and date-boundary reset
- [x] Add focused fixtures for clock and prefix behavior

### Track P2 — Market-feature parity

- [ ] Match VWAP, developing session high/low, ATR, and opening-range calculations
- [ ] Match prior-day high/low initialization and rollover
- [ ] Match VIX opening, event-time lookup, carry-forward, and missing-VIX behavior
- [ ] Match option-PCR lookup, cutoff, and unavailable-value behavior
- [ ] Add field-by-field feature comparison tests

### Track P3 — Location and cell parity

- [x] Port the canonical all-location detector independently into `ftx-paper`
- [x] Support VWAP, session high/low, new high/low, opening-range high/low, and prior-day locations
- [x] Match proximity thresholds, simultaneous-location combinations, and opening-range completion
- [x] Match transition debouncing and transition-pattern filtering
- [x] Add location/cell golden fixtures

### Track P4 — Score parity

- [x] Verify selling-structure values for every factor and insufficient-history case
- [x] Match the nine score factors, grinding override, thresholds, and setup-type mapping
- [x] Compare paper and canonical score factors/value-by-value on shared fixtures

### Track P5 — Session-policy parity

- [ ] Match morning and afternoon half-open session windows
- [ ] Match configured composite cells and fixed policy directions
- [ ] Remove price-relative direction inference from the paper decision path
- [ ] Match transition-policy eligibility and rejection reasons
- [ ] Add session-boundary and policy-matrix tests

### Track P6 — Stop and sizing parity

- [ ] Match adaptive ATR/VIX stop calculation, bounds, and expiry-day adjustment
- [ ] Match setup-size multipliers and low-VIX reduction
- [ ] Match capital, equity, peak-equity, drawdown, and risk-budget semantics
- [ ] Match vehicle-specific sizing and synthetic-premium lookup behavior
- [ ] Add stop and quantity golden fixtures

### Track P7 — Risk-gate parity

- [ ] Match directional exposure and concurrency limits
- [ ] Match thesis-failure handling
- [ ] Match per-cell post-exit cooldown behavior
- [ ] Match risk-gate state reset and persistence across session segments
- [ ] Add focused gate-state replay tests

### Track P8 — Exit parity

- [ ] Implement canonical per-cell signal, trail, target, hard-stop, and end-of-day modes
- [ ] Match trail activation, distance, breakeven lock, and intrabar ordering
- [ ] Match exit timestamps, held bars, and exit reasons
- [ ] Add deterministic exit fixtures for every exit mode

### Track P9 — Decision-replay comparator

- [ ] Build a harness that feeds identical bundles to paper and canonical implementations
- [ ] Compare event identity, cell, direction, score, factors, stop, quantity, reason, and timing
- [ ] Emit a machine-readable first-difference report
- [ ] Add regression tests for every discovered mismatch

### Track P10 — Ledger and vehicle comparator

- [ ] Compare futures exits, fills, costs, and final trade ledgers
- [ ] Compare synthetic premium entry/exit, sizing, costs, and ledgers
- [ ] Compare daily PnL, drawdown, and rejection counts
- [ ] Produce a reproducible cross-vehicle comparison report

### Track P11 — Cutover

- [ ] Run the complete holdout replay and achieve zero decision differences
- [ ] Obtain explicit approval for any remaining exceptions
- [ ] Remove or quarantine the simplified paper `on_bar` path
- [ ] Switch runtime execution to the parity-proven decision path
- [ ] Remove the canonical live implementation from NiftyZoning only after the parity gate passes
