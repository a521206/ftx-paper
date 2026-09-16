# FTX Paper Task List

Open parity and cutover work is tracked in the numbered tracks below.

## Bit-for-bit canonical FTX parity

### Track P0 — Parity contract

- [ ] Approve [PARITY_CONTRACT.md](PARITY_CONTRACT.md) as the working specification

### Track P2 — Market-feature parity

- [ ] Match VWAP, developing session high/low, ATR, and opening-range calculations
- [ ] Match prior-day high/low initialization and rollover
- [ ] Match VIX opening, event-time lookup, carry-forward, and missing-VIX behavior
- [ ] Match option-PCR lookup, cutoff, and unavailable-value behavior
- [ ] Add field-by-field feature comparison tests

### Track P5 — Session-policy parity

- [ ] Match morning and afternoon half-open session windows
- [ ] Match configured composite cells and fixed policy directions
- [ ] Remove price-relative direction inference from the paper decision path
- [ ] Match transition-policy eligibility and rejection reasons
- [ ] Add session-boundary and policy-matrix tests

### Track P8 — Exit parity

- [ ] Implement canonical per-cell signal, trail, target, hard-stop, and end-of-day modes
- [ ] Match trail activation, distance, breakeven lock, and intrabar ordering
- [ ] Match exit timestamps, held bars, and exit reasons
- [ ] Add deterministic exit fixtures for every exit mode

### Track P9 — Decision-replay comparator

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
- [ ] Switch runtime execution to the parity-proven decision path
- [ ] Remove the canonical live implementation from NiftyZoning only after the parity gate passes
