# Phase 0 behavioral contract

This is the fixture-level baseline for aligning `ftx-paper` with the canonical
FTX implementation. The executable fixture is
`tests/fixtures/p0_behavioral_contract.json`; it captures externally visible
behavior, not an implementation abstraction.

## Canonical behavior discovered

- Candidate evaluation is ordered as policy, thesis, shared directional
  concurrency, risk permission, sizing, final downward quantity rounding, and
  acceptance. The first failed stage owns the rejection reason.
- The architecture contract above is the intended canonical order. The current
  `src/ftx.engine.run_execution()` code still performs its explicit risk/sizing
  block before its explicit concurrency check; this is an observed
  canonical-implementation mismatch to resolve in the decision-ordering phase,
  not a Phase 0 paper change.
- Portfolio available capital is the capital ledger's capital less open margin.
  With Rs 2,500,000 capital and Rs 350,000 reserved, available capital is
  Rs 2,150,000 and margin utilization is 14%.
- Lot quantities are integer quantities and are never promoted from a
  sub-lot result. A risk ceiling of one lot scaled by 0.6 produces zero lots.
- Directional headroom is signed and shared: long entries increase net exposure,
  short entries decrease it, and cancellation/exit releases the same amount.
- Entry reservation, fill, cancellation, exit settlement, equity, realized P&L,
  and costs have one portfolio owner. Synthetic settlement receives an accepted
  futures plan and premium lookups only.
- Date transitions reset date-scoped gate state and causal session state; they do
  not turn synthetic reporting into another decision stream.

## Current paper behavior

The paper implementation already has passing focused coverage for signed
headroom, reservation/cancellation, portfolio idempotency, snapshot restore,
and date-scoped `RiskGateState` resets. Those tests remain in the established
`test_risk_state.py` and `test_core_boundaries.py` files.

The following mismatches are intentionally documented for later phases and are
not changed by Phase 0:

1. `IndependentLiveDecisionEngine` defaults to both `futures` and `synthetic`
   and loops over enabled vehicles, so synthetic can currently perform an
   independent sizing/decision path.
2. `RiskSizer` computes available capital as
   `min(capital, equity) - open_margin_used`; canonical `PortfolioState`
   available capital is capital less open margin.
3. The paper sizer uses `round()` for its scaled quantity and performs its own
   vehicle-level risk path. Canonical final quantities use ordered ceilings and
   downward integer-lot rounding.
4. Runtime and execution still contain vehicle-specific state paths that must
   be consolidated after the behavioral baseline is accepted.

## Phase 2 disposition

The decision boundary now evaluates futures only. Synthetic reporting is
carried as optional contract and premium-lookup metadata; it cannot create a
second decision, reserve margin, update a gate, or mutate futures exposure.
The remaining vehicle-specific runtime paths are retained for the later
execution ownership and synthetic settlement phases, where they will be
removed or reduced to derived settlement handling.

## Fixture mapping

The fixture and focused tests cover gate order, available capital and margin,
downward lot behavior, shared directional headroom, reservation/cancellation,
futures-first synthetic semantics, and date/session reset ownership. Phase 0
does not implement any of the listed mismatches.
