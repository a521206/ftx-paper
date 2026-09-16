# Canonical FTX Decision Parity Contract

Status: P0 specification. The current `ftx-paper` implementation does not
yet satisfy this contract; the open implementation work is tracked as
P0, P5, P8, P9, P10, P11 in `TASKS.md`.

## P9 phased verification

P9 verifies this contract in two phases:

1. **Phase 1 — canonical-run trade comparison.** Run the canonical pipeline
   for the requested date range and use its audit bundle as the baseline for
   comparing FTX Paper accepted decisions and fills. This is the current
   `scripts/ftx/compare_pipeline_to_paper.py` comparator. It is a practical
   entry/trade diagnostic and may report exits or P&L as `unavailable` when
   Paper does not persist a reliable linked value.
2. **Phase 2 — detailed decision replay comparison.** Run both independent
   implementations against the same deterministic input replay and compare
   the complete ordered decision, execution, exit, and ledger streams.

Phase 1 is not a claim that this full contract has passed. The complete
decision-parity and production-cutover gate remains Phase 2. The detailed
scope, reports, and completion criteria are defined in
`P9_DECISION_REPLAY_COMPARATOR.md`.

## Scope

Parity means that `ftx-paper` and the canonical FTX implementation produce the
same decision stream when given the same completed market inputs. The
canonical reference is:

- Historical extraction: `src/ftx/signal_extraction.py`
- Canonical event features: `src/ftx/event_features.py`
- Canonical live session: `src/ftx/live_session.py`
- Session policies: `src/ftx/live_strategy_config.py`

The contract covers candidate detection, eligibility, rejection, acceptance,
order intent, stop, quantity, and exit decisions. Broker acknowledgements,
fill prices, network timing, and persistence timestamps are outside decision
parity and are covered by the ledger/runtime tracks.

## Input contract

Each decision minute is evaluated from exactly one completed futures bar and a
causal prefix containing only bars at or before that minute. Supporting inputs
are:

| Input | Contract |
|---|---|
| Futures | Completed OHLCV bar, ordered by canonical IST minute; current bar is the entry bar. |
| VIX | Opening value from the first available 09:15 bar; event value is the latest completed value at or before the decision minute. Missing VIX follows the canonical rejection path. |
| Option PCR | Value associated with the decision minute, or `None` when unavailable according to the canonical lookup cutoff. |
| Prior-day levels | Previous trading day high/low, reset at each trading-date boundary; `None` when unavailable. |
| Opening range | Causal range levels once the opening-range window is complete. |
| Expiry calendar | Canonical weekly-expiry membership used for stop adjustment. |
| Configuration | Strategy version, session windows, cells, directions, exit modes, stability, cooldown, sizing, and risk settings. |

All timestamps used for decisions are timezone-aware instants. Session and
minutes-from-open calculations are performed in Asia/Kolkata. `created_at`
and other persistence timestamps must never influence a decision.

## Determinism rules

- Inputs are processed in canonical minute order.
- A minute is processed at most once.
- Late supporting inputs cannot mutate an already emitted decision.
- Features and eligibility use no future bars or future supporting values.
- Missing values use canonical defaults or canonical rejection reasons; paper
  must not invent a substitute value silently.
- Numeric values are serialized using the same field names and normalized
  scalar types. Parity comparison uses exact equality after normalization;
  no tolerance is permitted for score, stop, quantity, or decision fields.
- Decision IDs may be derived from an equivalent deterministic identity
  formula (decision minute, sequence, event kind, cell) rather than compared
  as raw strings. The equivalence mapping must be defined before Phase 2 and
  the underlying identity fields must compare exactly.

## Decision output contract

For every candidate minute and cell, the comparator must be able to compare:

```text
decision_at
bundle/event identity
sequence
cell
direction
entry price
setup type
score
score factors
VIX at event and VIX open
PCR at event
structural proximity
stop basis / hard stop
sizing inputs and results
requested quantity
gate outcomes
accepted or rejected outcome
rejection reason
exit mode and exit result, when accepted
```

The ordered decision stream must include the same candidate, rejection, warmup,
sizing-rejection, acceptance, and exit events in the same order. A missing
event, extra event, changed field, changed ordering, or changed timestamp is a
parity failure.

## Decision gate ordering

For every candidate, the decision engine evaluates gates in this order:

```text
policy and transition eligibility
    -> thesis gate
    -> shared directional concurrency
    -> risk permission and capital availability
    -> ordered sizing (score/stability, drawdown scaling, directional headroom, and vehicle lot limits)
    -> final quantity limits and downward lot rounding
    -> acceptance
```

Sizing must not run before a candidate passes the earlier thesis and
concurrency gates. A rejected candidate reports the first failed gate; later
stage sizing diagnostics must not be presented as if the candidate was
eligible for sizing. The risk liquidity ceiling and the final vehicle lot cap
are separate checks and must not be collapsed.

Final quantity remains bounded by every applicable risk, margin, liquidity,
stability, vehicle-lot, and shared directional-headroom limit. Integer lot
normalization is downward-only. A positive quantity must never be created by
promoting a zero quantity with `max(1, ...)` or an equivalent fallback.

Acceptance leads directly into futures entry and exit simulation/settlement
within the same stateful lifecycle; the ordered decision stream must also
include the downstream typed trade and exit artifacts.

## Futures and synthetic vehicle semantics

Futures are the only decision and stateful execution vehicle:

```text
accepted futures decision
    -> immutable FuturesExecutionPlan
    -> optional SyntheticSettlement
```

Synthetic reporting is derived from accepted futures plans and premium lookup
data. Synthetic processing must not create an independent candidate,
acceptance, or rejection; run independent risk, sizing, or exit logic; consume
or update directional concurrency; or mutate futures portfolio or capital
state.

Synthetic settlement may produce `settled`, `missing_entry_premium`,
`missing_exit_premium`, or `invalid_contract` statuses. Missing synthetic
premiums are explicit settlement statuses and never reject, alter, or remove
the underlying futures decision.

## Capital and execution ownership

`ftx-paper` has one authoritative futures capital and position state.
`PortfolioState` (`ftx_paper.core.portfolio`) owns reservations, positions, equity, realized P&L, costs,
drawdown, and terminal settlement state. The runtime owns broker interaction, fill
reconciliation, persistence, and audit events. Transient broker metadata may
support reconciliation but is not a second position or P&L authority. `PortfolioState`
is the single equity/reservation owner; the decision engine reads
`equity` and `daily_baseline` from it and advances the baseline via
`start_day()` at date boundaries.

There is one mutation path for entry reservation, fill, cancellation, exit
settlement, equity update, and directional exposure release. A separate
ledger or runtime-side P&L calculation must not disagree with the authoritative
portfolio.

## State lifetimes

| State | Lifetime |
|---|---|
| Portfolio, drawdown, and futures positions | Evaluated run/session; positions end at simulated exit or session boundary |
| Shared directional exposure | Evaluated run/session; reset at the date boundary |
| Thesis failure state | Configured thesis-reset window |
| Cell cooldown state | Configured post-exit/session boundary |
| Causal bars, sequence, and feature trackers | Trading date, with recoverable replay/live snapshots |
| Synthetic settlement | Stateless with respect to futures portfolio and risk state |

Date transitions must not leak causal bars, supporting inputs, thesis state, or
cooldown state into the next date. Synthetic settlement receives no portfolio
or concurrency state.

## Typed artifact boundary

The parity path uses typed records internally for:

- `TradePlan` — the accepted or rejected causal decision record;
- `FuturesExecutionPlan` — the immutable futures execution handoff;
- `SyntheticSettlement` — the derived synthetic result or unavailable status;
- decision traces — typed projections of decision and gate outcomes.

Dictionary payloads are permitted only at API, persistence, and serialization
boundaries. Typed artifacts must contain canonical decision fields and reject
vestigial, oracle-only, or silently invented values.

## Legacy execution prohibition

`ftx-paper` is not live and must not retain obsolete execution behavior in the
name of backward compatibility. No legacy `on_bar` strategy path,
compatibility dispatch, fallback sizing, price-relative direction inference,
independent synthetic execution, or second equity/reservation owner may remain
in the parity path. The removed `execution/ledger.py` reconciler and the
removed engine-level equity-mirror helpers must not be reintroduced; all
capital mutation flows through `PortfolioState` via the execution coordinator.

Compatibility handling is limited to read-only migration of already-persisted
data when strictly necessary; it must not create an alternate decision or
execution architecture.

## Component mapping

`ftx-paper` remains independently runnable and must not import NiftyZoning
implementation modules. Its independently maintained components map to the
canonical contract as follows:

| Canonical responsibility | Independent paper responsibility | Required result |
|---|---|---|
| Completed-bar decision clock | `ftx_paper.core.bundles` and runtime feed | Same causal prefix, ordering, warmup, and missing-input semantics |
| Market features | `ftx_paper.core.features` and bundle feature path | Same VWAP, session levels, ATR, opening range, VIX, PCR, and prior-day values |
| Location/cell detection | `ftx_paper.core.location_engine` (independent `LocationDetector`) | Same simultaneous locations and composite cell names |
| Selling structure and setup score | `ftx_paper.core.scoring` | Same factor booleans, score, and setup tier |
| Session policy | `ftx_paper.strategy.config` and decision engine; policy gating via `ftx_paper.core.policy` | Same half-open windows, cell eligibility, fixed directions, and transition rules |
| Stops and futures sizing | `ftx_paper.core.risk` (stop/risk inputs), `ftx_paper.core.adaptive_stop`, and `ftx_paper.core.sizing` (ordered sizing pipeline) | Same stop distance, expiry/VIX adjustments, gate ordering, quantity, and rejection behavior |
| Futures risk gates | `ftx_paper.core.risk_state` (`RiskGateState`) over authoritative `PortfolioState` | Same drawdown, shared concurrency, thesis, cooldown, and reset behavior |
| Exits | `ftx_paper.core.exits` | Same per-cell exit mode, intrabar ordering, trail, hard stop, and EOD result |
| Futures execution | `ftx_paper.execution` (`PaperExecutionCoordinator`) and runtime, mutating only `PortfolioState` | One authoritative futures reservation, fill, exit, portfolio, and exposure lifecycle |
| Accepted decisions and order intent | `ftx_paper` core engine | Same ordered typed decision and order-intent events |
| Fill/order reconciliation | execution coordinator and runtime session | Same reconciliation semantics; fill prices and broker timing may differ |
| Broker mapping/auth/feed | `broker/zerodha` only | No alternate decision or execution path |
| Persistence, recovery, audit | `runtime` only | Storage metadata may differ; normalized decision fields and ordering match |
| HTTP/UI | `api` and `ui` | No business logic or direct DB access |
| Synthetic settlement | `ftx_paper.core.settlement` at the settlement boundary | Derived only from immutable futures plans and premium lookups |
| Costs | `ftx_paper.core.cost` | Single cost source; no local redefinition |
| Audit output | `ftx_paper` decision/order events | Same normalized typed decision fields and ordered lifecycle; storage metadata may differ |

This mapping is a responsibility map, not permission to share code. The
implementations remain duplicated and isolated by design. A strategy/config
version and configuration hash must be recorded so that a paper run can be
traced to the canonical policy it was aligned against.

## Acceptance gate

P0 is complete when this contract is approved as the working specification.
The implementation cutover gate is stricter:

1. Run both implementations against the same deterministic fixture set.
2. Run both implementations against the same holdout replay.
3. Compare the complete ordered decision stream field-by-field, including gate
   ordering and intermediate sizing fields.
4. Compare futures exits, reservations, fills, costs, and ledgers separately
   from synthetic settlement.
5. Confirm synthetic processing does not change futures decisions or state.
6. Require zero unexplained differences.
7. Record any intentional exception with the exact field, reason, expected
   value on each side, and explicit approval before cutover.

No production switch or removal of the canonical implementation is allowed
until the acceptance gate passes.
