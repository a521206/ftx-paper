# Canonical FTX Decision Parity Contract

Status: P0 approved working specification. The current `ftx-paper`
implementation does not yet satisfy this contract; the open implementation
work is tracked as P5, P8, P9, P10, and P11 in `TASKS.md`.

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

Phase 2 is required because trade-level agreement alone does not prove causal
parity. Two implementations can produce similar completed trades while
disagreeing about candidates, transition eligibility, rejection reasons,
sizing gates, event ordering, or state mutations. Phase 2 is the final
decision-parity gate before runtime cutover.

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
- Session policies: `src/ftx/frozen_research_config.py`

The contract covers candidate detection, eligibility, rejection, acceptance,
order intent, stop, quantity, and exit decisions. Broker acknowledgements,
fill prices, network timing, and persistence timestamps are outside decision
parity and are covered by the ledger/runtime tracks.

### Candidate-policy boundary

Candidate-policy parity is configuration-dependent. The standard FTX Paper
parity configuration uses a no-op candidate policy. Research-only policies,
including `LogisticPolicy`, are supported by the canonical research pipeline
through the optional candidate-policy boundary. They are not required for
Paper parity unless explicitly promoted into the Paper strategy configuration.
See [`docs/FTX_EXPERIMENT_INDEX.md`](../docs/FTX_EXPERIMENT_INDEX.md) for the
workflow, artifact, and paired-run contract.

When a candidate policy is enabled in a compared configuration, it must be
identified by a reproducible policy artifact. Given the same event-time
feature vector and policy artifact, both implementations must produce the same
eligibility result and rejection reason. The policy branch must not own
direction, setup scoring, risk, sizing, exits, costs, or portfolio state.

Candidate-policy artifacts must record, at minimum:

- policy ID and artifact hash;
- feature names and feature version;
- training and evaluation date ranges;
- thresholds and model parameters;
- feature-coverage and missing-value behavior.

Compared candidate-policy events must include the policy ID, eligibility
outcome, rejection reason, and any policy outputs required by that policy
(for example probability and logit for `LogisticPolicy`). Policy metadata may
be stored once in the run manifest rather than repeated in every event.

The no-op baseline and the enabled-policy branch are compared as separate
canonical research configurations. A research-only policy becomes part of
Paper parity only after an explicit configuration change and approval.

## Input contract

Each decision minute is evaluated from exactly one completed futures bar and a
causal prefix containing only bars at or before that minute. Supporting inputs
are:

| Input | Contract |
|---|---|
| Futures | Completed OHLCV bar, ordered by canonical IST minute; current bar is the entry bar. |
| VIX | Opening value from the first available 09:15 bar; event value is the latest completed value at or before the decision minute. If either required VIX value is unavailable, the candidate follows the explicit `missing_vix` rejection path described below. |
| Option PCR | Value associated with the decision minute, or `None` when unavailable according to the canonical lookup cutoff. |
| Prior-day levels | Previous trading day high/low, reset at each trading-date boundary; `None` when unavailable. |
| Opening range | Causal range levels once the opening-range window is complete. |
| Expiry calendar | Canonical weekly-expiry membership used for stop adjustment. |
| Configuration | Strategy version, session windows, cells, directions, exit modes, stability, cooldown, sizing, risk settings, and candidate-policy mode/artifact when enabled. |

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
- When VIX is unavailable, `vix_open`, `vix_at_event`, and all VIX-dependent
  derived fields are serialized as `null`/unavailable. The candidate is
  rejected with `missing_vix`; only candidate identity, timing, availability,
  event ordering, and rejection reason are compared for that candidate. Any
  internal numeric fallback used to avoid calculation errors is diagnostic-only,
  is not a market input, and is excluded from parity comparison.
- Numeric values are serialized using the same field names and normalized
  scalar types. Parity comparison uses exact equality after normalization;
  no tolerance is permitted for score, stop, quantity, or decision fields.
- Raw implementation-specific decision IDs are retained for diagnostics but
  are not compared directly. Phase 2 compares this normalized identity:

  ```text
  candidate identity = (session_date, decision_minute, sequence, cell)
  event identity = (candidate identity, event_kind, rejection_reason)
  ```

  Candidate and accepted events share the same candidate identity. Rejected
  events add their exact rejection reason. Orders, fills, exits, and
  settlements link back to the candidate/decision identity. The underlying
  date, minute, sequence, cell, event kind, and rejection reason must compare
  exactly; only the raw ID encoding may differ.

## Decision output contract

For every candidate minute and cell, the comparator must be able to compare:

- the complete `score_factors` mapping from the decision bar, with identical
  keys and identical boolean/numeric values; unavailable or inapplicable
  factors must be represented explicitly as `null` on both sides;

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
candidate-policy ID and eligibility outcome, when enabled
candidate-policy rejection reason and outputs, when enabled
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

The maintained pipeline evaluates each candidate in this order:

```text
policy and transition eligibility
    -> thesis / occupancy gate
    -> risk permission and capital availability
    -> ordered quantity stages (score multiplier, directional headroom,
       vehicle limit, drawdown scale, policy stability)
    -> scoped date/session/cell/direction risk ceiling
    -> final quantity limits and downward lot rounding
    -> acceptance
```

The canonical capital path currently passes no score into the sizing
multiplier, so its effective score multiplier is 1.0. Score remains a setup
qualification and audit value. Paper must report the risk and sizing stages
only after a candidate passes its policy and thesis/occupancy gates. The
directional headroom check belongs inside the sizing waterfall; the risk
liquidity ceiling and vehicle lot cap remain separate limits.

### Current core behavior matrix

| Area | Active Pipeline behavior | Paper status | Research-only or unresolved |
|---|---|---|---|
| Candidate score | Signed close-location volume proxy from OHLCV; score qualifies and audits the setup | Aligned; Paper does not treat bar-derived proxy as reported exchange order flow | Hypothesis veto scoring remains research-only |
| Gate order | Policy/transition, thesis and occupancy, risk permission, sizing waterfall, scoped risk, acceptance | Aligned in the decision path | Sweep-specific gate combinations remain research-only |
| Sizing | Score multiplier is 1.0; directional headroom, vehicle, drawdown, stability, then scoped risk | Paper research profile now mirrors Pipeline research: 3% risk, 8 max lots, 12 directional lots, and no drawdown scaling | Verify identical intermediate sizing decisions on the same candidate stream |
| Research cells | Morning 09:15–11:45: five active cells; Afternoon 13:30–14:15: two active cells | Paper-owned manifest now mirrors Pipeline cell, direction, exit mode, hypothesis, and stability assignments | Verify feature-to-cell identity and ordered accept/reject parity |
| Replay state | State is continuous within each session and resets at the date boundary; each date is seeded with the preceding futures high/low | Aligned, including prior-day context and legacy snapshot migration | — |
| Cooldown and occupancy | 15-bar per-cell entry spacing; 30 bars after any exit; `block_open_positions=false` | Aligned | Open-position blocking and alternative cooldown scopes are sweep variants, not active policy |

These settings describe the processing contract and the active research run
profile. Paper keeps its own profile type and implementation, but its default
research values are intentionally like-for-like with Pipeline for quantity
parity.

The scoped risk allowance is
`initial_capital × max_daily_loss × cell_session_risk_buffer_fraction`.
Each accepted position reserves its stop/stress loss plus round-trip futures
costs from a `(date, session, cell, direction)` bucket. Open reservations reduce
available risk; settlement adjusts the bucket by net P&L, capped at its
configured allowance.

The active replay configuration uses a 15-bar per-cell entry cooldown and a
30-bar post-exit cooldown after any exit. Thesis state persists continuously
through each session and resets at the trading-date boundary. The active
canonical config sets `block_open_positions=false`, so a same-cell open
position alone does not reject another entry; the entry and post-exit cooldowns
still apply. Occupancy sweeps and alternative candidate-policy settings are
research-only unless promoted into the canonical configuration.

Final quantity remains bounded by every applicable risk, margin, liquidity,
stability, vehicle-lot, and shared directional-headroom limit. Integer lot
normalization is downward-only. A positive quantity must never be created by
promoting a zero quantity with `max(1, ...)` or an equivalent fallback.

Acceptance leads directly into futures entry and exit simulation/settlement
within the same stateful lifecycle; the ordered decision stream must also
include the downstream typed trade and exit artifacts.

## Exit capability and active configuration

`ftx-paper` must support the complete canonical exit-mode vocabulary, even
when a particular live or holdout run uses only a subset of the modes. The
supported modes are:

- `signal`
- `target`
- `risk_reward`
- `trail`
- `adaptive`
- `hard_stop`
- `eod`

Parity is evaluated against the mode and parameters selected by the canonical
strategy configuration for the run. The active configuration is authoritative
for each cell and must include the exit mode, stop/target/trail parameters,
counter-move settings, and session-close behavior. Supporting a mode does not
activate it for a run unless the canonical configuration activates it.

The behavior of every supported mode must remain aligned with the canonical
pipeline, including trigger precedence when multiple conditions occur in one
bar, counter-move and volatility-climax rules, trail activation and
breakeven-lock behavior, gap versus level fills, trigger versus fill
timestamps, and the `bars_held` convention. The canonical references are
`src/ftx/exit_sim.py`, `src/ftx/frozen_research_config.py`, and the canonical
trail implementation. A newly approved mode that is already in this
vocabulary should require a configuration change and replay validation, not a
Paper implementation rewrite. A genuinely new algorithm requires an explicit
contract and capability update before it can enter the parity path.

### Replay causality and thesis updates

The canonical batch engine currently resolves an accepted candidate's complete
future exit path before it evaluates the next candidate. It therefore applies a
future hard-stop result to the thesis gate before later decision minutes (for
example, the 2026-09-15 13:45 candidate can affect the 13:51 decision). That
ordering is a canonical replay artifact, not a causal live-trading rule.

Paper replay preserves causal semantics: it evaluates exits only on completed
bars observed so far, settles them before the next decision bundle, and never
uses a future exit result to gate an earlier decision. Paper must not emulate
the canonical batch lookahead merely to make a later decision stream match.
The canonical replay flow requires correction or an explicit causal replay
variant before those downstream acceptance differences can be considered exit
parity failures. Until then, comparator reports must classify them as
canonical-replay ordering differences, while exit trigger, timestamp, held-bar,
reason, fill, and settlement comparisons remain required and causal.

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

Capital parity is compared semantically, not by internal class shape. For each
decision and mutation, normalize and compare:

```text
initial_capital
effective_equity / peak_equity
daily_baseline / daily_pnl
realized_pnl / total_costs
open_margin_used / available_capital
drawdown_amount / drawdown_pct
net_directional_lots
active_positions / reservations
```

The canonical values come from `src.capital.ledger.PortfolioState`; Paper
values come from `ftx_paper.core.portfolio.PortfolioState` plus its
`RiskGateState` for directional exposure. The comparison must document the
available-capital floor, drawdown denominator, cost timing, and terminal
closeout policy. Compare entry reservation, fill, cancellation, exit
settlement, exposure release, daily reset, and terminal closeout in order.

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
| Exits | `ftx_paper.core.exits` | Full canonical exit-mode vocabulary; active per-cell mode and parameters must match the run configuration, including intrabar ordering, trail, hard stop, and EOD result |
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
