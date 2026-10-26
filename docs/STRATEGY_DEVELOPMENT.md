# Strategy Development

This guide describes the minimum contract for adding a strategy to
`ftx-paper`. Strategy behavior belongs in `ftx_paper.strategy`; runtime,
execution, persistence, and broker concerns stay outside the strategy.
The extraction plan for making these capabilities reusable is in
[Strategy Reuse Refactor](STRATEGY_REFACTORING.md).

## Design Contract

A strategy must:

- implement the `Strategy` protocol in `strategy/protocol.py`;
- expose stable `StrategyMetadata` (`name`, `version`, and `config_hash`);
- consume completed, normalized `DecisionBundle` values;
- return deterministic `DecisionEvent` values from `on_bundle` and `evaluate`;
- handle fills and cancellations in `on_execution_event` when they affect state;
- return only serializable state from `snapshot`.

The strategy may keep deterministic in-memory state. It must not read the clock,
access SQLite, call a broker, authorize orders, or mutate the shared portfolio.
The runtime owns timing, account context, risk authorization, execution, and
persistence.

## Reusable Capabilities

New strategies should reuse these existing capabilities instead of reimplementing
them:

| Capability | How a new strategy uses it |
|---|---|
| Normalized market bundles | Read completed `DecisionBundle` values in `on_bundle` or `evaluate`; do not read broker data directly. |
| Structural locations and `Cell` values | Use the market location services to express entry conditions as composable policy inputs. |
| Session windows and square-off | Define entry windows in strategy configuration; let the runtime enforce lifecycle and end-of-day boundaries. |
| Admission filters | Compose causal PCR, VIX, volume, momentum, and missing-input checks through strategy admission policy. |
| Exit state machines | Select `SIGNAL` or `TRAIL` behavior and feed execution/market events into the existing exit logic. |
| Adaptive stops | Use the ATR/VIX stop calculator and apply its bounded result rather than creating an independent stop convention. |
| Risk and sizing | Pass the decision context, capital profile, vehicle limits, score, and stop distance through the shared sizing and authorization path. |
| Futures and synthetic vehicles | Declare enabled vehicles and vehicle risk limits; preserve the existing reporting-only restrictions for synthetic positions. |
| Cooldowns and drawdown guards | Configure cooldowns and rely on shared daily-loss, peak-drawdown, margin, and exposure guardrails. |
| Metadata and snapshots | Return stable metadata and serializable state; version any custom snapshot fields. |
| Replay and parity artifacts | Run the strategy through `ReplayWorker`; use input manifests, causal traces, event streams, and P9/P10 artifacts for comparison. |
| Multi-strategy attribution | Supply a stable `strategy_id`; the runtime handles shared portfolio execution, order attribution, and API/UI filtering. |

These capabilities are shared services and contracts. A new strategy should add
only its policy and decision differences: what market conditions qualify, which
direction and vehicle to use, how the trade is hypothesized to behave, and which
existing exit/risk modes apply. If a new rule needs a new reusable capability,
add it to the appropriate market, strategy-policy, or runtime layer rather than
embedding infrastructure code in the strategy.

## Implementation Steps

1. Define the strategy's configuration, metadata, decision state, entry rules,
   exits, sizing inputs, and rejection behavior.
2. Add the implementation under `src/ftx_paper/strategy/`. Reuse shared market,
   contract, risk, and policy types instead of adding infrastructure there.
3. Implement a `StrategyFactory` or constructor compatible with
   `ConfiguredStrategyFactory`.
4. Select the factory at the application composition boundary. For multiple
   strategies, wrap instances in `StrategyInstance` and `MultiStrategyEngine`.
5. Keep each instance identified by a stable `strategy_id`. Instances share the
   one runtime portfolio and capital context; do not create a second runtime or
   account.
6. Version and validate the snapshot schema if the strategy has state that must
   be persisted. Snapshots must not restore a separate runtime owner.

## Required Tests

Add focused tests for:

- causal decisions and boundary conditions;
- metadata and configuration identity;
- entries, exits, sizing, and rejection behavior;
- execution notification handling;
- snapshot round trips and invalid snapshot input;
- shared-runtime execution and strategy attribution when used with
  `MultiStrategyEngine`.

Run a deterministic replay and compare decisions, fills, and audit events before
enabling the strategy in a runtime session. For Python changes, run the relevant
tests, Pyright on changed files, and Ruff.

## Replay And Comparison

Use the existing replay path rather than a strategy-specific simulator:

1. Inspect `/api/v1/replay/inputs?date=YYYY-MM-DD` and record the input
   fingerprint, provenance, availability, selected contract, and bar coverage.
2. Run the same session date, sessions, vehicles, capital profile, and strategy
   configuration for each candidate being compared.
3. Retrieve the completed run from `/api/v1/replay/{run_id}`. Use
   `/trace` for causal feature and location transitions and `/events` for the
   full event stream.
4. Compare the generated parity artifacts, not timestamps or run IDs. The
   artifact separates `p9.decision_records` from `p10.execution` and exits,
   and includes strategy metadata, configuration hash, input identity, and
   coverage.

The comparison must report differences in:

- decision identity, timing, policy, direction, and rejection reason;
- order authorization, suppression, fills, exits, and realized ledger values;
- input availability, bar coverage, strategy version, and configuration hash.

Treat missing inputs, changed contracts, changed capital settings, or changed
configuration hashes as a comparison failure or a separately labelled run, not
as an unexplained strategy improvement. A new strategy may differ in decisions,
but the replay must remain causal, deterministic, and attributable.

## Boundaries

Do not add per-strategy databases, broker sessions, live order routing, external
position reconciliation, or strategy-specific runtime processes. Shared
portfolio and execution behavior remains governed by `RuntimeSession`.
