# Strategy Development

This guide describes the minimum contract for adding a strategy to
`ftx-paper`. Strategy behavior belongs in `ftx_paper.strategies`; runtime,
execution, persistence, and broker concerns stay outside the strategy.
Shared ownership and isolation rules are defined in
[Strategy boundaries](STRATEGY_BOUNDARIES.md). This guide covers the current
extension contract; it is not a roadmap for a future refactor.

Strategies that need static daily or pre-market inputs should use the generic
[daily setup materialization design](DAILY_SETUP_MATERIALIZATION.md) rather
than calculating those inputs in replay or owning a worker themselves.

## Design Contract

A strategy must:

- implement the shared strategy protocol in `strategies/shared/protocol.py`;
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
| Structural locations and `Cell` values | Use an explicitly shared location contract when one exists; research location detection remains research-specific. |
| Session windows and square-off | Define entry windows in strategy configuration; the runtime owns lifecycle and end-of-day execution boundaries. |
| Exit state machines | Reuse the shared exit contract where the strategy selects an existing exit mode. |
| Risk and sizing | Use the runtime account and authorization contracts. Research-specific admission, stop, risk, and sizing code is not a shared strategy API. |
| Futures and synthetic vehicles | Declare enabled vehicles and vehicle risk limits; preserve the existing reporting-only restrictions for synthetic positions. |
| Cooldowns and drawdown guards | Configure cooldowns and rely on shared daily-loss, peak-drawdown, margin, and exposure guardrails. |
| Metadata and snapshots | Return stable metadata and serializable state; version any custom snapshot fields. |
| Replay and parity artifacts | Run the strategy through `ReplayWorker`; compare the persisted `run_scope`, `decision`, `execution_settlement`, and `portfolio_ledger` layers. |
| Multi-strategy attribution | Supply a stable `strategy_id`; the runtime handles shared portfolio execution, order attribution, and API/UI filtering. |

The listed runtime capabilities are shared services and contracts. A new strategy
should add only its policy and decision differences: what market conditions qualify, which
direction and vehicle to use, how the trade is hypothesized to behave, and which
existing exit/risk modes apply. If a new rule needs a new reusable capability,
add it to the appropriate market, strategy-policy, or runtime layer rather than
embedding infrastructure code in the strategy.

## Implementation Steps

1. Define the strategy's configuration, metadata, decision state, entry rules,
   exits, sizing inputs, and rejection behavior.
2. Add the implementation under `src/ftx_paper/strategies/<name>/`. Reuse shared market,
   contract, and policy types instead of adding infrastructure there.
3. Implement the shared typed `StrategyFactory` port for the strategy.
4. Register the factory at the application composition boundary. For multiple
   strategies, wrap instances in `StrategyInstance` and `MultiStrategyEngine`.
5. Keep each instance identified by a stable `strategy_id`. Instances share the
   one runtime portfolio and capital context; do not create a second runtime or
   account.
6. Version and validate the snapshot schema if the strategy has state that must
   be persisted. Snapshots must not restore a separate runtime owner.

## Minimal Bring-Up

Use this path to prove that a new strategy is correctly wired before adding
strategy behavior. A minimal strategy can intentionally return no decisions.

1. Add the strategy package under `src/ftx_paper/strategies/<name>/` with:
   - a strategy class implementing the shared protocol;
   - stable metadata and a serializable `snapshot`;
   - a factory implementing `StrategyFactory`.
2. Register the factory and its stable `strategy_id` in
   `src/ftx_paper/application/strategy_registry.py`.
3. Add a focused test that constructs the strategy, checks metadata and the
   snapshot, and confirms that a minimal completed bundle produces a tuple of
   decisions. For a no-op scaffold, that tuple should be empty.
4. Start the API from the repository root. The API owns the replay worker and
   must be the only process that owns the runtime store:

   ```powershell
   .venv\Scripts\python.exe -m ftx_paper.cli
   ```

5. Select a date that exists in the runtime market-bar store and run the
   strategy through the API-owned replay path:

   ```powershell
   .venv\Scripts\python.exe -m ftx_paper.replay_cli 2026-10-29 `
     --base-url http://127.0.0.1:8501 `
     --strategy-id ftx-paper-<name> `
     --vehicles futures `
     --include-parity-layers `
     --json
   ```

   The request must use `mode: "replay"`, include `futures`, and use a date
    with stored market data. The CLI submits
    `POST /api/v1/strategies/{strategy_id}/replays`, waits for the terminal run,
    and fetches `GET /api/v1/strategies/{strategy_id}/replays/{run_id}`.
6. Treat `status: "completed"` as the wiring check. For a no-op scaffold,
   `bars_seen` should be positive while `decisions_emitted`, `orders_filled`,
   and trades remain zero. If parity is enabled, also verify the persisted
   manifest and its four layers: `run_scope`, `decision`,
   `execution_settlement`, and `portfolio_ledger`.
7. Before enabling non-empty behavior, run the focused tests, Pyright on the
   changed Python files, and Ruff. Do not use a strategy-specific simulator or
   call the broker to validate the scaffold.

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

1. Inspect the stored market-data availability for `date=YYYY-MM-DD` and record
   the input fingerprint, provenance, selected contract, and bar coverage.
2. Run the same session date, sessions, vehicles, capital profile, and strategy
   configuration for each candidate being compared.
3. Retrieve the completed run from
   `/api/v1/strategies/{strategy_id}/replays/{run_id}`. Use the persisted
   parity manifest and bounded layer endpoints for comparison data; producer
   replay facts remain internal and are not API reporting surfaces.
4. Compare the generated parity artifacts, not timestamps or run IDs. The
   artifact contains the four contract layers: `run_scope`, `decision`,
   `execution_settlement`, and `portfolio_ledger`, with normalized scope,
   input identity, and coverage.

The comparison must report differences in:

- decision identity, timing, policy, direction, and rejection reason;
- order authorization, suppression, fills, exits, and realized ledger values;
- input availability, bar coverage, strategy version, and configuration hash.

Treat missing inputs, changed contracts, changed capital settings, or changed
configuration hashes as a comparison failure or a separately labelled run, not
as an unexplained strategy improvement. A new strategy may differ in decisions,
but the replay must remain causal, deterministic, and attributable.

For isolation and ownership rules, see [Strategy boundaries](STRATEGY_BOUNDARIES.md).
