# Architecture

This document describes the system's design contracts for maintainers and
extension authors. It is not an installation, operations, or strategy-parameter
runbook; see [Deployment](DEPLOYMENT.md) and [Research strategy rules](strategies/research/STRATEGY_RULES.md)
for those concerns.

`ftx-paper` is a paper-trading modular monolith. The API process owns one
in-process `RuntimeSession`; the UI is a separate presentation process. The
system may use a real broker for market data, but orders always go through
`PaperBroker` and the paper execution coordinator.

## Boundaries

```text
domain / contracts / strategy / market / execution -> no infrastructure code
application -> domain / contracts + ports
runtime -> domain + broker + execution + runtime store
api -> runtime + runtime store
infrastructure.sqlite -> ports + domain / contracts
```

SQLite connections, SQL, database paths, and row mapping belong in
`ftx_paper.infrastructure.sqlite`. API and CLI code use the typed
`ftx_paper.runtime.store` facade rather than selecting SQLite adapters.
Composition and adapter selection belong in `application.composition` and
runtime wiring.

Strategy policy lives in `ftx_paper.strategies`. Market normalization and
decision-clock processing live in `ftx_paper.market`.

## Single Source Of Truth

`PortfolioState` owns positions, reservations, capital, and settlement. The
runtime creates one portfolio and one immutable capital context, then shares
them with strategy and execution. Facades and projections must delegate to
these objects; they must not create competing state.

`AccountAggregate` is a view over the same portfolio for authorization,
reservations, revisions, and strategy decision snapshots. `ExecutionService`
and `PaperExecutionCoordinator` apply order, fill, cancellation, and
settlement changes to that portfolio.

## Critical Processing

An order-producing decision follows this sequence:

```text
market input -> normalized input -> account context -> strategy evaluation
-> authorization and reservation -> PaperBroker execution
-> portfolio fill/settlement -> persistence -> critical strategy notification
-> optional observers
```

Authorization is rechecked immediately before dispatch. Critical failures
must reach runtime health and stop the affected path. Optional observers such
as diagnostics and UI projections may be isolated, but must not mutate account
state or issue orders.

SQLite writes follow in-memory mutations; they are not a rollback for engine,
portfolio, broker, or execution changes. Do not blindly resubmit after a
persistence or notification failure because a simulated fill may already have
occurred.

## Order Lifecycle

Orders use an explicit transition graph, including rejection, cancellation,
partial fills, and uncertainty. Terminal states are `FILLED`, `CANCELLED`,
and `REJECTED`; `UNKNOWN` is non-terminal and must not trigger resubmission or
reservation release without evidence.

Track cumulative filled and remaining quantities independently of status.
Reject overfills, duplicate fills, stale transitions, and conflicting retries.
Stable client order IDs make command retries idempotent; fill IDs make fill
delivery idempotent. A cancellation after a partial fill cancels only the
remaining quantity.

`RECONCILIATION_REQUIRED` is an audit/startup-gating value only. It does not
enable external reconciliation or automatic resubmission. Unsupported paper
adapter capabilities must remain explicit `NotImplementedError` operations,
not false success.

## Runtime And Recovery

The process lease prevents two API processes from owning one runtime directory.
The store serializes status patches within one process; it is not a
cross-process compare-and-set mechanism.

On interruption, startup marks the session `STOPPED` and records a
`RUNTIME_RECOVERY` audit event. It does not reconstruct the in-memory engine or
portfolio from SQLite. Paper positions are internal simulated state and are
not reconciled with broker positions; recovery is an audit and startup-gating
operation only.

The in-process facades are `FeedManager`, `OrderManager`, `StrategyRunner`,
and `PnlManager`. They delegate to the existing runtime owner and aggregate;
`PnlManager` is read-only. The UI remains separate and facades must not create
another runtime owner.

## Strategy Extension

### Multiple Strategy Instances

`MultiStrategyEngine` is the runtime-owned coordinator for running more than
one configured strategy in a single `RuntimeSession`. Each `StrategyInstance`
has an independent strategy object, stable `strategy_id`, and enabled flag. The
coordinator dispatches each completed bundle to enabled instances. All
instances share the one runtime portfolio and capital context. If any strategy
fails while processing a bundle, the runtime treats the bundle as failed and
stops the runtime rather than continuing with a partial multi-strategy result.
Client order IDs are prefixed with the strategy ID to keep concurrent
instances idempotent.

The portfolio and capital context remain owned by `RuntimeSession` and are
bound to every instance. Strategies do not own competing positions; execution
still authorizes and settles against the one runtime portfolio.
Execution notifications are routed by `strategy_id`, while unattributed
notifications are delivered to all instances for compatibility.

Strategies are evaluated in configured order. The runtime collects their
decisions, then authorizes orders sequentially against shared capital and
reservations, rejecting orders that exceed current limits. An order that would
offset another strategy's open exposure in the same instrument is rejected;
each strategy may close only its own attributed quantity. If any strategy
fails, the runtime stops without rollback or automatic retry. Mutations and
paper fills already applied are preserved. Enabled strategies are configured
at startup, and interrupted sessions remain `STOPPED` without snapshot-based
resumption.

Strategy snapshots are keyed by `strategy_id` and contain serializable strategy
state only; they never restore a second portfolio or runtime owner. Runtime and
activity API responses expose strategy attribution and optional filtering, while
positions, capital, and health remain shared. The dashboard provides the same
simple strategy filter and labels shared account values accordingly.

Use the coordinator at the composition boundary:

```python
engine = MultiStrategyEngine({
    "opening-range": StrategyInstance("opening-range", opening_range),
    "mean-reversion": StrategyInstance("mean-reversion", mean_reversion),
})
session = RuntimeSession(store, auth, specifications, engine=engine)
```

The single `PaperEngine` remains supported for existing deployments and tests.

Strategies live in `ftx_paper.strategies` and are selected at the composition
boundary. The `Strategy` protocol is the stable extension point:

```text
normalized market bars -> DecisionBundle -> Strategy -> LiveDecision values
                                      execution events -> Strategy
```

A strategy may keep deterministic in-memory state, but it must not read the
clock, access SQLite, call a broker, or mutate the portfolio. The runtime owns
market timing, account context, authorization, reservations, execution, and
persistence. Implement both decision methods: `on_bundle` for bundle-only
evaluation and `evaluate` when the account `DecisionContext` is required.
Implement `on_execution_event` when fills or cancellations affect strategy
state. `snapshot` should return serializable state for diagnostics and replay.

Every implementation must expose `StrategyMetadata` with a stable name,
version, and configuration hash. This metadata is recorded with decisions so
that a result can be attributed to the exact strategy configuration that
produced it.

To add a strategy:

1. Add the implementation under `ftx_paper.strategies` and depend only on
   contracts, domain decision context, market bundles, and other strategy
   policy modules.
2. Define its configuration and deterministic state explicitly. Do not put
   persistence or broker calls in the strategy.
3. Implement `StrategyFactory` (or pass a constructor to
   `ConfiguredStrategyFactory`) and select it in the application composition
   root. Do not create a second runtime or portfolio.
4. Add unit and integration tests for decisions, metadata, snapshots, sizing,
   exits, execution notifications, and rejection/authorization behavior.
5. Run the same replay inputs against the new strategy and compare decisions,
   fills, and audit events before enabling it for a runtime session.

### Strategy-Neutral Host Boundary

The strategy extension point is intentionally narrower than the runtime host.
Adding or removing a strategy must not require changes to the common market
clock, bundle contract, replay worker, account owner, or generic API routes.
The shared processing path is:

```text
broker/input adapter -> normalized MarketBar -> market decision clock
    -> strategy-neutral DecisionBundle -> Strategy
    -> DecisionEvent / OrderIntent -> runtime authorization and reservation
    -> PaperExecutionCoordinator / PaperBroker -> PortfolioState
    -> runtime events, persistence, and API projections
```

Ownership is divided as follows:

- `contracts` and `domain` own stable market, decision, order, execution,
  portfolio, capital, reservation, and settlement contracts. They must not
  import a concrete strategy, runtime, API, broker adapter, or infrastructure.
- `market` owns broker-neutral normalization, role mapping, completed-bar
  aggregation, decision-clock ordering, deadlines, incomplete-input diagnostics,
  and bundle timing only. It does not compute strategy features or own a feature
  cache. `DecisionBundle` carries bars and generic input diagnostics, but must
  not contain strategy context, observations, callbacks, or feature artifacts.
- `strategies` own configuration, metadata, deterministic policy state,
  strategy-specific context and evidence, evaluation, snapshots, and execution
  notification handling. Strategies must not read SQLite, call brokers, own the
  clock, authorize orders, or mutate the portfolio.
- `runtime` owns feed lifecycle, bar ingestion, bundle dispatch, account
  context, shared portfolio state, authorization, execution, settlement,
  persistence, recovery, and health. It must not branch on strategy names or
  instantiate strategy-specific producers, context stores, or report builders.
- `replay` is a bounded host operation that drives the same bundle and strategy
  transition as live processing. It owns input scope, deterministic clock
  control, isolated replay state, generic capture, and comparison only.
- `api` exposes generic attributed projections for decisions, orders, trades,
  diagnostics, replay runs, and shared positions/capital. Strategy-specific
  views are optional projections and must not be required for execution.
- The composition root is the only place that maps configured strategy names to
  constructors. Market construction uses explicit role mappings and does not
  consume strategy requirements.

Shared contracts must remain strategy-neutral. Strategies receive the completed
bundle and own feature calculation, historical warmup, feature caches, and
feature availability semantics. The runtime may provide immutable market bars
and bounded input history through neutral ports, but it must not calculate or
cache strategy features. Replay artifacts use generic source/run,
decision-lifecycle, execution/settlement, and portfolio ledger layers, with
strategy ID and metadata used only for attribution.

Live and replay may differ in input source, clock control, persistence, and
capture mode, but they must call the same strategy transition. A fixed input
prefix processed incrementally and through bounded replay must produce the same
decision prefix. Strategy-specific ATR, context materialization, source
preparation, and reporting belong to the strategy or an explicit strategy
adapter, never to generic market or runtime helpers.

The architectural removal test is mandatory: deleting a strategy package and its
composition registration must leave generic market aggregation, runtime
lifecycle, replay, persistence, API projections, and other strategies importable
and behaviorally unchanged.

### Strategy Registration And Composition

Strategy registration is explicit, centralized, and composition-owned. A
strategy package exposes its own factory and neutral requirements; it does not
register itself through import side effects and it does not know about sibling
strategies.

The application composition root builds a catalog from definitions:

```text
StrategyDefinition(strategy_id, factory)
    -> StrategyCatalog
    -> live MultiStrategyEngine
    -> replay StrategySelector
    -> union of market requirements
```

The catalog is the only component that maps configured strategy IDs to concrete
factories. It provides strategy creation and enabled-strategy metadata. The
same catalog or selector is injected into live and replay construction.

`api.create_app()` must receive already-composed runtime and replay dependencies;
it must not instantiate a registry or import concrete strategy factories.
`RuntimeSession`, `ReplayWorker`, and `market` must not contain strategy-name
branches. Configuration controls which definitions are included, while the
catalog remains an ordinary dependency that can be replaced in tests.

Adding a strategy requires only:

1. Implement its strategy and factory under `ftx_paper.strategies.<name>`.
2. Add one definition in the application composition root.
3. Add strategy tests and host-boundary tests.

Removing a strategy removes its definition and package without changing generic
market, runtime, replay, persistence, API, or other strategy code. The catalog
must not expose mutable `PortfolioState` or broker objects to strategies. Runtime
account ownership remains behind read-only decision context and execution-event
notifications.

### Boundary Migration Plan

Implement the redesign in ordered phases. Each phase must pass its exit gate
before the next phase changes behavior.

#### Phase 1: Freeze generic contracts (implemented)

- Remove feature snapshots, feature services, and feature providers from
  `DecisionBundle` and the market aggregator.
- Move feature requirements, calculation, cache, and warmup protocols into each
  strategy package.
- Preserve generic bar normalization, bundle timing, and incomplete-input
  handling. Feature calculation is strategy-owned.
- Restore generic replay summary fields and result validation.

Exit gate: market and generic replay tests pass without importing a concrete
strategy package.

#### Phase 2: Establish the composition catalog (implemented)

- Define `StrategyDefinition`, `StrategyCatalog`, and `StrategyFactory` in the
  application composition layer.
- Make research and orderflow factories package-local and sibling-independent.
- Register enabled definitions only in application composition.
- Inject the same catalog/selector into live and replay construction.

Exit gate: API, runtime, and replay construct no concrete strategy factory and a
test strategy can be added or removed by changing composition only.

#### Phase 3: Remove host policy ownership (implemented)

- Move all feature calculation, ATR/context warmup, source preparation, and
  policy state behind strategy-owned adapters.
- Remove strategy-name branches and strategy-specific imports from `market` and
  `runtime`.
- Make strategy construction inputs read-only snapshots or neutral value ports.
- Keep `PortfolioState`, authorization, reservations, and execution owned by
  runtime.

Exit gate: deleting a strategy package and its composition definition leaves
  generic live and replay behavior unchanged.

#### Phase 4: Make replay and API generic (implemented)

- Make replay artifacts and summaries strategy-neutral with attribution only.
- Move optional strategy reports behind an injected report adapter.
- Remove strategy-specific report types and endpoints from generic runtime/API
  code.
- Ensure API composition receives dependencies rather than creating a catalog.

Exit gate: replay and API tests pass with a minimal test strategy and no
strategy-specific imports in generic workers or routes.

#### Phase 5: Verify, simplify, and remove legacy traces (implemented for backend)

- Run architecture-boundary checks, focused tests, Ruff, and Pyright.
- Review all changed code for duplicate factories, compatibility shims, stale
  aliases, dead exports, strategy-name branches, and obsolete report fields.
- Delete legacy orderflow coupling rather than preserving fallback paths.
- Repeat review and simplification until the forbidden-coupling search is empty.

Final gate: adding or removing a strategy changes only its package, composition
definitions, and strategy/host tests; no legacy strategy traces remain in common
market, runtime, replay, API, or contracts code.

### Current Implementation Status

The boundary migration is implemented in the current runtime:

- `market` owns only normalized bars, explicit role mapping, completed-bar
  aggregation, and decision-clock timing.
- Research feature calculation and location detection live under
  `strategies/research`.
- Orderflow templates and policy helpers live under `strategies/orderflow`.
- Feature snapshot persistence and the shared feature-cache schema were removed.
- `StrategyCatalog` is composed by the application/CLI and injected into live
  and replay hosts.
- Replay summaries and API projections use generic attribution and no longer
  require orderflow-specific metrics.
- The acceptance suite covers strategy-boundary imports, live/replay bundle
  processing, replay contracts, API composition, and obsolete feature
  infrastructure removal.

## Broker Extension

Broker integrations are adapters, not domain objects. Keep provider-specific
authentication, sockets, historical data, order payloads, and response mapping
under `ftx_paper.broker.<provider>`. Convert provider data into the shared
`MarketBar`, `Instrument`, `OrderIntent`, `OrderAck`, and `Fill` contracts at
the adapter boundary. Market normalization remains broker-neutral in
`ftx_paper.market`.

To add a broker:

1. Add provider auth and feed code implementing the `MarketFeed` lifecycle
   (`start`, `stop`, and `flush`) and map quotes/bars to shared market
   contracts.
2. Add historical backfill and instrument resolution where the provider
   requires them, keeping retries and provider errors inside the adapter.
3. If an execution adapter is needed, implement the `Broker` contract
   (`submit`, `poll_fill`, and `close`) with stable client-order and fill
   identifiers, idempotency, quantity validation, and explicit uncertainty.
4. Register adapter construction in the composition/runtime wiring and add
   contract tests for authentication, feed lifecycle, normalization, order
   acknowledgements, fills, partial fills, and failures.

This application is paper-trading only. `build_runtime_session` deliberately
ignores injected execution brokers and the runtime enforces `PaperBroker` for
orders. A real broker may supply market data and authentication, but provider
orders must not bypass paper execution. Unsupported capabilities must remain
explicit failures rather than being reported as successful operations.

## UI And API Boundary

The UI is a separate presentation process. It must communicate with the API
over HTTP and must not import runtime, domain, broker, SQLite, or strategy
implementation modules. Configure its API origin with `FTX_API_BASE_URL`
(default `http://127.0.0.1:8501`).

The existing Flask UI follows this model: it renders presentation templates,
passes the API base URL to the page, and contains no backend imports. A
different web, desktop, or mobile client can use the same API without adding
another runtime owner.

Clients consume the versioned `/api/v1` contract and treat responses as
projections. They must not read the runtime database, infer hidden portfolio
state, or issue orders outside API-owned commands. OpenAPI discovery,
pagination, queued responses, authentication, and CORS are API contract
details maintained with the route implementation.

With multiple strategies enabled, the API remains the single projection
boundary for the shared runtime. Decision, order, execution, and diagnostic
projections include the stable `strategy_id` when an event is attributable to
one strategy. The first UI pass keeps the existing dashboard, defaults to an
`All strategies` filter, and supports filtering decisions, orders, fills, and
diagnostics by strategy. Activity tables include a `Strategy` column, and a
small strategy list shows each strategy's name and enabled status. Positions,
capital, reservations, and runtime health remain shared runtime views labelled
`Shared portfolio`, not separate per-strategy accounts. A failure in any
strategy is reported through one prominent runtime failure banner naming the
source, and the UI shows the other strategies as stopped.

## Extension Rules

Keep new code inside the existing packages; do not reintroduce a shared
`core/` package. Add typed ports and contract tests before adding another
broker or process boundary. Preserve tests for shared portfolio identity,
authorization, idempotency, legal lifecycle transitions, quantity handling,
critical failure propagation, observer isolation, recovery, and dependency
boundaries.
