# Architecture

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

Strategy policy lives in `ftx_paper.strategy`. Market normalization and
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
portfolio from SQLite, and paper trading has no external positions to
reconcile.

The in-process facades are `FeedManager`, `OrderManager`, `StrategyRunner`,
and `PnlManager`. They delegate to the existing runtime owner and aggregate;
`PnlManager` is read-only. The UI remains separate and facades must not create
another runtime owner.

## Strategy Extension

Strategies live in `ftx_paper.strategy` and are selected at the composition
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

1. Add the implementation under `ftx_paper.strategy` and depend only on
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

## UI And API Clients

The UI is a separate presentation process. It must communicate with the API
over HTTP and must not import runtime, domain, broker, SQLite, or strategy
implementation modules. Configure its API origin with `FTX_API_BASE_URL`
(default `http://127.0.0.1:8501`).

Build a UI client as follows:

1. Discover the contract from `GET /api/v1/openapi.json`; use the versioned
   `/api/v1` routes rather than reading the runtime database.
2. Use `GET /api/v1/health` for availability, then poll or refresh
   `/runtime`, `/diagnostics`, `/events`, `/decisions`, `/positions`,
   `/capital`, and `/trades` for live views. Use the replay routes for
   historical investigation and progress updates.
3. Treat responses as projections. Do not infer hidden portfolio state or
   issue orders from the UI; runtime commands and paper execution remain owned
   by the API process.
4. Preserve cursor pagination for decisions and replay events, handle `202`
   responses for queued/incomplete work, and display error payloads using
   their stable `error.code` and `error.message` fields.
5. Send the configured authentication token when the API is not using its
   loopback-only development allowance, and restrict browser origins through
   the API's configured CORS policy.

The existing Flask UI follows this model: it renders presentation templates,
passes the API base URL to the page, and contains no backend imports. A
different web, desktop, or mobile client can use the same API without adding
another runtime owner.

## Extension Rules

Keep new code inside the existing packages; do not reintroduce a shared
`core/` package. Add typed ports and contract tests before adding another
broker or process boundary. Preserve tests for shared portfolio identity,
authorization, idempotency, legal lifecycle transitions, quantity handling,
critical failure propagation, observer isolation, recovery, and dependency
boundaries.
