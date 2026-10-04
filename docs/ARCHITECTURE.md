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

## Extension Rules

Keep new code inside the existing packages; do not reintroduce a shared
`core/` package. Add typed ports and contract tests before adding another
broker or process boundary. Preserve tests for shared portfolio identity,
authorization, idempotency, legal lifecycle transitions, quantity handling,
critical failure propagation, observer isolation, recovery, and dependency
boundaries.
