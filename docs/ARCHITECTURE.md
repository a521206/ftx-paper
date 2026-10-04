# Architecture

## Dependency Boundaries

The main dependency direction is:

```text
domain / contracts / strategy -> no infrastructure-specific code
application -> domain / contracts + ports
execution -> domain / contracts
runtime -> domain / contracts + broker + execution + runtime store facade
api -> runtime + runtime store facade
infrastructure.sqlite -> ports + domain / contracts
```

`application.composition` is the wiring boundary that selects the SQLite
adapters. The API currently calls runtime and store services directly rather
than routing every request through an application use case.

SQLite connections, SQL statements, filesystem database paths, and database row
mapping live in `ftx_paper.infrastructure.sqlite`. Application code can use
typed records and repository protocols from `ftx_paper.ports`. The
`ftx_paper.runtime.store` module is the runtime-facing facade for the SQLite
adapter; API and CLI code use this facade rather than selecting a concrete
infrastructure implementation. It is the intentional exception to the
otherwise strict infrastructure boundary.

Strategy policy configuration and decision policy live in
`ftx_paper.strategy`; market normalization and decision-clock processing live
in `ftx_paper.market`.

## Pipeline Alignment Checkpoint

The active Paper strategy was aligned on 2026-10-03 against pipeline commits
`233a1ce0` (morning R1 policy), `1fa30fbd` and `09fb711c` (afternoon R2
policy), and `6d958a3d` (09:15-11:15 and 13:15-14:15 session windows).
This alignment is recorded by Paper commit `31782a4` (`Align active paper
strategy with pipeline`).

The canonical portfolio aggregate lives in `ftx_paper.domain.portfolio`.
Capital limits and the immutable runtime capital context are exposed through
`ftx_paper.domain.capital`; they are business policy shared by sizing,
execution, strategy, and runtime code.

Runtime composition creates one `PortfolioState` and one
`CapitalRuntimeContext`, then injects both into the strategy and execution
coordinator. A strategy may use the aggregate to evaluate decisions, but it
does not create or select the account state. This keeps portfolio, capital,
margin, and execution behavior centralized while allowing the strategy factory
to replace decision policy.

Application use cases that use a unit of work obtain a fresh product for each
operation. A unit of work commits only when the application explicitly calls
`commit`; exceptions roll back and the connection is always closed. Nested
transactions are not supported.

The current runtime and API also use `SqliteRuntimeStore` directly. Store
operations use short-lived SQLite connections, and status read/modify/write
patches are serialized by an `RLock` within one store instance. The process
lease prevents two API processes from owning the same runtime directory, but
the status lock is not a cross-process compare-and-set mechanism.

Engine, portfolio, and broker mutations are in memory and are not rolled back
by SQLite. Runtime processing persists events and status after state changes;
there is no full engine/portfolio checkpoint today. If the process is
interrupted, startup recovery marks an in-progress runtime `STOPPED`, records a
`RUNTIME_RECOVERY` audit event, and does not reconstruct the in-memory engine.
Paper trading has no external positions to reconcile during this recovery.

Replay state transitions and contract/event writes use the persistence rules in
the SQLite adapter. Callers must not assume that a runtime state mutation and
its corresponding SQLite write form one rollbackable transaction.

`domain/`, `contracts/`, `strategy/`, `market/`, and `execution/` are business
packages covered by the same infrastructure-import checks. Runtime orchestration
belongs in `runtime/`; new code must not reintroduce a shared `core/` package.

## Modular-Monolith Scaffold

The next architectural step is a modular monolith, not a distributed
microservice deployment. Feed, order, strategy, and portfolio responsibilities
may have explicit in-process facades and typed contracts while remaining owned
by the single API process and its one `RuntimeSession`.

The scaffold must preserve the paper-trading boundary. The initial composition
may use a real broker's market-data adapter for normalized market input, but
orders continue to use `PaperBroker` and the existing paper execution
coordinator. External broker execution and external-position reconciliation are
out of scope. The UI remains a separate presentation process; facades do not
create another runtime owner or bypass the runtime-directory process lease.

This section describes the intended scaffold, not a claim that the new ports,
facades, event bus, or complete lifecycle graph are already implemented.

### Ownership And Sequencing

`PortfolioState` remains the single canonical owner of positions, reservations,
capital, and settlement. Any future portfolio/P&L facade delegates to this
aggregate and must not maintain a competing position store. `AccountAggregate`
wraps that same portfolio for account authorization, reservations, revisions,
and strategy-facing `DecisionContext` snapshots. `ExecutionService` coordinates
authorization with `PaperExecutionCoordinator`, which applies submission,
fill, cancellation, and settlement operations to the same portfolio.
Persistence follows in-memory mutation; SQLite writes must not be described as
rollback of those mutations. Explicit reservation cleanup is an in-memory
compensation operation, not a database rollback.

Critical state transitions are not optional event subscribers. The runtime must
apply execution and portfolio mutations in a defined sequence, propagate a
failure to runtime health, and halt the affected processing path rather than
allowing strategy evaluation to continue against stale state. Exception
isolation is appropriate only for optional observers such as diagnostics,
notifications, and UI projections.

Runtime wiring must preserve serialized critical processing under the existing
session ownership and locking. An optional observer must not mutate canonical
account state or issue orders. Strategy execution notifications that update
decision or protective-exit state are critical, even if delivered as events.

The intended critical sequence for a decision that produces an order is:

```text
market input -> normalized decision input -> current account context
-> strategy evaluation -> order validation and current-account authorization
-> reservation/pending-order mutation -> authorization persistence
-> PaperBroker execution -> PaperExecutionCoordinator fill/settlement mutation
-> execution persistence -> critical strategy execution notification
-> optional observer delivery
```

Authorization rechecks account state for each order immediately before dispatch;
a decision snapshot does not authorize later submission. Preserve the existing
tick-based protective exits as well as decision-clock processing. Complete the
critical path before evaluating subsequent decisions. Persistence or critical
notification failure must halt affected processing without blindly resubmitting
an order whose simulated fill may already have occurred.

Startup recovery remains unchanged: an interrupted runtime becomes `STOPPED`,
retains a `RUNTIME_RECOVERY` audit event, and does not reconstruct the
in-memory engine or portfolio from SQLite. Existing persisted order rows may be
marked `RECONCILIATION_REQUIRED` for audit/startup gating; that label does not
authorize external broker reconciliation or automatic order resubmission.
Strategy snapshot loading is separate from interrupted-session recovery and
does not provide a complete engine/portfolio checkpoint.

### Contract Rules

Contracts should carry event identity, timestamps, and provenance. Correlation
and ownership metadata is added only where it has meaning:

```text
shared quote: event_id, instrument, broker/source, exchange_time, received_time
order command: event_id, request_id, strategy_id, account_id, instrument
portfolio event: event_id, order_id, strategy_id/account_id when applicable
aggregate health: event_id, component, status, observed_at
```

A shared quote or aggregate health event must not fabricate a strategy or
account identifier. Broker-specific fields are normalized at the broker
adapter boundary; market normalization and decision-clock processing remain in
`ftx_paper.market`.

### Order Lifecycle Model

Order states are a transition graph with explicit branches, not a linear list:

```text
CREATED -> VALIDATED -> AUTHORIZED -> SUBMITTING
CREATED/VALIDATED -> REJECTED
CREATED/VALIDATED/AUTHORIZED -> CANCELLED
SUBMITTING -> REJECTED
SUBMITTING -> ACKNOWLEDGED
SUBMITTING -> FILLED
SUBMITTING -> UNKNOWN
ACKNOWLEDGED -> FILLED
ACKNOWLEDGED -> PARTIALLY_FILLED -> FILLED
ACKNOWLEDGED -> CANCEL_REQUESTED -> CANCELLED
PARTIALLY_FILLED -> CANCEL_REQUESTED -> CANCELLED
ACKNOWLEDGED/PARTIALLY_FILLED -> UNKNOWN
CANCEL_REQUESTED -> FILLED/PARTIALLY_FILLED/CANCELLED/UNKNOWN
UNKNOWN -> ACKNOWLEDGED/PARTIALLY_FILLED/FILLED/CANCELLED/REJECTED
```

Immediate paper fills may transition from `SUBMITTING` directly to `FILLED`.
Cancellation and fill races must be represented by observed events and
idempotent transition handling, not inferred from command order. Terminal
states are `FILLED`, `CANCELLED`, and `REJECTED`; `UNKNOWN` is non-terminal.

`CANCELLED` after a partial fill cancels only the remaining quantity; completed
fills and their portfolio effects remain. A partial fill during cancellation
must retain the outstanding cancellation intent. Track cumulative filled and
remaining quantities independently of status, reject overfills, and prevent
duplicate or stale events from applying a fill twice or regressing a terminal
state. Stable client order IDs identify command retries; fill IDs identify
duplicate fill delivery. Reuse an existing order result for a matching retry
and reject reuse of its ID with a different command.

`UNKNOWN` records uncertainty and must not trigger automatic resubmission or
release reservations without evidence of the outcome. Unsupported partial-fill
and cancellation behavior remains an explicit capability limitation of the
paper adapter until implemented and tested; this graph does not enable external
execution or reconciliation.

### Staged Scaffold

Before adding another broker or any process boundary, introduce only typed
ports, in-process facades, and contract tests around existing components:

1. Define typed market-data, paper-execution, and status ports around existing
   behavior. `MarketFeed` is already separate from the current `Broker`
   protocol; add further splits only where callers need distinct capabilities.
2. Add a `FeedManager` facade that delegates normalization to `market` and owns
   subscriptions and feed health.
3. Add an `OrderManager` facade that delegates paper execution to the existing
   execution service/coordinator and owns lifecycle validation.
4. Add a strategy-runner facade around the existing single `PaperEngine`.
5. Add a P&L query/projection facade over the canonical `PortfolioState`; do not
   create a second mutable portfolio.
6. Introduce an in-process event bus only after critical versus optional
   delivery semantics are defined and tested.
7. Keep composition in `application.composition`/runtime wiring so tests can
   substitute fakes without changing domain or strategy code.

Extend the established component tests with contract checks for shared portfolio
identity, pre-dispatch authorization/reservation, command and fill idempotency,
legal lifecycle transitions, quantity preservation in cancellation races,
critical failure propagation, optional observer isolation, and unchanged
startup recovery. Preserve dependency-boundary tests and the runtime store
facade when extracting responsibilities.

This scaffold creates replaceable boundaries without implying live execution,
distributed delivery, automatic broker reconciliation, or rollback semantics
that the current runtime does not provide.
