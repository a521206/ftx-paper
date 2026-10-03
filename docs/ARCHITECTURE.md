# Architecture

## Dependency Boundaries

The main dependency direction is:

```text
core / contracts / strategy -> no infrastructure-specific code
application -> core / contracts + ports
execution -> core / contracts
runtime -> core / contracts + broker + execution + runtime store facade
api -> runtime + runtime store facade
infrastructure.sqlite -> ports + core / contracts
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

Strategy policy configuration lives in `ftx_paper.core.strategy_config`.
`ftx_paper.strategy.config` is retained only as an import compatibility shim,
so core decision code does not depend on the concrete strategy package.

## Pipeline Alignment Checkpoint

The active Paper strategy was aligned on 2026-10-03 against pipeline commits
`233a1ce0` (morning R1 policy), `1fa30fbd` and `09fb711c` (afternoon R2
policy), and `6d958a3d` (09:15-11:15 and 13:15-14:15 session windows).
This alignment is recorded by Paper commit `31782a4` (`Align active paper
strategy with pipeline`).

Capital limits and the immutable runtime capital context live in
`ftx_paper.core.capital_config` and `ftx_paper.core.capital_context`; they are
business policy shared by sizing, execution, strategy, and runtime code.

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

`core/`, `contracts/`, and `strategy/` are business packages and are covered by
the same infrastructure-import checks. There is no separate `domain` package;
the domain model is currently organized across `contracts` and `core`.
