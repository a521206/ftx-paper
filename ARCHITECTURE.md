# FTX Paper Trading Architecture

`ftx-paper` is an independently shipped application. It has no runtime or
packaging dependency on NiftyZoning. NiftyZoning may remain the historical
ancestor of the strategy implementation, but changes there must not alter this
package.

## Boundaries

```text
UI -> HTTP/JSON API -> RuntimeController -> RuntimeSession -> core engine
                                  \-> broker protocol -> Zerodha adapter
```

- `contracts` contains versioned, broker-neutral dataclasses.
- `core` is pure trading logic and has no Flask, SQLite, filesystem, or broker imports.
- `core.strategy.Strategy` is the only strategy entry point used by the runtime.
- `broker/zerodha` contains all Kite-specific authentication, feed, and order mapping.
- `runtime` owns persistence, session lifecycle, audit events, and recovery.
- `api` owns HTTP endpoints and runtime actions.
- `ui` consumes the API and contains no business logic or direct database access.

The Zerodha adapter is intentionally not a paper-fill implementation. It only
translates broker requests and order acknowledgements into package contracts.
Fill reconciliation and paper-position state belong to the session/execution
layer. `OrderAck` and `Fill` are deliberately separate types.

## Paper portfolio semantics

Paper intentionally uses one shared portfolio across all enabled vehicles.
Directional exposure, margin reservations, equity, drawdown, positions, and
settlement are portfolio-level state; vehicle sizing remains a vehicle gate.
Replay and live execution use the same `PaperPortfolio` and execution
coordinator lifecycle. This differs from the canonical runner when it creates
one portfolio per vehicle, so parity comparisons must explicitly configure the
canonical side to the shared semantics above.

## Event timestamp contract

`decision_at` is the sole decision timestamp field in decision event payloads.
It is the timezone-aware instant at which the strategy evaluated the completed
market bar. Replay and live decision events write that instant to `decision_at`;
the runtime parses it into an aware `datetime`, normalizes it to IST for
operator-facing date/session decisions, and serializes it only at API/storage
boundaries. `created_at` is the SQLite persistence/audit timestamp and is never
used as a decision time. The runtime store explicitly migrates legacy decision
fields and reports rows that cannot be migrated; it never guesses from
`created_at`.

## Deliberate duplication

The live strategy is copied from the research code at an explicitly recorded
release point and then simplified for session-time execution. It is not a
shared library. Each runtime records its strategy name, version, and config
hash so that paper trades remain reproducible.

## Extraction gates

1. Contracts and characterization tests are stable.
2. Core replay tests pass without external services.
3. Zerodha adapter passes mocked authentication/feed/order tests.
4. Runtime health survives API restart by returning interrupted paper sessions
   to `STOPPED` and recording an audit event.
5. API contract tests pass with injected controller/session dependencies.
6. UI uses API endpoints exclusively.
7. Only then is `scripts.ftx.run_ftx_web` changed to launch `ftx-paper`.
# Runtime architecture

The API owns one in-process `RuntimeController`, which owns one push-based
`RuntimeSession`. The session authenticates and resolves Zerodha instruments,
runs futures/VIX warmup, starts `ZerodhaFeed`, and handles each closed bar
under one lifecycle lock. There is no subprocess, PID, command queue, or
pull-based `bars()` abstraction.
