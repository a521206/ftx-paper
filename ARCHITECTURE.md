# FTX Paper Trading Architecture

`ftx-paper` is an independently shipped application. It has no runtime or
packaging dependency on NiftyZoning. NiftyZoning may remain the historical
ancestor of the strategy implementation, but changes there must not alter this
package.

## Boundaries

```text
UI -> HTTP/JSON API -> runtime worker -> core engine
                                  \-> broker protocol -> Zerodha adapter
```

- `contracts` contains versioned, broker-neutral dataclasses.
- `core` is pure trading logic and has no Flask, SQLite, filesystem, or broker imports.
- `core.strategy.Strategy` is the only strategy entry point used by the runtime.
- `broker/zerodha` contains all Kite-specific authentication, feed, and order mapping.
- `runtime` owns persistence, process lifecycle, audit events, and recovery.
- `api` owns HTTP endpoints and runtime commands.
- `ui` consumes the API and contains no business logic or direct database access.

The Zerodha adapter is intentionally not a paper-fill implementation. It only
translates broker requests and order acknowledgements into package contracts.
Fill reconciliation and paper-position state belong to the worker/execution
layer. `OrderAck` and `Fill` are deliberately separate types.

## Deliberate duplication

The live strategy is copied from the research code at an explicitly recorded
release point and then simplified for session-time execution. It is not a
shared library. Each runtime records its strategy name, version, and config
hash so that paper trades remain reproducible.

## Extraction gates

1. Contracts and characterization tests are stable.
2. Core replay tests pass without external services.
3. Zerodha adapter passes mocked authentication/feed/order tests.
4. Worker survives restart and produces an auditable event stream.
5. API contract tests pass with a fake worker/store.
6. UI uses API endpoints exclusively.
7. Only then is `scripts.ftx.run_ftx_web` changed to launch `ftx-paper`.
