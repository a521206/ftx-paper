# Multi-Strategy Implementation

This document describes the first-pass implementation for running multiple
paper strategies inside one `RuntimeSession`. It complements
[Architecture](ARCHITECTURE.md) and is an implementation guide, not a
strategy-rule specification.

## First-Pass Scope

Keep the change deliberately small:

- one runtime process and one `RuntimeSession`;
- one shared `PortfolioState`, capital context, and paper broker;
- multiple configured strategy objects;
- stable `strategy_id` on generated decisions and orders; and
- the existing dashboard with a simple strategy filter.

Do not add per-strategy accounts, portfolios, databases, runtime processes, or
dynamic enable/disable controls. Existing single-strategy `PaperEngine` usage
must continue to work unchanged.

## Existing Building Blocks

Reuse the current implementation rather than introducing new layers:

- `StrategyInstance` identifies a strategy and its enabled state.
- `MultiStrategyEngine` dispatches bundles and prefixes client order IDs.
- `RuntimeSession` owns the portfolio, capital context, execution coordinator,
  persistence, and lifecycle.
- `OrderIntent`, `ExecutionNotification`, and `RuntimeEvent` already support
  nullable `strategy_id` and `account_id` fields.
- Existing runtime event storage powers the API and UI projections.

## First-Pass Behavior

1. Configure strategies at the composition boundary and preserve insertion
   order.
2. Bind the same runtime portfolio and capital context to every strategy.
3. Send each completed bundle to every enabled strategy.
4. Add `strategy_id` to each returned event and order. Prefix each client order
   ID once with `<strategy_id>:` for retry idempotency.
5. Submit resulting orders through the existing shared authorization and
   `PaperBroker` path, in configured order.
6. Route execution notifications by `strategy_id` when present. Keep the
   existing broadcast behavior for unattributed notifications.
7. If a strategy raises, stop processing the bundle, mark the shared runtime
   failed, and do not continue with later strategy orders.
8. Persist the existing strategy snapshot shape, extended only as needed to
   store one keyed snapshot per strategy. Do not restore portfolio state from
   strategy snapshots.

The shared portfolio remains the source of truth. This first pass does not
attempt to calculate separate strategy P&L or resolve competing exposure
policies beyond the existing shared authorization behavior.

## Implementation Steps

### 1. Runtime Wiring

Files:

- `src/ftx_paper/runtime/engine.py`
- `src/ftx_paper/runtime/session.py`
- `src/ftx_paper/runtime/__init__.py`

Tasks:

1. Keep `StrategyInstance` validation minimal: non-empty ID and matching map
   key.
2. Keep `MultiStrategyEngine` as a thin loop over `PaperEngine` instances.
3. Bind `portfolio` and `capital_context` once from `RuntimeSession`.
4. Preserve `PaperEngine` as the default when no engine is supplied.
5. Change strategy exceptions from silent continuation to a runtime failure
   signal handled by `RuntimeSession`.
6. Preserve current exit, tick, fill, and settlement forwarding paths.

Acceptance checks:

- One strategy behaves as before.
- Two strategies receive the same bundle in stable order.
- Both strategies reference the same portfolio object.
- A strategy failure does not submit later orders from the bundle.

### 2. Event And Order Attribution

Files:

- `src/ftx_paper/runtime/session.py`
- `src/ftx_paper/runtime/facades.py`
- `src/ftx_paper/contracts/orders.py`
- `src/ftx_paper/execution/events.py`

Tasks:

1. Preserve `strategy_id` when writing decision, order intent, acknowledgement,
   fill, cancellation, and diagnostic events.
2. Use the existing client order ID as the idempotency key after applying the
   strategy prefix.
3. Do not change execution or portfolio accounting for this first pass.
4. Leave legacy events with a null strategy ID and display them as
   `Unattributed`.

Acceptance checks:

- Combined activity contains attribution where available.
- Reprocessing the same order does not create a second lifecycle.
- Existing single-strategy event payloads remain readable.

### 3. Minimal Snapshot Support

Files:

- `src/ftx_paper/runtime/engine.py`
- `src/ftx_paper/runtime/session.py`

Tasks:

1. Store a simple object with `schema_version` and an `instances` mapping.
2. Key each instance by stable `strategy_id`.
3. Store only the strategy's serializable state and enabled flag.
4. Restore only configured matching strategies; leave missing strategies at
   their configured initial state.
5. Keep interrupted-session recovery as `STOPPED`; do not add resume behavior.

Acceptance checks:

- Existing single-strategy snapshots still load.
- Multiple strategy snapshots round-trip independently.
- No snapshot creates a second portfolio or runtime owner.

### 4. Minimal API Projection

Files:

- `src/ftx_paper/api/app.py`
- `src/ftx_paper/api/schemas.py`
- `src/ftx_paper/runtime/store.py` only if a small read helper is needed

Tasks:

1. Add a `strategies` list to the existing runtime response with ID, name,
   enabled state, and failure state.
2. Add an optional `strategy_id` filter to the existing decisions, events,
   trades, and diagnostics endpoints where filtering is straightforward.
3. Keep positions, capital, reservations, and health shared and unfiltered.
4. Update the OpenAPI descriptions for the added field and query parameter.
5. Use existing bounded event reads; do not introduce a new reporting store.

Acceptance checks:

- Default API responses show all strategies together.
- A selected strategy filters attributable activity.
- Shared account values do not change when a filter is applied.

### 5. Minimal UI Filter

Files:

- `src/ftx_paper/ui/templates/index.html`
- Existing UI static files, if required

Tasks:

1. Add a small strategy selector with `All strategies` as the default.
2. Pass the selected ID to the existing API requests.
3. Add a `Strategy` column to activity tables.
4. Label the existing account panels `Shared portfolio`.
5. Show one simple failure banner when the runtime reports a failed strategy.
6. Keep the existing layout and make the selector usable on narrow screens.

## Tests

Add focused coverage to existing test files:

- `tests/test_runtime_facades.py`: dispatch, shared object identity, order
  attribution, ID prefixing, notification routing, and failure behavior.
- Runtime session tests: shared order execution and strategy snapshot round trip.
- API tests: combined response, strategy filter, shared projections, and
  failure status.
- UI tests: default selector, selected strategy, attribution column, and
  failure banner.

Run only relevant tests while iterating. For Python changes, also run:

```text
.venv\Scripts\pyright.exe <changed Python files>
.venv\Scripts\ruff.exe check .
```

Before enabling multiple strategies, run one deterministic replay through the
existing single-strategy path and verify that decisions, fills, and audit
events remain unchanged.

## Deferred Work

Leave these out of the first pass:

- separate strategy accounts or P&L;
- dynamic enable/disable and hot reload;
- cross-strategy position allocation or conflict policy;
- snapshot migrations and session resumption;
- new reporting tables or analytics services;
- comparison screens and strategy-specific dashboards; and
- live broker order routing or external-position reconciliation.

## Definition Of Done

The first pass is complete when multiple configured strategies run through one
shared runtime, generated activity is attributable, the dashboard can filter
activity, failures stop the common runtime, existing single-strategy behavior
remains compatible, and focused tests plus required type/lint checks pass.
