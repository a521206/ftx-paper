# ftx-paper Agent Guide

## Environment

- Windows with PowerShell; use `.venv\Scripts\python.exe` for Python. Run commands directly through the configured shell, without nesting another PowerShell invocation.
- Use the `ftx_paper` package imports from `src/`; do not add repository-root import hacks.

## Engineering Principles

- Follow the agreed design and canonical implementation; investigate discrepancies and fix their underlying causes rather than layering patches.
- For orderflow, port behavior from `NiftyZoning/src/orderflow`, especially `execution.py`, `opportunity_coordinator.py`, and `state_machine_reporting.py`. Trace remaining gaps to the canonical implementation; do not add Paper policy or comparator-driven shortcuts.
- Prefer the smallest clear solution and established project patterns. Avoid speculative abstractions, indirection, fallbacks, and parallel implementations.
- Make forward-only changes: remove obsolete paths, dead code, and compatibility shims unless backward compatibility is explicitly required.
- For substantial work, use a concise phased plan; keep each phase focused, verify it against the design, and simplify before proceeding.
- If strategy behavior cannot be resolved from the design or canonical code, ask rather than inventing a rule.
- Before completion, review the affected diff for unnecessary complexity, confirm obsolete paths are removed, and run the relevant validation commands below.
- Wait for explicit user instructions before creating a commit; validation and diff review may be completed without committing.

## Validation

- Install development dependencies with `.venv\Scripts\python.exe -m pip install -e ".[dev]"` when needed.
- Run `.venv\Scripts\python.exe -m pytest` from the repository root, targeting relevant tests when iterating.
- Run `.venv\Scripts\ruff.exe check .` from the repository root. If that executable is unavailable, use `py -m ruff check .`. Add `--fix` only when intentionally applying autofixes.
- For tasks that change Python files, run `.venv\Scripts\pyright.exe <changed Python files>` from the repository root and resolve reported errors.

## Testing

- Add tests for reusable logic, not experiment scripts.
- Extend the established test file for the source module; avoid duplicate test names.
- Test project behavior, not Python language semantics.
- Prefer high-value tests for end-to-end workflows, public contracts, causal/replay rules, and critical regressions.
- Avoid duplicate implementation-detail tests; keep lower-level tests only when integration tests do not provide equivalent coverage.
- Keep tests focused, deterministic, and maintainable.
- By default, run only tests relevant to changed code; do not run the entire suite unless explicitly requested.

## Critical Boundaries

- **Paper trading only.** Do not describe simulated execution as live trading or add behavior that assumes external positions can be reconciled. Preserve explicit runtime, recovery, and audit semantics.
- **Keep `ftx-paper` independent from NiftyZoning.** They are separate deployments and virtual environments; do not copy code, configuration, databases, or generated outputs between them.
- **One API-owned runtime.** The API owns one in-process `RuntimeSession` for its runtime directory. Do not introduce a separate market process or a second process that mutates the same runtime state.
- **Loopback deployment.** Keep the API bound to loopback by default. Never commit broker credentials, auth databases, runtime databases, or environment files.
- **Layering:** `domain`, `contracts`, and `strategy` must not depend on infrastructure. `application` depends on domain/contracts and ports; `execution` depends on domain/contracts; `runtime` coordinates domain, broker, execution, and the runtime store facade; `api` uses runtime and the store facade.

## Code And Data Practices

- Keep SQLite connections, SQL statements, database paths, and row mapping in `src/ftx_paper/infrastructure/sqlite/`.
- Use typed records and repository protocols from `ftx_paper.ports`. API and CLI code should use `ftx_paper.runtime.store` rather than selecting concrete SQLite adapters.
- Keep strategy policy and decision policy in `ftx_paper.strategies`; keep market normalization and decision-clock processing in `ftx_paper.market`.
- Treat runtime, portfolio, broker, and execution mutations as in-memory operations that are persisted afterward; do not imply that SQLite writes roll back those mutations.
- Preserve startup recovery behavior: interrupted runtime sessions become `STOPPED` and retain an audit event without reconstructing the in-memory engine.
- Do not commit files under `data/runtime/`, SQLite databases, broker configuration, `.env` files, or other generated/runtime artifacts.
- For `apply_patch` edits, read the current symbol and patch the smallest stable hunk. After changing a function signature, search all call sites before applying follow-up patches.

## Broker Integrations & Architecture

- **Supported Providers:** `xts` (IIFL Symphony XTS), `shoonya` (Finvasia), and `zerodha` (Kite Connect), structured under `src/ftx_paper/broker/<provider>/`. Broker selection is governed by `FTX_BROKER`, CLI `--broker`, or automatic config detection in `PaperConfig.from_env()`.
- **Market Data Only:** External brokers supply market data feeds, ticks, historical startup backfill, and option surface discovery. Order execution is strictly in-process simulation via `PaperBroker`.
- **Standard Broker Module Structure:**
  - `protocol.py`: Wire protocol formatting, binary packet decoding (`decode_binary_packets` for XTS Engine.IO raw deflate frames), and segment/exchange mapping.
  - `client.py`: HTTP REST API endpoints (login, search, quotes, OHLC historical backfill). Always respect API limits (e.g., chunk XTS subscription calls into batches of <= 100 instruments).
  - `auth.py`: Token management and storage in `auth.sqlite3`. Supports automated headless login (XTS, Shoonya) or OAuth redirect flows (Zerodha).
  - `socket.py`: Single-owner asynchronous WebSocket transport with automated ping/pong keepalive and resubscription on reconnect.
  - `feed.py`: 1-minute completed bar aggregation delivering normalized `MarketBar` sequences to `RuntimeSession`.
  - `adapter.py`: Instrument role classification, contract resolution (filter out dual-expiry calendar spreads to isolate monthly futures), and option chain discovery.
- **Dedicated Broker Test Coverage:** Maintain and extend unit tests under `tests/test_<broker>_{adapter,auth,protocol,socket}.py` whenever broker behavior is modified.

## Supporting References

- See `docs/ARCHITECTURE.md` for dependency boundaries, persistence behavior, and runtime composition.
- See `docs/DEPLOYMENT.md` for process ownership, loopback deployment, configuration, and upgrade rules.
- See `docs/strategies/research/STRATEGY_RULES.md` for live research strategy rules, session windows, location cells, exits, and risk parameters.

