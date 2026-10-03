# ftx-paper Agent Guide

## Environment

- Windows with PowerShell; use `.venv\Scripts\python.exe` for Python. Run commands directly through the configured shell, without nesting another PowerShell invocation.
- Use the `ftx_paper` package imports from `src/`; do not add repository-root import hacks.
- Run `.venv\Scripts\python.exe -m pytest` from the repository root for the test suite, or target the relevant test files when iterating.
- Run `.venv\Scripts\ruff.exe check .` from the repository root when Ruff is available. Add `--fix` only when intentionally applying autofixes.

## Validation

- For every task that changes Python files, run `.venv\Scripts\pyright.exe <changed Python files>` from the repository root before finishing. This uses the root `pyrightconfig.json`; do not substitute an editor-only Pylance check. Resolve all reported errors in changed files, or explicitly report the remaining errors and why they cannot be fixed. Do not skip the check because unrelated baseline errors exist. Include the exact Pyright command and its pass/fail result in the final response. If the executable is unavailable, run `.venv\Scripts\python.exe -m pyright <changed Python files>` and report the environment problem if that also fails.

## Critical Boundaries

- **Paper trading only.** Do not describe simulated execution as live trading or add behavior that assumes external positions can be reconciled. Preserve explicit runtime, recovery, and audit semantics.
- **Keep `ftx-paper` independent from NiftyZoning.** They are separate deployments and virtual environments; do not copy code, configuration, databases, or generated outputs between them.
- **One API-owned runtime.** The API owns one in-process `RuntimeSession` for its runtime directory. Do not introduce a separate market process or a second process that mutates the same runtime state.
- **Loopback deployment.** Keep the API bound to loopback by default. Never commit broker credentials, auth databases, runtime databases, or environment files.
- **Layering:** `domain`, `contracts`, and `strategy` must not depend on infrastructure. `application` depends on domain/contracts and ports; `execution` depends on domain/contracts; `runtime` coordinates domain, broker, execution, and the runtime store facade; `api` uses runtime and the store facade.

## Code And Data Practices

- Keep SQLite connections, SQL statements, database paths, and row mapping in `src/ftx_paper/infrastructure/sqlite/`.
- Use typed records and repository protocols from `ftx_paper.ports`. API and CLI code should use `ftx_paper.runtime.store` rather than selecting concrete SQLite adapters.
- Keep strategy policy and decision policy in `ftx_paper.strategy`; keep market normalization and decision-clock processing in `ftx_paper.market`.
- Treat runtime, portfolio, broker, and execution mutations as in-memory operations that are persisted afterward; do not imply that SQLite writes roll back those mutations.
- Preserve startup recovery behavior: interrupted runtime sessions become `STOPPED` and retain an audit event without reconstructing the in-memory engine.
- Add tests for reusable behavior and architecture boundaries. Extend the established test file for the source module and test project behavior rather than Python language semantics.
- Do not commit files under `data/runtime/`, SQLite databases, broker configuration, `.env` files, or other generated/runtime artifacts.
- For `apply_patch` edits, read the current symbol and patch the smallest stable hunk. After changing a function signature, search all call sites before applying follow-up patches.

## Supporting References

- See `docs/ARCHITECTURE.md` for dependency boundaries, persistence behavior, and runtime composition.
- See `docs/DEPLOYMENT.md` for process ownership, loopback deployment, configuration, and upgrade rules.
