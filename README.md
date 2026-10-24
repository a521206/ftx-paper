# ftx-paper

Standalone paper-trading runtime, REST API, and dashboard for Zerodha market
data. The API owns the in-process paper runtime; the UI is a separate,
presentation-only process.

## Local Setup

Use Python 3.14 or newer and install the package with Zerodha support:

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -e ".[zerodha]"
```

Provide broker configuration through `FTX_PAPER_CONFIG_PATH` or the default
configuration location, then start the processes in separate terminals:

```powershell
.\run-api.ps1
.\run-ui.ps1
```

The API runs on `http://127.0.0.1:8501`; the dashboard runs on
`http://127.0.0.1:8502`. Keep the API loopback-only.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Strategy rules](docs/STRATEGY_RULES.md)
- [Changelog](CHANGELOG.md)

## Releases And Upgrades

Production-facing changes are maintained in the `Unreleased` section of
[`CHANGELOG.md`](CHANGELOG.md). When changes are pushed to production, move
them into a dated version section and bump the package version in
`pyproject.toml` and `src/ftx_paper/__init__.py`.

When upgrading an existing runtime database with the legacy
`runtime_contracts` primary key, run the one-time migration before starting the
new build:

```powershell
.venv\Scripts\python.exe scripts\migrate_runtime_contracts_key.py <runtime-dir-or-db>
```
