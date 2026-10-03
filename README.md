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
