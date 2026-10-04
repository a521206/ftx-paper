# ftx-paper Deployment

`ftx-paper` runs independently of NiftyZoning. Install it in its own virtual
environment and run one API process plus the separate UI process. Never start
a second market process: the API owns the single `RuntimeSession`.

## Install

Install the API and UI:

```text
pip install ftx-paper
```

For the Zerodha feed or login flow:

```text
pip install "ftx-paper[zerodha]"
```

The package provides `ftx-paper-api` and `ftx-paper-ui`. On Windows, the
repository scripts `run-api.ps1` and `run-ui.ps1` use the local `.venv` and
source tree.

## Configure

Set these values in the service environment:

```text
FTX_PAPER_HOME=/var/lib/ftx-paper
FTX_PAPER_RUNTIME_DIR=/var/lib/ftx-paper/runtime
FTX_PAPER_AUTH_DB=/var/lib/ftx-paper/auth/auth.sqlite3
FTX_PAPER_CONFIG_PATH=/etc/ftx-paper/zerodha.json
FTX_API_BASE_URL=http://127.0.0.1:8501
FTX_WEB_HOST=127.0.0.1
FTX_WEB_PORT=8501
FTX_UI_PORT=8502
```

Keep the home directory, runtime directory, and auth database on persistent
storage. Keep broker secrets out of source control. For local setup, use
`.env.example` as a starting point and choose a writable auth database path.

The supported deployment is loopback-only. Do not bind the API to a public
interface. Loopback requests intentionally do not require a bearer token, and
the UI calls the API directly.

## Run

```text
ftx-paper-api     # REST API and runtime orchestration
ftx-paper-ui      # presentation-only dashboard
```

Run one API instance per runtime directory. The process lease prevents two
instances from owning the same directory. On startup after an interruption,
the previous session is marked `STOPPED` and the interruption remains in the
audit log.

For local Windows startup:

```powershell
./run-api.ps1
./run-ui.ps1
```

The defaults are API port `8501` and UI port `8502`. Use `-Port` to override a
port and `-ApiBaseUrl` to point the UI at another API URL.

## Upgrade

1. Stop the runtime and confirm it is `STOPPED`.
2. Back up the runtime and auth SQLite databases.
3. Install the new wheel, including the `zerodha` extra when needed.
4. Start the API and verify `/api/v1/health` and `/api/v1/openapi.json`.
5. Start the runtime and inspect recovery or audit events.
6. Start the UI and verify runtime, capital, positions, trades, and events.

To roll back, reinstall the previous wheel against the backups. Do not delete
or recreate the runtime database during an upgrade.
