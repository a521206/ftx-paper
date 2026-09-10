# ftx-paper deployment and upgrades

`ftx-paper` is deployed independently of NiftyZoning. Install it into its own
virtual environment and run the API and UI as separate processes. The API owns
one in-process `RuntimeSession`; do not launch a separate market process.

## Configuration

Set these variables explicitly in the service environment:

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

Keep `FTX_PAPER_HOME`, the runtime directory, and the auth database on
persistent storage. Never place broker secrets in source control.

For local setup, copy `.env.example` into your service environment and change
`FTX_PAPER_AUTH_DB` to a writable persistent location.

## Processes

```text
ftx-paper-api     # REST API and runtime orchestration
ftx-paper-ui      # presentation-only dashboard
```

The API starts and stops the in-process session directly. An interrupted
session is marked `RECOVERY_REQUIRED`; inspect the audit log before starting
again. Deploy one API instance per runtime state directory.

For local Windows startup, run `.\run-api.ps1` for the API and `.\run-ui.ps1` for the dashboard. The API defaults to port 8501 and the UI to port 8502. Override ports with `-Port`; override the UI API target with `-ApiBaseUrl`.

## Upgrade procedure

1. Stop the runtime session and confirm it has reached `STOPPED`.
2. Back up the runtime and auth SQLite databases.
3. Install the new `ftx-paper` wheel in the package environment.
4. Start the API and verify `/api/v1/health` and `/api/v1/openapi.json`.
5. Start the runtime session and inspect recovery/audit events.
6. Start the UI and verify runtime, capital, positions, trades, and events.

Rollback uses the previous wheel against the backed-up runtime data. Do not
delete or recreate the runtime database during an upgrade.
# Deployment

Run `ftx-paper-api`. The API process contains the live paper runtime; deploy
one instance per runtime state directory. Zerodha credentials and instrument
configuration must be available to that process.
