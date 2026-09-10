from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class RuntimeStore:
    """Persistence boundary for the runtime session and API."""

    def __init__(self, runtime_dir: str | Path) -> None:
        self.root = Path(runtime_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.database = self.root / "runtime.sqlite3"
        with sqlite3.connect(self.database) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runtime_status (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runtime_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    idempotency_key TEXT UNIQUE
                );
                """
            )
            columns = {row[1] for row in connection.execute("PRAGMA table_info(runtime_events)")}
            if "idempotency_key" not in columns:
                connection.execute("ALTER TABLE runtime_events ADD COLUMN idempotency_key TEXT")
            connection.execute(
                "DELETE FROM runtime_events WHERE idempotency_key IS NOT NULL AND rowid NOT IN "
                "(SELECT MIN(rowid) FROM runtime_events WHERE idempotency_key IS NOT NULL GROUP BY idempotency_key)"
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_runtime_events_idempotency "
                "ON runtime_events(idempotency_key) WHERE idempotency_key IS NOT NULL"
            )
            connection.execute("DROP TABLE IF EXISTS runtime_commands")

    def read_status(self) -> dict[str, Any]:
        with sqlite3.connect(self.database) as connection:
            row = connection.execute("SELECT payload FROM runtime_status WHERE id = 1").fetchone()
        return json.loads(row[0]) if row else {}

    def write_status(self, status: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                """
                INSERT INTO runtime_status(id, payload, updated_at) VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at
                """,
                (json.dumps(status), now),
            )

    def patch_status(self, updates: dict[str, Any]) -> None:
        current = self.read_status()
        current.update(updates)
        self.write_status(current)

    def append_event(self, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None) -> bool:
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO runtime_events(event_type, payload, created_at, idempotency_key) VALUES (?, ?, ?, ?)",
                (event_type, json.dumps(payload), datetime.now(timezone.utc).isoformat(), idempotency_key),
            )
            return cursor.rowcount == 1

    def recover_interrupted(self) -> bool:
        status = self.read_status()
        if status.get("state") not in {"RUNNING", "STARTING", "START_REQUESTED"}:
            return False
        previous_state = status.get("state")
        self.patch_status({"state": "RECOVERY_REQUIRED", "recovered_from": previous_state})
        self.append_event("RUNTIME_RECOVERY", {"previous_state": previous_state}, "recovery:interrupted")
        return True

    def read_events(self, limit: int = 100) -> list[dict[str, Any]]:
        if limit < 1:
            return []
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute(
                "SELECT event_type, payload, created_at FROM runtime_events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {"event_type": event_type, "payload": json.loads(payload), "created_at": created_at}
            for event_type, payload, created_at in rows
        ]
