from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class RuntimeStore:
    """Persistence boundary for the worker and API; no strategy logic belongs here."""

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
                CREATE TABLE IF NOT EXISTS runtime_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    command TEXT NOT NULL,
                    consumed_at TEXT
                );
                """
            )

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
        if status.get("state") != "RUNNING":
            return False
        self.patch_status({"state": "RECOVERY_REQUIRED"})
        self.append_event("RUNTIME_RECOVERY", {"previous_state": "RUNNING"}, "recovery:running")
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

    def enqueue_command(self, command: str) -> int:
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute("INSERT INTO runtime_commands(command) VALUES (?)", (command,))
            return int(cursor.lastrowid)

    def claim_commands(self, limit: int = 10) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute("SELECT id, command FROM runtime_commands WHERE consumed_at IS NULL ORDER BY id LIMIT ?", (limit,)).fetchall()
            for command_id, _ in rows:
                connection.execute("UPDATE runtime_commands SET consumed_at=? WHERE id=?", (now, command_id))
        return [{"id": command_id, "command": command} for command_id, command in rows]
