from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .events import DECISION_EVENT_TYPES, LEGACY_DECISION_TIME_FIELDS, event_category, is_decision_event


class ProcessAlreadyRunningError(RuntimeError):
    """Raised when another live process owns a runtime service lease."""


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        # Windows can report invalid or stale PIDs as WinError 87 instead of
        # raising ProcessLookupError. Treat those leases as recoverable.
        return False
    return True


class RuntimeStore:
    """Persistence boundary for the runtime session and API."""

    def __init__(self, runtime_dir: str | Path) -> None:
        self.root = Path(runtime_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.database = self.root / "runtime.sqlite3"
        self._event_counts_cache: dict[str, Any] | None = None
        self.decision_timestamp_migration: dict[str, int] = {"migrated": 0, "unmigratable": 0}
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
                CREATE TABLE IF NOT EXISTS process_leases (
                    service TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    instance_id TEXT NOT NULL
                );
                """
            )
            columns = {row[1] for row in connection.execute("PRAGMA table_info(runtime_events)")}
            if "timestamp" in columns and "created_at" not in columns:
                connection.execute("ALTER TABLE runtime_events RENAME COLUMN timestamp TO created_at")
                columns.remove("timestamp")
                columns.add("created_at")
            elif "timestamp" in columns and "created_at" in columns:
                connection.execute(
                    "UPDATE runtime_events SET created_at = COALESCE(created_at, timestamp) "
                    "WHERE created_at IS NULL"
                )
                connection.execute("ALTER TABLE runtime_events DROP COLUMN timestamp")
                columns.remove("timestamp")
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
            connection.execute(
                "CREATE TABLE IF NOT EXISTS runtime_migrations ("
                "name TEXT PRIMARY KEY, applied_at TEXT NOT NULL, migrated_rows INTEGER NOT NULL, "
                "unmigratable_rows INTEGER NOT NULL)"
            )
            self._migrate_decision_minutes(connection)

    def _migrate_decision_minutes(self, connection: sqlite3.Connection) -> None:
        """Copy legacy decision timestamps into ``minute`` exactly once."""
        marker = connection.execute(
            "SELECT 1 FROM runtime_migrations WHERE name = 'decision-minute-v1'"
        ).fetchone()
        if marker is not None:
            row = connection.execute(
                "SELECT migrated_rows, unmigratable_rows FROM runtime_migrations "
                "WHERE name = 'decision-minute-v1'"
            ).fetchone()
            if row is not None:
                self.decision_timestamp_migration = {"migrated": int(row[0]), "unmigratable": int(row[1])}
            return

        migrated = 0
        unmigratable = 0
        placeholders = ", ".join("?" for _ in DECISION_EVENT_TYPES)
        rows = connection.execute(
            f"SELECT id, payload FROM runtime_events WHERE event_type IN ({placeholders})",
            tuple(sorted(DECISION_EVENT_TYPES)),
        ).fetchall()
        for row in rows:
            payload = json.loads(row[1])
            minute = payload.get("minute")
            if not isinstance(minute, str) or not minute.strip():
                minute = next(
                    (payload.get(name) for name in LEGACY_DECISION_TIME_FIELDS
                     if isinstance(payload.get(name), str) and payload.get(name).strip()),
                    None,
                )
                if minute is None:
                    unmigratable += 1
                    continue
                payload["minute"] = minute
                migrated += 1
            normalized = self._normalize_decision_minute(payload)
            if normalized != payload.get("minute"):
                payload["minute"] = normalized
                migrated += 1
            for name in LEGACY_DECISION_TIME_FIELDS:
                payload.pop(name, None)
            connection.execute(
                "UPDATE runtime_events SET payload = ? WHERE id = ?",
                (json.dumps(payload), row[0]),
            )
        applied_at = datetime.now(timezone.utc).isoformat()
        connection.execute(
            "INSERT INTO runtime_migrations(name, applied_at, migrated_rows, unmigratable_rows) "
            "VALUES ('decision-minute-v1', ?, ?, ?)",
            (applied_at, migrated, unmigratable),
        )
        self.decision_timestamp_migration = {"migrated": migrated, "unmigratable": unmigratable}

    @staticmethod
    def _normalize_decision_minute(payload: dict[str, Any]) -> str | None:
        """Expand a legacy time-only minute using its persisted session date."""
        minute = payload.get("minute")
        session_date = payload.get("session_date")
        if not isinstance(minute, str) or "T" in minute or not isinstance(session_date, str):
            return minute if isinstance(minute, str) else None
        value = minute.strip()
        if len(value) == 5:
            return f"{session_date}T{value}:00+05:30"
        return minute

    def acquire_process_lease(self, service: str) -> str:
        """Atomically claim a service lease, removing leases for dead PIDs."""
        pid = os.getpid()
        instance_id = str(uuid4())
        started_at = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.database, timeout=10) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT pid FROM process_leases WHERE service = ?", (service,)
            ).fetchone()
            if row is not None:
                existing_pid = int(row[0])
                if _pid_is_alive(existing_pid):
                    connection.rollback()
                    raise ProcessAlreadyRunningError(
                        f"{service} process already running with PID {existing_pid}"
                    )
                connection.execute("DELETE FROM process_leases WHERE service = ?", (service,))
            connection.execute(
                "INSERT INTO process_leases(service, pid, started_at, instance_id) VALUES (?, ?, ?, ?)",
                (service, pid, started_at, instance_id),
            )
            connection.commit()
        return instance_id

    def release_process_lease(self, service: str, instance_id: str) -> None:
        """Release only the lease owned by this process instance."""
        with sqlite3.connect(self.database, timeout=10) as connection:
            connection.execute(
                "DELETE FROM process_leases WHERE service = ? AND pid = ? AND instance_id = ?",
                (service, os.getpid(), instance_id),
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

    def append_event(self, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None,
                     *, timestamp: str | None = None) -> bool:
        event_type = str(event_type).upper()
        payload = dict(payload)
        if is_decision_event(event_type):
            if not isinstance(payload.get("minute"), str) or not payload["minute"].strip():
                raise ValueError("decision events require canonical payload field 'minute'")
            payload["minute"] = self._normalize_decision_minute(payload)
            for name in LEGACY_DECISION_TIME_FIELDS:
                payload.pop(name, None)
        # ``timestamp`` is retained as a source-compatibility argument only;
        # it must never become the persistence timestamp or decision time.
        event_timestamp = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO runtime_events(event_type, payload, created_at, idempotency_key) VALUES (?, ?, ?, ?)",
                (event_type, json.dumps(payload), event_timestamp, idempotency_key),
            )
            inserted = cursor.rowcount == 1
        if inserted:
            self._event_counts_cache = None
        return inserted

    def clear_replay_events(self, session_date: str) -> int:
        """Remove only startup-replay audit rows for one trading date."""
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute(
                "DELETE FROM runtime_events WHERE json_extract(payload, '$.decision_source') = 'replay' "
                "AND json_extract(payload, '$.session_date') = ?",
                (session_date,),
            )
            deleted = cursor.rowcount
        self._event_counts_cache = None
        return deleted

    def recover_interrupted(self) -> bool:
        status = self.read_status()
        if status.get("state") not in {"RUNNING", "STARTING", "START_REQUESTED"}:
            return False
        previous_state = status.get("state")
        # Paper trading has no external positions to reconcile.  An interrupted
        # process can therefore resume from a clean stopped state; retain an
        # audit event so the interruption remains visible in the UI.
        self.patch_status({
            "state": "STOPPED",
            "feed_connected": False,
            "recovered_from": previous_state,
        })
        self.append_event(
            "RUNTIME_RECOVERY",
            {"previous_state": previous_state, "resulting_state": "STOPPED"},
            "recovery:interrupted",
        )
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
            {"event_type": event_type, "category": event_category(event_type),
             "payload": json.loads(payload), "created_at": created_at}
            for event_type, payload, created_at in rows
        ]

    def event_counts(self) -> dict[str, Any]:
        if self._event_counts_cache is not None:
            return {key: dict(value) for key, value in self._event_counts_cache.items()}
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute("SELECT event_type, COUNT(*) FROM runtime_events GROUP BY event_type").fetchall()
            reasons = connection.execute("SELECT json_extract(payload, '$.reason'), COUNT(*) FROM runtime_events WHERE json_extract(payload, '$.reason') IS NOT NULL GROUP BY json_extract(payload, '$.reason')").fetchall()
        self._event_counts_cache = {"by_event_type": {str(kind): int(count) for kind, count in rows},
                                    "by_rejection_reason": {str(reason): int(count) for reason, count in reasons}}
        return {key: dict(value) for key, value in self._event_counts_cache.items()}
