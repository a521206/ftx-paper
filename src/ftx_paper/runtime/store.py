from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo
from collections.abc import Iterable

from ftx_paper.contracts import MarketBar, MarketRole, OptionRole, role_to_key
from .events import (
    DECISION_EVENT_TYPES,
    LEGACY_DECISION_TIME_FIELDS,
    DecisionTimestampError,
    event_category,
    is_decision_event,
    parse_decision_at,
    serialize_datetime,
)


class ProcessAlreadyRunningError(RuntimeError):
    """Raised when another live process owns a runtime service lease."""


def _json_safe(value: Any, *, path: str) -> Any:
    """Convert supported domain values before writing JSON to the store."""
    if isinstance(value, datetime):
        return serialize_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            role_to_key(key) if isinstance(key, (MarketRole, OptionRole)) else key: _json_safe(item, path=f"{path}.{key}")
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"{path} contains unsupported JSON value of type {type(value).__name__}")


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
        self.decision_timestamp_migration: dict[str, Any] = {
            "migrated": 0, "unmigratable": 0, "unmigratable_rows": [],
        }
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
                CREATE TABLE IF NOT EXISTS expiry_dates (
                    date TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    copied_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS market_bars (
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    minute TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL,
                    open_interest REAL,
                    instrument_type TEXT,
                    expiry TEXT,
                    strike REAL,
                    option_type TEXT,
                    source TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, exchange, minute)
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
                "unmigratable_rows INTEGER NOT NULL, unmigratable_details TEXT NOT NULL DEFAULT '[]')"
            )
            migration_columns = {row[1] for row in connection.execute("PRAGMA table_info(runtime_migrations)")}
            if "unmigratable_details" not in migration_columns:
                connection.execute("ALTER TABLE runtime_migrations ADD COLUMN unmigratable_details TEXT NOT NULL DEFAULT '[]'")
            self._migrate_decision_timestamps(connection)

    def _migrate_decision_timestamps(self, connection: sqlite3.Connection) -> None:
        """Explicitly migrate legacy decision fields into ``decision_at``."""
        marker = connection.execute(
            "SELECT 1 FROM runtime_migrations WHERE name = 'decision-at-v2'"
        ).fetchone()
        if marker is not None:
            row = connection.execute(
                "SELECT migrated_rows, unmigratable_rows, unmigratable_details FROM runtime_migrations "
                "WHERE name = 'decision-at-v2'"
            ).fetchone()
            if row is not None:
                self.decision_timestamp_migration = {
                    "migrated": int(row[0]), "unmigratable": int(row[1]),
                    "unmigratable_rows": json.loads(row[2] or "[]"),
                }
            return

        migrated = 0
        unmigratable = 0
        unmigratable_rows: list[dict[str, str | int]] = []
        placeholders = ", ".join("?" for _ in DECISION_EVENT_TYPES)
        rows = connection.execute(
            f"SELECT id, payload FROM runtime_events WHERE event_type IN ({placeholders})",
            tuple(sorted(DECISION_EVENT_TYPES)),
        ).fetchall()
        for row in rows:
            payload = json.loads(row[1])
            candidates = [(name, payload.get(name)) for name in LEGACY_DECISION_TIME_FIELDS
                          if payload.get(name) not in (None, "")]
            if payload.get("decision_at") not in (None, ""):
                candidates.insert(0, ("decision_at", payload.get("decision_at")))
            if not candidates:
                unmigratable += 1
                unmigratable_rows.append({"id": int(row[0]), "reason": "missing decision_at"})
                continue
            distinct = {str(value).strip() for _, value in candidates}
            if len(distinct) > 1 and "decision_at" not in {name for name, _ in candidates}:
                unmigratable += 1
                unmigratable_rows.append({"id": int(row[0]), "reason": "conflicting legacy timestamps"})
                continue
            source_name, source_value = candidates[0]
            if source_name == "minute" and isinstance(source_value, str) and "T" not in source_value:
                session_date = payload.get("session_date")
                if not isinstance(session_date, str) or not session_date.strip():
                    unmigratable += 1
                    unmigratable_rows.append({"id": int(row[0]), "reason": "time-only minute lacks session_date"})
                    continue
                source_value = f"{session_date}T{source_value}:00+05:30"
            try:
                normalized = serialize_datetime(parse_decision_at(source_value))
            except (DecisionTimestampError, TypeError) as exc:
                unmigratable += 1
                unmigratable_rows.append({"id": int(row[0]), "reason": str(exc)})
                continue
            payload["decision_at"] = normalized
            for name in LEGACY_DECISION_TIME_FIELDS:
                payload.pop(name, None)
            if source_name != "decision_at":
                migrated += 1
            connection.execute(
                "UPDATE runtime_events SET payload = ? WHERE id = ?",
                (json.dumps(_json_safe(payload, path=f"event {row[0]} payload")), row[0]),
            )
        applied_at = datetime.now(timezone.utc).isoformat()
        connection.execute(
            "INSERT INTO runtime_migrations(name, applied_at, migrated_rows, unmigratable_rows, unmigratable_details) "
            "VALUES ('decision-at-v2', ?, ?, ?, ?)",
            (applied_at, migrated, unmigratable, json.dumps(
                _json_safe(unmigratable_rows, path="migration unmigratable_rows")
            )),
        )
        self.decision_timestamp_migration = {
            "migrated": migrated, "unmigratable": unmigratable,
            "unmigratable_rows": unmigratable_rows,
        }

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

    def read_expiry_dates(self) -> frozenset[str]:
        """Return the one-time imported weekly expiry calendar."""
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute("SELECT date FROM expiry_dates ORDER BY date").fetchall()
        return frozenset(str(row[0]) for row in rows)

    def append_market_bars(self, bars: Iterable[MarketBar], *, source: str) -> None:
        """Persist normalized historical or live bars in the runtime database."""
        now = self._now()
        rows = []
        for bar in bars:
            timestamp = bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata"))
            minute = timestamp.replace(second=0, microsecond=0).isoformat()
            rows.append((
                bar.instrument.symbol, bar.instrument.exchange, minute,
                bar.open, bar.high, bar.low, bar.close, bar.volume,
                bar.open_interest, bar.instrument.instrument_type,
                bar.instrument.expiry,
                bar.instrument.strike,
                str(bar.instrument.option_type) if bar.instrument.option_type is not None else None,
                source, now,
            ))
        if not rows:
            return
        with sqlite3.connect(self.database) as connection:
            connection.executemany(
                """
                INSERT OR REPLACE INTO market_bars
                (symbol, exchange, minute, open, high, low, close, volume,
                 open_interest, instrument_type, expiry, strike, option_type,
                 source, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def read_market_bars(self, session_date: str) -> list[dict[str, Any]]:
        """Read one IST trading session of normalized bars for export."""
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT symbol, exchange, minute, open, high, low, close, volume,
                       open_interest, instrument_type, expiry, strike, option_type,
                       source, ingested_at
                FROM market_bars
                WHERE substr(minute, 1, 10) = ?
                ORDER BY minute, exchange, symbol
                """,
                (session_date,),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def import_expiry_dates_once(self, dates: list[str], *, source: str) -> int:
        """Copy expiry dates once and record an idempotent migration marker."""
        migration_name = "weekly-expiry-dates-v1"
        normalized = sorted({str(value)[:10] for value in dates if str(value).strip()})
        if not normalized:
            raise ValueError("cannot apply expiry migration with no dates")
        now = self._now()
        with sqlite3.connect(self.database) as connection:
            if connection.execute(
                "SELECT 1 FROM runtime_migrations WHERE name = ?", (migration_name,)
            ).fetchone() is not None:
                return 0
            connection.executemany(
                "INSERT OR IGNORE INTO expiry_dates(date, source, copied_at) VALUES (?, ?, ?)",
                [(date, source, now) for date in normalized],
            )
            connection.execute(
                "INSERT INTO runtime_migrations(name, applied_at, migrated_rows, unmigratable_rows, unmigratable_details) VALUES (?, ?, ?, 0, '[]')",
                (migration_name, now, len(normalized)),
            )
        return len(normalized)

    def write_status(self, status: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                """
                INSERT INTO runtime_status(id, payload, updated_at) VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at
                """,
                (json.dumps(_json_safe(status, path="status")), now),
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
            if "decision_at" not in payload:
                raise DecisionTimestampError("decision events require canonical payload field 'decision_at'")
            normalized = parse_decision_at(payload["decision_at"])
            payload["decision_at"] = serialize_datetime(normalized)
            forbidden = set(LEGACY_DECISION_TIME_FIELDS) & payload.keys()
            if forbidden:
                raise DecisionTimestampError(
                    "decision events cannot contain legacy decision timestamp fields: "
                    + ", ".join(sorted(forbidden))
                )
        # ``timestamp`` is retained as a source-compatibility argument only;
        # it must never become the persistence timestamp or decision time.
        event_timestamp = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO runtime_events(event_type, payload, created_at, idempotency_key) VALUES (?, ?, ?, ?)",
                (event_type, json.dumps(_json_safe(payload, path=f"event {event_type} payload")), event_timestamp, idempotency_key),
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
        result = []
        for event_type, raw_payload, raw_created_at in rows:
            payload = json.loads(raw_payload)
            created = datetime.fromisoformat(str(raw_created_at).replace("Z", "+00:00"))
            if created.tzinfo is None or created.utcoffset() is None:
                raise ValueError(f"event {event_type} has timezone-naive created_at")
            if is_decision_event(event_type):
                payload["decision_at"] = parse_decision_at(payload.get("decision_at"))
            result.append({"event_type": event_type, "category": event_category(event_type),
                           "payload": payload, "created_at": created})
        return result

    def event_counts(self) -> dict[str, Any]:
        if self._event_counts_cache is not None:
            return {key: dict(value) for key, value in self._event_counts_cache.items()}
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute("SELECT event_type, COUNT(*) FROM runtime_events GROUP BY event_type").fetchall()
            reasons = connection.execute("SELECT json_extract(payload, '$.reason'), COUNT(*) FROM runtime_events WHERE json_extract(payload, '$.reason') IS NOT NULL GROUP BY json_extract(payload, '$.reason')").fetchall()
        self._event_counts_cache = {"by_event_type": {str(kind): int(count) for kind, count in rows},
                                    "by_rejection_reason": {str(reason): int(count) for reason, count in reasons}}
        return {key: dict(value) for key, value in self._event_counts_cache.items()}
