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
    EXECUTION_EVENT_TYPES,
    LEGACY_DECISION_TIME_FIELDS,
    RISK_EVENT_TYPES,
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
                CREATE INDEX IF NOT EXISTS idx_runtime_events_id_desc
                    ON runtime_events(id DESC);
                CREATE INDEX IF NOT EXISTS idx_runtime_events_type_id
                    ON runtime_events(event_type, id DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS ux_runtime_events_idempotency
                    ON runtime_events(idempotency_key)
                    WHERE idempotency_key IS NOT NULL;
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
                CREATE INDEX IF NOT EXISTS ix_runtime_events_event_type
                    ON runtime_events(event_type);
                CREATE INDEX IF NOT EXISTS ix_runtime_events_decision_id
                    ON runtime_events(json_extract(payload, '$.decision_id'));
                CREATE TABLE IF NOT EXISTS replay_runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    request TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            connection.execute(
                "UPDATE market_bars SET instrument_type = 'INDEX' "
                "WHERE upper(symbol) IN ('INDIA VIX', 'INDIAVIX')"
            )
            connection.execute(
                "UPDATE market_bars SET expiry = NULL, strike = NULL, option_type = NULL "
                "WHERE upper(instrument_type) = 'INDEX'"
            )
            connection.execute(
                "UPDATE market_bars SET strike = NULL, option_type = NULL "
                "WHERE upper(instrument_type) IN ('FUT', 'FUTURES')"
            )
            connection.execute(
                "UPDATE market_bars SET open_interest = 0.0 "
                "WHERE upper(instrument_type) IN ('FUT', 'FUTURES') AND open_interest IS NULL"
            )
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
                (bar.open_interest if bar.open_interest is not None else 0.0
                 if str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES"} else None),
                ("FUT" if str(bar.instrument.instrument_type).upper() == "FUTURES"
                 else "INDEX" if bar.instrument.symbol.upper() in {"INDIA VIX", "INDIAVIX"}
                 else bar.instrument.instrument_type),
                bar.instrument.expiry if str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES", "CE", "PE"} else None,
                bar.instrument.strike if str(bar.instrument.instrument_type).upper() in {"CE", "PE"} else None,
                str(bar.instrument.option_type) if str(bar.instrument.instrument_type).upper() in {"CE", "PE"} and bar.instrument.option_type is not None else None,
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

    def read_market_dates(self) -> list[str]:
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute(
                "SELECT DISTINCT substr(minute, 1, 10) FROM market_bars ORDER BY 1"
            ).fetchall()
        return [str(row[0]) for row in rows]

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

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

    def create_replay_run(self, run_id: str, request: dict[str, Any]) -> dict[str, Any]:
        now = self._now()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "INSERT INTO replay_runs(run_id, status, request, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (run_id, "queued", json.dumps(_json_safe(request, path="replay request")), now, now),
            )
        return self.read_replay_run(run_id) or {}

    def update_replay_run(self, run_id: str, *, status: str, result: dict[str, Any] | None = None,
                          error: str | None = None) -> None:
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE replay_runs SET status=?, result=COALESCE(?, result), error=?, updated_at=? WHERE run_id=?",
                (status, json.dumps(_json_safe(result, path="replay result")) if result is not None else None,
                 error, self._now(), run_id),
            )

    def read_replay_run(self, run_id: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM replay_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["request"] = json.loads(value["request"])
        value["result"] = json.loads(value["result"]) if value["result"] else None
        return value

    def read_replay_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        with sqlite3.connect(self.database) as connection:
            ids = connection.execute("SELECT run_id FROM replay_runs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [run for row in ids if (run := self.read_replay_run(str(row[0]))) is not None]

    def clear_replay_runs(self) -> int:
        """Delete diagnostic replay history without touching live runtime data."""
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute("DELETE FROM replay_runs")
            deleted = cursor.rowcount
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

    def _events_from_rows(self, rows: Iterable[tuple[str, str, str]]) -> list[dict[str, Any]]:
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

    def read_events(self, limit: int = 100) -> list[dict[str, Any]]:
        if limit < 1:
            return []
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute(
                "SELECT event_type, payload, created_at FROM runtime_events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return self._events_from_rows(rows)

    def read_decision_events(self, session_date: str | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        """Read decision events on their own, independent of the general event stream.

        ``session_date`` filters on the IST calendar date of ``decision_at``. Decision
        Rows without a canonical timestamp are excluded rather than failing the
        whole projection.
        """
        if limit < 1:
            return []
        event_types = tuple(sorted(DECISION_EVENT_TYPES))
        placeholders = ", ".join("?" for _ in event_types)
        sql = (
            "SELECT event_type, payload, created_at FROM runtime_events "
            f"WHERE event_type IN ({placeholders}) "
            "AND json_extract(payload, '$.decision_at') <> ''"
            " AND (json_extract(payload, '$.decision_source') IS NULL "
            "OR lower(json_extract(payload, '$.decision_source')) <> 'replay')"
        )
        params: list[Any] = list(event_types)
        if session_date:
            sql += " AND substr(json_extract(payload, '$.decision_at'), 1, 10) = ?"
            params.append(session_date)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute(sql, params).fetchall()
        return self._events_from_rows(rows)

    def read_decision_lifecycle_events(self, decision_ids: Iterable[str]) -> list[dict[str, Any]]:
        """Read execution and risk events for the given decision ids."""
        ids = sorted({str(item) for item in decision_ids if item})
        if not ids:
            return []
        event_types = tuple(sorted(EXECUTION_EVENT_TYPES | RISK_EVENT_TYPES))
        type_placeholders = ", ".join("?" for _ in event_types)
        events: list[dict[str, Any]] = []
        chunk_size = 400
        with sqlite3.connect(self.database) as connection:
            for start in range(0, len(ids), chunk_size):
                window = ids[start:start + chunk_size]
                id_placeholders = ", ".join("?" for _ in window)
                rows = connection.execute(
                    "SELECT event_type, payload, created_at FROM runtime_events "
                    f"WHERE event_type IN ({type_placeholders}) "
                    f"AND json_extract(payload, '$.decision_id') IN ({id_placeholders}) "
                    "ORDER BY id DESC",
                    (*event_types, *window),
                ).fetchall()
                events.extend(self._events_from_rows(rows))
        return events

    def event_counts(self) -> dict[str, Any]:
        if self._event_counts_cache is not None:
            return {key: dict(value) for key, value in self._event_counts_cache.items()}
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute("SELECT event_type, COUNT(*) FROM runtime_events GROUP BY event_type").fetchall()
            reasons = connection.execute("SELECT json_extract(payload, '$.reason'), COUNT(*) FROM runtime_events WHERE json_extract(payload, '$.reason') IS NOT NULL GROUP BY json_extract(payload, '$.reason')").fetchall()
        self._event_counts_cache = {"by_event_type": {str(kind): int(count) for kind, count in rows},
                                    "by_rejection_reason": {str(reason): int(count) for reason, count in reasons}}
        return {key: dict(value) for key, value in self._event_counts_cache.items()}
