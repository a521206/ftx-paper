from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, TypedDict
from uuid import uuid4
from zoneinfo import ZoneInfo
from collections.abc import Iterable

from ftx_paper.contracts import (
    MarketBar, MarketRole, OptionRole, OrderLifecycle, OrderLifecycleState, is_legal_order_transition,
    role_to_key,
)
from ftx_paper.runtime.events import (
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


class DecisionSummary(TypedDict):
    detections: int
    candidates: int
    accepted: int
    rejected: int
    executed: int


class DecisionSummaryReport(TypedDict):
    totals: DecisionSummary
    sessions: dict[str, DecisionSummary]


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


class SqliteRuntimeStore:
    """Persistence boundary for the runtime session and API."""

    def __init__(self, runtime_dir: str | Path) -> None:
        self.root = Path(runtime_dir)
        self.database = self.root / "runtime.sqlite3"
        self._event_counts_cache: dict[str, Any] | None = None
        self._read_only = False
        self._status_lock = threading.RLock()
        self.initialize()

    @classmethod
    def open_read_only(cls, runtime_dir: str | Path) -> SqliteRuntimeStore:
        """Open an existing runtime without creating directories or schema."""
        instance = cls.__new__(cls)
        instance.root = Path(runtime_dir)
        instance.database = instance.root / "runtime.sqlite3"
        instance._event_counts_cache = None
        instance._read_only = True
        instance._status_lock = threading.RLock()
        if not instance.database.is_file():
            raise FileNotFoundError(f"runtime database not found: {instance.database}")
        return instance

    def _connect(self, *, timeout: float = 30) -> sqlite3.Connection:
        if self._read_only:
            uri = f"file:{self.database.resolve().as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=timeout)
        else:
            connection = sqlite3.connect(self.database, timeout=timeout)
        connection.execute(f"PRAGMA busy_timeout = {int(timeout * 1000)}")
        return connection

    def initialize(self) -> None:
        """Create the runtime directory and schema for a writable store."""
        self.root.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
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
                    instrument_type TEXT NOT NULL,
                    expiry TEXT NOT NULL,
                    strike REAL,
                    option_type TEXT,
                    source TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, exchange, minute, expiry)
                );
                CREATE TABLE IF NOT EXISTS market_dates (
                    date TEXT PRIMARY KEY
                );
                CREATE INDEX IF NOT EXISTS ix_market_bars_session_minute
                    ON market_bars(substr(minute, 1, 10), minute, exchange, symbol);
                CREATE INDEX IF NOT EXISTS ix_runtime_events_event_type
                    ON runtime_events(event_type);
                CREATE INDEX IF NOT EXISTS ix_runtime_events_decision_id
                    ON runtime_events(json_extract(payload, '$.decision_id'));
                CREATE INDEX IF NOT EXISTS ix_runtime_events_decision_date
                    ON runtime_events(substr(json_extract(payload, '$.decision_at'), 1, 10));
                CREATE TABLE IF NOT EXISTS replay_runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    request TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runtime_contracts (
                    exchange TEXT NOT NULL,
                    underlying TEXT NOT NULL,
                    role TEXT NOT NULL,
                    tradingsymbol TEXT NOT NULL,
                    instrument_token INTEGER NOT NULL,
                    expiry TEXT NOT NULL,
                    selected_at TEXT NOT NULL,
                    PRIMARY KEY (exchange, tradingsymbol)
                );
                CREATE TABLE IF NOT EXISTS runtime_orders (
                    client_order_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    account_revision INTEGER,
                    broker_order_id TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )
            primary_key = [
                row[1] for row in connection.execute("PRAGMA table_info(runtime_contracts)")
                if row[5]
            ]
            if primary_key == ["exchange", "underlying", "role"]:
                connection.executescript(
                    """
                    CREATE TABLE runtime_contracts_v2 (
                        exchange TEXT NOT NULL,
                        underlying TEXT NOT NULL,
                        role TEXT NOT NULL,
                        tradingsymbol TEXT NOT NULL,
                        instrument_token INTEGER NOT NULL,
                        expiry TEXT NOT NULL,
                        selected_at TEXT NOT NULL,
                        PRIMARY KEY (exchange, tradingsymbol)
                    );
                    INSERT INTO runtime_contracts_v2
                    SELECT exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at
                    FROM runtime_contracts;
                    DROP TABLE runtime_contracts;
                    ALTER TABLE runtime_contracts_v2 RENAME TO runtime_contracts;
                    """
                )
            connection.execute(
                "UPDATE market_bars SET instrument_type = 'INDEX' "
                "WHERE upper(symbol) IN ('NIFTY', 'NIFTY 50', 'INDIA VIX')"
            )
            connection.execute(
                "UPDATE market_bars SET expiry = '', strike = NULL, option_type = NULL "
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
            # Historical option rows are also the authoritative expiry source
            # when an external calendar has not been imported yet.
            connection.execute(
                "INSERT OR IGNORE INTO expiry_dates(date, source, copied_at) "
                "SELECT DISTINCT expiry, 'market_bars', ? FROM market_bars "
                "WHERE upper(instrument_type) IN ('CE', 'PE') "
                "AND expiry IS NOT NULL AND expiry <> ''",
                (datetime.now(timezone.utc).isoformat(),),
            )
        self.compact_replay_runs()

    def compact_replay_runs(self) -> int:
        """Remove legacy all-date and duplicate replay snapshots."""
        with self._connect() as connection:
            cursor = connection.execute(
                """
                DELETE FROM replay_runs
                WHERE json_extract(request, '$.session_date') IS NULL
                   OR json_extract(request, '$.session_date') = ''
                   OR run_id IN (
                       SELECT run_id FROM (
                           SELECT run_id,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY json_extract(request, '$.session_date')
                                      ORDER BY created_at DESC
                                  ) AS rank_for_day
                           FROM replay_runs
                           WHERE json_extract(request, '$.session_date') IS NOT NULL
                             AND json_extract(request, '$.session_date') <> ''
                       ) WHERE rank_for_day > 1
                   )
                """
            )
            return cursor.rowcount
    def acquire_process_lease(self, service: str) -> str:
        """Atomically claim a service lease, removing leases for dead PIDs."""
        pid = os.getpid()
        instance_id = str(uuid4())
        started_at = datetime.now(timezone.utc).isoformat()
        with self._connect(timeout=10) as connection:
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
        with self._connect(timeout=10) as connection:
            connection.execute(
                "DELETE FROM process_leases WHERE service = ? AND pid = ? AND instance_id = ?",
                (service, os.getpid(), instance_id),
            )

    def read_status(self) -> dict[str, Any]:
        with self._status_lock, self._connect() as connection:
            row = connection.execute("SELECT payload FROM runtime_status WHERE id = 1").fetchone()
        return json.loads(row[0]) if row else {}

    def read_runtime_contract(self, *, exchange: str, underlying: str, role: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at "
                "FROM runtime_contracts WHERE exchange = ? AND underlying = ? AND role = ? "
                "ORDER BY expiry, tradingsymbol LIMIT 1",
                (exchange, underlying, role),
            ).fetchone()
        return dict(row) if row else None

    def read_runtime_contracts(self, *, exchange: str, underlying: str, role: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at "
                "FROM runtime_contracts WHERE exchange = ? AND underlying = ? AND role = ? "
                "ORDER BY expiry, tradingsymbol",
                (exchange, underlying, role),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_runtime_contract(self, contract: dict[str, Any]) -> bool:
        required = ("exchange", "underlying", "role", "tradingsymbol", "instrument_token", "expiry")
        if any(contract.get(key) in (None, "") for key in required):
            raise ValueError("runtime contract is missing required identity fields")
        selected_at = datetime.now(timezone.utc).isoformat()
        event_payload = {**contract, "selected_at": selected_at}
        with self._connect() as connection:
            previous = connection.execute(
                "SELECT tradingsymbol, instrument_token, expiry FROM runtime_contracts "
                "WHERE exchange = ? AND tradingsymbol = ?",
                (contract["exchange"], contract["tradingsymbol"]),
            ).fetchone()
            changed = previous is None or tuple(previous) != (
                contract["tradingsymbol"], int(contract["instrument_token"]), str(contract["expiry"]),
            )
            connection.execute(
                "INSERT INTO runtime_contracts "
                "(exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(exchange, tradingsymbol) DO UPDATE SET "
                "underlying=excluded.underlying, role=excluded.role, instrument_token=excluded.instrument_token, "
                "expiry=excluded.expiry, selected_at=excluded.selected_at",
                (contract["exchange"], contract["underlying"], contract["role"],
                 contract["tradingsymbol"], int(contract["instrument_token"]), str(contract["expiry"]), selected_at),
            )
            if changed:
                connection.execute(
                    "INSERT OR IGNORE INTO runtime_events "
                    "(event_type, payload, created_at, idempotency_key) VALUES (?, ?, ?, ?)",
                    (
                        "CONTRACT_CHANGED", json.dumps(_json_safe(event_payload, path="contract event")),
                        selected_at,
                        f"contract:{contract['exchange']}:{contract['underlying']}:{contract['role']}"
                        f":{contract['tradingsymbol']}:{contract['expiry']}:{selected_at}",
                    ),
                )
        return changed

    def read_expiry_dates(self) -> frozenset[str]:
        """Return the one-time imported weekly expiry calendar."""
        with self._connect() as connection:
            rows = connection.execute("SELECT date FROM expiry_dates ORDER BY date").fetchall()
        return frozenset(str(row[0]) for row in rows)

    def replace_expiry_dates(self, expiry_dates: Iterable[str], *, source: str) -> bool:
        """Replace the imported expiry reference calendar without touching replay state."""
        if self._read_only:
            raise RuntimeError("cannot update expiry calendar through a read-only store")
        if not source.strip():
            raise ValueError("expiry calendar source must be non-empty")
        dates = sorted({date.fromisoformat(str(value)).isoformat() for value in expiry_dates})
        if not dates:
            raise ValueError("expiry calendar must not be empty")
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT date, source FROM expiry_dates ORDER BY date",
            ).fetchall()
            if existing == [(value, source) for value in dates]:
                return False
            copied_at = datetime.now(timezone.utc).isoformat()
            connection.execute("DELETE FROM expiry_dates")
            connection.executemany(
                "INSERT INTO expiry_dates (date, source, copied_at) VALUES (?, ?, ?)",
                ((value, source, copied_at) for value in dates),
            )
        return True

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
                else "INDEX" if bar.instrument.symbol.upper() in {"NIFTY", "NIFTY 50", "INDIA VIX"}
                 else bar.instrument.instrument_type),
                bar.instrument.expiry if str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES", "CE", "PE"} and bar.instrument.expiry else "",
                bar.instrument.strike if str(bar.instrument.instrument_type).upper() in {"CE", "PE"} else None,
                str(bar.instrument.option_type) if str(bar.instrument.instrument_type).upper() in {"CE", "PE"} and bar.instrument.option_type is not None else None,
                source, now,
            ))
        if not rows:
            return
        with self._connect() as connection:
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
            connection.executemany(
                "INSERT OR IGNORE INTO market_dates(date) VALUES (?)",
                ((session_date,) for session_date in {row[2][:10] for row in rows}),
            )
            connection.executemany(
                "INSERT OR IGNORE INTO expiry_dates(date, source, copied_at) VALUES (?, ?, ?)",
                (
                    (str(row[10])[:10], source, now)
                    for row in rows
                    if str(row[9]).upper() in {"CE", "PE"} and row[10]
                ),
            )

    def read_market_bars(self, session_date: str) -> list[dict[str, Any]]:
        """Read one IST trading session of normalized bars for export."""
        with self._connect() as connection:
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
        with self._connect() as connection:
            rows = connection.execute("SELECT date FROM market_dates ORDER BY date").fetchall()
        return [str(row[0]) for row in rows]

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def write_status(self, status: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._status_lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO runtime_status(id, payload, updated_at) VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at
                """,
                (json.dumps(_json_safe(status, path="status")), now),
            )

    def patch_status(self, updates: dict[str, Any]) -> None:
        with self._status_lock:
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
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO runtime_events(event_type, payload, created_at, idempotency_key) VALUES (?, ?, ?, ?)",
                (event_type, json.dumps(_json_safe(payload, path=f"event {event_type} payload")), event_timestamp, idempotency_key),
            )
            inserted = cursor.rowcount == 1
        if inserted:
            self._event_counts_cache = None
        return inserted

    def create_order_lifecycle(self, client_order_id: str, payload: dict[str, Any], *, state: str = "PROPOSED",
                               account_revision: int | None = None) -> bool:
        """Durably register an order before any external broker call."""
        normalized_state = str(state).upper()
        try:
            OrderLifecycleState(normalized_state)
        except ValueError as exc:
            raise ValueError(f"unknown order lifecycle state: {state}") from exc
        order_id = str(client_order_id)
        serialized_payload = json.dumps(_json_safe(payload, path="order payload"), sort_keys=True)
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT payload, state FROM runtime_orders WHERE client_order_id=?", (order_id,)
            ).fetchone()
            if existing is not None:
                if json.dumps(json.loads(existing[0]), sort_keys=True) != serialized_payload:
                    raise ValueError("client order ID was reused with a different command")
                return False
            cursor = connection.execute(
                "INSERT OR IGNORE INTO runtime_orders(client_order_id, state, payload, account_revision, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (order_id, normalized_state, serialized_payload, account_revision, self._now()),
            )
        return cursor.rowcount == 1

    def update_order_lifecycle(self, client_order_id: str, *, state: str,
                               broker_order_id: str | None = None,
                               account_revision: int | None = None) -> bool:
        normalized_state = str(state).upper()
        try:
            OrderLifecycleState(normalized_state)
        except ValueError as exc:
            raise ValueError(f"unknown order lifecycle state: {state}") from exc
        with self._connect() as connection:
            current = connection.execute(
                "SELECT state FROM runtime_orders WHERE client_order_id=?", (str(client_order_id),)
            ).fetchone()
            if current is None or not is_legal_order_transition(str(current[0]), normalized_state):
                return False
            cursor = connection.execute(
                "UPDATE runtime_orders SET state=?, broker_order_id=COALESCE(?, broker_order_id), "
                "account_revision=COALESCE(?, account_revision), updated_at=? WHERE client_order_id=?",
                (normalized_state, broker_order_id, account_revision, self._now(), str(client_order_id)),
            )
        return cursor.rowcount == 1

    def read_order_lifecycle(self, client_order_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT * FROM runtime_orders WHERE client_order_id=?", (str(client_order_id),)
            ).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["payload"] = json.loads(value["payload"])
        return value

    def record_order_fill(self, client_order_id: str, *, fill_id: str, quantity: int) -> bool:
        """Persist one observed fill with duplicate and overfill protection."""
        if not fill_id or quantity <= 0:
            raise ValueError("fill ID and positive quantity are required")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state, payload FROM runtime_orders WHERE client_order_id=?",
                (str(client_order_id),),
            ).fetchone()
            if row is None:
                return False
            payload = json.loads(row[1])
            fill_ids = set(payload.get("fill_ids", []))
            if fill_id in fill_ids:
                return False
            requested = int(payload.get("quantity", 0))
            lifecycle = OrderLifecycle(
                requested,
                state=OrderLifecycleState(str(row[0])),
                filled_quantity=int(payload.get("filled_quantity", 0)),
            )
            changed = lifecycle.apply_fill(fill_id, quantity)
            if not changed:
                return False
            fill_ids.add(fill_id)
            payload.update(
                filled_quantity=lifecycle.filled_quantity,
                remaining_quantity=lifecycle.remaining_quantity,
                fill_ids=sorted(fill_ids),
            )
            connection.execute(
                "UPDATE runtime_orders SET state=?, payload=?, updated_at=? WHERE client_order_id=?",
                (lifecycle.state.value, json.dumps(_json_safe(payload, path="order payload")),
                 self._now(), str(client_order_id)),
            )
            return True

    def read_in_flight_orders(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT * FROM runtime_orders WHERE state NOT IN ('FILLED', 'CANCELLED', 'REJECTED') "
                "ORDER BY updated_at"
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            value["payload"] = json.loads(value["payload"])
            result.append(value)
        return result

    def clear_replay_events(self, session_date: str) -> int:
        """Remove only startup-replay audit rows for one trading date."""
        with self._connect() as connection:
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
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO replay_runs(run_id, status, request, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (run_id, "queued", json.dumps(_json_safe(request, path="replay request")), now, now),
            )
        return self.read_replay_run(run_id) or {}

    def update_replay_run(self, run_id: str, *, status: str, result: dict[str, Any] | None = None,
                          error: str | None = None) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE replay_runs SET status=?, result=COALESCE(?, result), error=?, updated_at=? WHERE run_id=?",
                (status, json.dumps(_json_safe(result, path="replay result")) if result is not None else None,
                 error, self._now(), run_id),
            )

    def transition_replay_run(self, run_id: str, *, expected_status: str, status: str,
                              result: dict[str, Any] | None = None, error: str | None = None) -> bool:
        """Guard replay state changes so racing workers cannot overwrite terminal state."""
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE replay_runs SET status=?, result=COALESCE(?, result), error=?, updated_at=? "
                "WHERE run_id=? AND status=?",
                (status, json.dumps(_json_safe(result, path="replay result")) if result is not None else None,
                 error, self._now(), run_id, expected_status),
            )
            return cursor.rowcount == 1

    def read_replay_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM replay_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["request"] = json.loads(value["request"])
        value["result"] = json.loads(value["result"]) if value["result"] else None
        return value

    def read_replay_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as connection:
            ids = connection.execute("SELECT run_id FROM replay_runs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [run for row in ids if (run := self.read_replay_run(str(row[0]))) is not None]

    def read_replay_run_summaries(self, limit: int = 100) -> list[dict[str, Any]]:
        """Read the newest stored replay for each requested trading day."""
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                WITH ranked AS (
                    SELECT run_id, status, request, created_at, updated_at, error,
                           ROW_NUMBER() OVER (
                               PARTITION BY json_extract(request, '$.session_date')
                               ORDER BY created_at DESC
                           ) AS rank_for_day
                    FROM replay_runs
                    WHERE json_extract(request, '$.session_date') IS NOT NULL
                      AND json_extract(request, '$.session_date') <> ''
                )
                SELECT run_id, status, request, created_at, updated_at, error
                FROM ranked WHERE rank_for_day = 1
                ORDER BY created_at DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "run_id": row["run_id"],
                "status": row["status"],
                "request": json.loads(row["request"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "error": row["error"],
                "result": None,
            }
            for row in rows
        ]

    def read_latest_replay_for_date(self, session_date: str) -> dict[str, Any] | None:
        """Return the newest replay row for one trading day, if any."""
        normalized = str(session_date).strip()[:10]
        if not normalized:
            return None
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id FROM replay_runs
                WHERE substr(json_extract(request, '$.session_date'), 1, 10) = ?
                ORDER BY created_at DESC LIMIT 1
                """,
                (normalized,),
            ).fetchone()
        return self.read_replay_run(str(row[0])) if row else None

    def clear_replay_runs(self) -> int:
        """Delete diagnostic replay history without touching live runtime data."""
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM replay_runs")
            deleted = cursor.rowcount
        return deleted

    def clear_replay_runs_for_date(self, session_date: str) -> int:
        """Delete stored replay runs whose requested session is this date."""
        normalized = str(session_date).strip()[:10]
        if not normalized:
            return 0
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM replay_runs "
                "WHERE substr(json_extract(request, '$.session_date'), 1, 10) = ?",
                (normalized,),
            )
            return cursor.rowcount

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
            "health_state": "STOPPED",
            "feed_connected": False,
            "recovered_from": previous_state,
        })
        with self._connect() as connection:
            connection.execute(
                "UPDATE runtime_orders SET state='RECONCILIATION_REQUIRED', updated_at=? "
                "WHERE state IN ('AUTHORIZED', 'SUBMITTING', 'ACKNOWLEDGED', 'PARTIALLY_FILLED')",
                (self._now(),),
            )
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
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT event_type, payload, created_at FROM runtime_events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return self._events_from_rows(rows)

    def read_market_bar_stats(self) -> dict[str, Any]:
        """Read market-bar coverage for diagnostics without mutating state."""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*), COUNT(DISTINCT substr(minute, 1, 10)), "
                "MIN(minute), MAX(minute) FROM market_bars",
            ).fetchone()
            instrument_rows = connection.execute(
                "SELECT COALESCE(instrument_type, 'UNKNOWN'), COUNT(*) "
                "FROM market_bars GROUP BY instrument_type ORDER BY instrument_type",
            ).fetchall()
        return {
            "bars": int(row[0]),
            "days": int(row[1]),
            "from": str(row[2])[:10] if row[2] else None,
            "to": str(row[3])[:10] if row[3] else None,
            "by_instrument": {str(kind): int(count) for kind, count in instrument_rows},
        }

    def read_decision_summary(self, session_date: str) -> DecisionSummaryReport:
        """Aggregate a complete session day without decoding event payloads."""
        event_types = tuple(sorted(DECISION_EVENT_TYPES))
        event_placeholders = ", ".join("?" for _ in event_types)
        executed_types = ("FILL", "EXECUTEDDECISION")
        executed_placeholders = ", ".join("?" for _ in executed_types)
        sql = (
            "SELECT event_type, substr(json_extract(payload, '$.decision_at'), 12, 5), COUNT(*), "
            "SUM(CASE WHEN EXISTS (SELECT 1 FROM runtime_events lifecycle "
            "WHERE lifecycle.event_type IN (" + executed_placeholders + ") "
            "AND json_extract(lifecycle.payload, '$.decision_id') = "
            "json_extract(decision.payload, '$.decision_id')) THEN 1 ELSE 0 END) "
            "FROM runtime_events decision "
            f"WHERE event_type IN ({event_placeholders}) "
            "AND substr(json_extract(payload, '$.decision_at'), 1, 10) = ? "
            "AND (json_extract(payload, '$.decision_source') IS NULL "
            "OR lower(json_extract(payload, '$.decision_source')) <> 'replay') "
            "GROUP BY event_type, substr(json_extract(payload, '$.decision_at'), 12, 5)"
        )
        with self._connect() as connection:
            rows = connection.execute(sql, (*executed_types, *event_types, session_date)).fetchall()

        sessions: dict[str, DecisionSummary] = {
            name: {"detections": 0, "candidates": 0, "accepted": 0, "rejected": 0, "executed": 0}
            for name in ("Pre", "Morning", "Mid", "Afternoon", "Post")
        }
        totals: DecisionSummary = {
            "detections": 0, "candidates": 0, "accepted": 0, "rejected": 0, "executed": 0,
        }
        for event_type, minute, count, executed in rows:
            if not minute:
                continue
            session_minute = int(minute[:2]) * 60 + int(minute[3:5])
            session_name = (
                "Pre" if session_minute < 615 else
                "Morning" if session_minute < 675 else
                "Mid" if session_minute < 810 else
                "Afternoon" if session_minute < 855 else "Post"
            )
            stats = sessions[session_name]
            amount = int(count)
            stats["detections"] += amount
            stats["candidates"] += amount if "CANDIDATE" in event_type else 0
            stats["accepted"] += amount if event_type == "ACCEPTEDDECISION" else 0
            stats["rejected"] += amount if event_type == "REJECTEDDECISION" else 0
            stats["executed"] += int(executed or 0)
        for stats in sessions.values():
            for key in totals:
                totals[key] += stats[key]
        return {"totals": totals, "sessions": sessions}

    def read_decision_events(
        self, session_date: str | None = None, limit: int = 50, *,
        before_id: int | None = None, session_name: str | None = None,
    ) -> list[dict[str, Any]]:
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
            "SELECT id, event_type, payload, created_at FROM runtime_events "
            f"WHERE event_type IN ({placeholders}) "
            "AND json_extract(payload, '$.decision_at') <> ''"
            " AND (json_extract(payload, '$.decision_source') IS NULL "
            "OR lower(json_extract(payload, '$.decision_source')) <> 'replay')"
        )
        params: list[Any] = list(event_types)
        if session_date:
            sql += " AND substr(json_extract(payload, '$.decision_at'), 1, 10) = ?"
            params.append(session_date)
        if session_name:
            session_ranges = {
                "Pre": (None, "10:15"), "Morning": ("10:15", "11:15"),
                "Mid": ("11:15", "13:30"), "Afternoon": ("13:30", "14:15"),
                "Post": ("14:15", None),
            }
            lower, upper = session_ranges[session_name]
            time_expr = "substr(json_extract(payload, '$.decision_at'), 12, 5)"
            if lower:
                sql += f" AND {time_expr} >= ?"
                params.append(lower)
            if upper:
                sql += f" AND {time_expr} < ?"
                params.append(upper)
        if before_id is not None:
            sql += " AND id < ?"
            params.append(before_id)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        events = self._events_from_rows([row[1:] for row in rows])
        for event, row in zip(events, rows):
            event["_cursor_id"] = int(row[0])
        return events

    def read_decision_lifecycle_events(self, decision_ids: Iterable[str]) -> list[dict[str, Any]]:
        """Read execution and risk events for the given decision ids."""
        ids = sorted({str(item) for item in decision_ids if item})
        if not ids:
            return []
        event_types = tuple(sorted(EXECUTION_EVENT_TYPES | RISK_EVENT_TYPES))
        type_placeholders = ", ".join("?" for _ in event_types)
        events: list[dict[str, Any]] = []
        chunk_size = 400
        with self._connect() as connection:
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
        with self._connect() as connection:
            rows = connection.execute("SELECT event_type, COUNT(*) FROM runtime_events GROUP BY event_type").fetchall()
            reasons = connection.execute("SELECT json_extract(payload, '$.reason'), COUNT(*) FROM runtime_events WHERE json_extract(payload, '$.reason') IS NOT NULL GROUP BY json_extract(payload, '$.reason')").fetchall()
        self._event_counts_cache = {"by_event_type": {str(kind): int(count) for kind, count in rows},
                                    "by_rejection_reason": {str(reason): int(count) for reason, count in reasons}}
        return {key: dict(value) for key, value in self._event_counts_cache.items()}
