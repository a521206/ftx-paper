from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import zlib
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, TypedDict
from uuid import uuid4
from collections.abc import Iterable, Mapping

from ftx_paper.config import IST
from ftx_paper.contracts import (
    MarketBar, MarketRole, OptionRole, OrderLifecycle, OrderLifecycleState, build_decision_view,
    canonical_market_exchange,
    canonical_market_symbol,
    is_legal_order_transition, validate_decision_view,
    validate_market_bar,
    role_to_key,
)
from ftx_paper.runtime.events import (
    EXECUTION_EVENT_TYPES,
    LEGACY_DECISION_TIME_FIELDS,
    RUNTIME_EVENT_SCHEMA_VERSION,
    RISK_EVENT_TYPES,
    DecisionTimestampError,
    OPERATIONAL_ACTIVITY_EVENT_TYPES,
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
    unavailable: int
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


_REPLAY_RESULT_CHUNK_BYTES = 4 * 1024 * 1024
_REPLAY_RESULT_STORAGE = "replay_result_chunks"


def _encode_replay_result(value: dict[str, Any]) -> tuple[str, list[bytes]]:
    """Compress and split results so no SQLite value grows without bound."""
    payload = json.dumps(
        _json_safe(value, path="replay result"), separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    compressed = zlib.compress(payload, level=6)
    chunks = [compressed[offset:offset + _REPLAY_RESULT_CHUNK_BYTES]
              for offset in range(0, len(compressed), _REPLAY_RESULT_CHUNK_BYTES)]
    descriptor = json.dumps({"__storage__": _REPLAY_RESULT_STORAGE, "version": 1, "chunks": len(chunks)})
    return descriptor, chunks


def _decode_replay_result(connection: sqlite3.Connection, run_id: str, value: str | None) -> Any:
    if not value:
        return None
    descriptor = json.loads(value)
    if not isinstance(descriptor, dict) or descriptor.get("__storage__") != _REPLAY_RESULT_STORAGE:
        return descriptor
    rows = connection.execute(
        "SELECT payload FROM replay_result_chunks WHERE run_id=? ORDER BY chunk_index", (run_id,)
    ).fetchall()
    expected = int(descriptor.get("chunks", -1))
    if len(rows) != expected:
        raise ValueError(f"replay result chunks missing for {run_id}")
    compressed = b"".join(bytes(row[0]) for row in rows)
    return json.loads(zlib.decompress(compressed).decode("utf-8"))


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if sys.platform == "win32":
        try:
            import ctypes
            import ctypes.wintypes

            th32cs_snapprocess = 0x00000002

            class PROCESSENTRY32(ctypes.Structure):
                _fields_ = [
                    ("dwSize", ctypes.wintypes.DWORD),
                    ("cntUsage", ctypes.wintypes.DWORD),
                    ("th32ProcessID", ctypes.wintypes.DWORD),
                    ("th32DefaultHeapID", ctypes.c_void_p),
                    ("th32ModuleID", ctypes.wintypes.DWORD),
                    ("cntThreads", ctypes.wintypes.DWORD),
                    ("th32ParentProcessID", ctypes.wintypes.DWORD),
                    ("pcPriClassBase", ctypes.c_long),
                    ("dwFlags", ctypes.wintypes.DWORD),
                    ("szExeFile", ctypes.c_char * 260),
                ]

            h_snap = ctypes.windll.kernel32.CreateToolhelp32Snapshot(th32cs_snapprocess, 0)
            if h_snap == -1 or h_snap == 0xFFFFFFFF:
                return False
            try:
                pe = PROCESSENTRY32()
                pe.dwSize = ctypes.sizeof(PROCESSENTRY32)
                if not ctypes.windll.kernel32.Process32First(h_snap, ctypes.byref(pe)):
                    return False
                while True:
                    if pe.th32ProcessID == pid:
                        return True
                    if not ctypes.windll.kernel32.Process32Next(h_snap, ctypes.byref(pe)):
                        break
                return False
            finally:
                ctypes.windll.kernel32.CloseHandle(h_snap)
        except Exception:
            pass

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
            # API reads must remain available while the runtime persists a
            # batch of events or bars. WAL lets readers use the last committed
            # snapshot instead of waiting behind the writer transaction.
            connection.execute("PRAGMA journal_mode = WAL")
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
                    provider TEXT NOT NULL DEFAULT 'unknown',
                    exchange TEXT NOT NULL,
                    underlying TEXT NOT NULL,
                    role TEXT NOT NULL,
                    tradingsymbol TEXT NOT NULL,
                    instrument_token INTEGER NOT NULL,
                    expiry TEXT NOT NULL,
                    selected_at TEXT NOT NULL,
                    PRIMARY KEY (provider, exchange, underlying, role, tradingsymbol)
                );
                 CREATE TABLE IF NOT EXISTS runtime_orders (
                    client_order_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    account_revision INTEGER,
                    broker_order_id TEXT,
                     updated_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS strategy_snapshots (
                     strategy_id TEXT PRIMARY KEY,
                     schema_version INTEGER NOT NULL,
                     payload TEXT NOT NULL,
                     revision INTEGER NOT NULL DEFAULT 0,
                     updated_at TEXT NOT NULL
                 );
                CREATE TABLE IF NOT EXISTS replay_result_chunks (
                    run_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (run_id, chunk_index)
                );
                CREATE INDEX IF NOT EXISTS ix_replay_result_chunks_run
                    ON replay_result_chunks(run_id, chunk_index);
                """
            )
            connection.execute(
                "UPDATE market_bars SET instrument_type = 'INDEX' "
                "WHERE upper(symbol) IN ('NIFTY', 'NIFTY 50', 'INDIA VIX') "
                "AND upper(instrument_type) <> 'INDEX'"
            )
            connection.execute(
                "UPDATE market_bars SET expiry = '', strike = NULL, option_type = NULL "
                "WHERE upper(instrument_type) = 'INDEX' "
                "AND (expiry <> '' OR strike IS NOT NULL OR option_type IS NOT NULL)"
            )
            connection.execute(
                "UPDATE market_bars SET strike = NULL, option_type = NULL "
                "WHERE upper(instrument_type) IN ('FUT', 'FUTURES') "
                "AND (strike IS NOT NULL OR option_type IS NOT NULL)"
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
            connection.execute(
                "DELETE FROM replay_result_chunks WHERE run_id NOT IN (SELECT run_id FROM replay_runs)"
            )
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
                            WHERE status IN ('completed', 'failed', 'cancelled', 'rejected')
                              AND json_extract(request, '$.session_date') IS NOT NULL
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

    def read_runtime_contract(self, *, exchange: str, underlying: str, role: str, provider: str | None = None) -> dict[str, Any] | None:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT provider, exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at "
                "FROM runtime_contracts WHERE exchange = ? AND underlying = ? AND role = ? "
                + ("AND provider = ? " if provider is not None else "")
                + "ORDER BY expiry, tradingsymbol LIMIT 1",
                (exchange, underlying, role) if provider is None else (exchange, underlying, role, provider),
            ).fetchone()
        return dict(row) if row else None

    def read_runtime_contracts(self, *, exchange: str, underlying: str, role: str, provider: str | None = None) -> list[dict[str, Any]]:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT provider, exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at "
                "FROM runtime_contracts WHERE exchange = ? AND underlying = ? AND role = ? "
                + ("AND provider = ? " if provider is not None else "")
                + "ORDER BY expiry, tradingsymbol",
                (exchange, underlying, role) if provider is None else (exchange, underlying, role, provider),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_runtime_contract(self, contract: dict[str, Any]) -> bool:
        contract = {**contract, "provider": contract.get("provider", "unknown")}
        required = ("provider", "exchange", "underlying", "role", "tradingsymbol", "instrument_token", "expiry")
        if any(contract.get(key) in (None, "") for key in required):
            raise ValueError("runtime contract is missing required identity fields")
        selected_at = datetime.now(timezone.utc).isoformat()
        event_payload = {**contract, "selected_at": selected_at}
        with self._connect() as connection:
            previous = connection.execute(
                "SELECT tradingsymbol, instrument_token, expiry FROM runtime_contracts "
                "WHERE provider = ? AND exchange = ? AND underlying = ? AND role = ? AND tradingsymbol = ?",
                (contract["provider"], contract["exchange"], contract["underlying"], contract["role"],
                 contract["tradingsymbol"]),
            ).fetchone()
            changed = previous is None or tuple(previous) != (
                contract["tradingsymbol"], int(contract["instrument_token"]), str(contract["expiry"]),
            )
            connection.execute(
                "INSERT INTO runtime_contracts "
                "(provider, exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(provider, exchange, underlying, role, tradingsymbol) DO UPDATE SET "
                "instrument_token=excluded.instrument_token, "
                "expiry=excluded.expiry, selected_at=excluded.selected_at",
                (contract["provider"], contract["exchange"], contract["underlying"], contract["role"],
                 contract["tradingsymbol"], int(contract["instrument_token"]), str(contract["expiry"]), selected_at),
            )
            if changed:
                connection.execute(
                    "INSERT OR IGNORE INTO runtime_events "
                    "(event_type, payload, created_at, idempotency_key) VALUES (?, ?, ?, ?)",
                    (
                        "CONTRACT_CHANGED", json.dumps(_json_safe(event_payload, path="contract event")),
                        selected_at,
                        f"contract:{contract['provider']}:{contract['exchange']}:{contract['underlying']}:{contract['role']}"
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
            validate_market_bar(bar)
            timestamp = bar.timestamp.astimezone(IST)
            minute = timestamp.replace(second=0, microsecond=0).isoformat()
            symbol = canonical_market_symbol(bar.instrument.symbol)
            exchange = canonical_market_exchange(symbol, bar.instrument.exchange)
            rows.append((
                symbol, exchange, minute,
                bar.open, bar.high, bar.low, bar.close, bar.volume,
                (bar.open_interest if bar.open_interest is not None else 0.0
                 if str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES"} else None),
                ("FUT" if str(bar.instrument.instrument_type).upper() == "FUTURES"
                else "INDEX" if symbol.upper() in {"NIFTY", "NIFTY 50", "INDIA VIX"}
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

    def read_market_bars_between(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Read normalized futures bars across an inclusive IST date range."""
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT symbol, exchange, minute, open, high, low, close, volume,
                       open_interest, instrument_type, expiry, strike, option_type,
                       source, ingested_at
                FROM market_bars
                WHERE substr(minute, 1, 10) BETWEEN ? AND ?
                ORDER BY minute, exchange, symbol
                """,
                (start_date, end_date),
            ).fetchall()
        return [dict(row) for row in rows]

    def read_dates(self) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT substr(minute, 1, 10) AS date "
                "FROM market_bars ORDER BY date"
            ).fetchall()
        return [str(row[0]) for row in rows]

    def read_market_dates(self) -> list[str]:
        """Return persisted market-bar session dates.

        Keep the explicit market-data name as a compatibility API for
        integrations that distinguish these dates from other runtime dates.
        """
        return self.read_dates()

    def read_market_coverage(self, session_date: str) -> dict[str, Any]:
        """Return per-symbol, exchange, and source coverage for one session."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT symbol, exchange, instrument_type, source, COUNT(*) AS bars,
                       MIN(minute), MAX(minute)
                FROM market_bars
                WHERE substr(minute, 1, 10) = ?
                GROUP BY symbol, exchange, instrument_type, source
                ORDER BY symbol, exchange, source
                """,
                (session_date,),
            ).fetchall()
        return {
            "session_date": session_date,
            "rows": [
                {
                    "symbol": str(row[0]), "exchange": str(row[1]),
                    "instrument_type": str(row[2]), "source": str(row[3]),
                    "bars": int(row[4]), "first_minute": str(row[5]),
                    "last_minute": str(row[6]),
                }
                for row in rows
            ],
        }

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

    def write_strategy_snapshot(
        self, strategy_id: str, snapshot: dict[str, Any], *, revision: int = 0,
    ) -> None:
        """Persist one versioned strategy snapshot independently of status."""
        if not strategy_id.strip():
            raise ValueError("strategy_id must not be empty")
        schema_version = snapshot.get("schema_version", 1)
        if type(schema_version) is not int or schema_version < 1:
            raise ValueError("strategy snapshot schema_version must be a positive integer")
        if revision < 0:
            raise ValueError("strategy snapshot revision must be non-negative")
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO strategy_snapshots(strategy_id, schema_version, payload, revision, updated_at) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(strategy_id) DO UPDATE SET "
                "schema_version=excluded.schema_version, payload=excluded.payload, "
                "revision=excluded.revision, updated_at=excluded.updated_at",
                (
                    strategy_id, schema_version,
                    json.dumps(_json_safe(snapshot, path="strategy snapshot"), separators=(",", ":")),
                    revision, self._now(),
                ),
            )

    def read_strategy_snapshot(self, strategy_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT schema_version, payload, revision, updated_at FROM strategy_snapshots "
                "WHERE strategy_id = ?", (strategy_id,),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row[1])
        if not isinstance(payload, dict):
            raise ValueError("stored strategy snapshot must be an object")
        return {
            "strategy_id": strategy_id,
            "schema_version": int(row[0]),
            "payload": payload,
            "revision": int(row[2]),
            "updated_at": str(row[3]),
        }

    def read_strategy_snapshots(self) -> dict[str, dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT strategy_id, schema_version, payload, revision, updated_at "
                "FROM strategy_snapshots ORDER BY strategy_id",
            ).fetchall()
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            payload = json.loads(row[2])
            if not isinstance(payload, dict):
                raise ValueError("stored strategy snapshot must be an object")
            result[str(row[0])] = {
                "strategy_id": str(row[0]), "schema_version": int(row[1]),
                "payload": payload, "revision": int(row[3]), "updated_at": str(row[4]),
            }
        return result

    def append_event(self, event_type: str, payload: dict[str, Any], idempotency_key: str | None = None,
                     *, timestamp: str | None = None) -> bool:
        event_type = str(event_type).upper()
        payload = dict(payload)
        if payload.get("event_schema_version", RUNTIME_EVENT_SCHEMA_VERSION) != RUNTIME_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported runtime event schema version")
        if (
            "decision_view" not in payload
            and payload.get("decision_id")
            and payload.get("outcome") in {"accepted", "rejected", "cancelled", "superseded", "recorded", "triggered", "unavailable"}
        ):
            payload["decision_view"] = validate_decision_view(
                build_decision_view(payload, event_type=event_type)
            )
        if is_decision_event(event_type, payload):
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

    def read_activity(
        self,
        *,
        strategy_id: str | None = None,
        session_date: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
        before_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Read operational activity from the authoritative event store."""
        if limit < 1:
            return []
        type_placeholders = ", ".join("?" for _ in OPERATIONAL_ACTIVITY_EVENT_TYPES)
        clauses: list[str] = [f"event_type IN ({type_placeholders})"]
        params: list[Any] = list(OPERATIONAL_ACTIVITY_EVENT_TYPES)
        if strategy_id is not None:
            clauses.append("json_extract(payload, '$.strategy_id') = ?")
            params.append(strategy_id)
        if session_date:
            clauses.append("json_extract(payload, '$.session_date') = ?")
            params.append(session_date)
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type.upper())
        if before_id is not None:
            clauses.append("id < ?")
            params.append(before_id)
        where = f"WHERE {' AND '.join(clauses)}"
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT id, event_type, payload, created_at FROM runtime_events {where} "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                params,
            ).fetchall()
        events = self._events_from_rows([(row[1], row[2], row[3]) for row in rows])
        return [
            {**event, "activity_id": int(row[0])}
            for event, row in zip(events, rows)
        ]

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
        self.append_event(
            "REPLAY_QUEUED",
            {"run_id": run_id, **request},
            f"replay_activity:queued:{run_id}",
            timestamp=now,
        )
        return self.read_replay_run(run_id) or {}

    def create_or_get_replay_run(
        self, run_id: str, request: dict[str, Any],
    ) -> tuple[str, bool]:
        """Atomically reuse an in-flight equivalent or create a new replay."""
        now = self._now()
        session_date = str(request.get("session_date") or "").strip()[:10]
        encoded_request = json.dumps(_json_safe(request, path="replay request"), sort_keys=True)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT run_id, request FROM replay_runs "
                "WHERE status IN ('queued', 'running') "
                "AND substr(json_extract(request, '$.session_date'), 1, 10) = ?",
                (session_date,),
            ).fetchall()
            for existing_id, existing_request in rows:
                if json.loads(existing_request) == json.loads(encoded_request):
                    return str(existing_id), False

            connection.execute(
                "DELETE FROM replay_result_chunks WHERE run_id IN "
                "(SELECT run_id FROM replay_runs WHERE status IN "
                "('completed', 'failed', 'cancelled', 'rejected') AND "
                "substr(json_extract(request, '$.session_date'), 1, 10) = ?)",
                (session_date,),
            )
            connection.execute(
                "DELETE FROM replay_runs WHERE status IN "
                "('completed', 'failed', 'cancelled', 'rejected') AND "
                "substr(json_extract(request, '$.session_date'), 1, 10) = ?",
                (session_date,),
            )
            connection.execute(
                "INSERT INTO replay_runs(run_id, status, request, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, "queued", encoded_request, now, now),
            )
        self.append_event(
            "REPLAY_QUEUED",
            {"run_id": run_id, **request},
            f"replay_activity:queued:{run_id}",
            timestamp=now,
        )
        return run_id, True

    def update_replay_run(self, run_id: str, *, status: str, result: dict[str, Any] | None = None,
                          error: str | None = None) -> None:
        updated_at = self._now()
        with self._connect() as connection:
            encoded_result = None
            chunks: list[bytes] = []
            if result is not None:
                encoded_result, chunks = _encode_replay_result(result)
            connection.execute(
                "UPDATE replay_runs SET status=?, result=COALESCE(?, result), error=?, updated_at=? WHERE run_id=?",
                (status, encoded_result, error, updated_at, run_id),
            )
            if result is not None:
                connection.execute("DELETE FROM replay_result_chunks WHERE run_id=?", (run_id,))
                connection.executemany(
                    "INSERT INTO replay_result_chunks(run_id, chunk_index, payload) VALUES (?, ?, ?)",
                    ((run_id, index, payload) for index, payload in enumerate(chunks)),
                )
        run = self.read_replay_run(run_id)
        request = run.get("request", {}) if run else {}
        self.append_event(
            "REPLAY_STATUS",
            {"run_id": run_id, "status": status, "strategy_id": request.get("strategy_id"),
             "session_date": request.get("session_date"), "error": error},
            f"replay_activity:{status}:{run_id}:{updated_at}",
            timestamp=updated_at,
        )

    def write_replay_artifact(self, run_id: str, artifact: dict[str, Any]) -> None:
        """Persist a completed artifact without exposing SQLite to replay code."""
        run = self.read_replay_run(run_id)
        if run is None:
            raise KeyError(f"unknown replay run: {run_id}")
        self.update_replay_run(
            run_id,
            status=str(run.get("status") or "running"),
            result=artifact,
        )

    def read_replay_artifact(self, run_id: str) -> dict[str, Any] | None:
        """Read the persisted production replay artifact."""
        run = self.read_replay_run(run_id)
        result = run.get("result") if run is not None else None
        return result if isinstance(result, dict) else None

    def transition_replay_run(self, run_id: str, *, expected_status: str, status: str,
                              result: dict[str, Any] | None = None, error: str | None = None) -> bool:
        """Guard replay state changes so racing workers cannot overwrite terminal state."""
        with self._connect() as connection:
            encoded_result = None
            chunks: list[bytes] = []
            if result is not None:
                encoded_result, chunks = _encode_replay_result(result)
            cursor = connection.execute(
                "UPDATE replay_runs SET status=?, result=COALESCE(?, result), error=?, updated_at=? "
                "WHERE run_id=? AND status=?",
                (status, encoded_result,
                 error, self._now(), run_id, expected_status),
            )
            changed = cursor.rowcount == 1
            if changed and result is not None:
                connection.execute("DELETE FROM replay_result_chunks WHERE run_id=?", (run_id,))
                connection.executemany(
                    "INSERT INTO replay_result_chunks(run_id, chunk_index, payload) VALUES (?, ?, ?)",
                    ((run_id, index, payload) for index, payload in enumerate(chunks)),
                )
            if changed:
                request_row = connection.execute(
                    "SELECT request FROM replay_runs WHERE run_id=?", (run_id,),
                ).fetchone()
            else:
                request_row = None
        if changed:
            request = json.loads(request_row[0]) if request_row else {}
            self.append_event(
                "REPLAY_STATUS",
                {"run_id": run_id, "status": status, "strategy_id": request.get("strategy_id"),
                 "session_date": request.get("session_date"), "error": error},
                f"replay_activity:{status}:{run_id}:{self._now()}",
            )
        return changed

    def read_replay_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM replay_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                return None
            value = dict(row)
            value["request"] = json.loads(value["request"])
            value["result"] = _decode_replay_result(connection, run_id, value["result"])
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
                               PARTITION BY json_extract(request, '$.session_date'),
                                            json_extract(request, '$.strategy_id')
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
            connection.execute("DELETE FROM replay_result_chunks")
            cursor = connection.execute("DELETE FROM replay_runs")
            deleted = cursor.rowcount
        return deleted

    def clear_replay_runs_for_date(self, session_date: str) -> int:
        """Delete terminal replay runs whose requested session is this date."""
        normalized = str(session_date).strip()[:10]
        if not normalized:
            return 0
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM replay_result_chunks WHERE run_id IN "
                "(SELECT run_id FROM replay_runs WHERE status IN "
                "('completed', 'failed', 'cancelled', 'rejected') AND "
                "substr(json_extract(request, '$.session_date'), 1, 10) = ?)",
                (normalized,),
            )
            cursor = connection.execute(
                "DELETE FROM replay_runs "
                "WHERE status IN ('completed', 'failed', 'cancelled', 'rejected') AND "
                "substr(json_extract(request, '$.session_date'), 1, 10) = ?",
                (normalized,),
            )
            return cursor.rowcount

    def delete_replay_run(self, run_id: str) -> bool:
        """Delete one replay and any separately stored result chunks."""
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM replay_runs WHERE run_id=?", (run_id,))
            if cursor.rowcount != 1:
                return False
            connection.execute("DELETE FROM replay_result_chunks WHERE run_id=?", (run_id,))
        return True

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
            if is_decision_event(event_type, payload):
                payload["decision_at"] = parse_decision_at(payload.get("decision_at"))
                view = payload.get("decision_view")
                if not isinstance(view, Mapping):
                    raise ValueError("decision event is missing its canonical decision view")
                payload["decision_view"] = validate_decision_view(view)
            result.append({"event_type": event_type, "category": event_category(event_type, payload),
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
        events = self._events_from_rows(rows)
        def ordering_key(event: dict[str, Any]) -> tuple[str, int, str, str, str, str]:
            payload = event["payload"]
            raw_sequence = payload.get("sequence")
            sequence = raw_sequence if isinstance(raw_sequence, int) and not isinstance(raw_sequence, bool) else 2**31
            return (
                str(payload.get("session_date", "")), sequence,
                str(payload.get("decision_at") or payload.get("available_at") or event["created_at"].isoformat()),
                str(payload.get("decision_id", "")), event["event_type"],
                json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
            )
        return sorted(events, key=ordering_key, reverse=True)

    def read_orderflow_facts(
        self, *, limit: int = 1000, stream: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read persisted canonical orderflow facts without recomputation."""
        facts = [item for item in self.read_events(limit) if item["event_type"] == "ORDERFLOW_FACT"]
        if stream is None:
            return facts
        return [item for item in facts if item["payload"].get("stream") == stream]

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

    def read_decision_summary(
        self, session_date: str, *, strategy_id: str | None = None, canonical_only: bool = False,
    ) -> DecisionSummaryReport:
        """Aggregate a complete session day without decoding event payloads."""
        executed_types = ("FILL",)
        executed_placeholders = ", ".join("?" for _ in executed_types)
        strategy_clause = ""
        if strategy_id is not None:
            strategy_clause = "AND json_extract(payload, '$.strategy_id') = ? "
        canonical_clause = "AND json_type(payload, '$.decision_view') = 'object' " if canonical_only else ""
        sql = (
            "SELECT event_type, substr(json_extract(payload, '$.decision_at'), 12, 5), "
            "json_extract(payload, '$.outcome'), COUNT(*), "
            "SUM(CASE WHEN EXISTS (SELECT 1 FROM runtime_events lifecycle "
            "WHERE lifecycle.event_type IN (" + executed_placeholders + ") "
            "AND json_extract(lifecycle.payload, '$.decision_id') = "
            "json_extract(decision.payload, '$.decision_id')) THEN 1 ELSE 0 END) "
            "FROM runtime_events decision "
            "WHERE json_type(payload, '$.decision_view') = 'object' "
            "AND substr(json_extract(payload, '$.decision_at'), 1, 10) = ? "
            + canonical_clause
            + strategy_clause +
            "AND (json_extract(payload, '$.decision_source') IS NULL "
            "OR lower(json_extract(payload, '$.decision_source')) <> 'replay') "
            "GROUP BY event_type, substr(json_extract(payload, '$.decision_at'), 12, 5), "
            "json_extract(payload, '$.outcome')"
        )
        params: list[Any] = [*executed_types]
        params.append(session_date)
        if strategy_id is not None:
            params.append(strategy_id)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()

        sessions: dict[str, DecisionSummary] = {
            name: {"detections": 0, "candidates": 0, "accepted": 0, "rejected": 0, "unavailable": 0, "executed": 0}
            for name in ("Pre", "Morning", "Mid", "Afternoon", "Post")
        }
        totals: DecisionSummary = {
            "detections": 0, "candidates": 0, "accepted": 0, "rejected": 0, "unavailable": 0, "executed": 0,
        }
        for event_type, minute, outcome, count, executed in rows:
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
            stats["candidates"] += amount
            stats["accepted"] += amount if outcome == "accepted" else 0
            stats["rejected"] += amount if outcome == "rejected" else 0
            stats["unavailable"] += amount if outcome == "unavailable" else 0
            stats["executed"] += int(executed or 0)
        for stats in sessions.values():
            for key in totals:
                totals[key] += stats[key]
        return {"totals": totals, "sessions": sessions}

    def read_decision_events(
        self, session_date: str | None = None, limit: int = 50, *, strategy_id: str | None = None,
        before_id: int | None = None, session_name: str | None = None, canonical_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Read decision events on their own, independent of the general event stream.

        ``session_date`` filters on the IST calendar date of ``decision_at``. Decision
        Rows without a canonical timestamp are excluded rather than failing the
        whole projection.
        """
        if limit < 1:
            return []
        sql = (
            "SELECT id, event_type, payload, created_at FROM runtime_events "
            "WHERE json_type(payload, '$.decision_view') = 'object' "
            "AND json_extract(payload, '$.decision_at') <> ''"
            + (" AND json_type(payload, '$.decision_view') = 'object'" if canonical_only else "")
            + " AND (json_extract(payload, '$.decision_source') IS NULL "
            "OR lower(json_extract(payload, '$.decision_source')) <> 'replay')"
        )
        params: list[Any] = []
        if session_date:
            sql += " AND substr(json_extract(payload, '$.decision_at'), 1, 10) = ?"
            params.append(session_date)
        if strategy_id is not None:
            sql += " AND json_extract(payload, '$.strategy_id') = ?"
            params.append(strategy_id)
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
