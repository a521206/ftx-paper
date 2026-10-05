"""SQLite event repository."""
import json
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

from .mappers import event_from_row
from .connection import SqliteDatabase
from ftx_paper.ports.records import StoredEvent


class SqliteEventRepository:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def append(self, event_type: str, payload: Mapping[str, object], idempotency_key: str | None = None) -> bool:
        cursor = self._connection.execute("INSERT OR IGNORE INTO runtime_events(event_type, payload, created_at, idempotency_key) VALUES (?, ?, ?, ?)", (event_type.upper(), json.dumps(dict(payload), default=str), datetime.now(timezone.utc).isoformat(), idempotency_key))
        return cursor.rowcount == 1

    def read_recent(self, limit: int = 100) -> Sequence[StoredEvent]:
        self._connection.row_factory = sqlite3.Row
        rows = self._connection.execute("SELECT id, event_type, payload, created_at FROM runtime_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return tuple(event_from_row(row) for row in rows)


class SqliteEventQueryRepository:
    """Read-only event projections with one connection per query."""

    def __init__(self, database: SqliteDatabase):
        self._database = database

    def read_decisions(
        self, session_date: str | None = None, limit: int = 50,
        before_id: int | None = None, session_name: str | None = None,
    ) -> Sequence[StoredEvent]:
        connection = self._database.connect()
        try:
            connection.row_factory = sqlite3.Row
            sql = "SELECT id, event_type, payload, created_at FROM runtime_events WHERE event_type IN ('CANDIDATEDECISION', 'EXITDECISION')"
            params = []
            if session_date:
                sql += " AND substr(json_extract(payload, '$.decision_at'), 1, 10) = ?"
                params.append(session_date)
            if before_id is not None:
                sql += " AND id < ?"
                params.append(before_id)
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
            sql += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            return tuple(event_from_row(row) for row in connection.execute(sql, params).fetchall())
        finally:
            connection.close()
