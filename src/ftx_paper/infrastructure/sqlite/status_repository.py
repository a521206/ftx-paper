"""SQLite runtime status repository."""
import json
import sqlite3
from datetime import datetime, timezone

from ftx_paper.ports.records import RuntimeStatus


class SqliteStatusRepository:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def get(self):
        row = self._connection.execute("SELECT payload, updated_at FROM runtime_status WHERE id=1").fetchone()
        if row is None:
            return RuntimeStatus({})
        updated_at = datetime.fromisoformat(str(row[1]).replace("Z", "+00:00"))
        if updated_at.tzinfo is None or updated_at.utcoffset() is None:
            raise ValueError("stored runtime status timestamp must be timezone-aware")
        return RuntimeStatus(json.loads(row[0]), updated_at.astimezone(timezone.utc))

    def update(self, status):
        self._connection.execute("INSERT INTO runtime_status(id, payload, updated_at) VALUES (1, ?, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at", (json.dumps(dict(status), default=str), datetime.now(timezone.utc).isoformat()))

    def patch(self, updates):
        current = self.get()
        payload = dict(current.payload)
        payload.update(updates)
        self.update(payload)
