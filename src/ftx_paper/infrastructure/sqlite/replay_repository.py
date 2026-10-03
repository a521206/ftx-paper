"""SQLite replay repository with guarded state transitions."""
import json
import sqlite3
from datetime import datetime, timezone

from .mappers import replay_run_from_row


class SqliteReplayRepository:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def create(self, run_id, request):
        now = datetime.now(timezone.utc).isoformat()
        self._connection.execute(
            "INSERT INTO replay_runs(run_id, status, request, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (run_id, "queued", json.dumps(dict(request), default=str), now, now),
        )
        return self.get(run_id)

    def transition(self, run_id, expected_status, new_status, result=None, error=None):
        cursor = self._connection.execute("UPDATE replay_runs SET status=?, result=COALESCE(?, result), error=?, updated_at=? WHERE run_id=? AND status=?", (new_status, json.dumps(dict(result)) if result is not None else None, error, datetime.now(timezone.utc).isoformat(), run_id, expected_status))
        return cursor.rowcount == 1

    def get(self, run_id):
        self._connection.row_factory = sqlite3.Row
        row = self._connection.execute("SELECT * FROM replay_runs WHERE run_id=?", (run_id,)).fetchone()
        return replay_run_from_row(row) if row else None
