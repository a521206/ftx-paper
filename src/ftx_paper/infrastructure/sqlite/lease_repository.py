"""SQLite process lease repository."""
import sqlite3


class SqliteLeaseRepository:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def release(self, service, instance_id, pid):
        self._connection.execute("DELETE FROM process_leases WHERE service=? AND instance_id=? AND pid=?", (service, instance_id, pid))
