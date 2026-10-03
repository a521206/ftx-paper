"""SQLite expiry calendar repository."""
import sqlite3


class SqliteExpiryRepository:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def get_all(self):
        return frozenset(row[0] for row in self._connection.execute("SELECT date FROM expiry_dates ORDER BY date"))
