"""SQLite market-bar repository entry point."""
import sqlite3


class SqliteMarketBarRepository:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def read_dates(self):
        return tuple(row[0] for row in self._connection.execute("SELECT date FROM market_dates ORDER BY date"))
