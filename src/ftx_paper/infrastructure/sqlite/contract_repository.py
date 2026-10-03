"""SQLite runtime contract repository."""
import sqlite3

from .mappers import contract_from_row


class SqliteContractRepository:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def get(self, exchange, underlying, role):
        self._connection.row_factory = sqlite3.Row
        row = self._connection.execute("SELECT exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at FROM runtime_contracts WHERE exchange=? AND underlying=? AND role=?", (exchange, underlying, role)).fetchone()
        return contract_from_row(row) if row else None
