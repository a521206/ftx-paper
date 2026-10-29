"""SQLite runtime contract repository."""
import sqlite3

from .mappers import contract_from_row


class SqliteContractRepository:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def get(self, exchange, underlying, role, provider=None):
        self._connection.row_factory = sqlite3.Row
        query = "SELECT provider, exchange, underlying, role, tradingsymbol, instrument_token, expiry, selected_at FROM runtime_contracts WHERE exchange=? AND underlying=? AND role=?"
        params = (exchange, underlying, role)
        if provider is not None:
            query += " AND provider=?"
            params += (provider,)
        row = self._connection.execute(query + " ORDER BY expiry, tradingsymbol LIMIT 1", params).fetchone()
        return contract_from_row(row) if row else None
