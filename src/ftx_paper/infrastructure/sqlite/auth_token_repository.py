"""SQLite implementation of broker token persistence."""

import sqlite3
from datetime import date

from ftx_paper.ports.records import AuthToken


class SqliteTokenRepository:
    def __init__(self, database) -> None:
        self.database = database

    def _connect(self):
        self.database.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(self.database)

    def save(self, token: AuthToken) -> None:
        with self._connect() as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS auth_tokens (provider TEXT PRIMARY KEY, token_date TEXT NOT NULL, access_token TEXT NOT NULL)")
            connection.execute(
                "INSERT INTO auth_tokens(provider, token_date, access_token) VALUES (?, ?, ?) "
                "ON CONFLICT(provider) DO UPDATE SET token_date=excluded.token_date, access_token=excluded.access_token",
                (token.provider, token.token_date.isoformat(), token.access_token),
            )

    def get_valid(self, provider: str, token_date: date) -> AuthToken | None:
        if not self.database.exists():
            return None
        with self._connect() as connection:
            row = connection.execute("SELECT provider, token_date, access_token FROM auth_tokens WHERE provider = ?", (provider,)).fetchone()
        if row is None or row[1] != token_date.isoformat() or not row[2]:
            return None
        return AuthToken(str(row[0]), date.fromisoformat(str(row[1])), str(row[2]))
