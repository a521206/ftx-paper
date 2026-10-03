"""SQLite connection factory owned by infrastructure."""

import sqlite3
from pathlib import Path


class SqliteDatabase:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def connect(self, *, timeout: float = 10) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(self.path, timeout=timeout)
