"""SQLite unit-of-work shell used by repository adapters."""

import sqlite3
from collections.abc import Callable
from typing import Any

from ftx_paper.ports.repositories import EventRepository, MarketBarRepository, ReplayRepository, RuntimeStatusRepository
from ftx_paper.ports.unit_of_work import UnitOfWork


class SqliteUnitOfWork:
    events: EventRepository
    market_bars: MarketBarRepository
    replay_runs: ReplayRepository
    status: RuntimeStatusRepository

    def __init__(self, connection: sqlite3.Connection, repositories: dict[str, object] | None = None) -> None:
        self.connection = connection
        self._repositories = repositories or {}
        self._committed = False
        for name, repository in self._repositories.items():
            setattr(self, name, repository)

    def __enter__(self) -> "SqliteUnitOfWork":
        self.connection.execute("BEGIN")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if exc_type is not None or not self._committed:
            self.rollback()
        self.connection.close()

    def commit(self) -> None:
        self.connection.commit()
        self._committed = True

    def rollback(self) -> None:
        self.connection.rollback()


class SqliteUnitOfWorkFactory:
    def __init__(self, database: Any, repository_factory: Callable[[sqlite3.Connection], dict[str, object]]) -> None:
        self.database = database
        self.repository_factory = repository_factory

    def __call__(self) -> UnitOfWork:
        connection = self.database.connect()
        return SqliteUnitOfWork(connection, self.repository_factory(connection))
