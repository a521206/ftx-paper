"""Transaction boundary for application use cases."""

from typing import Protocol

from .repositories import (
    EventRepository, MarketBarRepository, ReplayRepository,
    RuntimeStatusRepository,
)


class UnitOfWork(Protocol):
    events: EventRepository
    market_bars: MarketBarRepository
    replay_runs: ReplayRepository
    status: RuntimeStatusRepository

    def __enter__(self) -> "UnitOfWork": ...
    def __exit__(self, exc_type, exc, traceback) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


class UnitOfWorkFactory(Protocol):
    """Create an independent transaction for one operation."""

    def __call__(self) -> UnitOfWork: ...
