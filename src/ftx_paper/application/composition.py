"""Explicit application composition root."""

from dataclasses import dataclass
from pathlib import Path

from ftx_paper.infrastructure.sqlite.connection import SqliteDatabase
from ftx_paper.infrastructure.sqlite.event_repository import SqliteEventQueryRepository, SqliteEventRepository
from ftx_paper.infrastructure.sqlite.replay_repository import SqliteReplayRepository
from ftx_paper.infrastructure.sqlite.status_repository import SqliteStatusRepository
from ftx_paper.infrastructure.sqlite.unit_of_work import SqliteUnitOfWorkFactory

from .process_market_bar import ProcessMarketBar
from .query_decisions import QueryDecisions
from .recover_runtime import RecoverRuntime
from .run_replay import RunReplay
from .runtime_operations import RuntimeOperations
from .start_runtime import StartRuntime
from .stop_runtime import StopRuntime


@dataclass(frozen=True, slots=True)
class Application:
    process_market_bar: ProcessMarketBar
    start_runtime: StartRuntime
    stop_runtime: StopRuntime
    run_replay: RunReplay
    query_decisions: QueryDecisions
    recover_runtime: RecoverRuntime
    runtime: RuntimeOperations | None = None


def build_application(database_path: str | Path, strategy, replay_worker) -> Application:
    """Build services with one fresh SQLite UoW per operation."""
    from ftx_paper.infrastructure.sqlite.runtime_store import SqliteRuntimeStore

    database = SqliteDatabase(database_path)
    SqliteRuntimeStore(Path(database_path).parent).initialize()

    def repositories(connection):
        return {
            "events": SqliteEventRepository(connection),
            "replay_runs": SqliteReplayRepository(connection),
            "status": SqliteStatusRepository(connection),
        }

    uow_factory = SqliteUnitOfWorkFactory(database, repositories)
    return Application(
        process_market_bar=ProcessMarketBar(uow_factory, strategy),
        start_runtime=StartRuntime(uow_factory),
        stop_runtime=StopRuntime(uow_factory),
        run_replay=RunReplay(uow_factory, replay_worker),
        query_decisions=QueryDecisions(SqliteEventQueryRepository(database)),
        recover_runtime=RecoverRuntime(uow_factory),
    )
