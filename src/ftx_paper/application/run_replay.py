"""Replay application use case with guarded state transitions."""

from collections.abc import Mapping

from ftx_paper.ports.unit_of_work import UnitOfWorkFactory


class RunReplay:
    def __init__(self, uow_factory: UnitOfWorkFactory, worker) -> None:
        self.uow_factory = uow_factory
        self.worker = worker

    def execute(self, run_id: str, request: Mapping[str, object]):
        with self.uow_factory() as uow:
            uow.replay_runs.create(run_id, request)
            uow.commit()
        return self.worker(run_id, request)
