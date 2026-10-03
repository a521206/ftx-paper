"""Stop-runtime application use case."""

from ftx_paper.ports.unit_of_work import UnitOfWorkFactory


class StopRuntime:
    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self.uow_factory = uow_factory

    def execute(self) -> None:
        with self.uow_factory() as uow:
            uow.status.patch({"state": "STOPPED", "feed_connected": False})
            uow.commit()
