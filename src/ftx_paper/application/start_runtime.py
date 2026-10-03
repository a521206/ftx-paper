"""Start-runtime application use case."""

from ftx_paper.ports.unit_of_work import UnitOfWorkFactory


class StartRuntime:
    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self.uow_factory = uow_factory

    def execute(self, status: dict[str, object]) -> None:
        with self.uow_factory() as uow:
            uow.status.patch({**status, "state": "STARTING"})
            uow.commit()
