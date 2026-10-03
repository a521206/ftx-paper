"""Interrupted-runtime recovery application use case."""

from ftx_paper.ports.unit_of_work import UnitOfWorkFactory


class RecoverRuntime:
    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self.uow_factory = uow_factory

    def execute(self) -> bool:
        with self.uow_factory() as uow:
            status = uow.status.get()
            state = status.payload.get("state")
            if state not in {"RUNNING", "STARTING", "START_REQUESTED"}:
                uow.rollback()
                return False
            uow.status.patch({"state": "STOPPED", "health_state": "STOPPED", "feed_connected": False, "recovered_from": state})
            uow.events.append("RUNTIME_RECOVERY", {"previous_state": state, "resulting_state": "STOPPED"}, "recovery:interrupted")
            uow.commit()
            return True
