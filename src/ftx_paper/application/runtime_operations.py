"""Application boundary for live runtime and replay commands."""

from typing import Any


class RuntimeOperations:
    """Expose runtime orchestration without leaking runtime objects to adapters."""

    def __init__(self, session: Any, replay_worker: Any, controller: Any | None = None) -> None:
        self.session = session
        self.replay_worker = replay_worker
        self.controller = controller

    def request_start(self) -> None:
        (self.controller.request_start if self.controller is not None else self.session.start)()

    def request_stop(self) -> None:
        (self.controller.request_stop if self.controller is not None else self.session.stop)()

    def request_restart(self) -> None:
        (self.controller.request_restart if self.controller is not None else self.session.restart)()

    def submit_replay(self, request: dict[str, object]) -> str:
        return self.replay_worker.submit(request)

    def cancel_replay(self, run_id: str) -> bool:
        return self.replay_worker.cancel(run_id)
