from __future__ import annotations

from .store import RuntimeStore
from .session import RuntimeSession


class RuntimeController:
    """Serialized lifecycle boundary used by the API."""

    def __init__(self, store: RuntimeStore, session: RuntimeSession) -> None:
        self.store, self.session = store, session

    def request_start(self) -> None:
        self.session.start()

    def request_stop(self) -> None:
        self.session.stop()

    def request_restart(self) -> None:
        self.session.restart()
