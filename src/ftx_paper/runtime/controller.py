from __future__ import annotations

from datetime import datetime, timezone

from .store import RuntimeStore


class RuntimeController:
    """Command boundary used by the API; process supervision is added here."""

    def __init__(self, store: RuntimeStore) -> None:
        self.store = store

    def request_start(self) -> None:
        self._request("START")

    def request_stop(self) -> None:
        self._request("STOP")

    def request_restart(self) -> None:
        self._request("RESTART")

    def _request(self, command: str) -> None:
        self.store.enqueue_command(command)
        self.store.write_status({"state": f"{command}_REQUESTED", "at": datetime.now(timezone.utc).isoformat()})
