"""Runtime orchestration exports loaded lazily to keep imports acyclic."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .controller import RuntimeController
    from .session import RuntimeSession
    from .store import ProcessAlreadyRunningError, RuntimeStore

__all__ = ["ProcessAlreadyRunningError", "RuntimeController", "RuntimeSession", "RuntimeStore"]


def __getattr__(name: str) -> object:
    if name in {"ProcessAlreadyRunningError", "RuntimeStore"}:
        from .store import ProcessAlreadyRunningError, RuntimeStore
        return {"ProcessAlreadyRunningError": ProcessAlreadyRunningError, "RuntimeStore": RuntimeStore}[name]
    if name == "RuntimeController":
        from .controller import RuntimeController
        return RuntimeController
    if name == "RuntimeSession":
        from .session import RuntimeSession
        return RuntimeSession
    raise AttributeError(name)
