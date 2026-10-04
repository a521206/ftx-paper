"""Runtime orchestration exports loaded lazily to keep imports acyclic."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .controller import RuntimeController
    from .event_bus import EventBus, EventHandler
    from .facades import FeedController, FeedManager, OrderManager, PnlManager, StrategyRunner
    from .session import RuntimeSession
    from .store import ProcessAlreadyRunningError, RuntimeStore

__all__ = [
    "EventBus", "EventHandler", "FeedController", "FeedManager", "OrderManager",
    "ProcessAlreadyRunningError", "PnlManager", "RuntimeController", "RuntimeSession", "RuntimeStore", "StrategyRunner",
]


def __getattr__(name: str) -> object:
    if name in {"ProcessAlreadyRunningError", "RuntimeStore"}:
        from .store import ProcessAlreadyRunningError, RuntimeStore
        return {"ProcessAlreadyRunningError": ProcessAlreadyRunningError, "RuntimeStore": RuntimeStore}[name]
    if name == "RuntimeController":
        from .controller import RuntimeController
        return RuntimeController
    if name in {"EventBus", "EventHandler"}:
        from .event_bus import EventBus, EventHandler
        return {"EventBus": EventBus, "EventHandler": EventHandler}[name]
    if name in {"FeedController", "FeedManager", "OrderManager", "PnlManager", "StrategyRunner"}:
        from .facades import FeedController, FeedManager, OrderManager, PnlManager, StrategyRunner
        return {
            "FeedController": FeedController, "FeedManager": FeedManager,
            "OrderManager": OrderManager, "PnlManager": PnlManager, "StrategyRunner": StrategyRunner,
        }[name]
    if name == "RuntimeSession":
        from .session import RuntimeSession
        return RuntimeSession
    raise AttributeError(name)
