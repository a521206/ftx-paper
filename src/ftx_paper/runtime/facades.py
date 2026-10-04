"""Typed in-process facades for the single runtime owner."""

from __future__ import annotations

from typing import Any, Protocol

from ftx_paper.contracts import MarketBar, OrderIntent
from ftx_paper.execution import ExecutionService
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.market.bundles import DecisionBundle

from .engine import EngineResult, PaperEngine


class FeedController(Protocol):
    """Minimal lifecycle capability required by the runtime feed facade."""

    def start(self) -> None: ...

    def stop(self) -> object: ...

    def flush(self) -> None: ...


class FeedManager:
    """Own feed lifecycle delegation without owning market or runtime state."""

    def __init__(self, feed: FeedController | None = None) -> None:
        self._feed = feed

    @property
    def feed(self) -> FeedController | None:
        return self._feed

    def attach(self, feed: FeedController) -> None:
        self._feed = feed

    def start(self) -> None:
        if self._feed is None:
            raise RuntimeError("feed is not attached")
        self._feed.start()

    def stop(self) -> bool:
        if self._feed is None:
            return True
        result = self._feed.stop()
        return result is not False

    def flush(self) -> None:
        if self._feed is not None:
            self._feed.flush()


class OrderManager:
    """Execution facade that delegates to the existing execution service."""

    def __init__(self, execution: ExecutionService) -> None:
        self.execution = execution

    def authorize(self, order: OrderIntent, *, payload: dict[str, object], timestamp: str | None) -> None:
        self.execution.authorize(order, payload=payload, timestamp=timestamp)

    def publish(self, notification: ExecutionNotification) -> None:
        self.execution.publish(notification)


class StrategyRunner:
    """Strategy facade around the one runtime-owned ``PaperEngine``."""

    def __init__(self, engine: PaperEngine) -> None:
        self.engine = engine

    def evaluate(self, bundle: DecisionBundle, context: Any = None) -> EngineResult:
        if context is None:
            return self.engine.on_bundle(bundle)
        return self.engine.on_bundle(bundle, context=context)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        handler = getattr(self.engine, "on_execution_event", None)
        if callable(handler):
            handler(event)

    def on_tick(self, bar: MarketBar) -> tuple[Any, ...]:
        return self.engine.on_tick(bar)

    def on_closed_bar(self, bar: MarketBar) -> tuple[Any, ...]:
        return self.engine.on_closed_bar(bar)


__all__ = ["FeedController", "FeedManager", "OrderManager", "StrategyRunner"]
