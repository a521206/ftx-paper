"""Typed in-process facades for the single runtime owner."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, cast

from ftx_paper.contracts import Instrument, MarketBar, OrderIntent
from ftx_paper.domain.portfolio import PortfolioState
from ftx_paper.execution import ExecutionService
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.market.bundles import DecisionBundle

from .engine import EngineResult, PaperEngine


class FeedController(Protocol):
    """Lifecycle-only subset of the future ``MarketDataPort`` capability."""

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

    def subscribe(self, instruments: Iterable[Instrument]) -> None:
        method = getattr(self._require_feed(), "subscribe", None)
        if not callable(method):
            raise NotImplementedError("feed subscriptions are not supported by this adapter")
        method(instruments)

    def unsubscribe(self, instruments: Iterable[Instrument]) -> None:
        method = getattr(self._require_feed(), "unsubscribe", None)
        if not callable(method):
            raise NotImplementedError("feed unsubscriptions are not supported by this adapter")
        method(instruments)

    def request_quote(self, instrument: Instrument) -> MarketBar | None:
        method = getattr(self._require_feed(), "request_quote", None)
        if not callable(method):
            raise NotImplementedError("quote requests are not supported by this adapter")
        return cast(MarketBar | None, method(instrument))

    def health(self) -> dict[str, object]:
        method = getattr(self._require_feed(), "health_snapshot", None)
        if not callable(method):
            method = getattr(self._require_feed(), "health", None)
        if not callable(method):
            raise NotImplementedError("feed health is not supported by this adapter")
        snapshot = method()
        if not isinstance(snapshot, dict):
            raise TypeError("feed health snapshot must be a dictionary")
        return snapshot

    def _require_feed(self) -> FeedController:
        if self._feed is None:
            raise RuntimeError("feed is not attached")
        return self._feed


class OrderManager:
    """Paper-order facade over authorization and coordinator submission.

    Broker dispatch remains owned by ``RuntimeSession`` so this facade cannot
    accidentally become a second execution path.
    """

    def __init__(self, execution: ExecutionService) -> None:
        self.execution = execution

    def authorize(self, order: OrderIntent, *, payload: dict[str, object], timestamp: str | None) -> None:
        self.execution.authorize(order, payload=payload, timestamp=timestamp)

    def publish(self, notification: ExecutionNotification) -> None:
        self.execution.publish(notification)

    def authorize_and_submit(self, order: OrderIntent, *, payload: dict[str, object] | None = None,
                             timestamp: str | None = None) -> None:
        """Run authorization/reservation and coordinator submission for a paper order."""
        self.execution.authorize(
            order,
            payload=payload or {
                "client_order_id": order.client_order_id,
                "strategy_id": order.strategy_id,
                "account_id": order.account_id,
            },
            timestamp=timestamp,
        )

    def lifecycle(self, client_order_id: str) -> dict[str, object] | None:
        lookup = getattr(self.execution.store, "read_order_lifecycle", None)
        if not callable(lookup):
            raise NotImplementedError("order lifecycle lookup is unavailable")
        return cast(dict[str, object] | None, lookup(client_order_id))

    def modify(self, _order: OrderIntent) -> None:
        raise NotImplementedError("paper order modification is not supported")

    def cancel(self, _client_order_id: str) -> None:
        raise NotImplementedError("paper order cancellation is not exposed by this facade")


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


class PnlManager:
    """Read-only view over the one canonical portfolio aggregate."""

    def __init__(self, portfolio: PortfolioState) -> None:
        self._portfolio = portfolio

    def snapshot(self) -> dict[str, float]:
        return self._portfolio.capital_snapshot()

    def pnl_snapshot(self) -> dict[str, float]:
        return self.snapshot()


__all__ = ["FeedController", "FeedManager", "OrderManager", "PnlManager", "StrategyRunner"]
