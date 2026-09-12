from __future__ import annotations

from dataclasses import dataclass

from ftx_paper.contracts import MarketBar, OrderIntent
from .bundles import DecisionBundle
from .strategy import Strategy, StrategyMetadata


@dataclass(frozen=True, slots=True)
class EngineResult:
    orders: tuple[OrderIntent, ...] = ()
    events: tuple[dict[str, object], ...] = ()


class PaperEngine:
    """Runtime coordinator around a replaceable, broker-neutral strategy."""

    def __init__(self, strategy: Strategy | None = None) -> None:
        self.bars_seen = 0
        self.strategy = strategy

    @property
    def strategy_metadata(self) -> StrategyMetadata | None:
        return self.strategy.metadata if self.strategy is not None else None

    def on_bar(self, bar: MarketBar) -> EngineResult:
        self.bars_seen += 1
        orders = self.strategy.on_bar(bar) if self.strategy is not None else ()
        return EngineResult(orders=orders, events=({
            "symbol": bar.instrument.symbol,
            "timestamp": bar.timestamp.isoformat(),
            "close": bar.close,
            "bars_seen": self.bars_seen,
        },))

    def reset(self) -> None:
        """Reset deterministic strategy state before a fresh replay."""
        self.bars_seen = 0
        reset = getattr(self.strategy, "reset", None)
        if callable(reset):
            reset()

    def on_bundle(self, bundle: DecisionBundle) -> EngineResult:
        """Evaluate one synchronized minute, then hand orders to execution."""
        self.bars_seen += len(bundle.bars)
        if self.strategy is None or not hasattr(self.strategy, "on_bundle"):
            return EngineResult(events=({"bundle_id": bundle.bundle_id, "minute": bundle.minute,
                                         "bundle_complete": bundle.complete},))
        decisions = self.strategy.on_bundle(bundle)
        orders = tuple(order for item in decisions if (order := item.order) is not None)
        events = tuple({"event_type": item.event_type, **dict(item.payload)} for item in decisions)
        return EngineResult(orders=orders, events=events)

    def record_exit(self, **kwargs: object) -> None:
        """Forward a settled exit to strategies that maintain risk-gate state."""
        record_exit = getattr(self.strategy, "record_exit", None)
        if callable(record_exit):
            record_exit(**kwargs)

    def on_tick(self, bar: MarketBar):
        """Forward a live tick to the strategy's protective exit state."""
        on_tick = getattr(self.strategy, "on_tick", None)
        return on_tick(bar) if callable(on_tick) else ()

    def register_entry(self, order: OrderIntent, *, fill_price: float | None = None,
                       entry_fill_time=None) -> None:
        register = getattr(self.strategy, "register_entry", None)
        if callable(register):
            try:
                register(order, fill_price=fill_price, entry_fill_time=entry_fill_time)
            except TypeError as exc:
                if "entry_fill_time" not in str(exc):
                    raise
                register(order, fill_price=fill_price)

    def on_closed_bar(self, bar: MarketBar):
        handler = getattr(self.strategy, "on_closed_bar", None)
        return handler(bar) if callable(handler) else ()

    def settle_exit(self, exit_order_id: str, *, filled: bool) -> None:
        handler = getattr(self.strategy, "settle_exit", None)
        if callable(handler):
            handler(exit_order_id, filled=filled)
