from __future__ import annotations

from dataclasses import dataclass

from ftx_paper.contracts import MarketBar, OrderIntent
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
