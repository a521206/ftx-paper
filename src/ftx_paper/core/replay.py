from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable
from ftx_paper.contracts import MarketBar, OrderIntent
from .engine import PaperEngine


@dataclass(frozen=True, slots=True)
class ReplayResult:
    orders: tuple[OrderIntent, ...]
    events: tuple[dict[str, object], ...]
    bars_seen: int


def replay(engine: PaperEngine, bars: Iterable[MarketBar]) -> ReplayResult:
    """Run a deterministic completed-bar transcript through the live engine."""
    previous = None
    orders: list[OrderIntent] = []
    events: list[dict[str, object]] = []
    for bar in bars:
        if previous is not None and bar.timestamp <= previous:
            raise ValueError("replay bars must be strictly increasing")
        previous = bar.timestamp
        result = engine.on_bar(bar)
        orders.extend(result.orders)
        events.extend(result.events)
    return ReplayResult(tuple(orders), tuple(events), engine.bars_seen)
