from __future__ import annotations

from dataclasses import dataclass, field

from ftx_paper.contracts import MarketBar, OrderIntent


@dataclass(slots=True)
class SessionState:
    trading_date: str | None = None
    bars_seen: int = 0
    capital: float = 0.0
    open_positions: dict[str, int] = field(default_factory=dict)


class LiveSession:
    """Session-time orchestration boundary around the pure engine."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self.state = SessionState()

    def on_bar(self, bar: MarketBar) -> tuple[OrderIntent, ...]:
        self.state.bars_seen += 1
        result = self.engine.on_bar(bar)
        return result.orders
