from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from ftx_paper.contracts import MarketBar, OrderIntent, OrderRole, OrderSide
from .features import LiveFeatures


@dataclass(frozen=True, slots=True)
class SetupDecision:
    setup: str
    side: OrderSide
    reference: str
    entry_price: float
    reason: str


class SetupPolicy:
    """Small, deterministic setup policy over public core features."""

    def __init__(self, *, proximity: float = 15.0, start: time = time(10, 15), end: time = time(14, 15)) -> None:
        if proximity <= 0 or start >= end:
            raise ValueError("invalid setup policy bounds")
        self.proximity, self.start, self.end = proximity, start, end
        self._last_timestamp = None

    def evaluate(self, bar: MarketBar, features: LiveFeatures, *, prior_high: float | None = None, prior_low: float | None = None) -> SetupDecision | None:
        if self._last_timestamp is not None and bar.timestamp <= self._last_timestamp:
            return None
        self._last_timestamp = bar.timestamp
        if not self.start <= bar.timestamp.timetz().replace(tzinfo=None) <= self.end:
            return None
        if features.vwap is None:
            return None
        references = (("prior_high", prior_high, OrderSide.SELL), ("prior_low", prior_low, OrderSide.BUY), ("vwap", features.vwap, OrderSide.BUY if bar.close >= features.vwap else OrderSide.SELL))
        for name, level, side in references:
            if level is not None and abs(bar.close - level) <= self.proximity:
                setup = "reversal_at_" + name
                return SetupDecision(setup, side, name, bar.close, f"close_within_{self.proximity:g}_points")
        return None

    @staticmethod
    def to_order(decision: SetupDecision, bar: MarketBar, quantity: int, client_order_id: str) -> OrderIntent:
        if quantity < 1:
            raise ValueError("quantity must be positive")
        return OrderIntent(client_order_id, bar.instrument, decision.side, quantity, reason=decision.reason, role=OrderRole.ENTRY)
