from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from ftx_paper.contracts import Instrument, OrderIntent, OrderSide


@dataclass(frozen=True, slots=True)
class PositionState:
    instrument: Instrument
    entry_price: float
    stop_price: float
    quantity: int
    side: OrderSide
    peak_price: float | None = None


@dataclass(frozen=True, slots=True)
class ExitAction:
    reason: str
    price: float
    intent: OrderIntent


class ExitStateMachine:
    """Deterministic protective-exit rules for one open position."""

    def __init__(self, *, trail_distance: float | None = None, close_time: time = time(15, 20)) -> None:
        if trail_distance is not None and trail_distance <= 0:
            raise ValueError("trail distance must be positive")
        self.trail_distance, self.close_time = trail_distance, close_time

    def evaluate(self, position: PositionState, *, timestamp: datetime, high: float, low: float, close: float, client_order_id: str) -> ExitAction | None:
        favorable = high if position.side is OrderSide.BUY else low
        stop_hit = low <= position.stop_price if position.side is OrderSide.BUY else high >= position.stop_price
        if stop_hit:
            return self._action(position, position.stop_price, "stop", client_order_id)
        if self.trail_distance is not None:
            trail = favorable - self.trail_distance if position.side is OrderSide.BUY else favorable + self.trail_distance
            if (position.side is OrderSide.BUY and low <= trail and favorable > position.entry_price) or (position.side is OrderSide.SELL and high >= trail and favorable < position.entry_price):
                return self._action(position, trail, "trailing_stop", client_order_id)
        if timestamp.timetz().replace(tzinfo=None) >= self.close_time:
            return self._action(position, close, "session_close", client_order_id)
        return None

    @staticmethod
    def _action(position: PositionState, price: float, reason: str, client_order_id: str) -> ExitAction:
        side = OrderSide.SELL if position.side is OrderSide.BUY else OrderSide.BUY
        intent = OrderIntent(client_order_id, position.instrument, side, position.quantity, reason=reason)
        return ExitAction(reason, price, intent)
