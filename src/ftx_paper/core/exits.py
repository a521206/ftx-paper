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
    cell: str | None = None
    exit_mode: str = "signal"


@dataclass(frozen=True, slots=True)
class ExitAction:
    reason: str
    price: float
    intent: OrderIntent
    cell: str | None = None


class ExitStateMachine:
    """Deterministic protective-exit rules for one open position."""

    def __init__(self, *, trail_distance: float | None = None, close_time: time = time(15, 20)) -> None:
        if trail_distance is not None and trail_distance <= 0:
            raise ValueError("trail distance must be positive")
        self.trail_distance, self.close_time = trail_distance, close_time
        self._closes: list[float] = []

    def evaluate(self, position: PositionState, *, timestamp: datetime, high: float, low: float, close: float, client_order_id: str, include_signal: bool = True) -> ExitAction | None:
        favorable = high if position.side is OrderSide.BUY else low
        self._closes.append(close)
        stop_hit = low <= position.stop_price if position.side is OrderSide.BUY else high >= position.stop_price
        if stop_hit:
            return self._action(position, position.stop_price, "stop", client_order_id)
        if self.trail_distance is not None:
            trail = favorable - self.trail_distance if position.side is OrderSide.BUY else favorable + self.trail_distance
            if (position.side is OrderSide.BUY and low <= trail and favorable > position.entry_price) or (position.side is OrderSide.SELL and high >= trail and favorable < position.entry_price):
                return self._action(position, trail, "trailing_stop", client_order_id)
        if include_signal and position.exit_mode == "signal" and len(self._closes) >= 3:
            recent = self._closes[-3:]
            adverse = (recent[0] > recent[1] > recent[2] if position.side is OrderSide.BUY
                        else recent[0] < recent[1] < recent[2])
            if adverse:
                return self._action(position, close, "counter_move", client_order_id)
        if timestamp.timetz().replace(tzinfo=None) >= self.close_time:
            return self._action(position, close, "session_close", client_order_id)
        return None

    def evaluate_tick(self, position: PositionState, *, timestamp: datetime, price: float,
                      client_order_id: str) -> ExitAction | None:
        """Evaluate one live quote immediately; hard stops have no activation delay."""
        return self.evaluate(position, timestamp=timestamp, high=price, low=price,
                             close=price, client_order_id=client_order_id, include_signal=False)

    @staticmethod
    def _action(position: PositionState, price: float, reason: str, client_order_id: str) -> ExitAction:
        side = OrderSide.SELL if position.side is OrderSide.BUY else OrderSide.BUY
        intent = OrderIntent(client_order_id, position.instrument, side, position.quantity, reason=reason)
        return ExitAction(reason, price, intent, position.cell)
