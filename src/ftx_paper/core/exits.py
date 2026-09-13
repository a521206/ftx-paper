from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from ftx_paper.contracts import Instrument, OrderIntent, OrderRole, OrderSide


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
    entry_fill_time: datetime | None = None
    # Synthetic fills are marked in synthetic-future price space, while the
    # protective exit is driven by the underlying futures bar.  Keep the
    # reference entry separate so the stop/trail never mixes price domains.
    exit_reference_price: float | None = None
    target_price: float | None = None


@dataclass(frozen=True, slots=True)
class ExitAction:
    reason: str
    price: float
    intent: OrderIntent
    cell: str | None = None


class ExitStateMachine:
    """Deterministic protective-exit rules for one open position."""

    def __init__(self, *, trail_distance: float | None = None,
                 trail_activation_bp: float | None = None,
                 trail_distance_bp: float | None = None,
                 close_time: time = time(15, 30)) -> None:
        if trail_distance is not None and trail_distance <= 0:
            raise ValueError("trail distance must be positive")
        if trail_activation_bp is not None and trail_activation_bp <= 0:
            raise ValueError("trail activation must be positive")
        if trail_distance_bp is not None and trail_distance_bp <= 0:
            raise ValueError("trail distance must be positive")
        self.trail_distance = trail_distance
        self.trail_activation_bp = trail_activation_bp
        self.trail_distance_bp = trail_distance_bp
        self.close_time = close_time
        self._closes: list[float] = []
        self._max_favorable_price: float | None = None
        self._trail_active = False

    def evaluate(self, position: PositionState, *, timestamp: datetime, high: float, low: float,
                 close: float, client_order_id: str, include_signal: bool = True,
                 open: float | None = None) -> ExitAction | None:
        reference_entry = position.exit_reference_price or position.entry_price
        open_price = close if open is None else open
        self._closes.append(close)
        stop_hit = low <= position.stop_price if position.side is OrderSide.BUY else high >= position.stop_price
        if stop_hit:
            if position.side is OrderSide.BUY:
                stop_fill = open_price if open_price < position.stop_price else position.stop_price
            else:
                stop_fill = open_price if open_price > position.stop_price else position.stop_price
            return self._action(position, stop_fill, "hard_stop", client_order_id)
        favorable = high if position.side is OrderSide.BUY else low
        if position.exit_mode == "target" and position.target_price is not None:
            target_hit = (high >= position.target_price if position.side is OrderSide.BUY
                          else low <= position.target_price)
            if target_hit:
                target_fill = (open_price if (open_price > position.target_price if position.side is OrderSide.BUY
                                              else open_price < position.target_price)
                               else position.target_price)
                return self._action(position, target_fill, "target", client_order_id)
        previous_active = self._trail_active
        if self._max_favorable_price is None:
            self._max_favorable_price = favorable
        elif position.side is OrderSide.BUY:
            self._max_favorable_price = max(self._max_favorable_price, favorable)
        else:
            self._max_favorable_price = min(self._max_favorable_price, favorable)
        if self.trail_activation_bp is not None and self.trail_distance_bp is not None:
            favorable_bp = ((self._max_favorable_price - reference_entry) / reference_entry * 10000
                            if position.side is OrderSide.BUY else
                            (reference_entry - self._max_favorable_price) / reference_entry * 10000)
            if favorable_bp >= self.trail_activation_bp:
                self._trail_active = True
            if previous_active:
                distance = self.trail_distance_bp / 10000
                trail = (max(self._max_favorable_price * (1 - distance), reference_entry)
                         if position.side is OrderSide.BUY else
                         min(self._max_favorable_price * (1 + distance), reference_entry))
                if (position.side is OrderSide.BUY and low <= trail) or (position.side is OrderSide.SELL and high >= trail):
                    fill = open_price if (open_price < trail if position.side is OrderSide.BUY else open_price > trail) else trail
                    return self._action(position, fill, "trail_stop", client_order_id)
        elif self.trail_distance is not None:
            trail = (self._max_favorable_price - self.trail_distance if position.side is OrderSide.BUY
                     else self._max_favorable_price + self.trail_distance)
            if ((position.side is OrderSide.BUY and low <= trail and favorable > reference_entry) or
                    (position.side is OrderSide.SELL and high >= trail and favorable < reference_entry)):
                fill = open_price if (open_price < trail if position.side is OrderSide.BUY else open_price > trail) else trail
                return self._action(position, fill, "trail_stop", client_order_id)
        if include_signal and position.exit_mode == "signal" and len(self._closes) >= 3:
            recent = self._closes[-3:]
            adverse = (recent[0] > recent[1] > recent[2] if position.side is OrderSide.BUY
                        else recent[0] < recent[1] < recent[2])
            if adverse:
                return self._action(position, close, "counter_move", client_order_id)
        if timestamp.timetz().replace(tzinfo=None) >= self.close_time:
            return self._action(position, close, "eod", client_order_id)
        return None

    def evaluate_tick(self, position: PositionState, *, timestamp: datetime, price: float,
                      client_order_id: str) -> ExitAction | None:
        """Evaluate one live quote immediately; hard stops have no activation delay."""
        return self.evaluate(position, timestamp=timestamp, high=price, low=price,
                             close=price, open=price, client_order_id=client_order_id,
                             include_signal=False)

    @staticmethod
    def _action(position: PositionState, price: float, reason: str, client_order_id: str) -> ExitAction:
        side = OrderSide.SELL if position.side is OrderSide.BUY else OrderSide.BUY
        intent = OrderIntent(client_order_id, position.instrument, side, position.quantity, reason=reason, role=OrderRole.EXIT)
        return ExitAction(reason, price, intent, position.cell)
