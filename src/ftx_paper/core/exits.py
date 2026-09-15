from __future__ import annotations

from collections import deque
from collections.abc import Mapping
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
    cell: str | None = None
    exit_mode: str = "signal"
    entry_fill_time: datetime | None = None
    # Synthetic fills are marked in synthetic-future price space, while the
    # protective exit is driven by the underlying futures bar.  Keep the
    # reference entry separate so the stop/trail never mixes price domains.
    exit_reference_price: float | None = None
    target_price: float | None = None
    vehicle: str = "futures"
    synthetic_legs: tuple[Instrument, Instrument] | None = None
    entry_bar: int | None = None
    entry_order_id: str | None = None


@dataclass(frozen=True, slots=True)
class ExitAction:
    reason: str
    price: float
    intent: OrderIntent
    cell: str | None = None


class ExitStateMachine:
    """Deterministic protective-exit rules for one open position at a time."""

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
        bp_values = (trail_activation_bp, trail_distance_bp)
        if (trail_activation_bp is None) != (trail_distance_bp is None):
            raise ValueError("basis-point trail activation and distance must be supplied together")
        if trail_distance is not None and any(value is not None for value in bp_values):
            raise ValueError("fixed-distance and basis-point trailing modes are mutually exclusive")
        self.trail_distance = trail_distance
        self.trail_activation_bp = trail_activation_bp
        self.trail_distance_bp = trail_distance_bp
        self.close_time = close_time
        self._closes: deque[float] = deque(maxlen=3)
        self._max_favorable_price: float | None = None
        self._trail_active = False

    def reset(self) -> None:
        """Clear position-specific history before evaluating a new position."""
        self._closes.clear()
        self._max_favorable_price = None
        self._trail_active = False

    def snapshot(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "trail_distance": self.trail_distance,
            "trail_activation_bp": self.trail_activation_bp,
            "trail_distance_bp": self.trail_distance_bp,
            "close_time": self.close_time.isoformat(),
            "closes": list(self._closes),
            "max_favorable_price": self._max_favorable_price,
            "trail_active": self._trail_active,
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "ExitStateMachine":
        if snapshot.get("schema_version") != 1:
            raise ValueError("unsupported exit-state snapshot schema")
        close_time = snapshot.get("close_time")
        if not isinstance(close_time, str):
            raise ValueError("exit-state snapshot close_time must be a string")
        try:
            parsed_close_time = time.fromisoformat(close_time)
        except ValueError as exc:
            raise ValueError("exit-state snapshot close_time is invalid") from exc
        machine = cls(
            trail_distance=snapshot.get("trail_distance"),
            trail_activation_bp=snapshot.get("trail_activation_bp"),
            trail_distance_bp=snapshot.get("trail_distance_bp"),
            close_time=parsed_close_time,
        )
        closes = snapshot.get("closes", ())
        if not isinstance(closes, (list, tuple)) or len(closes) > 3:
            raise ValueError("exit-state snapshot closes must contain at most three prices")
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in closes):
            raise ValueError("exit-state snapshot closes must be numeric")
        machine._closes.extend(float(value) for value in closes)
        maximum = snapshot.get("max_favorable_price")
        if maximum is not None and (isinstance(maximum, bool) or not isinstance(maximum, (int, float))):
            raise ValueError("exit-state snapshot max_favorable_price must be numeric")
        machine._max_favorable_price = float(maximum) if maximum is not None else None
        trail_active = snapshot.get("trail_active", False)
        if not isinstance(trail_active, bool):
            raise ValueError("exit-state snapshot trail_active must be boolean")
        machine._trail_active = trail_active
        return machine

    def evaluate(self, position: PositionState, *, timestamp: datetime, high: float, low: float,
                 close: float, client_order_id: str, include_signal: bool = True,
                 open: float | None = None) -> ExitAction | None:
        reference_entry = (position.exit_reference_price
                           if position.exit_reference_price is not None
                           else position.entry_price)
        open_price = close if open is None else open
        self._closes.append(close)
        stop_hit = (low <= position.stop_price if position.side is OrderSide.BUY
                    else high >= position.stop_price)
        if stop_hit:
            stop_fill = self._fill_at_or_beyond_level(position, open_price, position.stop_price)
            return self._action(position, stop_fill, "hard_stop", client_order_id)
        favorable = high if position.side is OrderSide.BUY else low
        if position.exit_mode == "target" and position.target_price is not None:
            target_hit = (high >= position.target_price if position.side is OrderSide.BUY
                          else low <= position.target_price)
            if target_hit:
                target_fill = (open_price if self._gap_fills(position, open_price, position.target_price)
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
                trail = self._trail_price(position, reference_entry, self._max_favorable_price,
                                          self.trail_distance_bp / 10000)
                if ((position.side is OrderSide.BUY and low <= trail)
                        or (position.side is OrderSide.SELL and high >= trail)):
                    fill = self._fill_at_or_beyond_level(position, open_price, trail)
                    return self._action(position, fill, "trail_stop", client_order_id)
        elif self.trail_distance is not None:
            trail = self._trail_price(position, reference_entry, self._max_favorable_price,
                                      self.trail_distance, absolute=True)
            if (((position.side is OrderSide.BUY and low <= trail)
                 or (position.side is OrderSide.SELL and high >= trail))
                    and ((position.side is OrderSide.BUY and favorable > reference_entry)
                         or (position.side is OrderSide.SELL and favorable < reference_entry))):
                fill = self._fill_at_or_beyond_level(position, open_price, trail)
                return self._action(position, fill, "trail_stop", client_order_id)
        if include_signal and position.exit_mode == "signal" and len(self._closes) >= 3:
            recent = tuple(self._closes)
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
    def _gap_fills(position: PositionState, open_price: float, level: float) -> bool:
        return open_price > level if position.side is OrderSide.BUY else open_price < level

    @staticmethod
    def _fill_at_or_beyond_level(position: PositionState, open_price: float, level: float) -> float:
        return open_price if (open_price < level if position.side is OrderSide.BUY else open_price > level) else level

    @staticmethod
    def _trail_price(position: PositionState, reference_entry: float, favorable: float,
                     distance: float, *, absolute: bool = False) -> float:
        if absolute:
            return favorable - distance if position.side is OrderSide.BUY else favorable + distance
        return (max(favorable * (1 - distance), reference_entry)
                if position.side is OrderSide.BUY else min(favorable * (1 + distance), reference_entry))

    @staticmethod
    def _action(position: PositionState, price: float, reason: str, client_order_id: str) -> ExitAction:
        side = OrderSide.SELL if position.side is OrderSide.BUY else OrderSide.BUY
        intent = OrderIntent(
            client_order_id, position.instrument, side, position.quantity,
            reason=reason, role=OrderRole.EXIT, vehicle=position.vehicle,
            synthetic_legs=position.synthetic_legs,
            entry_bar=position.entry_bar,
            entry_order_id=position.entry_order_id,
        )
        return ExitAction(reason, price, intent, position.cell)
