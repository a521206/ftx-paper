from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo
from typing import cast
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
    bars_held: int = 0
    mae_bp: float = 0.0
    mfe_bp: float = 0.0


class ExitStateMachine:
    """Deterministic protective-exit rules for one open position at a time."""

    def __init__(self, *, trail_distance: float | None = None,
                 trail_activation_bp: float | None = None,
                 trail_distance_bp: float | None = None,
                 close_time: time = time(15, 10),
                 initial_close: float | None = None,
                 counter_move_bars: int = 2) -> None:
        if counter_move_bars < 1:
            raise ValueError("counter-move bars must be positive")
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
        self.counter_move_bars = counter_move_bars
        self._closes: deque[float] = deque(maxlen=counter_move_bars + 1)
        if initial_close is not None:
            self._closes.append(float(initial_close))
        # Signal exits use the completed bars before the current bar for the
        # canonical volume-climax average.  Keep this causal history separate
        # from close history so replay and live evaluation share the same
        # event-time rule.
        self._volumes: deque[float] = deque(maxlen=10)
        self._max_favorable_price: float | None = None
        self._trail_active = False
        self._bars_held = 0
        self._mae_bp = 0.0
        self._mfe_bp = 0.0

    def reset(self) -> None:
        """Clear position-specific history before evaluating a new position."""
        self._closes.clear()
        self._volumes.clear()
        self._max_favorable_price = None
        self._trail_active = False
        self._bars_held = 0
        self._mae_bp = 0.0
        self._mfe_bp = 0.0

    def snapshot(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "trail_distance": self.trail_distance,
            "trail_activation_bp": self.trail_activation_bp,
            "trail_distance_bp": self.trail_distance_bp,
            "close_time": self.close_time.isoformat(),
            "counter_move_bars": self.counter_move_bars,
            "closes": list(self._closes),
            "volumes": list(self._volumes),
            "max_favorable_price": self._max_favorable_price,
            "trail_active": self._trail_active,
            "bars_held": self._bars_held,
            "mae_bp": self._mae_bp,
            "mfe_bp": self._mfe_bp,
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
            trail_distance=cast(float | None, snapshot.get("trail_distance")),
            trail_activation_bp=cast(float | None, snapshot.get("trail_activation_bp")),
            trail_distance_bp=cast(float | None, snapshot.get("trail_distance_bp")),
            close_time=parsed_close_time,
            counter_move_bars=int(cast(int, snapshot.get("counter_move_bars", 2))),
        )
        closes = snapshot.get("closes", ())
        if not isinstance(closes, (list, tuple)) or len(closes) > machine.counter_move_bars + 1:
            raise ValueError("exit-state snapshot closes exceed the counter-move window")
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in closes):
            raise ValueError("exit-state snapshot closes must be numeric")
        machine._closes.extend(float(value) for value in closes)
        volumes = snapshot.get("volumes", ())
        if not isinstance(volumes, (list, tuple)) or len(volumes) > 10:
            raise ValueError("exit-state snapshot volumes must contain at most ten values")
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in volumes):
            raise ValueError("exit-state snapshot volumes must be numeric")
        machine._volumes.extend(float(value) for value in volumes)
        maximum = snapshot.get("max_favorable_price")
        if maximum is not None and (isinstance(maximum, bool) or not isinstance(maximum, (int, float))):
            raise ValueError("exit-state snapshot max_favorable_price must be numeric")
        machine._max_favorable_price = float(maximum) if maximum is not None else None
        trail_active = snapshot.get("trail_active", False)
        if not isinstance(trail_active, bool):
            raise ValueError("exit-state snapshot trail_active must be boolean")
        machine._trail_active = trail_active
        for name in ("bars_held", "mae_bp", "mfe_bp"):
            value = snapshot.get(name, 0)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
                raise ValueError(f"exit-state snapshot {name} is invalid")
            setattr(machine, f"_{name}", int(value) if name == "bars_held" else float(value))
        return machine

    def evaluate(self, position: PositionState, *, timestamp: datetime, high: float, low: float,
                 close: float, client_order_id: str, include_signal: bool = True,
                 open: float | None = None, volume: float | None = None,
                 count_bar: bool = True) -> ExitAction | None:
        reference_entry = (position.exit_reference_price
                           if position.exit_reference_price is not None
                           else position.entry_price)
        open_price = close if open is None else open
        prior_volumes = tuple(self._volumes)
        self._closes.append(close)
        if count_bar:
            self._bars_held += 1
            if volume is not None:
                self._volumes.append(float(volume))
        if position.side is OrderSide.BUY:
            adverse = (reference_entry - low) / reference_entry * 10000
            favorable_excursion = (high - reference_entry) / reference_entry * 10000
        else:
            adverse = (high - reference_entry) / reference_entry * 10000
            favorable_excursion = (reference_entry - low) / reference_entry * 10000
        self._mae_bp = max(self._mae_bp, adverse)
        self._mfe_bp = max(self._mfe_bp, favorable_excursion)
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
        # Canonical signal management is armed only after a meaningful
        # favorable excursion.  Without this guard, Paper exits on an early
        # three-close reversal that canonical keeps open until a hard stop.
        if include_signal and position.exit_mode == "signal" and self._mfe_bp > 5:
            if prior_volumes:
                average_volume = sum(prior_volumes) / len(prior_volumes)
                if average_volume > 0 and volume is not None and volume > 2.5 * average_volume:
                    return self._action(position, close, "vol_climax", client_order_id)
            if len(self._closes) >= self.counter_move_bars + 1:
                recent = tuple(self._closes)
                if position.side is OrderSide.BUY:
                    adverse = all(recent[-index] < recent[-index - 1] for index in range(1, self.counter_move_bars + 1))
                else:
                    adverse = all(recent[-index] > recent[-index - 1] for index in range(1, self.counter_move_bars + 1))
                if adverse:
                    return self._action(position, close, "counter_move", client_order_id)
        local_time = timestamp.astimezone(ZoneInfo("Asia/Kolkata")).time()
        if local_time >= self.close_time:
            return self._action(position, close, "eod", client_order_id)
        return None

    def evaluate_tick(self, position: PositionState, *, timestamp: datetime, price: float,
                      client_order_id: str) -> ExitAction | None:
        """Evaluate one live quote immediately; hard stops have no activation delay."""
        state = (
            deque(self._closes), self._max_favorable_price, self._trail_active,
            self._bars_held, self._mae_bp, self._mfe_bp,
        )
        try:
            return self.evaluate(position, timestamp=timestamp, high=price, low=price,
                                 close=price, open=price, client_order_id=client_order_id,
                                 include_signal=False, count_bar=False)
        finally:
            (self._closes, self._max_favorable_price, self._trail_active,
             self._bars_held, self._mae_bp, self._mfe_bp) = state

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
        # Basis-point trails use a fixed distance measured from the entry
        # price, matching the canonical trail contract. Scaling the distance
        # from the favorable price creates a price-dependent drift.
        fixed_distance = reference_entry * distance
        return (max(favorable - fixed_distance, reference_entry)
                if position.side is OrderSide.BUY else min(favorable + fixed_distance, reference_entry))

    def _action(self, position: PositionState, price: float, reason: str, client_order_id: str) -> ExitAction:
        side = OrderSide.SELL if position.side is OrderSide.BUY else OrderSide.BUY
        intent = OrderIntent(
            client_order_id, position.instrument, side, position.quantity,
            reason=reason, role=OrderRole.EXIT, vehicle=position.vehicle,
            exit_mode=position.exit_mode,
            synthetic_legs=position.synthetic_legs,
            entry_bar=position.entry_bar,
            entry_order_id=position.entry_order_id,
        )
        return ExitAction(reason, price, intent, position.cell,
                          self._bars_held, self._mae_bp, self._mfe_bp)
