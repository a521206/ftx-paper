"""Strategy-agnostic incremental trailing-stop risk primitive."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TrailConfig:
    activation_bp: float = 10.0
    distance_bp: float = 4.0
    breakeven_lock: bool = True
    time_stop_bars: int | None = None

    def __post_init__(self) -> None:
        for name, value in (("activation_bp", self.activation_bp), ("distance_bp", self.distance_bp)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a finite value > 0, got {value!r}")


@dataclass(frozen=True, slots=True)
class TrailDecision:
    should_exit: bool
    fill_price: float | None = None
    reason: str = ""


class TrailTracker:
    def __init__(
        self,
        entry_price: float,
        direction: str,
        hard_stop_bp: float,
        config: TrailConfig | None = None,
    ) -> None:
        if entry_price <= 0:
            raise ValueError(f"entry_price must be > 0, got {entry_price}")
        if direction not in ("long", "short"):
            raise ValueError(f"direction must be 'long' or 'short', got {direction!r}")
        if hard_stop_bp <= 0:
            raise ValueError(f"hard_stop_bp must be > 0, got {hard_stop_bp}")
        self._entry = entry_price
        self._is_short = direction == "short"
        self._hard_stop_dist = hard_stop_bp / 10000.0
        self._config = config or TrailConfig()
        self._best_rel = 1.0
        self._activated = False
        self._bar_count = 0
        self._exited = False

    @property
    def is_active(self) -> bool:
        return self._activated

    @property
    def current_trail_price(self) -> float | None:
        if not self._activated:
            return None
        trail_dist = self._config.distance_bp / 10000.0
        if self._config.breakeven_lock:
            if self._is_short:
                return self._entry * min(self._best_rel + trail_dist, 1.0)
            return self._entry * max(self._best_rel - trail_dist, 1.0)
        if self._is_short:
            return self._entry * (self._best_rel + trail_dist)
        return self._entry * (self._best_rel - trail_dist)

    def update(
        self,
        bar_open: float,
        bar_high: float,
        bar_low: float,
        bar_close: float,
        is_eod_bar: bool = False,
        evaluate_exit: bool = True,
    ) -> TrailDecision:
        if self._exited and evaluate_exit:
            return TrailDecision(True, reason="already_exited")
        self._bar_count += 1
        o_rel = bar_open / self._entry
        h_rel = bar_high / self._entry
        lo_rel = bar_low / self._entry
        if self._is_short:
            self._best_rel = min(self._best_rel, lo_rel)
            fav_bp = (1.0 - self._best_rel) * 10000.0
        else:
            self._best_rel = max(self._best_rel, h_rel)
            fav_bp = (self._best_rel - 1.0) * 10000.0
        hard_limit = 1.0 + self._hard_stop_dist if self._is_short else 1.0 - self._hard_stop_dist
        if evaluate_exit and self._is_short and h_rel >= hard_limit:
            self._exited = True
            return TrailDecision(True, bar_open if o_rel > hard_limit else self._entry * hard_limit, "hard_stop")
        if evaluate_exit and not self._is_short and lo_rel <= hard_limit:
            self._exited = True
            return TrailDecision(True, bar_open if o_rel < hard_limit else self._entry * hard_limit, "hard_stop")
        was_active = self._activated
        if not self._activated and fav_bp >= self._config.activation_bp:
            self._activated = True
        if evaluate_exit and was_active:
            trail_price = self.current_trail_price
            if trail_price is not None:
                if self._is_short and h_rel >= trail_price / self._entry:
                    self._exited = True
                    return TrailDecision(True, bar_open if o_rel > trail_price / self._entry else trail_price, "trail_stop")
                if not self._is_short and lo_rel <= trail_price / self._entry:
                    self._exited = True
                    return TrailDecision(True, bar_open if o_rel < trail_price / self._entry else trail_price, "trail_stop")
        if evaluate_exit and is_eod_bar:
            self._exited = True
            return TrailDecision(True, bar_close, "eod")
        if evaluate_exit and self._config.time_stop_bars is not None and self._bar_count >= self._config.time_stop_bars:
            self._exited = True
            return TrailDecision(True, bar_close, "time_stop")
        return TrailDecision(False)
