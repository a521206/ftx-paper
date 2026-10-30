"""Pure scale diagnostics relative to a frozen ATR reference."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass(frozen=True, slots=True)
class AtrReference:
    """Immutable ATR value and causal provenance."""

    value: float | None
    status: Literal["available", "unavailable"]
    reason: str
    source: Literal["daily", "intraday"]
    lookback: int
    smoothing: Literal["wilder", "sma"]
    instrument_or_series: str
    timeframe: str
    source_session: str
    source_available_at: datetime | None
    frozen_at: datetime
    freeze_basis: str

    def __post_init__(self) -> None:
        if self.lookback < 1:
            raise ValueError("ATR lookback must be positive")
        if self.status == "available":
            if self.value is None or not math.isfinite(self.value) or self.value <= 0:
                raise ValueError("available ATR references require a positive finite value")
            if not self.reason:
                raise ValueError("available ATR references require a reason")
        elif self.status == "unavailable":
            if self.value is not None:
                raise ValueError("unavailable ATR references cannot contain a value")
            if not self.reason:
                raise ValueError("unavailable ATR references require a reason")
        else:
            raise ValueError("ATR reference status must be available or unavailable")
        if not self.instrument_or_series or not self.timeframe or not self.source_session:
            raise ValueError("ATR references require source identity")
        if not self.freeze_basis:
            raise ValueError("ATR references require a freeze basis")


def _safe_divide(numerator: float, atr_ref: AtrReference | float | None) -> float | None:
    if isinstance(atr_ref, AtrReference):
        atr_ref = atr_ref.value if atr_ref.status == "available" else None
    if atr_ref is None or not math.isfinite(atr_ref) or atr_ref <= 0:
        return None
    return numerator / atr_ref


def box_scale(upper: float, lower: float, atr_ref: AtrReference | float | None) -> float | None:
    return _safe_divide(upper - lower, atr_ref)


def clearance_up(upper: float, price_high: float, atr_ref: AtrReference | float | None) -> float | None:
    return _safe_divide(max(price_high - upper, 0.0), atr_ref)


def clearance_down(lower: float, price_low: float, atr_ref: AtrReference | float | None) -> float | None:
    return _safe_divide(max(lower - price_low, 0.0), atr_ref)


def displacement_since(anchor_close: float, close: float, atr_ref: AtrReference | float | None) -> float | None:
    return _safe_divide(abs(close - anchor_close), atr_ref)


def retracement_since(
    extreme_close: float,
    close: float,
    direction_up: bool,
    atr_ref: AtrReference | float | None,
) -> float | None:
    if direction_up:
        adverse = max(extreme_close - close, 0.0)
    else:
        adverse = max(close - extreme_close, 0.0)
    return _safe_divide(adverse, atr_ref)
