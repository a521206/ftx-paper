"""Canonical FTX adaptive hard-stop calculation."""

from __future__ import annotations

from collections.abc import Sequence

from ftx_paper.contracts import MarketBar

STOP_ATR_SCALE = 2.0
STOP_MIN_BP = 15.0
STOP_MAX_BP = 50.0
VIX_BASELINE = 16.0
EXPIRY_STOP_MULTIPLIER = 1.5


def adaptive_stop_bp(
    bars_before: Sequence[MarketBar], vix_at_event: float, *,
    is_expiry_day: bool = False, current_close: float | None = None,
) -> float:
    """Return the bounded ATR/VIX stop; expiry widening is post-clamp."""
    if len(bars_before) < 2:
        stop = STOP_MIN_BP
    else:
        recent = tuple(bars_before[-10:])
        last_close = float(current_close if current_close is not None else bars_before[-1].close)
        if last_close <= 0:
            stop = STOP_MIN_BP
        else:
            atr_points = sum(float(bar.high - bar.low) for bar in recent) / len(recent)
            atr_bp = atr_points / last_close * 10000.0
            vix_multiplier = max(1.0, float(vix_at_event) / VIX_BASELINE)
            stop = max(STOP_MIN_BP, min(STOP_MAX_BP, STOP_ATR_SCALE * atr_bp * vix_multiplier))
    return stop * EXPIRY_STOP_MULTIPLIER if is_expiry_day else stop


def stop_price(entry: float, direction: str, stop_bp: float) -> float:
    """Translate a stop distance into a directional stop price."""
    distance = float(entry) * float(stop_bp) / 10000.0
    normalized = str(direction).lower()
    if normalized in {"long", "buy"}:
        return float(entry) - distance
    if normalized in {"short", "sell"}:
        return float(entry) + distance
    raise ValueError(f"unsupported direction: {direction!r}")


__all__ = ["EXPIRY_STOP_MULTIPLIER", "STOP_ATR_SCALE", "STOP_MAX_BP", "STOP_MIN_BP", "VIX_BASELINE", "adaptive_stop_bp", "stop_price"]
