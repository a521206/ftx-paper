"""Causal directional scores for orderflow aggregates.

This is the Paper-owned equivalent of NiftyZoning's contextual score path.  It
only consumes the current completed aggregate, an earlier completed aggregate,
and already completed child scores.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence


def _value(item: Mapping[str, object], name: str) -> float:
    value = item.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"score input {name!r} must be numeric")
    return float(value)


def contextual_support(
    current: Mapping[str, object],
    prior: Mapping[str, object] | None,
    *,
    atr: float,
    volume_average: float | None = None,
) -> tuple[float, float]:
    """Return causal long/short support in the range ``[0, 1]``."""
    if prior is None or atr <= 0:
        return 0.0, 0.0

    def clip(value: float) -> float:
        return max(0.0, min(1.0, value))

    low = _value(current, "low")
    high = _value(current, "high")
    close = _value(current, "close")
    volume = _value(current, "volume")
    candle_range = max(high - low, 0.0)
    prior_range = max(_value(prior, "high") - _value(prior, "low"), atr)
    recovery = clip((close - low) / max(candle_range, atr * 0.2))
    rejection = clip((high - close) / max(candle_range, atr * 0.2))
    close_location = (close - low) / candle_range if candle_range > 0 else 0.5
    long_candle = clip((close_location - 0.5) * 2.0)
    short_candle = clip((0.5 - close_location) * 2.0)
    compression = clip(1.0 - candle_range / prior_range)
    prior_volume = _value(prior, "volume")
    baseline = volume_average if volume_average is not None else prior_volume
    volume_surprise = (
        math.tanh(max(math.log(max(volume, 1.0) / max(baseline, 1.0)), 0.0) / 0.5)
        if baseline > 0 else 0.0
    )
    return (
        clip(0.35 * recovery + 0.15 * compression * recovery
             + 0.20 * volume_surprise * recovery + 0.30 * long_candle),
        clip(0.35 * rejection + 0.15 * compression * rejection
             + 0.20 * volume_surprise * rejection + 0.30 * short_candle),
    )


def hierarchical_score(
    own_score: tuple[float, float],
    child_scores: Sequence[tuple[float, float]],
) -> tuple[float, float]:
    """Combine aggregate and completed-child support equally."""
    if not child_scores:
        return own_score
    child_long = sum(item[0] for item in child_scores) / len(child_scores)
    child_short = sum(item[1] for item in child_scores) / len(child_scores)
    return (0.5 * own_score[0] + 0.5 * child_long,
            0.5 * own_score[1] + 0.5 * child_short)


def score_aggregate(
    current: Mapping[str, object],
    prior: Mapping[str, object] | None,
    *,
    atr: float | None,
    child_scores: Sequence[tuple[float, float]] = (),
    volume_average: float | None = None,
) -> tuple[float, float] | None:
    """Calculate one aggregate's score, or ``None`` without ATR evidence."""
    if atr is None or atr <= 0:
        return None
    own = contextual_support(current, prior, atr=atr, volume_average=volume_average)
    return hierarchical_score(own, child_scores)


__all__ = ["contextual_support", "hierarchical_score", "score_aggregate"]
