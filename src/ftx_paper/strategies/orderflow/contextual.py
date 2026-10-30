"""Causal market-context derivation and publication."""

from __future__ import annotations

from collections.abc import Sequence
import math
from typing import Any, Mapping

from .orderflow_common import Aggregate


def contextual_support(
    bar: Aggregate | Mapping[str, Any],
    prior: Aggregate | Mapping[str, Any] | None,
    *,
    atr: float,
    volume_average: float | None = None,
) -> tuple[float, float]:
    if prior is None or atr <= 0:
        return 0.0, 0.0

    def value(item: Aggregate | Mapping[str, Any], name: str) -> float:
        return float(item[name] if isinstance(item, Mapping) else getattr(item, name))

    def clip(value_: float) -> float:
        return max(0.0, min(1.0, value_))

    low, high, close = value(bar, "low"), value(bar, "high"), value(bar, "close")
    bar_volume = value(bar, "volume")
    bar_range = max(high - low, 0.0)
    prior_range = max(value(prior, "high") - value(prior, "low"), atr)
    recovery = clip((close - low) / max(bar_range, atr * 0.2))
    rejection = clip((high - close) / max(bar_range, atr * 0.2))
    close_location = (close - low) / bar_range if bar_range > 0 else 0.5
    long_candle = clip((close_location - 0.5) * 2.0)
    short_candle = clip((0.5 - close_location) * 2.0)
    compression = clip(1.0 - bar_range / prior_range)
    prior_volume = value(prior, "volume")
    baseline = volume_average if volume_average is not None else prior_volume
    surprise = math.tanh(max(math.log(max(bar_volume, 1.0) / max(baseline, 1.0)), 0.0) / 0.5) if baseline > 0 else 0.0
    return (
        clip(0.35 * recovery + 0.15 * compression * recovery + 0.20 * surprise * recovery + 0.30 * long_candle),
        clip(0.35 * rejection + 0.15 * compression * rejection + 0.20 * surprise * rejection + 0.30 * short_candle),
    )


def hierarchical_score(
    aggregate: Aggregate,
    child_scores: Sequence[tuple[float, float]],
    *,
    own_support: tuple[float, float] | None = None,
) -> tuple[float, float]:
    own_long, own_short = own_support or _directional_support(aggregate)
    if not child_scores:
        return own_long, own_short
    child_long = sum(item[0] for item in child_scores) / len(child_scores)
    child_short = sum(item[1] for item in child_scores) / len(child_scores)
    return 0.5 * own_long + 0.5 * child_long, 0.5 * own_short + 0.5 * child_short


def build_context_scores(
    aggregate: Aggregate,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    aggregates: dict[str, list[Aggregate]],
    atr: float | None,
) -> tuple[float, float] | None:
    if atr is None:
        return None
    prior = [item for item in aggregates[aggregate.timeframe] if item.session_date == aggregate.session_date and item.end_time < aggregate.time]
    own = contextual_support(aggregate, prior[-1] if prior else None, atr=atr, volume_average=sum(item.volume for item in prior[-20:]) / min(len(prior), 20) if prior else None)
    child = aggregates.get("1m", [])
    if aggregate.timeframe == "5m":
        children = [item for item in child if aggregate.time <= item.time <= aggregate.end_time and item.available_at <= aggregate.available_at]
        child_scores = [
            contextual_support(
                item,
                child[child.index(item) - 1] if child.index(item) else None,
                atr=atr,
                volume_average=(sum(previous.volume for previous in child[max(0, child.index(item) - 20):child.index(item)]) / min(child.index(item), 20) if child.index(item) else None),
            )
            for item in children
        ]
    else:
        child_scores = []
    return hierarchical_score(aggregate, child_scores, own_support=own)


def _directional_support(aggregate: Aggregate) -> tuple[float, float]:
    if aggregate.close > aggregate.open:
        return 1.0, 0.0
    if aggregate.close < aggregate.open:
        return 0.0, 1.0
    return 0.0, 0.0


def _market_state(scores: tuple[float, float] | None) -> str:
    if scores is None:
        return "UNAVAILABLE"
    if scores[0] > scores[1]:
        return "LONG"
    if scores[1] > scores[0]:
        return "SHORT"
    return "BALANCE"


__all__ = ["build_context_scores", "contextual_support", "hierarchical_score"]
