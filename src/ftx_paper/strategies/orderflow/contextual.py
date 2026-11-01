"""Causal market-context derivation and publication."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
import math
from typing import Any, Mapping

from .orderflow_common import Aggregate
from .timestamps import nifty_label, parse_session_timestamp


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
    prior = [
        item for item in aggregates[aggregate.timeframe]
        if item.session_date == aggregate.session_date
        and item.end_time < aggregate.time
    ]
    own = contextual_support(
        aggregate,
        prior[-1] if prior else None,
        atr=atr,
        volume_average=(
            sum(item.volume for item in prior[-20:]) / min(len(prior), 20)
            if prior else None
        ),
    )
    if aggregate.timeframe != "5m":
        return hierarchical_score(aggregate, (), own_support=own)

    aggregate_clock = _clock_time(aggregate.time, aggregate.session_date)
    aggregate_end_clock = _clock_time(aggregate.end_time, aggregate.session_date)
    aggregate_available_clock = _clock_time(aggregate.available_at, aggregate.session_date)
    children = [
        row for row in raw_rows
        if aggregate_clock <= _clock_time(row["time"], str(row.get("session_date", aggregate.session_date))) <= aggregate_end_clock
        and _minute_after(_clock_time(row["time"], str(row.get("session_date", aggregate.session_date)))) <= aggregate_available_clock
    ]
    child_scores = [
        contextual_support(
            row,
            raw_rows[index - 1] if index else None,
            atr=atr,
            volume_average=(
                sum(float(previous["volume"]) for previous in raw_rows[max(0, index - 20):index])
                / min(index, 20)
                if index else None
            ),
        )
        for index, row in enumerate(raw_rows)
        if row in children
    ]
    return hierarchical_score(aggregate, child_scores, own_support=own)


def _clock_time(value: Any, session_date: str | None = None) -> str:
    if isinstance(value, datetime):
        return nifty_label(value).split("T")[-1][:5]
    text = str(value).strip()
    if session_date is not None:
        return nifty_label(parse_session_timestamp(session_date, text)).split("T")[-1][:5]
    return text.split("T")[-1][:5]


def _minute_after(value: str) -> str:
    hour, minute = (int(part) for part in value[:5].split(":"))
    minute += 1
    if minute == 60:
        hour += 1
        minute = 0
    return f"{hour:02d}:{minute:02d}"


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
