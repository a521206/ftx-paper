"""Canonical orderflow feature calculations used by Phase 3.5."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from .timestamps import session_minute

@dataclass(frozen=True, slots=True)
class _DailyFeature:
    session_date: str
    high: float
    low: float
    close: float


def _clock(session_date: str, value: object) -> int:
    if not isinstance(value, (datetime, str)):
        raise ValueError("feature row time must be a timestamp")
    hour, minute = (int(item) for item in session_minute(session_date, value)[-5:].split(":"))
    return hour * 60 + minute


def _number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"feature row {field} must be numeric")
    return float(value)


def _daily_features(rows_by_date: Mapping[str, Sequence[Mapping[str, object]]]) -> tuple[_DailyFeature, ...]:
    days: list[_DailyFeature] = []
    for session_date in sorted(rows_by_date):
        rows = sorted(rows_by_date[session_date], key=lambda row: _clock(session_date, row["time"]))
        if not rows:
            continue
        days.append(_DailyFeature(
            session_date,
            max(_number(row["high"], "high") for row in rows),
            min(_number(row["low"], "low") for row in rows),
            _number(rows[-1]["close"], "close"),
        ))
    return tuple(days)


def daily_atr(
    session_date: str,
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]],
) -> float | None:
    """Return the Wilder ATR(14) from completed prior sessions."""
    period = 14
    days = _daily_features(rows_by_date)
    prior = [day for day in days if day.session_date < session_date]
    if len(prior) < period:
        return None
    true_ranges = [
        day.high - day.low
        if index == 0
        else _true_range(day, prior[index - 1].close)
        for index, day in enumerate(prior)
    ]
    atr = sum(true_ranges[:period]) / period
    for true_range in true_ranges[period:]:
        atr = ((atr * (period - 1)) + true_range) / period
    return atr


def daily_atr_cache(
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]],
) -> dict[str, float | None]:
    """Build ATR values once for every session represented by the input rows."""
    period = 14
    days = _daily_features(rows_by_date)
    cache: dict[str, float | None] = {}
    true_ranges: list[float] = []
    atr: float | None = None
    previous: _DailyFeature | None = None
    for day in days:
        cache[day.session_date] = atr if len(true_ranges) >= period else None
        true_ranges.append(
            day.high - day.low
            if previous is None
            else _true_range(day, previous.close)
        )
        if len(true_ranges) == period:
            atr = sum(true_ranges) / period
        elif len(true_ranges) > period:
            assert atr is not None
            atr = ((atr * (period - 1)) + true_ranges[-1]) / period
        previous = day
    return cache


def _true_range(day: _DailyFeature, previous_close: float) -> float:
    return max(day.high - day.low, abs(day.high - previous_close), abs(day.low - previous_close))
