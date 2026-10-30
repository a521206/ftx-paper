"""Canonical orderflow feature calculations used by Phase 3.5."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .orderflow_common import Aggregate
from .state_machine_types import SessionLevels

ZONE_WINDOWS = (("Z1", 9 * 60 + 15, 11 * 60), ("Z2", 11 * 60, 13 * 60), ("Z3", 13 * 60, 15 * 60))


def derive_session_levels(
    aggregates5: Sequence[Aggregate],
    prior_rows: Sequence[Mapping[str, object]],
    session_date: str,
) -> SessionLevels:
    """Derive canonical opening-range and prior-day extrema levels."""
    first = next((item for item in aggregates5 if item.available_at), None)
    if first is None:
        raise ValueError(f"no completed 5m context available for {session_date}")
    prior_low = min((_number(row["low"], "low") for row in prior_rows), default=None)
    prior_high = max((_number(row["high"], "high") for row in prior_rows), default=None)
    return SessionLevels(
        opening_range_low=first.low,
        opening_range_high=first.high,
        prior_day_low=prior_low,
        prior_day_high=prior_high,
        opening_range_available_at=first.available_at,
        prior_day_available_at=f"{session_date}T09:15",
    )


@dataclass(frozen=True, slots=True)
class _DailyFeature:
    session_date: str
    high: float
    low: float
    close: float


def _clock(value: object) -> int:
    hour, minute = (int(item) for item in str(value).split("T")[-1][:5].split(":")[:2])
    return hour * 60 + minute


def _number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"feature row {field} must be numeric")
    return float(value)


def _daily_features(rows_by_date: Mapping[str, Sequence[Mapping[str, object]]]) -> tuple[_DailyFeature, ...]:
    days: list[_DailyFeature] = []
    for session_date in sorted(rows_by_date):
        rows = rows_by_date[session_date]
        zones: dict[str, list[Mapping[str, object]]] = {name: [] for name, _, _ in ZONE_WINDOWS}
        for row in rows:
            minute = _clock(row["time"])
            for name, start, end in ZONE_WINDOWS:
                if start <= minute < end:
                    zones[name].append(row)
                    break
        if any(not zones[name] for name, _, _ in ZONE_WINDOWS):
            continue
        all_rows = [row for name, _, _ in ZONE_WINDOWS for row in zones[name]]
        days.append(_DailyFeature(
            session_date,
            max(_number(row["high"], "high") for row in all_rows),
            min(_number(row["low"], "low") for row in all_rows),
            _number(zones["Z3"][-1]["close"], "close"),
        ))
    return tuple(days)


def daily_atr(
    session_date: str,
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]],
) -> float | None:
    """Return canonical simple-mean ATR from completed prior sessions."""
    period = 14
    days = _daily_features(rows_by_date)
    prior = [day for day in days if day.session_date < session_date]
    if len(prior) < period:
        return None
    window = prior[-period:]
    previous_close = prior[-period - 1].close if len(prior) > period else None
    true_ranges = [
        day.high - day.low if index == 0 and previous_close is None else _true_range(
            day, previous_close if index == 0 and previous_close is not None else window[index - 1].close,
        )
        for index, day in enumerate(window)
    ]
    return sum(true_ranges) / period


def _true_range(day: _DailyFeature, previous_close: float) -> float:
    return max(day.high - day.low, abs(day.high - previous_close), abs(day.low - previous_close))
