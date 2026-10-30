"""Paper-owned prior-session volume profile and publication adapter."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from math import isfinite

from .value_publication import PriorSessionValueReference


@dataclass(frozen=True, slots=True)
class PriorSessionValueConfig:
    value_area_fraction: float = 0.70
    tick_size: float = 0.05
    profile_version: str = "value_profile.v1"
    session_start: str = "09:15"
    session_end: str = "15:30"

    def __post_init__(self) -> None:
        if not 0.0 < self.value_area_fraction <= 1.0:
            raise ValueError("value area fraction must be in (0, 1]")
        if self.tick_size <= 0 or not self.profile_version:
            raise ValueError("value profile configuration is invalid")

    def fingerprint(self) -> str:
        payload = json.dumps({"value_area_fraction": self.value_area_fraction, "tick_size": self.tick_size, "profile_version": self.profile_version, "session_start": self.session_start, "session_end": self.session_end}, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class PriorSessionProfile:
    session_date: str
    value_area_low: float
    point_of_control: float
    value_area_high: float
    observed_at: datetime
    available_at: datetime
    lineage: tuple[str, ...]
    complete: bool = True

    def reference(self) -> PriorSessionValueReference | None:
        if not self.complete:
            return None
        return PriorSessionValueReference(self.session_date, self.value_area_low, self.point_of_control, self.value_area_high, self.observed_at, self.available_at, self.lineage)


def _number(row: Mapping[str, object], field: str) -> float:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"profile row {field} must be numeric")
    result = float(value)
    if not isfinite(result):
        raise ValueError("profile bars require finite OHLCV values")
    return result


def _levels(rows: Sequence[Mapping[str, object]], config: PriorSessionValueConfig) -> tuple[float, float, float] | None:
    histogram: dict[float, float] = {}
    for row in rows:
        volume = _number(row, "volume")
        if volume < 0:
            raise ValueError("profile volume cannot be negative")
        if volume == 0:
            continue
        price = round(((_number(row, "high") + _number(row, "low") + _number(row, "close")) / 3.0) / config.tick_size) * config.tick_size
        histogram[price] = histogram.get(price, 0.0) + volume
    if not histogram:
        return None
    prices = sorted(histogram)
    poc = max(histogram, key=lambda price: histogram[price])
    low = high = prices.index(poc)
    cumulative = histogram[poc]
    while cumulative < sum(histogram.values()) * config.value_area_fraction and (low > 0 or high < len(prices) - 1):
        below = histogram[prices[low - 1]] if low else -1.0
        above = histogram[prices[high + 1]] if high < len(prices) - 1 else -1.0
        if below >= above and low:
            low -= 1
            cumulative += histogram[prices[low]]
        elif high < len(prices) - 1:
            high += 1
            cumulative += histogram[prices[high]]
        elif low:
            low -= 1
            cumulative += histogram[prices[low]]
    return prices[low], poc, prices[high]


def build_prior_session_profile(rows: Sequence[Mapping[str, object]], *, session_date: str, config: PriorSessionValueConfig = PriorSessionValueConfig()) -> PriorSessionProfile | None:
    start = int(config.session_start[:2]) * 60 + int(config.session_start[3:])
    end = int(config.session_end[:2]) * 60 + int(config.session_end[3:])

    def session_minute(row: Mapping[str, object]) -> int:
        hour, minute = (int(part) for part in str(row["time"])[:5].split(":", 1))
        return hour * 60 + minute

    session_rows = tuple(
        sorted(
            (
                row for row in rows
                if str(row.get("session_date", session_date)) == session_date
                and start <= session_minute(row) < end
            ),
            key=lambda row: str(row["time"]),
        )
    )
    if not session_rows:
        return None
    expected = {f"{minute // 60:02d}:{minute % 60:02d}" for minute in range(start, end)}
    observed = {str(row["time"])[:5] for row in session_rows}
    levels = _levels(session_rows, config)
    if levels is None:
        return None
    last = max(session_rows, key=lambda row: str(row["time"]))
    observed_at = datetime.fromisoformat(f"{session_date}T{str(last['time'])[:5]}+00:00")
    available_at = datetime.fromisoformat(f"{session_date}T{config.session_end}:00+00:00")
    source = hashlib.sha256("|".join(str(row.get("source_fingerprint", "")) for row in session_rows).encode()).hexdigest()
    lineage = (f"vp:{session_date}:{config.fingerprint()}:{source}",)
    return PriorSessionProfile(session_date, *levels, observed_at, available_at, lineage, expected <= observed)


class PriorSessionValueStore:
    def __init__(self, profiles: Sequence[PriorSessionProfile] = ()) -> None:
        self._profiles = {profile.session_date: profile for profile in profiles}

    def prior_session_value(self, session_date: str) -> PriorSessionValueReference | None:
        prior = [profile for date, profile in self._profiles.items() if date < session_date]
        profile = max(prior, key=lambda item: item.session_date) if prior else None
        reference = profile.reference() if profile is not None else None
        if reference is None:
            return None
        return replace(
            reference,
            available_at=datetime.fromisoformat(
                f"{session_date}T09:15:00+00:00",
            ).astimezone(timezone.utc),
        )


__all__ = ["PriorSessionProfile", "PriorSessionValueConfig", "PriorSessionValueStore", "build_prior_session_profile"]
