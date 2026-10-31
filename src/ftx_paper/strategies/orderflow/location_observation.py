"""Typed causal observations of registered location bands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .timestamps import coerce_timestamp, serialize_utc



@dataclass(frozen=True, slots=True)
class LocationObservation:
    """One location measurement at one causally available aggregate."""

    location: str
    location_id: str | None
    reference_generation: int
    reference: float
    band_low: float
    band_high: float
    open: float
    high: float
    low: float
    close: float
    close_relation: str
    band_contact: bool
    source_aggregate_id: str
    observed_at: datetime
    available_at: datetime
    method: str
    observation_id: str = ""
    timeframe: str = ""
    child_observation_ids: tuple[str, ...] = ()
    source_observation_id: str | None = None
    reference_source_aggregate_id: str | None = None
    reference_source_timeframe: str | None = None

    def __post_init__(self) -> None:
        if self.band_low > self.band_high:
            raise ValueError("location observation band low must not exceed high")
        observed_at = coerce_timestamp(self.observed_at, field="location observation observed_at")
        available_at = coerce_timestamp(self.available_at, field="location observation available_at")
        if available_at < observed_at:
            raise ValueError("location observation must be causally available")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "available_at", available_at)
        if self.timeframe and self.timeframe not in {"1m", "5m"}:
            raise ValueError("location observation timeframe must be 1m or 5m")
        if self.location == "VWAP" and not self.source_observation_id:
            raise ValueError("VWAP observations require a source observation ID")
        if self.timeframe == "5m":
            if len(self.child_observation_ids) != 5:
                raise ValueError("5m location observations require exactly five child observations")
            if any(not child_id for child_id in self.child_observation_ids):
                raise ValueError("5m location observations require non-empty child IDs")
            if len(set(self.child_observation_ids)) != 5:
                raise ValueError("5m location observations require unique child IDs")

    def to_dict(self) -> dict[str, Any]:
        return {
            "location": self.location, "location_id": self.location_id,
            "reference_generation": self.reference_generation, "reference": self.reference,
            "band_low": self.band_low, "band_high": self.band_high, "open": self.open,
            "high": self.high, "low": self.low, "close": self.close,
            "close_relation": self.close_relation, "band_contact": self.band_contact,
            "source_aggregate_id": self.source_aggregate_id, "observed_at": serialize_utc(self.observed_at),
            "available_at": serialize_utc(self.available_at), "method": self.method,
            "observation_id": self.observation_id, "timeframe": self.timeframe,
            "child_observation_ids": list(self.child_observation_ids),
            "source_observation_id": self.source_observation_id,
            "reference_source_aggregate_id": self.reference_source_aggregate_id,
            "reference_source_timeframe": self.reference_source_timeframe,
        }
