"""Typed causal observations of registered location bands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from .orderflow_common import Aggregate
from .timestamps import coerce_timestamp, nifty_label, serialize_utc

if TYPE_CHECKING:
    from .state_machine_types import LocationRecord



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


def location_observation(
    aggregate: Aggregate,
    record: LocationRecord,
    *,
    child_observation_ids: tuple[str, ...] = (),
    available_at: datetime | str | None = None,
) -> LocationObservation:
    close_relation = (
        "ABOVE" if aggregate.close > record.band_high
        else "BELOW" if aggregate.close < record.band_low
        else "INSIDE_BAND"
    )
    location_key = record.location_id or f"{record.alias}:{record.generation}"
    source_aggregate_id = nifty_label(aggregate.time)
    observation_id = f"{source_aggregate_id}:{aggregate.timeframe}:{location_key}"
    return LocationObservation(
        location=record.alias,
        location_id=record.location_id,
        reference_generation=record.generation,
        reference=record.price,
        band_low=record.band_low,
        band_high=record.band_high,
        open=aggregate.open,
        high=aggregate.high,
        low=aggregate.low,
        close=aggregate.close,
        close_relation=close_relation,
        band_contact=record.band.intersects(high=aggregate.high, low=aggregate.low),
        source_aggregate_id=source_aggregate_id,
        observed_at=coerce_timestamp(aggregate.end_time),
        available_at=coerce_timestamp(available_at or aggregate.available_at),
        method=record.source_method or "replay_location_band_intersection",
        observation_id=observation_id,
        timeframe=aggregate.timeframe,
        child_observation_ids=child_observation_ids,
        source_observation_id=record.source_observation_id,
        reference_source_aggregate_id=record.source_aggregate_id,
        reference_source_timeframe=record.timeframe or aggregate.timeframe,
    )


def causal_children(
    aggregate: Aggregate,
    one_minute: Sequence[Aggregate],
) -> tuple[Aggregate, ...]:
    if aggregate.timeframe != "5m":
        return ()
    children = tuple(
        item for item in one_minute
        if aggregate.time <= item.time <= aggregate.end_time
        and coerce_timestamp(item.available_at) <= coerce_timestamp(aggregate.available_at)
    )
    expected = tuple(
        nifty_label(aggregate.time + timedelta(minutes=offset))
        for offset in range(5)
    )
    return children if tuple(nifty_label(item.time) for item in children) == expected else ()
