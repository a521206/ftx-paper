"""Immutable location records and as-of resolution for orderflow evidence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Any, Iterable

from .orderflow_geometry import BAND_ATR_FRACTION, LocationBand


def is_diagnostic_location(alias: str) -> bool:
    """Return whether a location is evidence-only, never an executable setup."""
    return alias.upper() == "VWAP"


class LocationStatus(StrEnum):
    ACTIVE = "active"
    TESTED = "tested"
    ACCEPTED_THROUGH = "accepted-through"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class LocationRecord:
    location_id: str
    alias: str
    price: float
    source_session: str
    created_at: datetime
    available_at: datetime
    stream_id: str
    band: LocationBand
    generation: int = 1
    status: str = "active"
    frozen_atr: float | None = None
    parent_extreme_generation: int | None = None
    source_aggregate_id: str | None = None
    source_observation_id: str | None = None
    timeframe: str = ""
    source_method: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("created_at", "available_at"):
            value = getattr(self, field_name)
            if isinstance(value, str):
                value = datetime.fromisoformat(value)
                object.__setattr__(self, field_name, value)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"location {field_name} must be timezone-aware")

    @property
    def band_low(self) -> float:
        return self.band.low

    @property
    def band_high(self) -> float:
        return self.band.high

    def with_status(self, status: str | LocationStatus) -> "LocationRecord":
        return replace(self, status=LocationStatus(status).value)

@dataclass(frozen=True, slots=True)
class LocationObservation:
    """Causal measurement of one frozen location on a completed parent."""

    location_id: str
    location_alias: str
    reference_generation: int
    reference: float
    band_lower: float
    band_upper: float
    open: float
    high: float
    low: float
    close: float
    band_contact: bool
    source_aggregate_id: str
    observed_at: datetime
    available_at: datetime
    observation_id: str
    location_type: str = "session_reference"
    source_observation_id: str | None = None
    timeframe: str = ""
    child_observation_ids: tuple[str, ...] = ()
    method: str = ""
    reference_source_aggregate_id: str | None = None
    reference_source_timeframe: str | None = None
    frozen_atr: float | None = None
    close_relation: str | None = None
    source_fingerprint: str | None = None

    def __post_init__(self) -> None:
        if not self.location_id or not self.location_alias or not self.observation_id:
            raise ValueError("location observation identity is required")
        if self.band_lower > self.band_upper:
            raise ValueError("location observation band bounds are inverted")
        if self.available_at < self.observed_at:
            raise ValueError("location observation availability is not causal")
        if self.timeframe and self.timeframe not in {"1m", "5m"}:
            raise ValueError("location observation timeframe must be 1m or 5m")
        if len(self.child_observation_ids) != len(set(self.child_observation_ids)):
            raise ValueError("location observation child IDs must be unique")
        if any(not child_id for child_id in self.child_observation_ids):
            raise ValueError("location observation child IDs must be non-empty")
        if self.timeframe == "5m" and len(self.child_observation_ids) != 5:
            raise ValueError("5m location observations require exactly five child observations")
        if self.location_alias.upper() == "VWAP" and not self.source_observation_id:
            raise ValueError(
                f"VWAP observation {self.observation_id!r} requires a source observation ID"
            )
        relation = (
            "BELOW" if self.close < self.band_lower
            else "ABOVE" if self.close > self.band_upper
            else "INSIDE_BAND"
        )
        if self.close_relation is not None and self.close_relation != relation:
            raise ValueError("location observation close relation does not match close")
        object.__setattr__(self, "close_relation", relation)

    def to_dict(self) -> dict[str, object]:
        return {
            "location": self.location_alias,
            "location_id": self.location_id,
            "location_alias": self.location_alias,
            "reference_generation": self.reference_generation,
            "reference": self.reference,
            "band_low": self.band_lower,
            "band_high": self.band_upper,
            "band_lower": self.band_lower,
            "band_upper": self.band_upper,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "close_relation": self.close_relation,
            "band_contact": self.band_contact,
            "source_aggregate_id": self.source_aggregate_id,
            "observed_at": self.observed_at.isoformat(),
            "available_at": self.available_at.isoformat(),
            "observation_id": self.observation_id,
            "location_type": self.location_type,
            "source_observation_id": self.source_observation_id,
            "timeframe": self.timeframe,
            "child_observation_ids": self.child_observation_ids,
            "method": self.method,
            "reference_source_aggregate_id": self.reference_source_aggregate_id,
            "reference_source_timeframe": self.reference_source_timeframe,
            "frozen_atr": self.frozen_atr,
            "source_fingerprint": self.source_fingerprint,
        }


class LocationRegistry:
    """Append-only registry; replacement never mutates an earlier record."""

    def __init__(self, stream_id: str) -> None:
        if not stream_id.strip():
            raise ValueError("location stream identity is required")
        self.stream_id = stream_id
        self.records: dict[str, LocationRecord] = {}
        self.aliases: dict[str, str] = {}

    def register(self, record: LocationRecord) -> None:
        if record.stream_id != self.stream_id:
            raise ValueError("location stream identity does not match registry")
        self.records[record.location_id] = record
        self.aliases[record.alias] = record.location_id

    def update_status(self, location_id: str, status: str | LocationStatus) -> LocationRecord:
        record = self.records.get(location_id)
        if record is None:
            raise KeyError(location_id)
        updated = record.with_status(status)
        self.records[location_id] = updated
        return updated

    def _register_level(
        self, alias: str, price: float, available_at: datetime, source_session: str,
        *, frozen_atr: float | None = None, source_aggregate_id: str | None = None,
    ) -> LocationRecord:
        if frozen_atr is None:
            raise ValueError("location bands require a frozen ATR")
        previous = self.resolve_as_of(alias, available_at)
        generation = max(
            (record.generation for record in self.records.values() if record.alias == alias),
            default=0,
        ) + 1
        band = LocationBand(
            float(price) - BAND_ATR_FRACTION * frozen_atr,
            float(price) + BAND_ATR_FRACTION * frozen_atr,
        )
        record = LocationRecord(
            location_id=f"{alias}:{source_session}:{generation}", alias=alias, price=float(price),
            source_session=source_session, created_at=available_at, available_at=available_at,
            stream_id=self.stream_id, band=band, generation=generation, frozen_atr=frozen_atr,
            parent_extreme_generation=previous.generation if previous is not None else None,
            source_aggregate_id=source_aggregate_id,
        )
        self.register(record)
        return record

    def register_opening_range(
        self, or_low: float, or_high: float, available_at: datetime, *,
        source_session: str = "current", atr: float | None = None,
    ) -> tuple[LocationRecord, LocationRecord]:
        return (
            self._register_level("OR_LOW", or_low, available_at, source_session, frozen_atr=atr),
            self._register_level("OR_HIGH", or_high, available_at, source_session, frozen_atr=atr),
        )

    def register_prior_day_extremes(
        self, pd_low: float, pd_high: float, available_at: datetime, *,
        source_session: str = "prior", atr: float | None = None,
    ) -> tuple[LocationRecord, LocationRecord]:
        return (
            self._register_level("PD_LOW", pd_low, available_at, source_session, frozen_atr=atr),
            self._register_level("PD_HIGH", pd_high, available_at, source_session, frozen_atr=atr),
        )

    def publish_session_extrema(
        self, aggregates: Iterable[Any], *, session_date: str, atr: float | None = None,
    ) -> tuple[tuple[LocationRecord, LocationRecord | None], ...]:
        current = [item for item in aggregates if getattr(item, "session_date") == session_date]
        current = [
            item for item in current
            if getattr(item, "timeframe") == "5m"
            and getattr(item, "available_at") >= getattr(item, "end_time")
        ]
        if not current:
            return ()
        high = max(current, key=lambda item: getattr(item, "high"))
        low = min(current, key=lambda item: getattr(item, "low"))
        published: list[tuple[LocationRecord, LocationRecord | None]] = []
        for alias, aggregate, price in (("SESSION_HIGH", high, high.high), ("SESSION_LOW", low, low.low)):
            aggregate_available_at = aggregate.available_at
            previous = self.resolve_as_of(alias, aggregate_available_at)
            if previous is not None and price == previous.price:
                continue
            record = self._register_level(
                alias, price, aggregate_available_at, session_date,
                frozen_atr=atr, source_aggregate_id=aggregate.time,
            )
            published.append((record, previous))
        return tuple(published)

    def publish_vwap(
        self, *, value: float, available_at: datetime, source_session: str,
        source_aggregate_id: str, source_observation_id: str, timeframe: str,
        atr: float, source_method: str | None = None,
    ) -> LocationRecord:
        if timeframe not in {"1m", "5m"}:
            raise ValueError("VWAP records require a 1m or 5m timeframe")
        previous = max(
            (record for record in self.records.values() if record.alias == "VWAP" and record.timeframe == timeframe),
            key=lambda record: record.generation, default=None,
        )
        generation = previous.generation + 1 if previous is not None else 1
        width = BAND_ATR_FRACTION * atr
        record = LocationRecord(
            location_id=f"VWAP:{source_session}:{timeframe}:{generation}", alias="VWAP", price=float(value),
            source_session=source_session, created_at=available_at, available_at=available_at,
            stream_id=self.stream_id, band=LocationBand(float(value) - width, float(value) + width),
            generation=generation, frozen_atr=atr, source_aggregate_id=source_aggregate_id,
            source_observation_id=source_observation_id, timeframe=timeframe, source_method=source_method,
        )
        self.register(record)
        return record

    def resolve_as_of(self, alias: str, available_at: datetime, *, timeframe: str | None = None) -> LocationRecord | None:
        candidates = [
            record for record in self.records.values()
            if record.alias == alias and record.available_at <= available_at
            and (timeframe is None or record.timeframe == timeframe)
        ]
        return max(candidates, key=lambda item: (item.available_at, item.generation), default=None)

    def resolve(self, alias_or_id: str) -> LocationRecord | None:
        return self.records.get(self.aliases.get(alias_or_id, alias_or_id))

    def available_as_of(self, available_at: datetime) -> tuple[LocationRecord, ...]:
        latest: dict[tuple[str, str], LocationRecord] = {}
        for record in self.records.values():
            if record.status != LocationStatus.ACTIVE.value or record.available_at > available_at:
                continue
            key = (record.alias, record.timeframe)
            previous = latest.get(key)
            if previous is None or (record.available_at, record.generation) > (
                previous.available_at, previous.generation,
            ):
                latest[key] = record
        return tuple(latest.values())

    @classmethod
    def from_dict(cls, raw: object, *, stream_id: str) -> "LocationRegistry":
        if not isinstance(raw, (list, tuple)):
            raise ValueError("location registry snapshot must be a sequence")
        registry = cls(stream_id)
        for item in raw:
            if not isinstance(item, dict):
                raise ValueError("location registry record must be an object")
            band = item.get("band")
            if not isinstance(band, dict):
                raise ValueError("location registry band must be an object")
            registry.register(LocationRecord(
                str(item["location_id"]), str(item["alias"]), float(item["price"]),
                str(item["source_session"]), datetime.fromisoformat(str(item["created_at"])),
                datetime.fromisoformat(str(item["available_at"])), str(item["stream_id"]),
                LocationBand(float(band["low"]), float(band["high"])), int(item["generation"]),
                str(item.get("status", LocationStatus.ACTIVE.value)),
                float(item["frozen_atr"]) if item.get("frozen_atr") is not None else None,
                int(item["parent_extreme_generation"]) if item.get("parent_extreme_generation") is not None else None,
                str(item["source_aggregate_id"]) if item.get("source_aggregate_id") is not None else None,
                str(item["source_observation_id"]) if item.get("source_observation_id") is not None else None,
                str(item.get("timeframe", "")),
                str(item["source_method"]) if item.get("source_method") is not None else None,
            ))
        return registry



__all__ = [
    "LocationBand", "LocationObservation",
    "LocationRecord", "LocationRegistry", "is_diagnostic_location",
]
