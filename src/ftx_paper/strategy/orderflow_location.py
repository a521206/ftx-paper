"""Immutable location records and as-of resolution for orderflow evidence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime


@dataclass(frozen=True, slots=True)
class LocationBand:
    lower: float
    upper: float

    def __post_init__(self) -> None:
        if self.lower > self.upper:
            raise ValueError("location band bounds are inverted")

    def contains(self, price: float) -> bool:
        return self.lower <= price <= self.upper


@dataclass(frozen=True, slots=True)
class LocationRecord:
    location_id: str
    alias: str
    price: float
    band: LocationBand
    generation: int
    available_at: datetime
    source_aggregate_id: str
    priority: int = 0
    timeframe: str = ""
    replaced_location_id: str | None = None
    location_type: str = "session_reference"
    owner: str = "orderflow"
    aliases: tuple[str, ...] = ()
    source_identity: str | None = None

    def __post_init__(self) -> None:
        if not self.location_id.strip() or not self.alias.strip():
            raise ValueError("location identity is required")
        if self.generation < 1 or not self.source_aggregate_id.strip():
            raise ValueError("location generation and source identity are required")
        if not self.band.contains(self.price):
            raise ValueError("location price must lie within its band")

    def to_dict(self) -> dict[str, object]:
        return {
            "location_id": self.location_id,
            "alias": self.alias,
            "aliases": self.aliases,
            "price": self.price,
            "band": {"lower": self.band.lower, "upper": self.band.upper},
            "generation": self.generation,
            "available_at": self.available_at.isoformat(),
            "source_aggregate_id": self.source_aggregate_id,
            "source_identity": self.source_identity,
            "priority": self.priority,
            "timeframe": self.timeframe,
            "location_type": self.location_type,
            "owner": self.owner,
            "replaced_location_id": self.replaced_location_id,
        }

    def matches(self, alias: str) -> bool:
        return alias == self.alias or alias in self.aliases


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

    def __post_init__(self) -> None:
        if not self.location_id or not self.location_alias or not self.observation_id:
            raise ValueError("location observation identity is required")
        if self.band_lower > self.band_upper:
            raise ValueError("location observation band bounds are inverted")
        if self.available_at < self.observed_at:
            raise ValueError("location observation availability is not causal")

    def to_dict(self) -> dict[str, object]:
        return {
            "location_id": self.location_id,
            "location_alias": self.location_alias,
            "reference_generation": self.reference_generation,
            "reference": self.reference,
            "band_lower": self.band_lower,
            "band_upper": self.band_upper,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "band_contact": self.band_contact,
            "source_aggregate_id": self.source_aggregate_id,
            "observed_at": self.observed_at.isoformat(),
            "available_at": self.available_at.isoformat(),
            "observation_id": self.observation_id,
        }


class LocationRegistry:
    """Append-only registry; replacement never mutates an earlier record."""

    def __init__(self) -> None:
        self._records: list[LocationRecord] = []
        self._boundaries: set[datetime] = set()

    def publish(self, record: LocationRecord) -> None:
        if any(item.location_id == record.location_id for item in self._records):
            raise ValueError("location ID already published")
        if record.replaced_location_id is None:
            previous = self.resolve_as_of(record.alias, record.available_at, timeframe=record.timeframe)
            if previous is not None and previous.location_id != record.location_id:
                record = replace(record, replaced_location_id=previous.location_id)
        self._records.append(record)
        self._records.sort(key=lambda item: (item.available_at, -item.priority, item.location_id))

    def resolve_as_of(self, alias: str, cutoff: datetime, *, timeframe: str | None = None) -> LocationRecord | None:
        candidates = [
            item for item in self._records
            if item.matches(alias) and item.available_at <= cutoff
            and (timeframe is None or item.timeframe == timeframe)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item.available_at, item.priority, item.generation, item.location_id))

    def available(self, cutoff: datetime, *, timeframe: str | None = None) -> tuple[LocationRecord, ...]:
        return tuple(
            item for item in self._records
            if item.available_at <= cutoff and (timeframe is None or item.timeframe == timeframe)
        )

    def resolve_owner(
        self, price: float, cutoff: datetime, *, timeframe: str | None = None,
    ) -> LocationRecord | None:
        """Resolve the canonical owner when multiple available bands overlap."""
        candidates = [
            item for item in self.available(cutoff, timeframe=timeframe)
            if item.band.contains(price)
        ]
        return max(
            candidates,
            key=lambda item: (item.priority, item.generation, item.available_at, item.location_id),
            default=None,
        )

    @property
    def records(self) -> tuple[LocationRecord, ...]:
        return tuple(self._records)

    def next_generation(
        self, *, at: datetime, carry: bool = False,
    ) -> tuple[LocationRecord, ...]:
        """Apply a session boundary once and return carried references."""
        if not carry:
            return ()
        if at in self._boundaries:
            return ()
        self._boundaries.add(at)
        current = [item for item in self.available(at)]
        carried: list[LocationRecord] = []
        for item in current:
            carried.append(replace(
                item,
                location_id=f"{item.location_id}:g{item.generation + 1}",
                generation=item.generation + 1,
                available_at=at,
                replaced_location_id=item.location_id,
            ))
        for item in carried:
            self.publish(item)
        return tuple(carried)


__all__ = ["LocationBand", "LocationObservation", "LocationRecord", "LocationRegistry"]
