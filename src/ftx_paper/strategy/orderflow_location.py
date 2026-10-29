"""Immutable location records and as-of resolution for orderflow evidence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .orderflow_vwap import VwapMeasurement


def is_diagnostic_location(location_type: str) -> bool:
    """Return whether a location is evidence-only, never an executable setup."""
    return location_type == "diagnostic_reference"


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
    source_observation_id: str | None = None
    frozen_atr: float | None = None
    session_id: str | None = None
    calculation_version: str | None = None

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
            "source_observation_id": self.source_observation_id,
            "frozen_atr": self.frozen_atr,
            "session_id": self.session_id,
            "calculation_version": self.calculation_version,
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
    location_type: str = "session_reference"
    source_observation_id: str | None = None
    timeframe: str = ""
    child_observation_ids: tuple[str, ...] = ()
    method: str = ""
    reference_source_aggregate_id: str | None = None
    reference_source_timeframe: str | None = None
    frozen_atr: float | None = None

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
            "location_type": self.location_type,
            "source_observation_id": self.source_observation_id,
            "timeframe": self.timeframe,
            "child_observation_ids": self.child_observation_ids,
            "method": self.method,
            "reference_source_aggregate_id": self.reference_source_aggregate_id,
            "reference_source_timeframe": self.reference_source_timeframe,
            "frozen_atr": self.frozen_atr,
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

    def publish_measurements(
        self, measurements: tuple[VwapMeasurement, ...], *, atr: float | None = None,
        band_fraction: float = 0.06,
    ) -> tuple[LocationRecord, ...]:
        """Publish provider-owned measurements without interpreting them."""
        from .orderflow_vwap import publish_vwap_locations
        return publish_vwap_locations(self, measurements, atr=atr, band_fraction=band_fraction)

    def remove_diagnostic_locations(self) -> None:
        """Keep diagnostic references session-scoped while retaining structure."""
        self._records = [item for item in self._records if item.location_type != "diagnostic_reference"]

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
        latest: dict[tuple[str, str], LocationRecord] = {}
        for item in self._records:
            if item.available_at > cutoff or (timeframe is not None and item.timeframe != timeframe):
                continue
            key = (item.alias, item.timeframe)
            previous = latest.get(key)
            if previous is None or (item.available_at, item.generation, item.location_id) > (
                previous.available_at, previous.generation, previous.location_id,
            ):
                latest[key] = item
        return tuple(latest.values())

    def available_as_of(
        self, cutoff: datetime, *, timeframe: str | None = None,
    ) -> tuple[LocationRecord, ...]:
        """Return the canonical latest generation visible at a causal cutoff."""
        return self.available(cutoff, timeframe=timeframe)

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

    @classmethod
    def from_dict(cls, raw: object) -> "LocationRegistry":
        if not isinstance(raw, (list, tuple)):
            raise ValueError("location registry snapshot must be a sequence")
        registry = cls()
        for item in raw:
            if not isinstance(item, dict):
                raise ValueError("location registry record must be an object")
            band = item.get("band")
            if not isinstance(band, dict):
                raise ValueError("location registry band must be an object")
            registry.publish(LocationRecord(
                str(item["location_id"]), str(item["alias"]), float(item["price"]),
                LocationBand(float(band["lower"]), float(band["upper"])), int(item["generation"]),
                datetime.fromisoformat(str(item["available_at"])), str(item["source_aggregate_id"]),
                int(item.get("priority", 0)), str(item.get("timeframe", "")),
                str(item["replaced_location_id"]) if item.get("replaced_location_id") is not None else None,
                str(item.get("location_type", "session_reference")), str(item.get("owner", "orderflow")),
                tuple(str(value) for value in item.get("aliases", ())),
                str(item["source_identity"]) if item.get("source_identity") is not None else None,
                str(item["source_observation_id"]) if item.get("source_observation_id") is not None else None,
                float(item["frozen_atr"]) if item.get("frozen_atr") is not None else None,
                str(item["session_id"]) if item.get("session_id") is not None else None,
                str(item["calculation_version"]) if item.get("calculation_version") is not None else None,
            ))
        return registry

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


__all__ = [
    "LocationBand", "LocationObservation",
    "LocationRecord", "LocationRegistry", "is_diagnostic_location",
]
