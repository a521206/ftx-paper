"""Canonical context/location state contracts required by Layers 1 and 2."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Iterable

from .hierarchy import ContextVersionRef, MarketContext
from .location_geometry import BAND_ATR_FRACTION, LocationBand
from .orderflow_common import Aggregate


@dataclass(frozen=True, slots=True)
class SessionLevels:
    opening_range_low: float
    opening_range_high: float
    prior_day_low: float | None
    prior_day_high: float | None
    opening_range_available_at: str
    prior_day_available_at: str


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
    created_at: str
    available_at: str
    stream_id: str
    band: LocationBand
    generation: int = 1
    status: LocationStatus = LocationStatus.ACTIVE
    frozen_atr: float | None = None
    parent_extreme_generation: int | None = None
    source_aggregate_id: str | None = None
    source_observation_id: str | None = None
    timeframe: str = ""
    source_method: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", LocationStatus(self.status))

    @property
    def band_low(self) -> float:
        return self.band.low

    @property
    def band_high(self) -> float:
        return self.band.high

    def with_status(self, status: str | LocationStatus) -> "LocationRecord":
        return replace(self, status=LocationStatus(status))


@dataclass
class LocationRegistry:
    stream_id: str
    records: dict[str, LocationRecord] = field(default_factory=dict)
    aliases: dict[str, str] = field(default_factory=dict)

    def publish_session_levels(self, levels: SessionLevels, *, source_session: str, prior_session: str | None = None, atr: float | None = None) -> None:
        self.register_opening_range(levels.opening_range_low, levels.opening_range_high, levels.opening_range_available_at, source_session=source_session, atr=atr)
        if levels.prior_day_low is not None and levels.prior_day_high is not None:
            self.register_prior_day_extremes(levels.prior_day_low, levels.prior_day_high, levels.prior_day_available_at, source_session=prior_session or "prior", atr=atr)

    def publish_session_extrema(self, aggregates: Iterable[Aggregate], *, session_date: str, atr: float | None = None) -> tuple[tuple[LocationRecord, LocationRecord | None], ...]:
        current = sorted((item for item in aggregates if item.session_date == session_date and item.timeframe == "5m" and item.source_bar_count == 5 and item.available_at >= item.end_time), key=lambda item: item.available_at)
        if not current:
            return ()
        published: list[tuple[LocationRecord, LocationRecord | None]] = []
        running_high: float | None = None
        running_low: float | None = None
        for aggregate in current:
            next_high = aggregate.high if running_high is None else max(running_high, aggregate.high)
            next_low = aggregate.low if running_low is None else min(running_low, aggregate.low)
            for alias, price, prior in (("SESSION_HIGH", next_high, running_high), ("SESSION_LOW", next_low, running_low)):
                if prior is not None and price == prior:
                    continue
                previous = self.resolve_as_of(alias, aggregate.available_at)
                published.append((self._register_level(alias, price, aggregate.available_at, session_date, frozen_atr=atr, source_aggregate_id=aggregate.time), previous))
            running_high = aggregate.high if running_high is None else max(running_high, aggregate.high)
            running_low = aggregate.low if running_low is None else min(running_low, aggregate.low)
        return tuple(published)

    def _register_level(self, alias: str, price: float, available_at: str, source_session: str, *, frozen_atr: float | None, source_aggregate_id: str | None = None) -> LocationRecord:
        if frozen_atr is None:
            raise ValueError("location bands require a frozen ATR")
        previous = self.resolve_as_of(alias, available_at)
        generation = max((item.generation for item in self.records.values() if item.alias == alias), default=0) + 1
        record = LocationRecord(
            f"{alias}:{source_session}:{generation}", alias, float(price), source_session,
            available_at, available_at, self.stream_id,
            LocationBand(float(price) - BAND_ATR_FRACTION * frozen_atr, float(price) + BAND_ATR_FRACTION * frozen_atr),
            generation=generation, frozen_atr=frozen_atr,
            parent_extreme_generation=previous.generation if previous else None,
            source_aggregate_id=source_aggregate_id,
        )
        self.register(record)
        return record

    def register_opening_range(self, or_low: float, or_high: float, available_at: str, *, source_session: str = "current", atr: float | None = None) -> tuple[LocationRecord, LocationRecord]:
        return (self._register_level("OR_LOW", or_low, available_at, source_session, frozen_atr=atr), self._register_level("OR_HIGH", or_high, available_at, source_session, frozen_atr=atr))

    def register_prior_day_extremes(self, pd_low: float, pd_high: float, available_at: str, *, source_session: str = "prior", atr: float | None = None) -> tuple[LocationRecord, LocationRecord]:
        return (self._register_level("PD_LOW", pd_low, available_at, source_session, frozen_atr=atr), self._register_level("PD_HIGH", pd_high, available_at, source_session, frozen_atr=atr))

    def register(self, record: LocationRecord) -> None:
        if record.stream_id != self.stream_id:
            raise ValueError("location stream identity does not match registry")
        self.records[record.location_id] = record
        self.aliases[record.alias] = record.location_id

    def publish_vwap(self, *, value: float, available_at: str, source_session: str, source_aggregate_id: str, source_observation_id: str, timeframe: str, atr: float) -> LocationRecord:
        if timeframe not in {"1m", "5m"}:
            raise ValueError("VWAP records require a 1m or 5m timeframe")
        previous = max((item for item in self.records.values() if item.alias == "VWAP" and item.timeframe == timeframe), key=lambda item: item.generation, default=None)
        generation = previous.generation + 1 if previous else 1
        record = LocationRecord(
            f"VWAP:{source_session}:{timeframe}:{generation}", "VWAP", float(value), source_session,
            available_at, available_at, self.stream_id,
            LocationBand(value - BAND_ATR_FRACTION * atr, value + BAND_ATR_FRACTION * atr),
            generation=generation, frozen_atr=atr, source_aggregate_id=source_aggregate_id,
            source_observation_id=source_observation_id, timeframe=timeframe,
            source_method="vwap_typical_price_volume_v1",
        )
        self.register(record)
        return record

    def resolve(self, alias_or_id: str) -> LocationRecord | None:
        return self.records.get(self.aliases.get(alias_or_id, alias_or_id))

    def resolve_as_of(self, alias: str, available_at: str, *, timeframe: str | None = None) -> LocationRecord | None:
        return max((item for item in self.records.values() if item.alias == alias and item.available_at <= available_at and (timeframe is None or item.timeframe == timeframe)), key=lambda item: (item.available_at, item.generation), default=None)

    def available_as_of(self, available_at: str) -> Iterable[LocationRecord]:
        latest: dict[tuple[str, str], LocationRecord] = {}
        for item in self.records.values():
            if item.available_at > available_at:
                continue
            key = (item.alias, item.timeframe)
            if key not in latest or (item.available_at, item.generation) > (latest[key].available_at, latest[key].generation):
                latest[key] = item
        return tuple(item for item in latest.values() if item.status is LocationStatus.ACTIVE)

    def update_status(self, location_id: str, status: str) -> LocationRecord:
        record = self.records[location_id].with_status(status)
        self.records[location_id] = record
        return record


@dataclass
class MarketContextStore:
    versions: dict[str, list[MarketContext]] = field(default_factory=dict)

    def publish(self, context: MarketContext) -> ContextVersionRef:
        versions = self.versions.setdefault(context.context_id, [])
        if versions and versions[-1] == context:
            return ContextVersionRef(context.context_id, context.version)
        if any(item.version == context.version for item in versions):
            raise ValueError("context versions must advance monotonically")
        versions.append(context)
        return ContextVersionRef(context.context_id, context.version)

    def resolve(self, reference: ContextVersionRef) -> MarketContext | None:
        return next((item for item in self.versions.get(reference.context_id, ()) if item.version == reference.version), None)

    def latest_as_of(self, stream_id: str, available_at: str) -> MarketContext | None:
        return max((item for item in self.versions.get(stream_id, ()) if item.available_at <= available_at), key=lambda item: item.available_at, default=None)


@dataclass
class ReplayContext:
    market_contexts: MarketContextStore = field(default_factory=MarketContextStore)
    location_registry: LocationRegistry | None = None
    regime: Any = None

    def publish_context(self, context: MarketContext) -> ContextVersionRef:
        return self.market_contexts.publish(context)

    def context(self, reference: ContextVersionRef) -> MarketContext | None:
        return self.market_contexts.resolve(reference)
