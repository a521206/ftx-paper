"""Canonical-owned Phase 4A evidence publication.

This module turns the Phase 3 aggregate artifact and host-supplied evidence
into immutable publications.  The replay boundary consumes these publications
as-is; it does not derive levels, VWAP, or context from presentation output.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from collections.abc import Iterable
from datetime import datetime

from .hierarchy import MarketContext
from .timestamps import coerce_timestamp
from .state_machine_types import LocationRecord
from .value_publication import ValuePublication
from .vwap_location import VwapMeasurement


@dataclass(frozen=True, slots=True)
class AtrPublication:
    value: float | None
    observed_at: datetime | None
    available_at: datetime | None
    source_observation_id: str | None
    lineage: tuple[str, ...] = ()
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        if self.value is None:
            if not self.unavailable_reason:
                raise ValueError("unavailable ATR publication requires a reason")
            if self.observed_at is not None or self.available_at is not None:
                raise ValueError("unavailable ATR cannot have timestamps")
        else:
            if (
                not math.isfinite(self.value)
                or self.value <= 0
                or self.observed_at is None
                or self.available_at is None
            ):
                raise ValueError("available ATR requires a positive value and timestamps")
            observed_at = coerce_timestamp(self.observed_at, field="ATR observed_at")
            available_at = coerce_timestamp(self.available_at, field="ATR available_at")
            if available_at < observed_at:
                raise ValueError("ATR availability cannot precede observation")
            if not self.source_observation_id or not self.lineage:
                raise ValueError("available ATR requires source lineage")
            if self.unavailable_reason is not None:
                raise ValueError("available ATR cannot have an unavailable reason")
            object.__setattr__(self, "observed_at", observed_at)
            object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "lineage", tuple(self.lineage))

    @classmethod
    def unavailable(cls, reason: str = "missing_atr") -> "AtrPublication":
        return cls(None, None, None, None, (), reason)


@dataclass(frozen=True, slots=True)
class EvidencePublication:
    """All Phase 4A evidence available to a causal replay."""

    atr: AtrPublication
    locations: tuple[LocationRecord, ...]
    vwap_measurements: tuple[VwapMeasurement, ...]
    contexts: tuple[MarketContext, ...]
    prior_value: ValuePublication
    unavailable: tuple[str, ...] = ()
    context_stream_id: str | None = None
    location_stream_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "locations", tuple(self.locations))
        object.__setattr__(self, "vwap_measurements", tuple(self.vwap_measurements))
        object.__setattr__(self, "contexts", tuple(self.contexts))
        unavailable = list(self.unavailable)
        if self.atr.value is None:
            unavailable.append(self.atr.unavailable_reason or "missing_atr")
        object.__setattr__(self, "unavailable", tuple(dict.fromkeys(unavailable)))
        stream_ids = {item.stream_id for item in self.locations}
        if len(stream_ids) > 1:
            raise ValueError("location publications must share one stream identity")
        if self.location_stream_id is not None and stream_ids and self.location_stream_id not in stream_ids:
            raise ValueError("location publication stream identity does not match supplied stream")
        if self.location_stream_id is None and stream_ids:
            object.__setattr__(self, "location_stream_id", next(iter(stream_ids)))
        vwap_locations = tuple(
            (item.source_aggregate_id, item.timeframe, item.source_observation_id)
            for item in self.locations if item.alias == "VWAP"
        )
        vwap_measurements = tuple(
            (item.source_aggregate_id, item.timeframe, item.source_observation_id)
            for item in self.vwap_measurements
        )
        if (
            len(set(vwap_locations)) != len(vwap_locations)
            or len(set(vwap_measurements)) != len(vwap_measurements)
            or set(vwap_locations) != set(vwap_measurements)
        ):
            raise ValueError("VWAP measurements and locations have divergent provenance")

def publish_phase4a_evidence(
    *,
    atr: AtrPublication,
    locations: Iterable[LocationRecord] = (),
    vwap_measurements: Iterable[VwapMeasurement] = (),
    prior_value: ValuePublication | None = None,
    contexts: Iterable[MarketContext] = (),
    unavailable: Iterable[str] = (),
    context_stream_id: str | None = None,
    location_stream_id: str | None = None,
) -> EvidencePublication:
    """Accept canonical publications for causal replay without deriving policy."""
    return EvidencePublication(
        atr=atr,
        locations=tuple(locations),
        vwap_measurements=tuple(vwap_measurements),
        contexts=tuple(contexts),
        prior_value=prior_value or ValuePublication(None, None, None, (), "missing_prior_session_value_evidence"),
        unavailable=tuple(dict.fromkeys(unavailable)),
        context_stream_id=context_stream_id,
        location_stream_id=location_stream_id,
    )


__all__ = ["AtrPublication", "EvidencePublication", "publish_phase4a_evidence"]
