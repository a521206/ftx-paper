"""Versioned, causal orderflow inputs owned by ``ftx-paper``."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import MappingProxyType


FeatureValue = float | int | str | None


def _mapping(value: Mapping[str, str] | Mapping[str, FeatureValue]) -> MappingProxyType:
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class OrderflowObservationContext:
    session_id: str
    completeness: str
    availability_status: str
    replay_provenance: Mapping[str, str]
    generation: int | None = None
    child_observation_ids: tuple[str, ...] = ()
    child_intervals: tuple[tuple[datetime, datetime], ...] = ()
    coverage_status: str | None = None
    causal_sequence: int | None = None

    def __post_init__(self) -> None:
        if not self.session_id.strip():
            raise ValueError("orderflow context session_id is required")
        if len(self.child_observation_ids) != len(set(self.child_observation_ids)):
            raise ValueError("orderflow child observation IDs must be unique")
        if len(self.child_intervals) != len(self.child_observation_ids):
            raise ValueError("orderflow child intervals must match child IDs")
        if any(start >= end for start, end in self.child_intervals):
            raise ValueError("orderflow child intervals must be increasing")
        object.__setattr__(self, "replay_provenance", _mapping(self.replay_provenance))


@dataclass(frozen=True, slots=True)
class OrderflowObservation:
    observation_id: str
    source_observation_id: str | None
    feature_version: str
    timeframe: str
    instrument: str
    observed_at: datetime
    available_at: datetime
    features: Mapping[str, FeatureValue]
    interval_start: datetime | None = None
    interval_end: datetime | None = None
    location: str | None = None
    interaction_id: str | None = None
    response_status: str | None = None
    evidence_ids: tuple[str, ...] = ()
    context: OrderflowObservationContext | None = None

    def __post_init__(self) -> None:
        if not self.observation_id.strip() or not self.feature_version.strip():
            raise ValueError("orderflow observation identity and feature version are required")
        if not self.timeframe.strip() or not self.instrument.strip():
            raise ValueError("orderflow timeframe and instrument are required")
        if self.available_at < self.observed_at:
            raise ValueError("orderflow availability cannot precede observation time")
        if (self.interval_start is None) != (self.interval_end is None):
            raise ValueError("orderflow interval requires both start and end")
        if self.interval_start is not None and self.interval_end is not None:
            if self.interval_start >= self.interval_end:
                raise ValueError("orderflow interval must be increasing")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("orderflow evidence IDs must be unique")
        object.__setattr__(self, "features", _mapping(self.features))

    def available_by(self, cutoff: datetime) -> bool:
        return self.available_at <= cutoff


@dataclass(frozen=True, slots=True)
class OrderflowContext:
    """Immutable causal input attached to one decision bundle."""

    session_id: str
    cutoff: datetime
    observations: tuple[OrderflowObservation, ...] = ()
    prefixes: Mapping[str, tuple[OrderflowObservation, ...]] = MappingProxyType({})

    def __post_init__(self) -> None:
        if not self.session_id.strip():
            raise ValueError("orderflow context session_id is required")
        observation_ids = [item.observation_id for item in self.observations]
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("orderflow observation IDs must be unique")
        for observation in self.observations:
            if not observation.available_by(self.cutoff):
                raise ValueError("orderflow context contains unavailable observation")
        normalized = {
            str(timeframe): tuple(values)
            for timeframe, values in self.prefixes.items()
        }
        prefix_ids = [
            observation.observation_id
            for values in normalized.values()
            for observation in values
        ]
        if len(prefix_ids) != len(set(prefix_ids)):
            raise ValueError("orderflow prefix observation IDs must be unique")
        for values in normalized.values():
            if any(not observation.available_by(self.cutoff) for observation in values):
                raise ValueError("orderflow prefix contains unavailable observation")
        object.__setattr__(self, "prefixes", MappingProxyType(normalized))

    def observations_for(self, timeframe: str) -> tuple[OrderflowObservation, ...]:
        return self.prefixes.get(timeframe, ())

    def validate_complete_parent(
        self,
        parent: OrderflowObservation,
        *,
        child_timeframe: str = "1m",
    ) -> bool:
        """Require exactly five ordered children covering a complete 5m parent."""
        children = self.observations_for(child_timeframe)
        if parent.timeframe != "5m" or len(children) != 5:
            return False
        child_ids = tuple(item.observation_id for item in children)
        if len(set(child_ids)) != 5 or any(item.timeframe != child_timeframe for item in children):
            return False
        if (
            parent.context is None
            or parent.context.completeness != "complete"
            or parent.context.availability_status != "available"
        ):
            return False
        if parent.interval_start is None or parent.interval_end is None:
            return False
        if parent.interval_start != parent.observed_at or parent.interval_end > parent.available_at:
            return False
        if parent.context.child_observation_ids != child_ids:
            return False
        intervals = parent.context.child_intervals
        if intervals[0][0] != parent.interval_start or intervals[-1][1] != parent.interval_end:
            return False
        if parent.instrument != children[0].instrument:
            return False
        if any(item.instrument != parent.instrument for item in children):
            return False
        if parent.location is not None and any(item.location != parent.location for item in children):
            return False
        if any(not item.available_by(self.cutoff) for item in children):
            return False
        parent_context = parent.context
        for index, child in enumerate(children):
            if child.context is None:
                return False
            if (
                child.context.session_id != parent_context.session_id
                or child.context.generation != parent_context.generation
                or child.context.completeness != parent_context.completeness
                or child.context.availability_status != parent_context.availability_status
                or child.context.causal_sequence != parent_context.causal_sequence
            ):
                return False
            if child.interval_start is None or child.interval_end is None:
                return False
            interval = intervals[index]
            if (child.interval_start, child.interval_end) != interval:
                return False
            if child.interval_end - child.interval_start != timedelta(minutes=1):
                return False
        parent_interval = intervals[-1][1] - intervals[0][0]
        if parent.interval_start is not None and parent.interval_end is not None:
            if (parent.interval_start, parent.interval_end) != (intervals[0][0], intervals[-1][1]):
                return False
        if parent_interval != timedelta(minutes=5):
            return False
        return all(left[1] == right[0] for left, right in zip(intervals, intervals[1:]))


__all__ = [
    "FeatureValue",
    "OrderflowContext",
    "OrderflowObservation",
    "OrderflowObservationContext",
]
