"""Explicit publication boundary for prior-session value references."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("value publication timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class PriorSessionValueReference:
    session_date: str
    value_area_low: float
    point_of_control: float
    value_area_high: float
    observed_at: datetime
    available_at: datetime
    lineage: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.session_date or not self.lineage:
            raise ValueError("value reference requires session date and lineage")
        observed = _utc(self.observed_at)
        available = _utc(self.available_at)
        if available < observed:
            raise ValueError("value availability cannot precede observation")
        if not self.value_area_low <= self.point_of_control <= self.value_area_high:
            raise ValueError("value reference levels must be ordered")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "lineage", tuple(self.lineage))


@dataclass(frozen=True, slots=True)
class ValuePublication:
    reference: PriorSessionValueReference | None
    observed_at: datetime | None
    available_at: datetime | None
    lineage: tuple[str, ...]
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        if self.reference is None:
            if not self.unavailable_reason:
                raise ValueError("unavailable publication requires a reason")
            object.__setattr__(self, "lineage", tuple(self.lineage))
            return
        if self.unavailable_reason is not None:
            raise ValueError("available publication cannot have an unavailable reason")
        if self.observed_at != self.reference.observed_at or self.available_at != self.reference.available_at:
            raise ValueError("publication timestamps must match its reference")
        if tuple(self.lineage) != self.reference.lineage:
            raise ValueError("publication lineage must match its reference")


class PriorSessionValueAdapter(Protocol):
    def prior_session_value(self, session_date: str) -> PriorSessionValueReference | None: ...


def publish_prior_session_value(
    adapter: PriorSessionValueAdapter, *, session_date: str
) -> ValuePublication:
    """Publish adapter-owned evidence; never infer or fall back to a profile."""
    reference = adapter.prior_session_value(session_date)
    if reference is None:
        return ValuePublication(None, None, None, (), "missing_prior_session_value_evidence")
    return ValuePublication(
        reference,
        reference.observed_at,
        reference.available_at,
        reference.lineage,
    )
