"""Versioned market context and opening-gap state for causal orderflow use."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from collections.abc import Iterable
from collections.abc import Sequence, Mapping


@dataclass(frozen=True, slots=True)
class ValueReference:
    """A prior-session value projection published only when complete."""

    reference_id: str
    high: float | None
    low: float | None
    available_at: datetime | None
    source_ids: tuple[str, ...] = ()
    complete: bool = False

    def __post_init__(self) -> None:
        if not self.reference_id.strip():
            raise ValueError("value reference identity is required")
        if self.complete and (self.high is None or self.low is None):
            raise ValueError("complete value reference requires both bounds")
        if self.high is not None and self.low is not None and self.low > self.high:
            raise ValueError("value reference bounds are inverted")
        if self.complete and not self.source_ids:
            raise ValueError("complete value reference requires source IDs")
        if self.complete and self.available_at is None:
            raise ValueError("complete value reference requires availability")


@dataclass(frozen=True, slots=True)
class SessionLevels:
    session_date: str
    opening_range_high: float | None = None
    opening_range_low: float | None = None
    prior_value_high: float | None = None
    prior_value_low: float | None = None
    prior_profile_complete: bool = False
    available_at: datetime | None = None
    source_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.session_date.strip():
            raise ValueError("session levels require a session date")
        if self.opening_range_high is not None and self.opening_range_low is not None:
            if self.opening_range_low > self.opening_range_high:
                raise ValueError("opening range bounds are inverted")
        if self.prior_value_high is not None and self.prior_value_low is not None:
            if self.prior_value_low > self.prior_value_high:
                raise ValueError("prior value bounds are inverted")


@dataclass(frozen=True, slots=True)
class OpeningGapState:
    status: str
    gap_points: float | None = None
    reference_price: float | None = None
    observed_at: datetime | None = None
    evidence_ids: tuple[str, ...] = ()
    completed_bars: int = 0
    reference_id: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"UNAVAILABLE", "NONE", "UP", "DOWN", "RECOVERED", "GAP_CANDIDATE", "GAP_ACCEPTED", "GAP_RECLAIMED", "RETURN_TO_VALUE", "EXPIRED"}:
            raise ValueError("invalid opening gap status")
        if self.status == "UNAVAILABLE" and self.gap_points is not None:
            raise ValueError("unavailable opening gap cannot have a value")


@dataclass(frozen=True, slots=True)
class ContextVersion:
    context_id: str
    version: int
    available_at: datetime
    levels: SessionLevels
    opening_gap: OpeningGapState
    permission: str
    evidence_ids: tuple[str, ...] = ()
    value_reference: ValueReference | None = None

    def __post_init__(self) -> None:
        if not self.context_id.strip() or self.version < 1:
            raise ValueError("context identity and positive version are required")
        if self.permission not in {"UNAVAILABLE", "LONG", "SHORT", "BOTH", "NONE"}:
            raise ValueError("invalid context permission")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("context evidence IDs must be unique")
        if self.available_at.tzinfo is None or self.available_at.utcoffset() is None:
            raise ValueError("context availability must be timezone-aware")

    def to_dict(self) -> dict[str, object]:
        return {
            "context_id": self.context_id, "version": self.version,
            "available_at": self.available_at.isoformat(),
            "levels": asdict(self.levels), "opening_gap": asdict(self.opening_gap),
            "permission": self.permission, "evidence_ids": list(self.evidence_ids),
            "value_reference": asdict(self.value_reference) if self.value_reference else None,
        }


class ContextHistory:
    """Append-only context versions resolved strictly as-of a replay clock."""

    def __init__(self, versions: Iterable[ContextVersion] = ()) -> None:
        self._versions: list[ContextVersion] = []
        for version in versions:
            self.publish(version)

    def publish(self, version: ContextVersion) -> None:
        if self._versions and version.available_at < self._versions[-1].available_at:
            raise ValueError("context versions must be published in availability order")
        if self._versions and version.version <= self._versions[-1].version:
            raise ValueError("context versions must increase")
        self._versions.append(version)

    def as_of(self, cutoff: datetime) -> ContextVersion | None:
        available = [version for version in self._versions if version.available_at <= cutoff]
        return available[-1] if available else None

    @property
    def versions(self) -> tuple[ContextVersion, ...]:
        return tuple(self._versions)

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "versions": [version.to_dict() for version in self._versions]}

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ContextHistory":
        if raw.get("schema_version") != 1:
            raise ValueError("unsupported context history schema")
        versions = raw.get("versions")
        if not isinstance(versions, (list, tuple)):
            raise ValueError("context history versions must be a sequence")
        decoded: list[ContextVersion] = []
        for item in versions:
            if not isinstance(item, Mapping):
                raise ValueError("context history version must be an object")
            levels_raw = item.get("levels")
            gap_raw = item.get("opening_gap")
            if not isinstance(levels_raw, Mapping) or not isinstance(gap_raw, Mapping):
                raise ValueError("context history version payload is invalid")
            value_raw = item.get("value_reference")
            value = None
            if isinstance(value_raw, Mapping):
                raw_available = value_raw.get("available_at")
                value = ValueReference(
                    str(value_raw["reference_id"]),
                    float(value_raw["high"]) if value_raw.get("high") is not None else None,
                    float(value_raw["low"]) if value_raw.get("low") is not None else None,
                    datetime.fromisoformat(str(raw_available)) if raw_available is not None else None,
                    tuple(str(source_id) for source_id in value_raw.get("source_ids", ())),
                    bool(value_raw.get("complete", False)),
                )
            levels = SessionLevels(
                str(levels_raw["session_date"]),
                float(levels_raw["opening_range_high"]) if levels_raw.get("opening_range_high") is not None else None,
                float(levels_raw["opening_range_low"]) if levels_raw.get("opening_range_low") is not None else None,
                float(levels_raw["prior_value_high"]) if levels_raw.get("prior_value_high") is not None else None,
                float(levels_raw["prior_value_low"]) if levels_raw.get("prior_value_low") is not None else None,
                bool(levels_raw.get("prior_profile_complete", False)),
                datetime.fromisoformat(str(levels_raw["available_at"])) if levels_raw.get("available_at") is not None else None,
                tuple(str(source_id) for source_id in levels_raw.get("source_ids", ())),
            )
            gap = OpeningGapState(
                str(gap_raw["status"]),
                float(gap_raw["gap_points"]) if gap_raw.get("gap_points") is not None else None,
                float(gap_raw["reference_price"]) if gap_raw.get("reference_price") is not None else None,
                datetime.fromisoformat(str(gap_raw["observed_at"])) if gap_raw.get("observed_at") is not None else None,
                tuple(str(evidence_id) for evidence_id in gap_raw.get("evidence_ids", ())),
                int(gap_raw.get("completed_bars", 0)),
                str(gap_raw["reference_id"]) if gap_raw.get("reference_id") is not None else None,
            )
            decoded.append(ContextVersion(
                str(item["context_id"]), int(item["version"]), datetime.fromisoformat(str(item["available_at"])),
                levels, gap, str(item["permission"]),
                tuple(str(value) for value in item.get("evidence_ids", ())), value,
            ))
        return cls(decoded)


def classify_opening_gap(
    *, opening_price: float | None, prior_close: float | None,
    observed_at: datetime | None = None, tolerance: float = 0.0,
    reference_id: str | None = None,
) -> OpeningGapState:
    if opening_price is None or prior_close is None:
        return OpeningGapState("UNAVAILABLE", observed_at=observed_at, reference_id=reference_id)
    gap = opening_price - prior_close
    if abs(gap) <= tolerance:
        status = "NONE"
    elif gap > 0:
        status = "UP"
    else:
        status = "DOWN"
    return OpeningGapState(status, gap, prior_close, observed_at, reference_id=reference_id)


def derive_session_levels(
    observations: Sequence[object],
    *,
    session_date: str,
    prior_high: float | None = None,
    prior_low: float | None = None,
    prior_complete: bool = False,
) -> SessionLevels:
    """Derive levels from completed 5m observations only."""
    completed = [
        item for item in observations
        if getattr(item, "timeframe", None) == "5m"
        and getattr(getattr(item, "context", None), "completeness", None) == "complete"
        and getattr(getattr(item, "context", None), "availability_status", None) == "available"
    ]
    opening = next((item for item in completed if getattr(item, "interval_start", None) is not None), None)
    features: Mapping[str, object] = getattr(opening, "features", {}) if opening is not None else {}
    high = features.get("high")
    low = features.get("low")
    return SessionLevels(
        session_date,
        float(high) if isinstance(high, (int, float)) and not isinstance(high, bool) else None,
        float(low) if isinstance(low, (int, float)) and not isinstance(low, bool) else None,
        prior_high if prior_complete else None,
        prior_low if prior_complete else None,
        prior_complete,
        getattr(opening, "available_at", None) if opening is not None else None,
        (getattr(opening, "observation_id"),) if opening is not None else (),
    )


def permission_for_context(
    *,
    opening_gap: OpeningGapState,
    relation: str | None,
    evidence_ids: tuple[str, ...],
) -> tuple[str, tuple[str, ...]]:
    """Return directional permission without treating unavailable evidence as neutral."""
    if opening_gap.status == "UNAVAILABLE" or relation is None:
        return "UNAVAILABLE", evidence_ids
    if opening_gap.status == "UP" and relation == "above_value":
        return "SHORT", evidence_ids
    if opening_gap.status == "DOWN" and relation == "below_value":
        return "LONG", evidence_ids
    return "BOTH", evidence_ids


def admission_recovery_available(
    *, recovery_timeframe: str, completed_timeframe: str,
    gap_end: datetime, evidence_available_at: datetime,
) -> bool:
    """Return true only for completed 5m evidence after a source gap."""
    return (
        recovery_timeframe == "5m"
        and completed_timeframe == "5m"
        and evidence_available_at >= gap_end
    )


def advance_opening_gap(
    state: OpeningGapState,
    *,
    close: float | None,
    available_at: datetime,
    evidence_id: str,
    timeframe: str = "1m",
    complete: bool = True,
) -> OpeningGapState:
    """Advance a gap only from one completed, causally available candle."""
    if timeframe != "1m" or not complete or close is None or state.status in {"UNAVAILABLE", "NONE", "RECOVERED", "EXPIRED", "RETURN_TO_VALUE"}:
        return state
    if state.reference_price is None:
        return state
    if evidence_id in state.evidence_ids:
        return state
    completed_bars = state.completed_bars + 1
    recovered = close <= state.reference_price if state.gap_points is not None and state.gap_points > 0 else close >= state.reference_price
    next_status = state.status
    if recovered:
        next_status = "RETURN_TO_VALUE"
    elif state.status == "GAP_CANDIDATE":
        next_status = "GAP_ACCEPTED"
    elif completed_bars >= 5:
        next_status = "EXPIRED"
    return OpeningGapState(
        next_status, state.gap_points, state.reference_price,
        available_at, (*state.evidence_ids, evidence_id), completed_bars,
        state.reference_id,
    )


__all__ = [
    "ContextHistory", "ContextVersion", "OpeningGapState", "SessionLevels", "ValueReference",
    "advance_opening_gap", "classify_opening_gap", "derive_session_levels",
    "permission_for_context", "admission_recovery_available",
]
