"""Versioned market context and opening-gap state for causal orderflow use."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from collections.abc import Iterable
from collections.abc import Mapping
from enum import StrEnum


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
    prior_value_low: float | None = None
    prior_value_high: float | None = None

    def __post_init__(self) -> None:
        if self.status not in {"UNAVAILABLE", "NONE", "UP", "DOWN", "RECOVERED", "GAP_CANDIDATE", "GAP_ACCEPTED", "GAP_RECLAIMED", "RETURN_TO_VALUE", "EXPIRED", "GAP_UP_CANDIDATE", "GAP_DOWN_CANDIDATE", "GAP_UP_ACCEPTED", "GAP_DOWN_ACCEPTED"}:
            raise ValueError("invalid opening gap status")
        if self.status == "UNAVAILABLE" and self.gap_points is not None:
            raise ValueError("unavailable opening gap cannot have a value")


@dataclass(frozen=True, slots=True)
class ContextVersionRef:
    """Stable reference to the exact context consulted by a decision."""

    context_id: str
    version: int


class ContextRelation(StrEnum):
    """Relationship between a location response and its parent context."""

    CONFIRMED = "confirmed"
    CONFLICTED = "conflicted"
    NEUTRAL = "neutral"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class MarketContext:
    context_id: str
    version: int
    available_at: datetime
    levels: SessionLevels
    opening_gap: OpeningGapState
    permission: str
    evidence_ids: tuple[str, ...] = ()
    value_reference: ValueReference | None = None
    location: str | None = None
    market_state: str | None = None
    long_score: float | None = None
    short_score: float | None = None
    source_timeframe: str = "5m"
    parent_context_id: str | None = None
    evidence: tuple[Mapping[str, object], ...] = ()
    source_provenance: tuple[Mapping[str, object], ...] = ()

    def __post_init__(self) -> None:
        if not self.context_id.strip() or self.version < 1:
            raise ValueError("context identity and positive version are required")
        if self.permission not in {"UNAVAILABLE", "LONG", "SHORT", "BOTH", "NONE"}:
            raise ValueError("invalid context permission")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("context evidence IDs must be unique")
        if self.available_at.tzinfo is None or self.available_at.utcoffset() is None:
            raise ValueError("context availability must be timezone-aware")
        if self.source_timeframe != "5m":
            raise ValueError("orderflow context must be sourced from completed 5m evidence")

    def to_dict(self) -> dict[str, object]:
        return {
            "context_id": self.context_id, "version": self.version,
            "available_at": self.available_at.isoformat(),
            "levels": asdict(self.levels), "opening_gap": asdict(self.opening_gap),
            "permission": self.permission, "evidence_ids": list(self.evidence_ids),
            "value_reference": asdict(self.value_reference) if self.value_reference else None,
            "location": self.location, "market_state": self.market_state,
            "long_score": self.long_score, "short_score": self.short_score,
            "source_timeframe": self.source_timeframe,
            "parent_context_id": self.parent_context_id,
            "evidence": [dict(item) for item in self.evidence],
            "source_provenance": [dict(item) for item in self.source_provenance],
        }


class ContextHistory:
    """Append-only context versions resolved strictly as-of a replay clock."""

    def __init__(self, versions: Iterable[MarketContext] = ()) -> None:
        self._versions: list[MarketContext] = []
        for version in versions:
            self.publish(version)

    def publish(self, version: MarketContext) -> None:
        if self._versions and version.available_at < self._versions[-1].available_at:
            raise ValueError("context versions must be published in availability order")
        if self._versions and version.version <= self._versions[-1].version:
            raise ValueError("context versions must increase")
        self._versions.append(version)

    def as_of(self, cutoff: datetime) -> MarketContext | None:
        available = [version for version in self._versions if version.available_at <= cutoff]
        return available[-1] if available else None

    def by_reference(self, reference: ContextVersionRef | None) -> MarketContext | None:
        if reference is None:
            return None
        return next((item for item in self._versions
                     if item.context_id == reference.context_id and item.version == reference.version), None)

    def as_of_location(self, location: str | None, cutoff: datetime) -> MarketContext | None:
        candidates = [
            item for item in self._versions
            if item.available_at <= cutoff
            and (location is None or item.location == location)
        ]
        if not candidates and location is not None:
            candidates = [
                item for item in self._versions
                if item.available_at <= cutoff and item.location is None
            ]
        return max(candidates, key=lambda item: item.available_at, default=None)

    @property
    def versions(self) -> tuple[MarketContext, ...]:
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
        decoded: list[MarketContext] = []
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
                float(gap_raw["prior_value_low"]) if gap_raw.get("prior_value_low") is not None else None,
                float(gap_raw["prior_value_high"]) if gap_raw.get("prior_value_high") is not None else None,
            )
            decoded.append(MarketContext(
                str(item["context_id"]), int(item["version"]), datetime.fromisoformat(str(item["available_at"])),
                levels, gap, str(item["permission"]),
                tuple(str(value) for value in item.get("evidence_ids", ())), value,
                str(item["location"]) if item.get("location") is not None else None,
                str(item["market_state"]) if item.get("market_state") is not None else None,
                float(item["long_score"]) if item.get("long_score") is not None else None,
                float(item["short_score"]) if item.get("short_score") is not None else None,
                str(item.get("source_timeframe", "5m")),
                str(item["parent_context_id"]) if item.get("parent_context_id") is not None else None,
                tuple(dict(entry) for entry in item.get("evidence", ())),
                tuple(dict(entry) for entry in item.get("source_provenance", ())),
            ))
        return cls(decoded)


def classify_opening_gap(
    *, opening_price: float | None, prior_close: float | None,
    observed_at: datetime | None = None, tolerance: float = 0.0,
    reference_id: str | None = None, prior_value_low: float | None = None,
    prior_value_high: float | None = None, reference_price: float | None = None,
) -> OpeningGapState:
    if opening_price is None or prior_close is None:
        return OpeningGapState("UNAVAILABLE", observed_at=observed_at, reference_id=reference_id)
    if reference_id is None:
        return OpeningGapState("NONE", 0.0, None, observed_at,
                               reference_id=None, prior_value_low=prior_value_low,
                               prior_value_high=prior_value_high)
    gap = opening_price - prior_close
    if abs(gap) <= tolerance:
        status = "NONE"
    elif gap > 0:
        status = "GAP_UP_CANDIDATE"
    else:
        status = "GAP_DOWN_CANDIDATE"
    return OpeningGapState(status, gap, reference_price if reference_price is not None else prior_close, observed_at, reference_id=reference_id,
                           prior_value_low=prior_value_low, prior_value_high=prior_value_high)


def classify_context_relation(*, location: str | None, direction: str | None,
                              evidence: Mapping[str, object]) -> ContextRelation:
    """Classify response evidence without allowing a wick to rewrite context."""
    if location is None or direction not in {"long", "short"}:
        return ContextRelation.NEUTRAL
    expected = {"OR_LOW": "long", "OR_HIGH": "short"}.get(location.upper())
    if expected is None:
        return ContextRelation.NEUTRAL
    strong = bool(evidence.get("displacement") or evidence.get("acceptance")
                  or evidence.get("confirmed"))
    if not strong:
        return ContextRelation.NEUTRAL
    return ContextRelation.CONFIRMED if direction == expected else ContextRelation.CONFLICTED


def advance_opening_gap(
    state: OpeningGapState,
    *,
    close: float | None,
    available_at: datetime,
    evidence_id: str,
    timeframe: str = "1m",
    complete: bool = True,
    acceptance_bars: int = 1,
    expiry_bars: int = 5,
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
    in_value = (
        state.prior_value_low is not None and state.prior_value_high is not None
        and state.prior_value_low <= close <= state.prior_value_high
    )
    next_status = state.status
    if recovered:
        next_status = "RETURN_TO_VALUE"
    elif in_value:
        next_status = "RETURN_TO_VALUE"
    elif state.status in {"GAP_CANDIDATE", "GAP_UP_CANDIDATE", "GAP_DOWN_CANDIDATE"} and completed_bars >= acceptance_bars:
        next_status = "GAP_UP_ACCEPTED" if (state.gap_points or 0) > 0 else "GAP_DOWN_ACCEPTED"
    elif completed_bars >= expiry_bars:
        next_status = "EXPIRED"
    return OpeningGapState(
        next_status, state.gap_points, state.reference_price,
        available_at, (*state.evidence_ids, evidence_id), completed_bars,
        state.reference_id, state.prior_value_low, state.prior_value_high,
    )


__all__ = [
    "ContextHistory", "MarketContext", "ContextVersionRef", "ContextRelation",
    "OpeningGapState", "SessionLevels", "ValueReference", "advance_opening_gap",
    "classify_opening_gap", "classify_context_relation",
]
