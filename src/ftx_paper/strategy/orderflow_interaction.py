"""Completed-candle location interactions with frozen reference provenance."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import cast

from .orderflow_contract import OrderflowObservation
from .orderflow_location import LocationRecord


class InteractionStatus(StrEnum):
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    ENDED = "ENDED"


class InteractionCloseReason(StrEnum):
    DEPARTED = "DEPARTED"
    CROSS_BAND = "CROSS_BAND"
    SAME_SIDE_DEPARTURE = "SAME_SIDE_DEPARTURE"
    OUTSIDE_BAND_DEPARTURE = "OUTSIDE_BAND_DEPARTURE"
    SESSION_END = "SESSION_END"
    DATA_GAP = "DATA_GAP"
    REFERENCE_REPLACED = "REFERENCE_REPLACED"


@dataclass(frozen=True, slots=True)
class InteractionCandle:
    observation_id: str
    timeframe: str
    observed_at: datetime
    available_at: datetime
    close: float
    high: float | None = None
    low: float | None = None
    open: float | None = None
    child_observation_ids: tuple[str, ...] = ()
    context_version: int | None = None


@dataclass(frozen=True, slots=True)
class ProjectionSnapshot:
    """Immutable aggregate of the completed interaction candles so far."""

    timeframe: str
    open: float
    high: float
    low: float
    close: float
    source_observation_ids: tuple[str, ...]
    child_observation_ids: tuple[str, ...]
    first_observed_at: datetime
    last_observed_at: datetime
    available_at: datetime
    context_version: int | None = None

    def __post_init__(self) -> None:
        if self.timeframe not in {"1m", "5m"}:
            raise ValueError("projection timeframe must be 1m or 5m")
        if not self.source_observation_ids or len(self.source_observation_ids) != len(set(self.source_observation_ids)):
            raise ValueError("projection source observation IDs must be unique")
        if len(self.child_observation_ids) != len(set(self.child_observation_ids)):
            raise ValueError("projection child observation IDs must be unique")
        if self.available_at < self.last_observed_at:
            raise ValueError("projection availability is not causal")


@dataclass(frozen=True, slots=True)
class SyntheticScoreSnapshot:
    """Scores computed once for one causal cumulative projection."""

    context_version: int | None
    observed_at: datetime
    available_at: datetime
    source_observation_ids: tuple[str, ...]
    long_score: float | None
    short_score: float | None

    def __post_init__(self) -> None:
        if self.available_at < self.observed_at:
            raise ValueError("synthetic score availability is not causal")
        if not self.source_observation_ids:
            raise ValueError("synthetic score requires source observations")
        if len(self.source_observation_ids) != len(set(self.source_observation_ids)):
            raise ValueError("synthetic score source observation IDs must be unique")
@dataclass(frozen=True, slots=True)
class EvidenceLineage:
    observation_id: str
    source_observation_id: str
    timeframe: str
    observed_at: datetime
    available_at: datetime
    replay_fingerprint: str | None = None


@dataclass(frozen=True, slots=True)
class HypothesisState:
    hypothesis_id: str
    policy_version: str
    status: str = "PENDING_RESPONSE"
    armed: bool = False
    direction: str | None = None
    evidence_ids: tuple[str, ...] = ()
    confirmation_observation_id: str | None = None
    confirmation_available_at: datetime | None = None
    expected_available_at: datetime | None = None
    invalidation_reason: str | None = None
    expiry_reason: str | None = None
    evidence_lineage: tuple[EvidenceLineage, ...] = ()

    def __post_init__(self) -> None:
        if not self.hypothesis_id.strip() or not self.policy_version.strip():
            raise ValueError("hypothesis identity is required")
        if self.status not in {"PENDING_RESPONSE", "UNAVAILABLE", "CONFIRMED", "REJECTED", "INVALIDATED", "EXPIRED"}:
            raise ValueError("invalid hypothesis status")
        if self.direction is not None and self.direction not in {"long", "short"}:
            raise ValueError("invalid hypothesis direction")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("hypothesis evidence IDs must be unique")
        if len({item.observation_id for item in self.evidence_lineage}) != len(self.evidence_lineage):
            raise ValueError("hypothesis evidence lineage IDs must be unique")


@dataclass(frozen=True, slots=True)
class LocationInteraction:
    interaction_id: str
    timeframe: str
    location_id: str
    location_alias: str
    reference_level: float
    reference_generation: int
    source_observation_id: str
    created_at: datetime
    status: InteractionStatus
    contact_candles: tuple[InteractionCandle, ...] = ()
    close_reason: InteractionCloseReason | None = None
    closed_at: datetime | None = None
    response_status: str = "PENDING_RESPONSE"
    band_lower: float | None = None
    band_upper: float | None = None
    hypothesis_states: tuple[HypothesisState, ...] = ()
    pd_role: str | None = None
    pd_role_transition_candle_id: str | None = None
    pd_role_transition_available_at: datetime | None = None
    location_type: str = "session_reference"
    reference_provenance: Mapping[str, str] = field(default_factory=dict)
    synthetic_snapshots: tuple[tuple[str, tuple[ProjectionSnapshot, ...]], ...] = ()
    score_history: tuple[SyntheticScoreSnapshot, ...] = ()
    source_cursors: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.interaction_id.strip() or not self.location_id.strip():
            raise ValueError("interaction identity is required")
        if self.status is InteractionStatus.ACTIVE and self.close_reason is not None:
            raise ValueError("active interaction cannot have a close reason")
        if self.status is InteractionStatus.ENDED and self.close_reason is None:
            raise ValueError("ended interaction requires a close reason")
        if self.response_status not in {"PENDING", "PENDING_RESPONSE", "CONFIRMED", "INVALIDATED", "EXPIRED"}:
            raise ValueError("invalid interaction response status")


def _projection_snapshot(candles: tuple[InteractionCandle, ...]) -> ProjectionSnapshot:
    if not candles:
        raise ValueError("projection requires at least one candle")
    candles = tuple(sorted(candles, key=lambda item: (item.observed_at, item.available_at, item.observation_id)))
    return ProjectionSnapshot(
        timeframe=candles[0].timeframe,
        open=candles[0].open if candles[0].open is not None else candles[0].close,
        high=max(item.high if item.high is not None else item.close for item in candles),
        low=min(item.low if item.low is not None else item.close for item in candles),
        close=candles[-1].close,
        source_observation_ids=tuple(item.observation_id for item in candles),
        child_observation_ids=tuple(
            child_id for item in candles for child_id in item.child_observation_ids
        ),
        context_version=candles[-1].context_version,
        first_observed_at=candles[0].observed_at,
        last_observed_at=candles[-1].observed_at,
        available_at=max(item.available_at for item in candles),
    )


def _append_projection(
    interaction: LocationInteraction,
    candles: tuple[InteractionCandle, ...],
) -> LocationInteraction:
    snapshot = _projection_snapshot(candles)
    existing = dict(interaction.synthetic_snapshots)
    history = existing.get(snapshot.timeframe, ())
    if not history or history[-1].source_observation_ids != snapshot.source_observation_ids:
        existing[snapshot.timeframe] = (*history, snapshot)
    return replace(
        interaction,
        synthetic_snapshots=tuple(
            (timeframe, existing[timeframe]) for timeframe in sorted(existing)
        ),
    )


def _append_source_projection(
    interaction: LocationInteraction,
    candle: InteractionCandle,
) -> LocationInteraction:
    """Advance one timeframe projection without making a contact candle."""
    existing = dict(interaction.synthetic_snapshots)
    history = existing.get(candle.timeframe, ())
    if history and candle.observation_id in history[-1].source_observation_ids:
        cursors = dict(interaction.source_cursors)
        cursors[candle.timeframe] = candle.observation_id
        return replace(interaction, source_cursors=tuple(sorted(cursors.items())))
    previous = history[-1] if history else None
    source_ids = (*previous.source_observation_ids, candle.observation_id) if previous else (candle.observation_id,)
    snapshot = ProjectionSnapshot(
        timeframe=candle.timeframe,
        open=previous.open if previous else (candle.open if candle.open is not None else candle.close),
        high=max(previous.high, candle.high if candle.high is not None else candle.close) if previous else (candle.high if candle.high is not None else candle.close),
        low=min(previous.low, candle.low if candle.low is not None else candle.close) if previous else (candle.low if candle.low is not None else candle.close),
        close=candle.close,
        source_observation_ids=source_ids,
        child_observation_ids=tuple(dict.fromkeys((*previous.child_observation_ids, *candle.child_observation_ids))) if previous else candle.child_observation_ids,
        first_observed_at=previous.first_observed_at if previous else candle.observed_at,
        last_observed_at=candle.observed_at,
        available_at=max(previous.available_at, candle.available_at) if previous else candle.available_at,
        context_version=candle.context_version,
    )
    existing[candle.timeframe] = (*history, snapshot)
    cursors = dict(interaction.source_cursors)
    cursors[candle.timeframe] = candle.observation_id
    return replace(
        interaction,
        synthetic_snapshots=tuple((key, existing[key]) for key in sorted(existing)),
        source_cursors=tuple(sorted(cursors.items())),
    )


class InteractionDetector:
    """One detector per timeframe; references are never mutated after creation."""

    def __init__(self, timeframe: str) -> None:
        if not timeframe.strip():
            raise ValueError("interaction timeframe is required")
        self.timeframe = timeframe
        self._active: dict[str, LocationInteraction] = {}
        self._closed: list[LocationInteraction] = []
        self._processed_observations: set[str] = set()
        self._session_first_observed_at: dict[str, datetime] = {}

    def observe(
        self,
        location: LocationRecord,
        observation: OrderflowObservation,
        *,
        band_contact: bool | None = None,
        observation_id: str | None = None,
    ) -> LocationInteraction | None:
        if observation.timeframe != self.timeframe:
            return None
        processed_id = observation_id or observation.observation_id
        if processed_id in self._processed_observations:
            return self._active.get(f"{location.alias}:{self.timeframe}")
        self._processed_observations.add(processed_id)
        if observation.context is None or observation.context.availability_status != "available":
            return None
        close = observation.features.get("close")
        open_price = observation.features.get("open", close)
        high = observation.features.get("high", close)
        low = observation.features.get("low", close)
        if (
            not isinstance(close, (int, float)) or isinstance(close, bool)
            or not isinstance(open_price, (int, float)) or isinstance(open_price, bool)
            or not isinstance(high, (int, float)) or isinstance(high, bool)
            or not isinstance(low, (int, float)) or isinstance(low, bool)
        ):
            return None
        close_value, open_value, high_value, low_value = float(close), float(open_price), float(high), float(low)
        session_id = observation.context.session_id if observation.context is not None else ""
        first_observed_at = self._session_first_observed_at.get(session_id)
        if first_observed_at is None or observation.observed_at < first_observed_at:
            self._session_first_observed_at[session_id] = observation.observed_at
            first_observed_at = observation.observed_at
        key = f"{location.alias}:{self.timeframe}"
        interaction = self._active.get(key)
        if interaction is not None and (
            interaction.location_id != location.location_id
            or interaction.reference_generation != location.generation
            or interaction.reference_level != location.price
            or interaction.band_lower != location.band.lower
            or interaction.band_upper != location.band.upper
        ):
            ended = replace(
                interaction,
                status=InteractionStatus.ENDED,
                close_reason=InteractionCloseReason.REFERENCE_REPLACED,
                closed_at=observation.available_at,
            )
            self._active.pop(key, None)
            self._closed.append(ended)
            interaction = None
        if interaction is not None and any(
            item.observation_id == observation.observation_id
            for item in interaction.contact_candles
        ):
            return interaction
        candle = InteractionCandle(
            observation.observation_id, observation.timeframe,
            observation.observed_at, observation.available_at, close_value, high_value, low_value,
            open_value,
            observation.context.child_observation_ids,
            observation.context.generation,
        )
        contact = location.band.lower <= high_value and low_value <= location.band.upper if band_contact is None else band_contact
        inside = location.band.contains(close_value)
        if interaction is None:
            # The opening-range seed defines the reference; its wick is not an
            # independent OR interaction in the canonical state machine.
            if (
                location.alias in {"OR_HIGH", "OR_LOW"}
                and observation.observed_at == first_observed_at
                and (
                    location.source_aggregate_id == observation.source_observation_id
                    or location.source_aggregate_id.startswith("aggregate:")
                )
            ):
                return None
            if not contact:
                return None
            interaction = LocationInteraction(
                ":".join((
                    observation.instrument,
                    observation.context.session_id if observation.context is not None else "",
                    location.alias,
                    self.timeframe,
                    observation.observed_at.isoformat(),
                )),
                self.timeframe, location.location_id, location.alias,
                location.price, location.generation, observation.observation_id,
                observation.observed_at, InteractionStatus.ACTIVE, (candle,),
                band_lower=location.band.lower, band_upper=location.band.upper,
                location_type=location.location_type,
                reference_provenance={
                    "source_observation_id": location.source_observation_id or observation.observation_id,
                    "source_aggregate_id": location.source_aggregate_id,
                    "source_timeframe": location.timeframe or observation.timeframe,
                },
            )
            interaction = _append_projection(interaction, interaction.contact_candles)
            self._active[key] = interaction
            return interaction
        if not inside:
            previous_close = interaction.contact_candles[0].close
            crossed = ((previous_close < interaction.reference_level < close_value)
                       or (previous_close > interaction.reference_level > close_value))
            close_reason = (
                InteractionCloseReason.CROSS_BAND if crossed
                else InteractionCloseReason.DEPARTED
                if previous_close == interaction.reference_level
                else InteractionCloseReason.SAME_SIDE_DEPARTURE
            )
            ended = replace(
                interaction, status=InteractionStatus.ENDED,
                close_reason=close_reason,
                closed_at=observation.available_at,
                contact_candles=(*interaction.contact_candles, candle),
            )
            ended = _append_projection(ended, ended.contact_candles)
            self._active.pop(key, None)
            self._closed.append(ended)
            return ended
        advanced = replace(interaction, contact_candles=(*interaction.contact_candles, candle))
        advanced = _append_projection(advanced, advanced.contact_candles)
        self._active[key] = advanced
        return advanced

    def advance_projections(
        self, sources: Mapping[str, tuple[OrderflowObservation, ...]], *, as_of: datetime,
    ) -> tuple[LocationInteraction, ...]:
        """Advance active interactions from newly available source observations."""
        updated: list[LocationInteraction] = []
        for key, interaction in tuple(self._active.items()):
            current = interaction
            cursors = dict(current.source_cursors)
            for timeframe, observations in sources.items():
                for observation in sorted(observations, key=lambda item: (item.observed_at, item.available_at, item.observation_id)):
                    if observation.available_at > as_of or observation.observed_at < current.created_at:
                        continue
                    if cursors.get(timeframe) == observation.observation_id:
                        continue
                    close = observation.features.get("close")
                    if not isinstance(close, (int, float)) or isinstance(close, bool):
                        continue
                    values = {
                        name: observation.features.get(name, close)
                        for name in ("open", "high", "low")
                    }
                    if any(not isinstance(value, (int, float)) or isinstance(value, bool) for value in values.values()):
                        continue
                    current = _append_source_projection(current, InteractionCandle(
                        observation.observation_id, timeframe, observation.observed_at,
                        observation.available_at, float(cast(float, close)),
                        float(cast(float, values["high"])),
                        float(cast(float, values["low"])),
                        float(cast(float, values["open"])),
                        observation.context.child_observation_ids if observation.context else (),
                        observation.context.generation if observation.context else None,
                    ))
                    cursors = dict(current.source_cursors)
            if current != interaction:
                self._active[key] = current
                updated.append(current)
        return tuple(updated)

    def record_score(
        self, interaction_id: str, *, snapshot: ProjectionSnapshot,
        context_version: int | None, long_score: float | None, short_score: float | None,
    ) -> LocationInteraction:
        """Record the first score for each cumulative projection snapshot."""
        for key, interaction in self._active.items():
            if interaction.interaction_id != interaction_id:
                continue
            if interaction.score_history and interaction.score_history[-1].source_observation_ids == snapshot.source_observation_ids:
                return interaction
            updated = replace(interaction, score_history=(*interaction.score_history, SyntheticScoreSnapshot(
                context_version, snapshot.last_observed_at, snapshot.available_at,
                snapshot.source_observation_ids, long_score, short_score,
            )))
            self._active[key] = updated
            return updated
        raise KeyError(interaction_id)

    def close_all(self, reason: InteractionCloseReason, at: datetime) -> tuple[LocationInteraction, ...]:
        ended = tuple(
            replace(item, status=InteractionStatus.ENDED, close_reason=reason, closed_at=at)
            for item in self._active.values()
        )
        self._active.clear()
        self._closed.extend(ended)
        return ended

    def observe_gap(self, at: datetime) -> tuple[LocationInteraction, ...]:
        """Close active episodes at a source boundary without inventing candles."""
        return self.close_all(InteractionCloseReason.DATA_GAP, at)

    def snapshot(self) -> dict[str, object]:
        def encode(item: LocationInteraction) -> dict[str, object]:
            return {
                "interaction_id": item.interaction_id,
                "timeframe": item.timeframe,
                "location_id": item.location_id,
                "location_alias": item.location_alias,
                "location_type": item.location_type,
                "reference_provenance": dict(item.reference_provenance),
                "synthetic_snapshots": {
                    timeframe: [
                        {
                            "timeframe": snapshot.timeframe,
                            "open": snapshot.open,
                            "high": snapshot.high,
                            "low": snapshot.low,
                            "close": snapshot.close,
                            "source_observation_ids": list(snapshot.source_observation_ids),
                            "child_observation_ids": list(snapshot.child_observation_ids),
                            "context_version": snapshot.context_version,
                            "first_observed_at": snapshot.first_observed_at.isoformat(),
                            "last_observed_at": snapshot.last_observed_at.isoformat(),
                            "available_at": snapshot.available_at.isoformat(),
                        }
                        for snapshot in values
                    ]
                    for timeframe, values in item.synthetic_snapshots
                },
                "score_history": [
                    {
                        "context_version": score.context_version,
                        "observed_at": score.observed_at.isoformat(),
                        "available_at": score.available_at.isoformat(),
                        "source_observation_ids": list(score.source_observation_ids),
                        "long_score": score.long_score,
                        "short_score": score.short_score,
                    }
                    for score in item.score_history
                ],
                "source_cursors": dict(item.source_cursors),
                "reference_level": item.reference_level,
                "reference_generation": item.reference_generation,
                "source_observation_id": item.source_observation_id,
                "created_at": item.created_at.isoformat(),
                "status": item.status.value,
                "contact_candles": [
                    {
                        "observation_id": candle.observation_id,
                        "timeframe": candle.timeframe,
                        "observed_at": candle.observed_at.isoformat(),
                        "available_at": candle.available_at.isoformat(),
                        "close": candle.close,
                        "open": candle.open,
                        "child_observation_ids": list(candle.child_observation_ids),
                        "context_version": candle.context_version,
                        "high": candle.high,
                        "low": candle.low,
                    }
                    for candle in item.contact_candles
                ],
                "close_reason": item.close_reason.value if item.close_reason else None,
                "closed_at": item.closed_at.isoformat() if item.closed_at else None,
                "response_status": item.response_status,
                "band_lower": item.band_lower,
                "band_upper": item.band_upper,
                "pd_role": item.pd_role,
                "pd_role_transition_candle_id": item.pd_role_transition_candle_id,
                "pd_role_transition_available_at": item.pd_role_transition_available_at.isoformat() if item.pd_role_transition_available_at else None,
                "hypothesis_states": [
                    {
                        "hypothesis_id": value.hypothesis_id,
                        "policy_version": value.policy_version,
                        "status": value.status,
                        "armed": value.armed,
                        "direction": value.direction,
                        "evidence_ids": list(value.evidence_ids),
                        "confirmation_observation_id": value.confirmation_observation_id,
                        "confirmation_available_at": value.confirmation_available_at.isoformat() if value.confirmation_available_at else None,
                        "expected_available_at": value.expected_available_at.isoformat() if value.expected_available_at else None,
                        "invalidation_reason": value.invalidation_reason,
                        "expiry_reason": value.expiry_reason,
                        "evidence_lineage": [
                            {
                                "observation_id": item.observation_id,
                                "source_observation_id": item.source_observation_id,
                                "timeframe": item.timeframe,
                                "observed_at": item.observed_at.isoformat(),
                                "available_at": item.available_at.isoformat(),
                                "replay_fingerprint": item.replay_fingerprint,
                            }
                            for item in value.evidence_lineage
                        ],
                    }
                    for value in item.hypothesis_states
                ],
            }

        return {
            "timeframe": self.timeframe,
            "active": [encode(item) for item in self._active.values()],
            "closed": [encode(item) for item in self._closed],
            "processed_observations": sorted(self._processed_observations),
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "InteractionDetector":
        timeframe = snapshot.get("timeframe")
        if not isinstance(timeframe, str) or not timeframe:
            raise ValueError("interaction snapshot timeframe is required")
        detector = cls(timeframe)

        def decode(raw: object) -> LocationInteraction:
            if not isinstance(raw, Mapping):
                raise ValueError("interaction snapshot item must be an object")
            candles_raw = raw.get("contact_candles", ())
            if not isinstance(candles_raw, (list, tuple)):
                raise ValueError("interaction snapshot candles must be a sequence")
            candles_list: list[InteractionCandle] = []
            for item in candles_raw:
                if not isinstance(item, Mapping):
                    raise ValueError("interaction snapshot candle must be an object")
                candles_list.append(InteractionCandle(
                    str(item["observation_id"]), str(item["timeframe"]),
                    datetime.fromisoformat(str(item["observed_at"])),
                    datetime.fromisoformat(str(item["available_at"])),
                    float(item["close"]),
                    float(item["high"]) if item.get("high") is not None else float(item["close"]),
                    float(item["low"]) if item.get("low") is not None else float(item["close"]),
                     float(item["open"]) if item.get("open") is not None else None,
                     tuple(str(value) for value in item.get("child_observation_ids", ())),
                     int(item["context_version"]) if item.get("context_version") is not None else None,
                ))
            candles = tuple(candles_list)
            closed_at = raw.get("closed_at")
            close_reason = raw.get("close_reason")
            def decode_hypothesis(value: object) -> HypothesisState:
                if isinstance(value, Mapping):
                    return HypothesisState(
                        str(value["hypothesis_id"]), str(value.get("policy_version", "response_v1")),
                        str(value.get("status", "PENDING_RESPONSE")), bool(value.get("armed", False)),
                        str(value["direction"]) if value.get("direction") is not None else None,
                        tuple(str(item) for item in value.get("evidence_ids", ())),
                        str(value["confirmation_observation_id"]) if value.get("confirmation_observation_id") is not None else None,
                        datetime.fromisoformat(str(value["confirmation_available_at"])) if value.get("confirmation_available_at") is not None else None,
                        datetime.fromisoformat(str(value["expected_available_at"])) if value.get("expected_available_at") is not None else None,
                        str(value["invalidation_reason"]) if value.get("invalidation_reason") is not None else None,
                        str(value["expiry_reason"]) if value.get("expiry_reason") is not None else None,
                        tuple(
                            EvidenceLineage(
                                str(item["observation_id"]),
                                str(item["source_observation_id"]),
                                str(item["timeframe"]),
                                datetime.fromisoformat(str(item["observed_at"])),
                                datetime.fromisoformat(str(item["available_at"])),
                                str(item["replay_fingerprint"]) if item.get("replay_fingerprint") is not None else None,
                            )
                            for item in value.get("evidence_lineage", ())
                            if isinstance(item, Mapping)
                        ),
                    )
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    return HypothesisState(str(value[0]), "response_v1", str(value[1]))
                raise ValueError("invalid hypothesis snapshot")

            raw_hypotheses = raw.get("hypothesis_states", ())
            if not isinstance(raw_hypotheses, (list, tuple)):
                raise ValueError("invalid hypothesis states")
            raw_projections = raw.get("synthetic_snapshots", {})
            if not isinstance(raw_projections, Mapping):
                raise ValueError("invalid synthetic projections")
            projections: list[tuple[str, tuple[ProjectionSnapshot, ...]]] = []
            for timeframe, values in raw_projections.items():
                if not isinstance(values, (list, tuple)):
                    raise ValueError("invalid synthetic projection snapshots")
                decoded_values: list[ProjectionSnapshot] = []
                for value in values:
                    if not isinstance(value, Mapping):
                        raise ValueError("synthetic projection snapshot must be an object")
                    decoded_values.append(ProjectionSnapshot(
                        str(value["timeframe"]), float(value["open"]), float(value["high"]),
                        float(value["low"]), float(value["close"]),
                        tuple(str(item) for item in value.get("source_observation_ids", ())),
                        tuple(str(item) for item in value.get("child_observation_ids", ())),
                        datetime.fromisoformat(str(value["first_observed_at"])),
                        datetime.fromisoformat(str(value["last_observed_at"])),
                        datetime.fromisoformat(str(value["available_at"])),
                        int(value["context_version"]) if value.get("context_version") is not None else None,
                    ))
                projections.append((str(timeframe), tuple(decoded_values)))
            raw_scores = raw.get("score_history", ())
            if not isinstance(raw_scores, (list, tuple)):
                raise ValueError("invalid synthetic score history")
            scores = tuple(
                SyntheticScoreSnapshot(
                    int(value["context_version"]) if value.get("context_version") is not None else None,
                    datetime.fromisoformat(str(value["observed_at"])),
                    datetime.fromisoformat(str(value["available_at"])),
                    tuple(str(item) for item in value.get("source_observation_ids", ())),
                    float(value["long_score"]) if value.get("long_score") is not None else None,
                    float(value["short_score"]) if value.get("short_score") is not None else None,
                )
                for value in raw_scores if isinstance(value, Mapping)
            )
            raw_cursors = raw.get("source_cursors", {})
            if not isinstance(raw_cursors, Mapping):
                raise ValueError("invalid interaction source cursors")
            return LocationInteraction(
                str(raw["interaction_id"]), str(raw["timeframe"]),
                str(raw["location_id"]), str(raw["location_alias"]),
                float(raw["reference_level"]), int(raw["reference_generation"]),
                str(raw["source_observation_id"]),
                datetime.fromisoformat(str(raw["created_at"])),
                InteractionStatus(str(raw["status"])), candles,
                InteractionCloseReason(str(close_reason)) if close_reason is not None else None,
                datetime.fromisoformat(str(closed_at)) if closed_at is not None else None,
                str(raw.get("response_status", "PENDING_RESPONSE")),
                float(raw["band_lower"]) if raw.get("band_lower") is not None else None,
                float(raw["band_upper"]) if raw.get("band_upper") is not None else None,
                tuple(decode_hypothesis(value) for value in raw_hypotheses),
                str(raw["pd_role"]) if raw.get("pd_role") is not None else None,
                str(raw["pd_role_transition_candle_id"]) if raw.get("pd_role_transition_candle_id") is not None else None,
                datetime.fromisoformat(str(raw["pd_role_transition_available_at"])) if raw.get("pd_role_transition_available_at") is not None else None,
                str(raw.get("location_type", "session_reference")),
                dict(raw.get("reference_provenance", {})),
                tuple(sorted(projections)),
                scores,
                tuple(sorted((str(key), str(value)) for key, value in raw_cursors.items())),
            )

        active = snapshot.get("active", ())
        closed = snapshot.get("closed", ())
        processed = snapshot.get("processed_observations", ())
        if not isinstance(active, (list, tuple)) or not isinstance(closed, (list, tuple)) or not isinstance(processed, (list, tuple)):
            raise ValueError("invalid interaction snapshot collections")
        for item in active:
            interaction = decode(item)
            if interaction.status is not InteractionStatus.ACTIVE:
                raise ValueError("active interaction snapshot has terminal status")
            key = f"{interaction.location_alias}:{interaction.timeframe}"
            if key in detector._active:
                raise ValueError("duplicate active interaction snapshot")
            detector._active[f"{interaction.location_alias}:{interaction.timeframe}"] = interaction
        for item in closed:
            interaction = decode(item)
            if interaction.status is not InteractionStatus.ENDED:
                raise ValueError("closed interaction snapshot has active status")
            detector._closed.append(interaction)
        if any(not isinstance(item, str) or not item for item in processed):
            raise ValueError("invalid processed interaction observation ID")
        if len(processed) != len(set(processed)):
            raise ValueError("duplicate processed interaction observation ID")
        detector._processed_observations = set(processed)
        return detector

    @property
    def closed_interactions(self) -> tuple[LocationInteraction, ...]:
        return tuple(self._closed)

    @property
    def active_interactions(self) -> tuple[LocationInteraction, ...]:
        return tuple(self._active.values())

    def update_response(self, interaction_id: str, response_status: str, hypothesis_id: str | None = None, *, state: HypothesisState | None = None) -> LocationInteraction | None:
        def update(item: LocationInteraction) -> LocationInteraction:
            if hypothesis_id is None:
                return replace(item, response_status=response_status)
            states = {value.hypothesis_id: value for value in item.hypothesis_states}
            current = states.get(hypothesis_id)
            if current is not None and current.status in {"CONFIRMED", "REJECTED", "INVALIDATED", "EXPIRED"}:
                if state is None or state.status != current.status:
                    return item
            states[hypothesis_id] = state or HypothesisState(hypothesis_id, "response_v1", response_status)
            return replace(item, hypothesis_states=tuple(states[key] for key in sorted(states)))
        for key, interaction in self._active.items():
            if interaction.interaction_id == interaction_id:
                updated = update(interaction)
                self._active[key] = updated
                return updated
        for index, interaction in enumerate(self._closed):
            if interaction.interaction_id == interaction_id:
                updated = update(interaction)
                self._closed[index] = updated
                return updated
        return None

    def update_prior_day_role(
        self, interaction_id: str, role: str, candle_id: str, available_at: datetime,
    ) -> LocationInteraction | None:
        if role not in {"SUPPORT", "RESISTANCE"}:
            raise ValueError("invalid prior-day role")
        for key, interaction in self._active.items():
            if interaction.interaction_id == interaction_id:
                updated = replace(
                    interaction, pd_role=role,
                    pd_role_transition_candle_id=candle_id,
                    pd_role_transition_available_at=available_at,
                    response_status="PENDING_RESPONSE",
                    hypothesis_states=(),
                )
                self._active[key] = updated
                return updated
        return None


__all__ = [
    "EvidenceLineage", "HypothesisState", "InteractionCandle", "InteractionCloseReason", "InteractionDetector", "ProjectionSnapshot", "SyntheticScoreSnapshot",
    "InteractionStatus", "LocationInteraction",
]
