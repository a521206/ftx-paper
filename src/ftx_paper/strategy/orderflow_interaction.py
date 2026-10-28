"""Completed-candle location interactions with frozen reference provenance."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum

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

    def __post_init__(self) -> None:
        if not self.hypothesis_id.strip() or not self.policy_version.strip():
            raise ValueError("hypothesis identity is required")
        if self.status not in {"PENDING_RESPONSE", "UNAVAILABLE", "CONFIRMED", "REJECTED", "INVALIDATED", "EXPIRED"}:
            raise ValueError("invalid hypothesis status")
        if self.direction is not None and self.direction not in {"long", "short"}:
            raise ValueError("invalid hypothesis direction")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("hypothesis evidence IDs must be unique")


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

    def __post_init__(self) -> None:
        if not self.interaction_id.strip() or not self.location_id.strip():
            raise ValueError("interaction identity is required")
        if self.status is InteractionStatus.ACTIVE and self.close_reason is not None:
            raise ValueError("active interaction cannot have a close reason")
        if self.status is InteractionStatus.ENDED and self.close_reason is None:
            raise ValueError("ended interaction requires a close reason")
        if self.response_status not in {"PENDING", "PENDING_RESPONSE", "CONFIRMED", "INVALIDATED", "EXPIRED"}:
            raise ValueError("invalid interaction response status")


class InteractionDetector:
    """One detector per timeframe; references are never mutated after creation."""

    def __init__(self, timeframe: str) -> None:
        if not timeframe.strip():
            raise ValueError("interaction timeframe is required")
        self.timeframe = timeframe
        self._active: dict[str, LocationInteraction] = {}
        self._closed: list[LocationInteraction] = []
        self._processed_observations: set[str] = set()

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
            return self._active.get(f"{location.location_id}:{self.timeframe}")
        self._processed_observations.add(processed_id)
        if observation.context is None or observation.context.availability_status != "available":
            return None
        close = observation.features.get("close")
        high = observation.features.get("high", close)
        low = observation.features.get("low", close)
        if (
            not isinstance(close, (int, float)) or isinstance(close, bool)
            or not isinstance(high, (int, float)) or isinstance(high, bool)
            or not isinstance(low, (int, float)) or isinstance(low, bool)
        ):
            return None
        close_value, high_value, low_value = float(close), float(high), float(low)
        key = f"{location.location_id}:{self.timeframe}"
        interaction = self._active.get(key)
        replacement = next(
            (item for item in self._active.values()
             if item.timeframe == self.timeframe and item.location_alias == location.alias
              and (item.location_id != location.location_id
                   or item.reference_generation != location.generation
                   or item.reference_level != location.price
                   or item.band_lower != location.band.lower
                   or item.band_upper != location.band.upper)),
            None,
        )
        if replacement is not None:
            self._active.pop(f"{replacement.location_id}:{replacement.timeframe}", None)
            self._closed.append(replace(
                replacement, status=InteractionStatus.ENDED,
                close_reason=InteractionCloseReason.REFERENCE_REPLACED,
                closed_at=observation.observed_at,
            ))
        candle = InteractionCandle(
            observation.observation_id, observation.timeframe,
            observation.observed_at, observation.available_at, close_value, high_value, low_value,
        )
        contact = (
            location.band.lower <= high_value and low_value <= location.band.upper
            if band_contact is None else band_contact
        )
        inside = location.band.lower <= high_value and low_value <= location.band.upper
        if interaction is None:
            if not contact:
                return None
            interaction = LocationInteraction(
                key, self.timeframe, location.location_id, location.alias,
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
            self._active[key] = interaction
            return interaction
        if not inside:
            previous_close = interaction.contact_candles[-1].close
            crossed = (
                (previous_close < interaction.reference_level < close_value)
                or (previous_close > interaction.reference_level > close_value)
            )
            if previous_close == interaction.reference_level:
                close_reason = InteractionCloseReason.DEPARTED
            elif crossed:
                close_reason = InteractionCloseReason.CROSS_BAND
            elif previous_close <= interaction.reference_level and close_value < interaction.reference_level:
                close_reason = InteractionCloseReason.SAME_SIDE_DEPARTURE
            elif previous_close >= interaction.reference_level and close_value > interaction.reference_level:
                close_reason = InteractionCloseReason.SAME_SIDE_DEPARTURE
            else:
                close_reason = InteractionCloseReason.OUTSIDE_BAND_DEPARTURE
            ended = replace(
                interaction, status=InteractionStatus.ENDED,
                close_reason=close_reason,
                closed_at=observation.observed_at,
                contact_candles=(*interaction.contact_candles, candle),
            )
            self._active.pop(key)
            self._closed.append(ended)
            return ended
        advanced = replace(interaction, contact_candles=(*interaction.contact_candles, candle))
        self._active[key] = advanced
        return advanced

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
                    )
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    return HypothesisState(str(value[0]), "response_v1", str(value[1]))
                raise ValueError("invalid hypothesis snapshot")

            raw_hypotheses = raw.get("hypothesis_states", ())
            if not isinstance(raw_hypotheses, (list, tuple)):
                raise ValueError("invalid hypothesis states")
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
            key = f"{interaction.location_id}:{interaction.timeframe}"
            if key in detector._active:
                raise ValueError("duplicate active interaction snapshot")
            detector._active[f"{interaction.location_id}:{interaction.timeframe}"] = interaction
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
    "HypothesisState", "InteractionCandle", "InteractionCloseReason", "InteractionDetector",
    "InteractionStatus", "LocationInteraction",
]
