"""Completed-candle location interactions with frozen reference provenance."""

from __future__ import annotations

from dataclasses import dataclass, replace
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
        if not isinstance(close, (int, float)) or isinstance(close, bool):
            return None
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
            observation.observed_at, observation.available_at, float(close),
        )
        contact = location.band.contains(float(close)) if band_contact is None else band_contact
        inside = location.band.contains(float(close))
        if interaction is None:
            if not contact:
                return None
            interaction = LocationInteraction(
                key, self.timeframe, location.location_id, location.alias,
                location.price, location.generation, observation.observation_id,
                observation.observed_at, InteractionStatus.ACTIVE, (candle,),
                band_lower=location.band.lower, band_upper=location.band.upper,
            )
            self._active[key] = interaction
            return interaction
        if not inside:
            previous_close = interaction.contact_candles[-1].close
            crossed = (
                (previous_close < interaction.reference_level < float(close))
                or (previous_close > interaction.reference_level > float(close))
            )
            if previous_close == interaction.reference_level:
                close_reason = InteractionCloseReason.DEPARTED
            elif crossed:
                close_reason = InteractionCloseReason.CROSS_BAND
            elif previous_close <= interaction.reference_level and float(close) < interaction.reference_level:
                close_reason = InteractionCloseReason.SAME_SIDE_DEPARTURE
            elif previous_close >= interaction.reference_level and float(close) > interaction.reference_level:
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
                    }
                    for candle in item.contact_candles
                ],
                "close_reason": item.close_reason.value if item.close_reason else None,
                "closed_at": item.closed_at.isoformat() if item.closed_at else None,
                "response_status": item.response_status,
                "band_lower": item.band_lower,
                "band_upper": item.band_upper,
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
                ))
            candles = tuple(candles_list)
            closed_at = raw.get("closed_at")
            close_reason = raw.get("close_reason")
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

    def update_response(self, interaction_id: str, response_status: str) -> LocationInteraction | None:
        for key, interaction in self._active.items():
            if interaction.interaction_id == interaction_id:
                updated = replace(interaction, response_status=response_status)
                self._active[key] = updated
                return updated
        for index, interaction in enumerate(self._closed):
            if interaction.interaction_id == interaction_id:
                updated = replace(interaction, response_status=response_status)
                self._closed[index] = updated
                return updated
        return None


__all__ = [
    "InteractionCandle", "InteractionCloseReason", "InteractionDetector",
    "InteractionStatus", "LocationInteraction",
]
