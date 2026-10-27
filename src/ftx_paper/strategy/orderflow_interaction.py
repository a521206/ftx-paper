"""Completed-candle location interactions with frozen reference provenance."""

from __future__ import annotations

from dataclasses import dataclass, replace
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

    def observe(self, location: LocationRecord, observation: OrderflowObservation) -> LocationInteraction | None:
        if observation.timeframe != self.timeframe:
            return None
        if observation.observation_id in self._processed_observations:
            return self._active.get(f"{location.location_id}:{self.timeframe}")
        self._processed_observations.add(observation.observation_id)
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
                  or item.reference_level != location.price)),
            None,
        )
        if replacement is not None:
            self._active.pop(replacement.interaction_id, None)
            self._closed.append(replace(
                replacement, status=InteractionStatus.ENDED,
                close_reason=InteractionCloseReason.REFERENCE_REPLACED,
                closed_at=observation.observed_at,
            ))
        candle = InteractionCandle(
            observation.observation_id, observation.timeframe,
            observation.observed_at, observation.available_at, float(close),
        )
        inside = location.band.contains(float(close))
        if interaction is None:
            if not inside:
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

    @property
    def closed_interactions(self) -> tuple[LocationInteraction, ...]:
        return tuple(self._closed)


__all__ = [
    "InteractionCandle", "InteractionCloseReason", "InteractionDetector",
    "InteractionStatus", "LocationInteraction",
]
