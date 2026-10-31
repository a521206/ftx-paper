"""Causal detection and advancement of registered-location interactions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .interactions import (
    candle_identity,
    CloseSide,
    InteractionStatus,
    InteractionSnapshot,
    LocationInteraction,
    LocationTemplate,
    ProjectionSnapshot,
)
from .location_observation import LocationObservation
from .orderflow_common import Aggregate
from .state_machine_types import LocationRecord, LocationRegistry


@dataclass(frozen=True, slots=True)
class InteractionTransition:
    interaction: InteractionSnapshot
    observation: LocationObservation | None = None
    created: bool = False
    departed: bool = False


class InteractionDetector:
    def __init__(self, registry: LocationRegistry) -> None:
        self.registry = registry
        self.interactions_by_timeframe = {"5m": {}, "1m": {}}

    @property
    def interactions(self) -> dict[str, LocationInteraction]:
        return {
            k: v
            for items in self.interactions_by_timeframe.values()
            for k, v in items.items()
        }

    def advance_projections(
        self, available1: list[Aggregate], *, atr: float | None = None
    ) -> list[InteractionTransition]:
        return (
            []
            if not available1
            else self._observe_timeframe(
                timeframe="1m", current=available1[-1], available=available1, atr=atr
            )
        )

    def observe_1m(
        self,
        *,
        current: Aggregate,
        available: list[Aggregate],
        atr: float | None,
        observations: tuple[LocationObservation, ...],
    ) -> list[InteractionTransition]:
        for observation in observations:
            if observation.timeframe != "1m":
                raise ValueError("1m detector input must contain 1m observations")
            if observation.source_aggregate_id != current.time:
                raise ValueError("1m observation does not match the evaluated aggregate")
            if observation.available_at > current.available_at:
                raise ValueError("1m observation is not causally available")
        by_key = {
            (item.location, item.reference_generation): item for item in observations
        }
        return self._observe_timeframe(
            timeframe="1m",
            current=current,
            available=available,
            atr=atr,
            allowed_locations=set(by_key),
            observations=by_key,
        )

    def interactions_for(self, timeframe: str) -> dict[str, LocationInteraction]:
        return self.interactions_by_timeframe[timeframe]

    def record_score(
        self,
        interaction_id: str,
        *,
        snapshot: ProjectionSnapshot,
        context_version: str,
        long_score: float | None,
        short_score: float | None,
    ) -> None:
        self._get(interaction_id).record_score(
            snapshot=snapshot,
            context_version=context_version,
            long_score=long_score,
            short_score=short_score,
        )

    def advance_session(
        self, interaction_id: str, *, available_at: str, carry_sessions: int
    ) -> InteractionSnapshot:
        interaction = self._get(interaction_id)
        interaction.sessions_carried += 1
        if interaction.sessions_carried > carry_sessions:
            interaction.end(
                InteractionStatus.ENDED,
                available_at=available_at,
                close_reason="SESSION_END",
            )
        return interaction.evaluation_snapshot()

    def end(
        self, interaction_id: str, *, available_at: str, close_reason: str
    ) -> InteractionSnapshot:
        interaction = self._get(interaction_id)
        interaction.end(
            InteractionStatus.ENDED,
            available_at=available_at,
            close_reason=close_reason,
        )
        return interaction.evaluation_snapshot()

    def observe(
        self,
        *,
        current: Aggregate,
        available5: list[Aggregate],
        available1: list[Aggregate],
        atr: float | None,
        observations5: tuple[LocationObservation, ...],
        observations1: tuple[LocationObservation, ...],
    ) -> list[InteractionTransition]:
        self._validate_observation_input(
            current, observations5=observations5, observations1=observations1
        )
        if not observations5:
            return []
        by_key = {
            (item.location, item.reference_generation): item for item in observations5
        }
        return self._observe_timeframe(
            timeframe="5m",
            current=current,
            available=available5,
            atr=atr,
            available1=available1,
            allowed_locations=set(by_key),
            observations=by_key,
        )

    @staticmethod
    def _validate_observation_input(
        current: Aggregate,
        *,
        observations5: tuple[LocationObservation, ...],
        observations1: tuple[LocationObservation, ...],
    ) -> None:
        observations_by_id = {
            item.observation_id: item
            for item in observations1
            if item.timeframe == "1m"
        }
        for observation in observations5:
            if observation.timeframe != "5m":
                raise ValueError("5m detector input must contain 5m observations")
            if observation.source_aggregate_id != current.time:
                raise ValueError(
                    "5m observation does not match the evaluated aggregate"
                )
            if observation.available_at > current.available_at:
                raise ValueError("5m observation is not causally available")
            if len(observation.child_observation_ids) != 5:
                raise ValueError("incomplete 5m observation cannot reach detector")
            children = tuple(observations_by_id.get(i) for i in observation.child_observation_ids)
            if any(child is None for child in children):
                raise ValueError("5m observation references missing 1m evidence")
            typed = tuple(child for child in children if child is not None)
            if any(
                child.location != observation.location
                or child.reference_generation != observation.reference_generation
                or child.available_at > current.available_at
                for child in typed
            ):
                raise ValueError(
                    "5m observation child evidence is not causally aligned"
                )
            parent_start = datetime.strptime(current.time, "%Y-%m-%dT%H:%M")
            expected = tuple(
                (parent_start + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M")
                for i in range(5)
            )
            if tuple(child.source_aggregate_id for child in typed) != expected:
                raise ValueError(
                    "5m observation children do not exactly cover the parent interval"
                )

    def _observe_timeframe(
        self,
        *,
        timeframe: str,
        current: Aggregate | None,
        available: list[Aggregate],
        atr: float | None,
        available1: list[Aggregate] | None = None,
        allowed_locations: set[tuple[str, int]] | None = None,
        observations: dict[tuple[str, int], LocationObservation] | None = None,
    ) -> list[InteractionTransition]:
        transitions: list[InteractionTransition] = []
        if not available:
            return transitions
        current = current or available[-1]
        interactions = self.interactions_by_timeframe[timeframe]
        first = next(
            (item for item in available if item.session_date == current.session_date),
            None,
        )
        for record in self._available_records(
            current.available_at, timeframe=timeframe
        ):
            if (
                first is not None
                and current.time == first.time
                and record.alias in {"OR_HIGH", "OR_LOW"}
            ):
                continue
            active = next(
                (
                    item
                    for item in interactions.values()
                    if str(item.location) == record.alias
                    and item.status is InteractionStatus.ACTIVE
                ),
                None,
            )
            observation = (
                observations.get((record.alias, record.generation))
                if observations
                else None
            )
            if active is not None:
                if active.creation_candle.time != current.time:
                    previous = active.terminal_candle_id
                    active.observe(current, observation=observation)
                    transitions.append(
                        InteractionTransition(
                            active.evaluation_snapshot(),
                            observation,
                            departed=active.terminal_candle_id != previous,
                        )
                    )
                continue
            if (
                allowed_locations is not None
                and (record.alias, record.generation) not in allowed_locations
            ):
                continue
            if any(
                str(item.location) == record.alias
                and item.reference_generation == record.generation
                and (
                    item.creation_candle.time == current.time
                    or item.terminal_candle_id == candle_identity(current)
                )
                for item in interactions.values()
            ):
                continue
            record_atr = record.frozen_atr if record.frozen_atr is not None else atr
            if record_atr is None:
                continue
            previous = next(
                (
                    item
                    for item in reversed(available)
                    if item.time < current.time
                    and item.session_date == current.session_date
                ),
                current,
            )
            interaction = LocationInteraction.create(
                instrument=current.symbol,
                session_date=current.session_date,
                location=record.alias,
                template_version="1",
                candle=current,
                reference_level=record.price,
                atr=record_atr,
                created_at=current.available_at,
                available_at=current.available_at,
                band=record.band,
                pre_contact_side=_fully_outside_side(previous, record.band),
                reference_generation=record.generation,
                reference_provenance={
                    "location_available_at": record.available_at,
                    "source_aggregate_id": record.source_aggregate_id,
                    "source_observation_id": record.source_observation_id,
                    "parent_extreme_generation": record.parent_extreme_generation,
                },
                observation=observation,
            )
            if interaction is None:
                continue
            interaction.seed_projections({timeframe: available})
            interaction.add_projection_candle(current) if not interaction.synthetic[
                timeframe
            ].candles else None
            interactions[interaction.interaction_id] = interaction
            transitions.append(
                InteractionTransition(
                    interaction.evaluation_snapshot(), observation, created=True
                )
            )
        return transitions

    def _available_records(
        self, available_at: str, *, timeframe: str
    ) -> tuple[LocationRecord, ...]:
        return tuple(
            record
            for record in self.registry.available_as_of(available_at)
            if self._is_supported_location(record.alias)
            and (not record.timeframe or record.timeframe == timeframe)
        )

    def _get(self, interaction_id: str) -> LocationInteraction:
        interaction = self.interactions.get(interaction_id)
        if interaction is None:
            raise KeyError(interaction_id)
        return interaction

    @staticmethod
    def _is_supported_location(alias: str) -> bool:
        try:
            LocationTemplate(alias.upper())
        except ValueError:
            return False
        return True


def _fully_outside_side(candle: Aggregate, band: Any) -> CloseSide | None:
    if candle.low > band.high:
        return CloseSide.ABOVE
    if candle.high < band.low:
        return CloseSide.BELOW
    return None
