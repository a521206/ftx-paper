"""Causal evidence assembly for orderflow policy evaluation."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING, cast

from ftx_paper.domain.decision_context import DecisionContext

from .orderflow_contract import OrderflowContext, OrderflowObservation, stable_orderflow_id
from .orderflow_helpers import observation_number, volume_average
from .orderflow_location import LocationObservation, LocationRecord, LocationRegistry, is_diagnostic_location
from .orderflow_scores import ScoreResult, score_aggregate
from .orderflow_vwap import MarketAggregate, VwapLocationProvider

if TYPE_CHECKING:
    from .orderflow_config import OrderflowConfig


class OrderflowEvidenceAssembler:
    """Prepare causally available observations and reconstruct confirmation scores."""

    def __init__(self, config: OrderflowConfig) -> None:
        self.config = config
        self.vwap_provider = VwapLocationProvider()
        self.observations: tuple[OrderflowObservation, ...] = ()
        self._observation_ids: frozenset[str] = frozenset()
        self._observation_indices: dict[str, int] = {}
        self._confirmation_score_cache: dict[str, ScoreResult | None] = {}
        self._observations_by_timeframe: dict[str, tuple[OrderflowObservation, ...]] = {}
        self._availability_by_timeframe: dict[str, tuple[datetime, ...]] = {}

    def confirmation_scores(
        self, observation: OrderflowObservation,
    ) -> tuple[float, float] | None:
        result = self.confirmation_score_result(observation)
        return result.scores if result is not None else None

    def confirmation_score_result(
        self, observation: OrderflowObservation,
    ) -> ScoreResult | None:
        cached = self._confirmation_score_cache.get(observation.observation_id)
        if observation.observation_id in self._confirmation_score_cache:
            return cached
        atr = observation.context.frozen_atr if observation.context is not None else None
        if atr is None:
            self._confirmation_score_cache[observation.observation_id] = None
            return None
        session_id = observation.context.session_id if observation.context else None
        by_timeframe = self._observations_by_timeframe or {
            timeframe: tuple(item for item in self.observations if item.timeframe == timeframe)
            for timeframe in {item.timeframe for item in self.observations}
        }
        visible_by_timeframe = {
            timeframe: values[:bisect_right(
                self._availability_by_timeframe.get(
                    timeframe, tuple(item.available_at for item in values),
                ),
                observation.available_at,
            )]
            for timeframe, values in by_timeframe.items()
        }
        def prior_items(item: OrderflowObservation) -> list[OrderflowObservation]:
            return [
                candidate for candidate in visible_by_timeframe.get(item.timeframe, ())
                if (
                    session_id is None
                    or candidate.context is not None and candidate.context.session_id == session_id
                )
                and (
                    candidate.observed_at < item.observed_at
                    if item.timeframe == "1m"
                    else self._interval_end(candidate) < item.observed_at
                )
            ][-20:]

        children = ()
        if observation.timeframe == "5m":
            parent_start = observation.interval_start or observation.observed_at
            parent_end = observation.interval_end or observation.observed_at
            children = tuple(
                item for item in visible_by_timeframe.get("1m", ())
                if (
                    session_id is None
                    or item.context is not None and item.context.session_id == session_id
                )
                and parent_start <= item.observed_at
                and self._interval_end(item) <= parent_end
                and self._interval_end(item) <= observation.available_at
            )
        child_scores: list[tuple[float, float]] = []
        for child in children:
            history = prior_items(child)
            prior = history[-1] if history else None
            try:
                score = score_aggregate(
                    child.features,
                    prior.features if prior is not None else None,
                    atr=atr,
                    volume_average=volume_average(history),
                )
            except ValueError:
                self._confirmation_score_cache[observation.observation_id] = None
                return None
            if score is None:
                self._confirmation_score_cache[observation.observation_id] = None
                return None
            child_scores.append(score)
        prior_parents = prior_items(observation)
        prior_parent = prior_parents[-1] if prior_parents else None
        try:
            scores = score_aggregate(
                observation.features,
                prior_parent.features if prior_parent is not None else None,
                atr=atr,
                child_scores=tuple(child_scores),
                volume_average=volume_average(prior_parents),
            )
        except ValueError:
            self._confirmation_score_cache[observation.observation_id] = None
            return None
        if scores is None:
            self._confirmation_score_cache[observation.observation_id] = None
            return None
        result = ScoreResult(
            scores=scores,
            prior_observation_id=prior_parent.observation_id if prior_parent else None,
            child_observation_ids=tuple(item.observation_id for item in children),
            causal_cutoff=observation.available_at,
        )
        self._confirmation_score_cache[observation.observation_id] = result
        return result

    @staticmethod
    def _interval_end(observation: OrderflowObservation) -> datetime:
        return observation.interval_end or observation.observed_at

    def clear_cache(self) -> None:
        self._confirmation_score_cache.clear()

    def prepare(
        self,
        orderflow: OrderflowContext,
        context: DecisionContext,
        location_registry: LocationRegistry,
    ) -> tuple[OrderflowObservation, ...]:
        incoming_observations = tuple(sorted(
            (*orderflow.observations_for("1m"), *orderflow.observations_for("5m")),
            key=lambda item: (item.available_at, item.timeframe, item.observed_at, item.observation_id),
        ))
        incoming_ids = frozenset(
            observation_id
            for item in incoming_observations
            for observation_id in (
                item.observation_id,
                *(location.observation_id for location in item.location_observations),
            )
        )
        if incoming_ids != self._observation_ids:
            self.observations = incoming_observations
            self._observations_by_timeframe = {
                timeframe: tuple(
                    item for item in incoming_observations if item.timeframe == timeframe
                )
                for timeframe in {item.timeframe for item in incoming_observations}
            }
            self._availability_by_timeframe = {
                timeframe: tuple(item.available_at for item in values)
                for timeframe, values in self._observations_by_timeframe.items()
            }
            self._observation_ids = incoming_ids
            self._confirmation_score_cache.clear()
            self._observation_indices = {
                item.observation_id: index
                for index, item in enumerate(incoming_observations)
            }
        observations = self.observations
        vwap_locations: tuple[LocationRecord, ...] = ()
        has_causal_5m = any(
            item.timeframe == "5m"
            and item.available_at <= min(context.as_of, orderflow.cutoff)
            for item in observations
        )
        if self.config.vwap_enabled and has_causal_5m:
            vwap_measurements = self.vwap_provider.publish(
                cast(tuple[MarketAggregate, ...], tuple(
                    item for item in observations
                    if item.context is not None
                    and (
                        item.timeframe != "5m"
                        or len(item.context.child_observation_ids) == 5
                    )
                )),
                cutoff=min(context.as_of, orderflow.cutoff),
            )
            atr = orderflow.daily_atr
            from .orderflow_vwap import publish_vwap_locations
            vwap_locations = publish_vwap_locations(
                location_registry,
                vwap_measurements, atr=atr,
            )
        first_parent_available = min(
            (item.available_at for item in observations if item.timeframe == "5m"),
            default=None,
        )
        locations_by_source: dict[str, list[LocationRecord]] = {}
        for location in vwap_locations:
            if location.source_observation_id is None or location.source_aggregate_id is None:
                continue
            if first_parent_available is not None and location.available_at < first_parent_available:
                continue
            locations_by_source.setdefault(location.source_aggregate_id, []).append(location)
        if locations_by_source:
            def numeric(item: OrderflowObservation, name: str) -> float:
                value = observation_number(item, name)
                if value is None:
                    raise ValueError(f"VWAP location requires numeric {name!r} evidence")
                return value

            enriched = list(observations)
            for source_id, locations in locations_by_source.items():
                index = self._observation_indices.get(source_id)
                if index is None:
                    continue
                item = enriched[index]
                existing_location_ids = {
                    location.observation_id for location in item.location_observations
                }
                enriched[index] = replace(
                    item,
                    location_observations=item.location_observations + tuple(
                        LocationObservation(
                            location.location_id, location.alias, location.generation, location.price,
                            location.band.low, location.band.high,
                            numeric(item, "open"), numeric(item, "high"),
                            numeric(item, "low"), numeric(item, "close"),
                            location.band.intersects(
                                high=numeric(item, "high"), low=numeric(item, "low"),
                            ),
                            item.observation_id, item.observed_at, item.available_at,
                            stable_orderflow_id("location_observation", item.observation_id, location.location_id),
                            "diagnostic_reference" if is_diagnostic_location(location.alias) else "session_reference",
                            location.source_observation_id,
                            timeframe=item.timeframe,
                            child_observation_ids=(
                                item.context.child_observation_ids
                                if item.timeframe == "5m" and item.context is not None else ()
                            ),
                            method="vwap_provider",
                            reference_source_aggregate_id=location.source_aggregate_id,
                            reference_source_timeframe=location.timeframe or item.timeframe,
                            frozen_atr=location.frozen_atr,
                        )
                        for location in locations
                        if stable_orderflow_id(
                            "location_observation", item.observation_id, location.location_id,
                        ) not in existing_location_ids
                    ),
                )
            observations = tuple(enriched)
            self.observations = observations
        return observations


__all__ = ["OrderflowEvidenceAssembler"]
