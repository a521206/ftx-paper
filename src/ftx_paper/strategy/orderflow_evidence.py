"""Causal evidence assembly for orderflow policy evaluation."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

from ftx_paper.domain.decision_context import DecisionContext

from .orderflow_contract import OrderflowContext, OrderflowObservation, stable_orderflow_id
from .orderflow_helpers import observation_number, volume_average
from .orderflow_location import LocationObservation, LocationRecord, LocationRegistry
from .orderflow_scores import score_aggregate
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
        self._confirmation_score_cache: dict[str, tuple[float, float] | None] = {}

    def confirmation_scores(
        self, observation: OrderflowObservation,
    ) -> tuple[float, float] | None:
        cached = self._confirmation_score_cache.get(observation.observation_id)
        if observation.observation_id in self._confirmation_score_cache:
            return cached
        atr = next(
            (item.frozen_atr for item in observation.location_observations
             if item.frozen_atr is not None and item.frozen_atr > 0),
            None,
        )
        if atr is None:
            atr = next(
                (
                    location.frozen_atr
                    for item in self.observations
                    if item.observed_at <= observation.observed_at
                    for location in item.location_observations
                    if location.frozen_atr is not None and location.frozen_atr > 0
                ),
                None,
            )
        if atr is None:
            self._confirmation_score_cache[observation.observation_id] = None
            return None
        all_observations = tuple(
            item for item in self.observations
            if item.available_at <= observation.available_at
        )
        children_by_id = {item.observation_id: item for item in all_observations}
        child_ids = observation.context.child_observation_ids if observation.context else ()
        children = tuple(
            children_by_id[item_id] for item_id in child_ids if item_id in children_by_id
        )
        if observation.timeframe == "5m" and len(children) != len(child_ids):
            self._confirmation_score_cache[observation.observation_id] = None
            return None
        child_scores: list[tuple[float, float]] = []
        for child in children:
            prior = next(
                (item for item in reversed(all_observations)
                 if item.timeframe == child.timeframe and item.observed_at < child.observed_at),
                None,
            )
            prior_items = [
                item for item in all_observations
                if item.timeframe == child.timeframe and item.observed_at < child.observed_at
            ][-20:]
            try:
                score = score_aggregate(
                    child.features,
                    prior.features if prior is not None else None,
                    atr=atr,
                    volume_average=volume_average(prior_items),
                )
            except ValueError:
                self._confirmation_score_cache[observation.observation_id] = None
                return None
            if score is None:
                self._confirmation_score_cache[observation.observation_id] = None
                return None
            child_scores.append(score)
        prior_parent = next(
            (item for item in reversed(all_observations)
             if item.timeframe == observation.timeframe
             and item.observed_at < observation.observed_at),
            None,
        )
        prior_parents = [
            item for item in all_observations
            if item.timeframe == observation.timeframe
            and item.observed_at < observation.observed_at
        ][-20:]
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
        self._confirmation_score_cache[observation.observation_id] = scores
        return scores

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
                    and item.context.replay_provenance.get("producer") == "ftx-paper"
                )),
                cutoff=min(context.as_of, orderflow.cutoff),
            )
            atr = next(
                (
                    location.frozen_atr
                    for item in observations
                    for location in item.location_observations
                    if location.location_alias.upper() != "VWAP"
                    and location.frozen_atr is not None
                    and location.frozen_atr > 0
                ),
                None,
            )
            vwap_locations = location_registry.publish_measurements(
                vwap_measurements, atr=atr,
                band_fraction=self.vwap_provider.config.band_atr_fraction,
            )
        first_parent_available = min(
            (item.available_at for item in observations if item.timeframe == "5m"),
            default=None,
        )
        locations_by_source: dict[str, list[LocationRecord]] = {}
        for location in vwap_locations:
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
                enriched[index] = replace(
                    item,
                    location_observations=item.location_observations + tuple(
                        LocationObservation(
                            location.location_id, location.alias, location.generation, location.price,
                            location.band.lower, location.band.upper,
                            numeric(item, "open"), numeric(item, "high"),
                            numeric(item, "low"), numeric(item, "close"),
                            location.band.contains(numeric(item, "high"))
                            and location.band.contains(numeric(item, "low")),
                            item.observation_id, item.observed_at, item.available_at,
                            stable_orderflow_id("location_observation", item.observation_id, location.location_id),
                            location.location_type,
                            location.source_observation_id,
                            timeframe=item.timeframe,
                            child_observation_ids=(
                                item.context.child_observation_ids
                                if item.timeframe == "5m" and item.context is not None else ()
                            ),
                            method="vwap_provider",
                            reference_source_aggregate_id=location.source_aggregate_id,
                            reference_source_timeframe=location.timeframe or item.timeframe,
                        )
                        for location in locations
                    ),
                )
            observations = tuple(enriched)
            self.observations = observations
        return observations


__all__ = ["OrderflowEvidenceAssembler"]
