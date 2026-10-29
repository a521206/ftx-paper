"""Causal evidence assembly for orderflow policy evaluation."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING, cast

from ftx_paper.domain.decision_context import DecisionContext

from .orderflow_contract import OrderflowContext, OrderflowObservation, stable_orderflow_id
from .orderflow_helpers import observation_number, volume_average
from .orderflow_location import LocationObservation, LocationRecord, LocationRegistry, is_diagnostic_location
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
        atr = observation.context.frozen_atr if observation.context is not None else None
        if atr is None:
            self._confirmation_score_cache[observation.observation_id] = None
            return None
        session_id = observation.context.session_id if observation.context else None
        all_observations = tuple(
            item for item in self.observations
            if item.available_at <= observation.available_at
            and (
                session_id is None
                or item.context is not None and item.context.session_id == session_id
            )
        )

        def prior_items(item: OrderflowObservation) -> list[OrderflowObservation]:
            return [
                candidate for candidate in all_observations
                if candidate.timeframe == item.timeframe
                and self._interval_end(candidate) < item.observed_at
            ][-20:]

        children = (
            tuple(
                item for item in all_observations
                if item.timeframe == "1m"
                and self._interval_end(item) <= observation.available_at
            )[-2:]
            if observation.timeframe == "5m" else ()
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
        self._confirmation_score_cache[observation.observation_id] = scores
        return scores

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
            from .orderflow_vwap import publish_vwap_locations
            vwap_locations = publish_vwap_locations(
                location_registry,
                vwap_measurements, atr=atr,
                band_fraction=self.vwap_provider.config.band_atr_fraction,
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
