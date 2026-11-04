"""Context and source publication for the shared orderflow clock."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from .contextual import build_context_scores
from .coordinator import OrderflowCoordinator, OrderflowInput
from .hierarchy import MarketContext
from .orderflow_common import Aggregate
from .state_machine_types import OrderflowState
from .timestamps import nifty_label


class PublicationOwner(Protocol):
    context: OrderflowState
    observation_owner: OrderflowCoordinator


class TransitionPublisher:
    """Publish causal context, source, and location facts for an engine."""

    def __init__(self, engine: PublicationOwner) -> None:
        self.engine = engine

    @property
    def context(self) -> OrderflowState:
        return self.engine.context

    def source_observations(self, aggregates: Sequence[Aggregate]) -> None:
        known = {
            str(fact.get("identity"))
            for fact in self.context.parity_streams.get("source_observation", ())
        }
        one_minute = self.engine.observation_owner.one_minute_aggregates
        for aggregate in aggregates:
            identity = f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}"
            if identity in known:
                continue
            children = tuple(
                child for child in one_minute
                if aggregate.timeframe == "5m"
                and aggregate.time <= child.time <= aggregate.end_time
                and child.available_at <= aggregate.available_at
            )
            self.context.record_parity_fact("source_observation", {
                "kind": "source_observation",
                "occurred_at": aggregate.end_time,
                "observed_at": aggregate.end_time,
                "available_at": aggregate.available_at,
                "sequence": len(self.context.parity_streams.get("source_observation", ())),
                "identity": identity,
                "causal_parent_id": (
                    f"aggregate:1m:{nifty_label(children[-1].time)}"
                    if children else None
                ),
                "values": {
                    "aggregate": aggregate.to_dict(),
                    "source_fingerprint": aggregate.source_fingerprint,
                },
                "unavailable": [],
                "lineage": [
                    f"aggregate:1m:{nifty_label(child.time)}" for child in children
                ],
            })
            known.add(identity)

    def context_location(self, item: OrderflowInput) -> None:
        ordinary = (
            self.context.market_contexts.resolve(self.context.market_contexts.ordinary_context)
            if self.context.market_contexts.ordinary_context is not None else None
        )
        observations_by_timeframe = {
            "1m": tuple(
                observation for observation in item.observations_1m
                if observation.timeframe == "1m"
                and observation.source_aggregate_id == nifty_label(item.one_minute.time)
                and observation.reference_source_timeframe in {None, "1m"}
            ),
        }
        aggregates = [(item.one_minute, observations_by_timeframe["1m"])]
        if item.five_minute is not None:
            observations_by_timeframe["5m"] = tuple(
                observation for observation in item.observations_5m
                if observation.timeframe == "5m"
                and observation.source_aggregate_id == nifty_label(item.five_minute.time)
            )
            aggregates.append((item.five_minute, observations_by_timeframe["5m"]))
        known = {
            str(fact.get("identity"))
            for fact in self.context.parity_streams.get("context_location", ())
        }
        for aggregate, observations in aggregates:
            identity = f"context-location:{aggregate.timeframe}:{nifty_label(aggregate.time)}"
            if identity in known:
                continue
            self.context.record_parity_fact("context_location", {
                "kind": "context_location_observed",
                "occurred_at": aggregate.end_time,
                "observed_at": aggregate.end_time,
                "available_at": aggregate.available_at,
                "identity": identity,
                "causal_parent_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
                "values": {
                    "timeframe": aggregate.timeframe,
                    "source_observation_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
                    "context": {
                        "context_id": ordinary.context_id,
                        "version": ordinary.version,
                        "location": ordinary.location,
                        "market_state": ordinary.market_state,
                        "available_at": ordinary.available_at,
                        "parent_context_id": ordinary.parent_context_id,
                        "definition_version": ordinary.definition_version,
                        "source_provenance": list(ordinary.source_provenance),
                    } if ordinary is not None else None,
                    "locations": [observation.to_dict() for observation in observations],
                },
                "unavailable": [] if ordinary is not None and observations else ["missing_context"],
            })
            known.add(identity)

    def ordinary_context(
        self, latest: Aggregate, *, market_state: str | None = None,
    ) -> None:
        scores = build_context_scores(
            latest,
            raw_rows=self.context.rows,
            aggregates=self.context.aggregates,
            atr=self.context.atr_by_session.get(latest.session_date),
        )
        long_score, short_score = scores if scores is not None else (None, None)
        context_id = f"5m-context-{latest.symbol}-{latest.session_date}-1m_5m"
        published = MarketContext(
            context_id=context_id,
            version=f"v{len(self.context.market_contexts.versions.get(context_id, ())) + 1}",
            timestamp=latest.time,
            available_at=latest.available_at,
            location=None,
            market_state=(
                self.context.market_state if market_state is None else market_state
            ),
            definition_version="1m_5m",
            source_timeframe="5m",
            long_score=long_score,
            short_score=short_score,
            evidence=({"completed_bar": True, "open": latest.open, "high": latest.high,
                       "low": latest.low, "close": latest.close},),
            source_provenance=({"source_candle_id": nifty_label(latest.time),
                                "available_at": latest.available_at},),
        )
        self.context.market_contexts.ordinary_context = self.context.publish_context(published)
        self.context.last_confirmation_context_time = latest.time


__all__ = ["TransitionPublisher"]
