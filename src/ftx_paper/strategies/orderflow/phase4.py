"""Narrow Phase-4 recording and context publication owners."""

from __future__ import annotations

from collections.abc import Sequence
from .coordinator import OrderflowCoordinator, OrderflowInput
from .hierarchy import MarketContext
from .orderflow_common import Aggregate
from .state_machine_types import OrderflowState
from .timestamps import nifty_label


class Phase4Recorder:
    """Record explicit source and context/location facts once per identity."""

    def __init__(
        self, context: OrderflowState, observation_owner: OrderflowCoordinator,
    ) -> None:
        self.context = context
        self.observation_owner = observation_owner

    def source_observations(self, aggregates: Sequence[Aggregate]) -> None:
        known = {
            str(fact.get("identity"))
            for fact in self.context.parity_streams.get("source_observation", ())
        }
        one_minute = self.observation_owner.one_minute_aggregates
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

    def context_location(self, item: OrderflowInput, *, timeframe: str) -> None:
        ordinary = (
            self.context.market_contexts.resolve(self.context.market_contexts.ordinary_context)
            if self.context.market_contexts.ordinary_context is not None else None
        )
        if timeframe == "1m":
            aggregate = item.one_minute
            observations = tuple(
                observation for observation in item.observations_1m
                if observation.timeframe == "1m"
                and observation.source_aggregate_id == nifty_label(aggregate.time)
                and observation.reference_source_timeframe in {None, "1m"}
            )
        elif timeframe == "5m" and item.five_minute is not None:
            aggregate = item.five_minute
            observations = tuple(
                observation for observation in item.observations_5m
                if observation.timeframe == "5m"
                and observation.source_aggregate_id == nifty_label(aggregate.time)
            )
        else:
            return
        known = {
            str(fact.get("identity"))
            for fact in self.context.parity_streams.get("context_location", ())
        }
        identity = f"context-location:{aggregate.timeframe}:{nifty_label(aggregate.time)}"
        if identity in known:
            return
        self.context.input_diagnostics.append({
            "kind": "phase4_context_recorded",
            "identity": identity,
            "timeframe": aggregate.timeframe,
            "aggregate_time": nifty_label(aggregate.time),
            "aggregate_available_at": aggregate.available_at,
            "context_id": ordinary.context_id if ordinary is not None else None,
            "context_version": ordinary.version if ordinary is not None else None,
            "context_market_state": ordinary.market_state if ordinary is not None else None,
            "current_market_state": self.context.market_state,
            "observation_count": len(observations),
        })
        self.context.record_parity_fact("context_location", {
            "kind": "context_location_observed",
            "occurred_at": aggregate.end_time,
            "observed_at": aggregate.end_time,
            "available_at": aggregate.available_at,
            "identity": identity,
            "causal_parent_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
            "values": {
                "timeframe": aggregate.timeframe,
                "source_observation_id": identity.replace("context-location:", "aggregate:"),
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


class ContextPublisher:
    """Publish the ordinary decision context consumed by permission policy."""

    def __init__(self, context: OrderflowState) -> None:
        self.context = context

    def ordinary_context(
        self,
        latest: Aggregate,
        scores: tuple[float | None, float | None] | None,
        *,
        market_state: str | None = None,
    ) -> None:
        state_before = self.context.market_state
        long_score, short_score = scores if scores is not None else (None, None)
        context_id = f"5m-context-{latest.symbol}-{latest.session_date}-1m_5m"
        published = MarketContext(
            context_id=context_id,
            version=f"v{len(self.context.market_contexts.versions.get(context_id, ())) + 1}",
            timestamp=latest.time,
            available_at=latest.available_at,
            location=None,
            market_state=self.context.market_state if market_state is None else market_state,
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
        self.context.input_diagnostics.append({
            "kind": "ordinary_context_publication",
            "context_id": published.context_id,
            "context_version": published.version,
            "context_available_at": published.available_at,
            "market_state_argument": market_state,
            "market_state_current": state_before,
            "market_state_before_publication": state_before,
            "market_state_after_publication": self.context.market_state,
            "published_market_state": published.market_state,
            "publication_matches_runtime_state": (
                published.market_state == self.context.market_state
            ),
            "publication_caller_phase": "permission",
            "phase4_snapshot_caller_phase": "context_location",
        })



__all__ = ["ContextPublisher", "Phase4Recorder"]
