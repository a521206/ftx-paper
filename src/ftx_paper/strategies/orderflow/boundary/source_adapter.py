"""Paper host adapters that project canonical Layer 1 observations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from ..market_observation import MarketObservation, observe_market
from ..runtime_boundary import CompletedObservation, LiveInputAdapter, ReplayInputAdapter


def _instant(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)


def completed_observations(rows: list[dict[str, Any]]) -> tuple[CompletedObservation, ...]:
    """Build boundary inputs from canonical aggregates, without re-aggregation."""
    observation = observe_market(rows)
    one_minute = observation.aggregates["1m"]
    identities = {
        aggregate.time: f"aggregate:1m:{aggregate.time}"
        for aggregate in one_minute
    }
    output: list[CompletedObservation] = []
    for timeframe in ("1m", "5m"):
        for aggregate in observation.aggregates[timeframe]:
            lineage = ()
            if timeframe == "5m":
                lineage = tuple(
                    identities[item.time]
                    for item in one_minute
                    if aggregate.time <= item.time <= aggregate.end_time
                )
            output.append(CompletedObservation(
                identity=f"aggregate:{timeframe}:{aggregate.time}",
                observed_at=_instant(aggregate.end_time),
                available_at=_instant(aggregate.available_at),
                values={"aggregate": aggregate.to_dict(), "source_fingerprint": observation.source_fingerprint},
                lineage=lineage,
            ))
    return tuple(sorted(output, key=lambda item: (item.available_at, item.identity)))


class LiveSourceAdapter:
    """Incremental live-style source adapter."""

    def __init__(self) -> None:
        self.boundary = LiveInputAdapter()

    def publish(self, observation: CompletedObservation):
        return self.boundary.on_completed_observation(observation)


class ReplaySourceAdapter:
    """Bounded replay source adapter."""

    def __init__(self) -> None:
        self.boundary = ReplayInputAdapter()

    def process(self, observations: Iterable[CompletedObservation]):
        return self.boundary.process(observations)


__all__ = ["completed_observations", "LiveSourceAdapter", "ReplaySourceAdapter", "MarketObservation"]
