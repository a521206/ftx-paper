"""Causal Phase 4 context and location replay boundary.

This module owns chronological orchestration only.  Context publications and
prior-session evidence are supplied by adapters; no presentation snapshot is
used to reconstruct a causal fact.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .hierarchy import MarketContext
from .location_observation import LocationObservation
from .market_observation import MarketObservation, observe_market
from .orderflow_common import Aggregate
from .runtime_boundary import CompletedObservation, ReplayInputAdapter
from .state_machine_types import LocationRecord, LocationRegistry, MarketContextStore
from .value_publication import ValuePublication
from .vwap_location import causal_vwap_prefix


def _instant(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _context_as_of(store: MarketContextStore, available_at: str) -> MarketContext | None:
    contexts = [
        context
        for versions in store.versions.values()
        for context in versions
        if context.available_at <= available_at
    ]
    return max(contexts, key=lambda context: (context.available_at, context.context_id, context.version), default=None)


def _location_observation(
    aggregate: Aggregate,
    record: LocationRecord,
    *,
    child_observation_ids: tuple[str, ...] = (),
) -> LocationObservation:
    close_relation = (
        "ABOVE_BAND" if aggregate.close > record.band_high
        else "BELOW_BAND" if aggregate.close < record.band_low
        else "INSIDE_BAND"
    )
    source_id = f"aggregate:{aggregate.timeframe}:{aggregate.time}"
    observation_id = f"location:{record.location_id}:{source_id}"
    return LocationObservation(
        location=record.alias,
        location_id=record.location_id,
        reference_generation=record.generation,
        reference=record.price,
        band_low=record.band_low,
        band_high=record.band_high,
        open=aggregate.open,
        high=aggregate.high,
        low=aggregate.low,
        close=aggregate.close,
        close_relation=close_relation,
        band_contact=record.band.intersects(high=aggregate.high, low=aggregate.low),
        source_aggregate_id=aggregate.time,
        observed_at=_timestamp(_instant(aggregate.end_time)),
        available_at=_timestamp(_instant(aggregate.available_at)),
        method=record.source_method or "registered_location",
        observation_id=observation_id,
        timeframe=aggregate.timeframe,
        child_observation_ids=child_observation_ids,
        source_observation_id=record.source_observation_id,
        reference_source_aggregate_id=record.source_aggregate_id,
        reference_source_timeframe=record.timeframe or None,
    )


def _context_fact(
    aggregate: Aggregate,
    context: MarketContext | None,
    observations: Sequence[LocationObservation],
    unavailable: Sequence[str],
) -> dict[str, Any]:
    identity = f"context-location:{aggregate.timeframe}:{aggregate.time}"
    return {
        "kind": "context_location_observed",
        "occurred_at": _timestamp(_instant(aggregate.end_time)),
        "observed_at": _timestamp(_instant(aggregate.end_time)),
        "available_at": _timestamp(_instant(aggregate.available_at)),
        "identity": identity,
        "causal_parent_id": f"aggregate:{aggregate.timeframe}:{aggregate.time}",
        "values": {
            "timeframe": aggregate.timeframe,
            "source_observation_id": f"aggregate:{aggregate.timeframe}:{aggregate.time}",
            "context": {
                "context_id": context.context_id,
                "version": context.version,
                "location": context.location,
                "market_state": context.market_state,
                "available_at": context.available_at,
                "parent_context_id": context.parent_context_id,
                "definition_version": context.definition_version,
                "source_provenance": list(context.source_provenance),
            } if context is not None else None,
            "locations": [item.to_dict() for item in observations],
        },
        "unavailable": list(unavailable),
    }


@dataclass(frozen=True, slots=True)
class ContextLocationReplay:
    """Causal source and context/location facts emitted by one replay."""

    source_observations: tuple[dict[str, Any], ...]
    context_location: tuple[dict[str, Any], ...]

    def parity_phase_facts(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "source_observation": [dict(item) for item in self.source_observations],
            "context_location": [dict(item) for item in self.context_location],
        }


def replay_context_location(
    rows: list[dict[str, Any]],
    *,
    session_date: str,
    contexts: Iterable[MarketContext] = (),
    prior_rows: Iterable[Mapping[str, Any]] = (),
    prior_value: ValuePublication | None = None,
    atr: float | None = None,
) -> ContextLocationReplay:
    """Replay completed aggregates and emit point-in-time causal facts."""
    return _replay_market(
        observe_market(rows), session_date=session_date, contexts=contexts,
        prior_rows=prior_rows, prior_value=prior_value, atr=atr,
    )


def replay_context_location_artifact(
    payload: Mapping[str, Any],
    *,
    session_date: str,
    contexts: Iterable[MarketContext] = (),
    prior_rows: Iterable[Mapping[str, Any]] = (),
    prior_value: ValuePublication | None = None,
    atr: float | None = None,
) -> ContextLocationReplay:
    """Replay the completed aggregate sections emitted by Phase 3."""
    raw_aggregates = payload.get("aggregates")
    if not isinstance(raw_aggregates, Mapping):
        raise ValueError("Phase 3 artifact is missing aggregates")
    aggregates: dict[str, tuple[Aggregate, ...]] = {}
    for timeframe in ("1m", "5m"):
        raw_items = raw_aggregates.get(timeframe)
        if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
            raise ValueError(f"Phase 3 artifact is missing {timeframe} aggregates")
        aggregates[timeframe] = tuple(
            Aggregate(
                str(item["time"]), str(item["end_time"]), str(item["available_at"]),
                str(item["timeframe"]), int(item["source_bar_count"]),
                float(item["open"]), float(item["high"]), float(item["low"]),
                float(item["close"]), int(item["volume"]), str(item["symbol"]),
                str(item["expiry"]), str(item["contract_type"]), str(item["session_date"]),
                str(item["source_fingerprint"]),
            )
            for item in raw_items if isinstance(item, Mapping)
        )
    source_fingerprint = str(payload.get("source_fingerprint", ""))
    if not source_fingerprint:
        raise ValueError("Phase 3 artifact is missing source_fingerprint")
    market = MarketObservation(aggregates, (), {}, source_fingerprint)
    return _replay_market(
        market, session_date=session_date, contexts=contexts,
        prior_rows=prior_rows, prior_value=prior_value, atr=atr,
    )


def _replay_market(
    market: MarketObservation,
    *,
    session_date: str,
    contexts: Iterable[MarketContext],
    prior_rows: Iterable[Mapping[str, Any]],
    prior_value: ValuePublication | None,
    atr: float | None,
) -> ContextLocationReplay:
    observations = _completed_observations(market.aggregates, session_date=session_date)
    source = ReplayInputAdapter()
    source.process(observations)
    store = MarketContextStore()
    for context in contexts:
        store.publish(context)
    registry = LocationRegistry(f"orderflow:{session_date}")
    aggregates = sorted(
        (item for timeframe in ("1m", "5m") for item in market.aggregates[timeframe] if item.session_date == session_date),
        key=lambda item: (item.available_at, item.timeframe, item.time),
    )
    first_five = next((item for item in aggregates if item.timeframe == "5m"), None)
    initialized = False
    running_high: float | None = None
    running_low: float | None = None
    vwap_prefix: dict[str, list[Aggregate]] = {"1m": [], "5m": []}
    facts: list[dict[str, Any]] = []
    for aggregate in aggregates:
        unavailable: list[str] = []
        if atr is None:
            unavailable.append("missing_atr")
        if first_five is None:
            unavailable.append("missing_opening_range")
        elif aggregate.available_at < first_five.available_at:
            unavailable.append("opening_range_not_yet_available")
        elif not initialized and aggregate is first_five and atr is not None:
            registry.register_opening_range(
                first_five.low, first_five.high, first_five.available_at,
                source_session=session_date, atr=atr,
            )
            prior = tuple(prior_rows)
            if prior:
                registry.register_prior_day_extremes(
                    min(float(item["low"]) for item in prior),
                    max(float(item["high"]) for item in prior),
                    first_five.available_at,
                    source_session=str(prior[0].get("session_date", "prior")), atr=atr,
                )
            initialized = True
        if aggregate.timeframe == "5m" and atr is not None:
            running_high = aggregate.high if running_high is None else max(running_high, aggregate.high)
            running_low = aggregate.low if running_low is None else min(running_low, aggregate.low)
            registry.publish_session_extreme("SESSION_HIGH", running_high, aggregate, atr=atr)
            registry.publish_session_extreme("SESSION_LOW", running_low, aggregate, atr=atr)
        if atr is not None:
            vwap_prefix[aggregate.timeframe].append(aggregate)
            measurement = causal_vwap_prefix(vwap_prefix[aggregate.timeframe], session_date=session_date)[-1]
            registry.publish_vwap(
                value=measurement.value, available_at=aggregate.available_at,
                source_session=session_date, source_aggregate_id=aggregate.time,
                source_observation_id=measurement.source_observation_id,
                timeframe=aggregate.timeframe, atr=atr,
            )
        context = _context_as_of(store, aggregate.available_at)
        if context is None:
            unavailable.append("missing_context")
        if (
            prior_value is None
            or prior_value.reference is None
            or prior_value.available_at is None
            or _timestamp(prior_value.available_at) > _timestamp(_instant(aggregate.available_at))
        ):
            unavailable.append("missing_prior_session_value_evidence")
        available_locations = tuple(
            record for record in registry.available_as_of(aggregate.available_at)
            if not record.timeframe or record.timeframe == aggregate.timeframe
        )
        if not available_locations:
            unavailable.append("missing_location_evidence")
        child_ids = tuple(
            f"aggregate:1m:{item.time}" for item in market.aggregates["1m"]
            if aggregate.timeframe == "5m" and aggregate.time <= item.time <= aggregate.end_time
        )
        location_values = tuple(_location_observation(aggregate, record, child_observation_ids=child_ids) for record in available_locations)
        facts.append(_context_fact(aggregate, context, location_values, unavailable))
    return ContextLocationReplay(tuple(item.to_dict() for item in source.state.events), tuple(facts))


def _completed_observations(aggregates: Mapping[str, Sequence[Aggregate]], *, session_date: str) -> tuple[CompletedObservation, ...]:
    one_minute = aggregates["1m"]
    identities = {aggregate.time: f"aggregate:1m:{aggregate.time}" for aggregate in one_minute}
    result: list[CompletedObservation] = []
    for timeframe in ("1m", "5m"):
        for aggregate in aggregates[timeframe]:
            if aggregate.session_date != session_date:
                continue
            lineage = tuple(
                identities[item.time] for item in one_minute
                if aggregate.time <= item.time <= aggregate.end_time
            ) if timeframe == "5m" else ()
            result.append(CompletedObservation(
                identity=f"aggregate:{timeframe}:{aggregate.time}",
                observed_at=_instant(aggregate.end_time),
                available_at=_instant(aggregate.available_at),
                values={"aggregate": aggregate.to_dict(), "source_fingerprint": aggregate.source_fingerprint},
                lineage=lineage,
            ))
    return tuple(sorted(result, key=lambda item: (item.available_at, item.identity)))


__all__ = ["ContextLocationReplay", "replay_context_location", "replay_context_location_artifact"]
