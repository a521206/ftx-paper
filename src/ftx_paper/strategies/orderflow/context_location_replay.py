"""Causal Phase 4 context and location replay boundary.

This module owns chronological orchestration only.  Context publications and
prior-session evidence are supplied by adapters; no presentation snapshot is
used to reconstruct a causal fact.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Any
from zoneinfo import ZoneInfo

from .evidence_publication import EvidencePublication
from .hierarchy import MarketContext
from .location_observation import LocationObservation
from .market_observation import MarketObservation, observe_market
from .orderflow_common import Aggregate
from .runtime_boundary import CompletedObservation, ReplayInputAdapter
from .state_machine_types import LocationRecord, LocationRegistry, MarketContextStore


def _instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    return parsed


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _context_as_of(
    store: MarketContextStore,
    available_at: str,
    context_stream_id: str | None,
) -> MarketContext | None:
    if context_stream_id is None:
        return None
    return store.latest_as_of(context_stream_id, available_at)


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
    evidence: EvidencePublication,
) -> ContextLocationReplay:
    """Replay completed aggregates and emit point-in-time causal facts."""
    return _replay_market(
        observe_market(rows), session_date=session_date, evidence=evidence,
        replay_clock=max(
            (
                f"{session_date}T{str(row['time']).split('T')[-1][:5]}"
                for row in rows
            ),
            default="",
        ),
    )


def replay_context_location_artifact(
    payload: Mapping[str, Any],
    *,
    session_date: str,
    evidence: EvidencePublication,
    replay_clock: str | None = None,
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
        parsed: list[Aggregate] = []
        for item in raw_items:
            if not isinstance(item, Mapping):
                raise ValueError(f"Phase 3 artifact contains an invalid {timeframe} aggregate")
            if str(item["timeframe"]) != timeframe:
                raise ValueError(f"Phase 3 {timeframe} section contains a mismatched timeframe")
            numbers = {
                key: float(item[key]) for key in ("open", "high", "low", "close")
            }
            if any(not math.isfinite(value) for value in numbers.values()):
                raise ValueError(f"Phase 3 artifact contains non-finite {timeframe} prices")
            aggregate = Aggregate(
                str(item["time"]), str(item["end_time"]), str(item["available_at"]),
                timeframe, int(item["source_bar_count"]), numbers["open"], numbers["high"],
                numbers["low"], numbers["close"], int(item["volume"]), str(item["symbol"]),
                str(item["expiry"]), str(item["contract_type"]), str(item["session_date"]),
                str(item["source_fingerprint"]),
            )
            if aggregate.available_at < aggregate.end_time:
                raise ValueError("Phase 3 aggregate is available before completion")
            if timeframe == "5m" and aggregate.source_bar_count != 5:
                raise ValueError("Phase 3 5m aggregate must contain five source bars")
            parsed.append(aggregate)
        aggregates[timeframe] = tuple(parsed)
    source_fingerprint = str(payload.get("source_fingerprint", ""))
    if not source_fingerprint:
        raise ValueError("Phase 3 artifact is missing source_fingerprint")
    market = MarketObservation(aggregates, (), {}, source_fingerprint)
    return _replay_market(
        market, session_date=session_date, evidence=evidence,
        replay_clock=replay_clock,
    )


def _replay_market(
    market: MarketObservation,
    *,
    session_date: str,
    evidence: EvidencePublication,
    replay_clock: str | None,
) -> ContextLocationReplay:
    observations = _completed_observations(
        market.aggregates, session_date=session_date, replay_clock=replay_clock,
    )
    source = ReplayInputAdapter()
    source.process(observations)
    store = MarketContextStore()
    for context in evidence.contexts:
        store.publish(context)
    registry = LocationRegistry(evidence.location_stream_id or f"orderflow:{session_date}")
    for location in evidence.locations:
        registry.register(location)
    aggregates = sorted(
        (
            item for timeframe in ("1m", "5m")
            for item in market.aggregates[timeframe]
            if item.session_date == session_date
            and (replay_clock is None or item.available_at <= replay_clock)
        ),
        key=lambda item: (item.available_at, item.timeframe, item.time),
    )
    facts: list[dict[str, Any]] = []
    for aggregate in aggregates:
        unavailable: list[str] = []
        unavailable.extend(
            reason for reason in evidence.unavailable
            if reason != "atr_not_yet_available"
        )
        if evidence.atr.value is None:
            unavailable.append(evidence.atr.unavailable_reason or "missing_atr")
        elif evidence.atr.available_at is not None and aggregate.available_at < evidence.atr.available_at:
            unavailable.append("atr_not_yet_available")
        opening = registry.resolve_as_of("OR_LOW", aggregate.available_at)
        if opening is None:
            unavailable.append(
                "missing_opening_range"
                if registry.resolve("OR_LOW") is None
                else "opening_range_not_yet_available"
            )
        elif aggregate.available_at < opening.available_at:
            unavailable.append("opening_range_not_yet_available")
        context_stream_id = evidence.context_stream_id
        if context_stream_id is None:
            context_ids = {item.context_id for item in evidence.contexts}
            context_stream_id = next(iter(context_ids), None) if len(context_ids) == 1 else None
        context = _context_as_of(store, aggregate.available_at, context_stream_id)
        if context is None:
            unavailable.append("missing_context")
        if (
            evidence.prior_value.reference is None
            or evidence.prior_value.available_at is None
            or _timestamp(evidence.prior_value.available_at) > _timestamp(_instant(aggregate.available_at))
        ):
            unavailable.append("missing_prior_session_value_evidence")
        available_locations = tuple(
            record for record in registry.available_as_of(aggregate.available_at)
            if not record.timeframe or record.timeframe == aggregate.timeframe
        )
        if not available_locations:
            unavailable.append("missing_location_evidence")
        children = _causal_children(aggregate, market.aggregates["1m"])
        child_ids = tuple(f"aggregate:1m:{item.time}" for item in children)
        location_values = (
            tuple(
                _location_observation(aggregate, record, child_observation_ids=child_ids)
                for record in available_locations
            )
            if aggregate.timeframe != "5m" or len(children) == 5
            else ()
        )
        facts.append(_context_fact(aggregate, context, location_values, unavailable))
    lineage_by_identity = {item.identity: list(item.lineage) for item in observations}
    source_facts = []
    for event in source.state.events:
        fact = event.to_dict()
        fact["lineage"] = lineage_by_identity.get(event.identity, [])
        source_facts.append(fact)
    return ContextLocationReplay(tuple(source_facts), tuple(facts))


def _completed_observations(
    aggregates: Mapping[str, Sequence[Aggregate]], *, session_date: str, replay_clock: str | None,
) -> tuple[CompletedObservation, ...]:
    one_minute = aggregates["1m"]
    identities = {aggregate.time: f"aggregate:1m:{aggregate.time}" for aggregate in one_minute}
    result: list[CompletedObservation] = []
    for timeframe in ("1m", "5m"):
        for aggregate in aggregates[timeframe]:
            if (
                aggregate.session_date != session_date
                or (replay_clock is not None and aggregate.available_at > replay_clock)
            ):
                continue
            children = _causal_children(aggregate, one_minute)
            if timeframe == "5m" and len(children) != 5:
                continue
            lineage = tuple(identities[item.time] for item in children) if timeframe == "5m" else ()
            result.append(CompletedObservation(
                identity=f"aggregate:{timeframe}:{aggregate.time}",
                observed_at=_instant(aggregate.end_time),
                available_at=_instant(aggregate.available_at),
                values={"aggregate": aggregate.to_dict(), "source_fingerprint": aggregate.source_fingerprint},
                lineage=lineage,
            ))
    return tuple(sorted(result, key=lambda item: (item.available_at, item.identity)))


def _causal_children(
    aggregate: Aggregate,
    one_minute: Sequence[Aggregate],
) -> tuple[Aggregate, ...]:
    if aggregate.timeframe != "5m":
        return ()
    children = tuple(
        item for item in one_minute
        if aggregate.time <= item.time <= aggregate.end_time
        and item.available_at <= aggregate.available_at
    )
    expected = tuple(
        (_instant(aggregate.time) + timedelta(minutes=offset)).strftime("%Y-%m-%dT%H:%M")
        for offset in range(5)
    )
    return children if tuple(item.time for item in children) == expected else ()


__all__ = ["ContextLocationReplay", "replay_context_location", "replay_context_location_artifact"]
