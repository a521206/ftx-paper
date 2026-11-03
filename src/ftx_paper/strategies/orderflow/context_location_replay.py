"""Causal Phase 4 context and location replay boundary.

This module owns chronological orchestration only.  Context publications and
prior-session evidence are supplied by adapters; no presentation snapshot is
used to reconstruct a causal fact.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
import math
from typing import Any

from .evidence_publication import EvidencePublication
from .hierarchy import MarketContext
from .location_observation import LocationObservation
from .market_observation import MARKET_CLOSE_TIME, MarketObservation, observe_market
from .orderflow_common import Aggregate
from .timestamps import coerce_timestamp, nifty_label, serialize_utc, session_minute
from .state_machine_types import LocationRecord, LocationRegistry, MarketContextStore


def _context_as_of(
    store: MarketContextStore,
    available_at: datetime,
    context_stream_id: str | None,
) -> MarketContext | None:
    if context_stream_id is None:
        return None
    return store.latest_as_of(context_stream_id, available_at)


def _location_observation(
    aggregate: Aggregate,
    record: LocationRecord,
    *,
    source_fingerprint: str,
    child_observation_ids: tuple[str, ...] = (),
    available_at: datetime | str | None = None,
) -> LocationObservation:
    close_relation = (
        "ABOVE" if aggregate.close > record.band_high
        else "BELOW" if aggregate.close < record.band_low
        else "INSIDE_BAND"
    )
    observation_id = (
        f"{source_fingerprint[:16]}:{nifty_label(aggregate.time)}:"
        f"{aggregate.timeframe}:{record.location_id or f'{record.alias}:{record.generation}'}"
    )
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
        source_aggregate_id=nifty_label(aggregate.time),
        observed_at=coerce_timestamp(aggregate.end_time),
        available_at=coerce_timestamp(available_at or aggregate.available_at),
        method=record.source_method or "replay_location_band_intersection",
        observation_id=observation_id,
        timeframe=aggregate.timeframe,
        child_observation_ids=child_observation_ids,
        source_observation_id=record.source_observation_id,
        reference_source_aggregate_id=record.source_aggregate_id,
        reference_source_timeframe=record.timeframe or aggregate.timeframe,
    )


def _vwap_publication_at(record: LocationRecord, aggregates: Sequence[Aggregate]) -> datetime | None:
    """Return the source aggregate's canonical VWAP publication time."""
    if record.alias != "VWAP" or record.source_aggregate_id is None:
        return None
    for aggregate in aggregates:
        if nifty_label(aggregate.time) == record.source_aggregate_id:
            return aggregate.available_at
    return None


def _location_publication_at(
    record: LocationRecord,
    aggregates: Mapping[str, Sequence[Aggregate]],
) -> datetime | None:
    if record.alias != "VWAP":
        return record.available_at
    timeframe = record.timeframe
    if timeframe not in aggregates:
        return None
    return _vwap_publication_at(record, aggregates[timeframe])


def _context_fact(
    aggregate: Aggregate,
    context: MarketContext | None,
    observations: Sequence[LocationObservation],
    unavailable: Sequence[str],
) -> dict[str, Any]:
    identity = f"context-location:{aggregate.timeframe}:{nifty_label(aggregate.time)}"
    context_value = None
    if context is not None:
        context_value = {
            "context_id": context.context_id,
            "version": context.version,
            "location": context.location,
            "market_state": context.market_state,
            "available_at": serialize_utc(coerce_timestamp(context.available_at)),
            "parent_context_id": context.parent_context_id,
            "definition_version": context.definition_version,
            "source_provenance": [
                {
                    key: serialize_utc(value) if isinstance(value, datetime) else value
                    for key, value in item.items()
                }
                for item in context.source_provenance
            ],
        }
    return {
        "kind": "context_location_observed",
        "occurred_at": serialize_utc(coerce_timestamp(aggregate.end_time)),
        "observed_at": serialize_utc(coerce_timestamp(aggregate.end_time)),
        "available_at": serialize_utc(coerce_timestamp(aggregate.available_at)),
        "identity": identity,
        "causal_parent_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
        "values": {
            "timeframe": aggregate.timeframe,
            "source_observation_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
            "context": context_value,
            "locations": [item.to_dict() for item in observations],
        },
        "unavailable": list(unavailable),
    }


@dataclass(frozen=True, slots=True)
class ContextLocationReplay:
    """Causal context/location facts emitted by one replay."""

    context_location: tuple[dict[str, Any], ...]


def attach_context_publications(
    replay: ContextLocationReplay,
    publications: Sequence[Mapping[str, Any]],
) -> ContextLocationReplay:
    """Attach owner-published contexts without replaying market aggregates."""
    store = MarketContextStore()
    context_ids: set[str] = set()
    for publication in publications:
        value = publication.get("context")
        if isinstance(value, MarketContext):
            context = value
        elif isinstance(value, Mapping):
            context = MarketContext(
                context_id=str(value["context_id"]), version=str(value["version"]),
                timestamp=coerce_timestamp(value["timestamp"]),
                available_at=coerce_timestamp(value["available_at"]),
                location=value.get("location"), market_state=value.get("market_state"),
                long_score=value.get("long_score"), short_score=value.get("short_score"),
                evidence=tuple(value.get("evidence", ())),
                source_timeframe=str(value.get("source_timeframe", "5m")),
                parent_context_id=value.get("parent_context_id"),
                definition_version=str(value.get("definition_version", "1")),
                source_provenance=tuple(value.get("source_provenance", ())),
            )
        else:
            continue
        store.publish(context)
        context_ids.add(context.context_id)
    stream_id = next(iter(context_ids), None) if len(context_ids) == 1 else None
    facts: list[dict[str, Any]] = []
    for raw in replay.context_location:
        fact = {**raw, "values": dict(raw.get("values", {}))}
        context = _context_as_of(store, coerce_timestamp(str(fact["available_at"])), stream_id)
        if context is None:
            facts.append(fact)
            continue
        fact["values"]["context"] = {
            "context_id": context.context_id, "version": context.version,
            "location": context.location, "market_state": context.market_state,
            "available_at": serialize_utc(coerce_timestamp(context.available_at)),
            "parent_context_id": context.parent_context_id,
            "definition_version": context.definition_version,
            "source_provenance": [
                {
                    key: serialize_utc(value) if isinstance(value, datetime) else value
                    for key, value in item.items()
                }
                for item in context.source_provenance
            ],
        }
        fact["unavailable"] = [
            reason for reason in fact.get("unavailable", ())
            if reason != "missing_context"
        ]
        facts.append(fact)
    return ContextLocationReplay(tuple(facts))


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
                session_minute(session_date, row["time"])
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
                coerce_timestamp(str(item["time"])), coerce_timestamp(str(item["end_time"])), coerce_timestamp(str(item["available_at"])),
                timeframe, int(item["source_bar_count"]), numbers["open"], numbers["high"],
                numbers["low"], numbers["close"], int(item["volume"]), str(item["symbol"]),
                str(item["expiry"]), str(item["session_date"]),
                str(item["source_fingerprint"]),
            )
            if coerce_timestamp(aggregate.available_at) < coerce_timestamp(aggregate.end_time):
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
    store = MarketContextStore()
    for context in evidence.contexts:
        store.publish(context)
    context_stream_id = evidence.context_stream_id
    registry = LocationRegistry(evidence.location_stream_id or f"orderflow:{session_date}")
    for location in evidence.locations:
        registry.register(location)
    aggregates = sorted(
        (
            item for timeframe in ("1m", "5m")
            for item in market.aggregates[timeframe]
            if item.session_date == session_date
            and not (
                item.timeframe == "1m"
                and session_minute(session_date, item.time)[-5:] >= MARKET_CLOSE_TIME
            )
            and (replay_clock is None or coerce_timestamp(item.available_at) <= coerce_timestamp(replay_clock))
        ),
            key=lambda item: (coerce_timestamp(item.available_at), item.timeframe, item.time),
    )
    facts: list[dict[str, Any]] = []
    for aggregate in aggregates:
        available5 = tuple(
            item for item in market.aggregates["5m"]
            if item.session_date == session_date
            and coerce_timestamp(item.available_at) <= coerce_timestamp(aggregate.available_at)
        )
        # Match StateMachine.handle_interactions: VWAP publication is deferred
        # until at least one completed 5m aggregate is available.
        vwap_published = bool(available5)
        unavailable: list[str] = []
        unavailable.extend(
            reason for reason in evidence.unavailable
            if reason != "atr_not_yet_available"
        )
        if evidence.atr.value is None:
            unavailable.append(evidence.atr.unavailable_reason or "missing_atr")
        elif evidence.atr.available_at is not None and coerce_timestamp(aggregate.available_at) < coerce_timestamp(evidence.atr.available_at):
            unavailable.append("atr_not_yet_available")
        if context_stream_id is None:
            context_ids = {item.context_id for item in evidence.contexts}
            context_stream_id = next(iter(context_ids), None) if len(context_ids) == 1 else None
        context = _context_as_of(store, aggregate.available_at, context_stream_id)
        if context is None:
            unavailable.append("missing_context")
        if (
            evidence.prior_value.reference is None
            or evidence.prior_value.available_at is None
            or evidence.prior_value.available_at > coerce_timestamp(aggregate.available_at)
        ):
            unavailable.append("missing_prior_session_value_evidence")
        available_locations = tuple(
            record for record in registry.available_as_of(aggregate.available_at)
            if (
                (not record.timeframe or record.timeframe == aggregate.timeframe)
                and (
                record.alias != "VWAP"
                or (
                    vwap_published
                    and
                    (publication_at := _location_publication_at(record, market.aggregates))
                    is not None
                    and coerce_timestamp(publication_at) <= coerce_timestamp(aggregate.available_at)
                )
                )
            )
        )
        if not available_locations:
            unavailable.append("missing_location_evidence")
        children = _causal_children(aggregate, market.aggregates["1m"])
        location_values = (
            tuple(
                observation
                for record in available_locations
                for observation in (
                    (
                        tuple(
                            _location_observation(
                                item,
                                record,
                                source_fingerprint=market.source_fingerprint,
                                available_at=max(
                                    coerce_timestamp(item.available_at),
                                    coerce_timestamp(record.available_at),
                                ),
                            )
                            for item in children
                        )
                        + (
                            _location_observation(
                                aggregate,
                                record,
                                source_fingerprint=market.source_fingerprint,
                                available_at=(
                                    _location_publication_at(record, market.aggregates)
                                    if record.alias == "VWAP" else None
                                ),
                                child_observation_ids=tuple(
                                    f"{market.source_fingerprint[:16]}:{nifty_label(item.time)}:1m:"
                                    f"{record.location_id or f'{record.alias}:{record.generation}'}"
                                    for item in children
                                ),
                            ),
                        )
                    )
                    if aggregate.timeframe == "5m" and len(children) == 5
                    else (_location_observation(
                        aggregate,
                        record,
                        source_fingerprint=market.source_fingerprint,
                    ),)
                )
            )
            if aggregate.timeframe != "5m" or len(children) == 5
            else ()
        )
        # A malformed or misaligned 5m aggregate is not valid evidence even
        # when the context and location registry are available.  Keep the
        # fact explicit so consumers cannot mistake an empty observation set
        # for a successfully observed bar.
        if not location_values:
            unavailable.append("missing_context")
        facts.append(_context_fact(aggregate, context, location_values, unavailable))
    return ContextLocationReplay(tuple(facts))


def _causal_children(
    aggregate: Aggregate,
    one_minute: Sequence[Aggregate],
) -> tuple[Aggregate, ...]:
    if aggregate.timeframe != "5m":
        return ()
    children = tuple(
        item for item in one_minute
        if aggregate.time <= item.time <= aggregate.end_time
        and coerce_timestamp(item.available_at) <= coerce_timestamp(aggregate.available_at)
    )
    expected = tuple(
        nifty_label(aggregate.time + timedelta(minutes=offset))
        for offset in range(5)
    )
    return children if tuple(nifty_label(item.time) for item in children) == expected else ()


__all__ = [
    "ContextLocationReplay",
    "attach_context_publications",
    "replay_context_location",
    "replay_context_location_artifact",
]
