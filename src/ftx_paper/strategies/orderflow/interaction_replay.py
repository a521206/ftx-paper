"""Runtime-neutral causal interaction and response replay boundary."""

from __future__ import annotations
# pyright: reportArgumentType=false

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, is_dataclass, replace
from datetime import datetime
from enum import Enum
from typing import Any

from .context_location_replay import ContextLocationReplay
from .interaction_detector import InteractionDetector
from .location_observation import LocationObservation
from .orderflow_common import Aggregate, parse_timestamp, serialize_timestamp
from .timestamps import NIFTY_ZONE, serialize_utc, nifty_label
from .state_machine_types import LocationRegistry
from .templates import DEFAULT_TEMPLATES, TRADE_TEMPLATES, TradeTemplate
from .value_publication import ValuePublication
from .opportunity_replay_host import bar_events_to_facts, build_opportunity_coordinator
from .opening_gap_policy import OpeningGapPolicy
from .transition import OrderflowTransitionOwner
from .contextual import build_context_scores
from .hierarchy import MarketContext

@dataclass(frozen=True, slots=True)
class InteractionResponseReplay:
    facts: tuple[dict[str, Any], ...]
    opportunity_facts: tuple[dict[str, Any], ...] = ()
    formations: tuple[Any, ...] = ()
    regime_publications: tuple[tuple[str, Any], ...] = ()
    context_publications: tuple[dict[str, Any], ...] = ()
    execution_phase_facts: dict[str, tuple[dict[str, Any], ...]] = field(default_factory=dict)
    parity_layers: dict[str, tuple[dict[str, Any], ...]] = field(default_factory=dict)
    execution_diagnostics: tuple[dict[str, Any], ...] = ()

    def parity_phase_facts(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "interaction_response": [_jsonable(dict(item)) for item in self.facts],
            "opportunity_lifecycle": [
                _jsonable(dict(item)) for item in self.opportunity_facts
                if item.get("kind") == "lifecycle_formed"
            ],
        }


def _fact(kind: str, identity: str, parent: str | None, snapshot: Mapping[str, Any], *, unavailable: Sequence[str] = (), observed_at: str | None = None, available_at: str | None = None) -> dict[str, Any]:
    candle = snapshot.get("candles", [{}])[-1] if snapshot.get("candles") else {}
    observed = observed_at or candle.get("end_time") or snapshot.get("available_at")
    available = available_at or candle.get("available_at") or snapshot.get("available_at")
    if isinstance(observed, datetime):
        observed = serialize_utc(observed)
    if isinstance(available, datetime):
        available = serialize_utc(available)
    if not isinstance(observed, str) or not isinstance(available, str):
        raise ValueError("interaction parity fact timestamps are required")
    observed_timestamp = serialize_timestamp(parse_timestamp(observed))
    available_timestamp = serialize_timestamp(parse_timestamp(available))
    session_date = parse_timestamp(observed).astimezone(NIFTY_ZONE).date().isoformat()
    return {"kind": kind, "occurred_at": observed_timestamp,
             "observed_at": observed_timestamp, "available_at": available_timestamp,
             "causal_observed_at": observed_timestamp,
             "causal_available_at": available_timestamp,
             "policy_version": "orderflow-v1", "provenance": "causal_replay",
             "identity": identity, "causal_parent_id": parent,
             "session_date": session_date, "values": _jsonable(snapshot),
             "unavailable": list(unavailable)}


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return serialize_utc(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if is_dataclass(value):
        return _jsonable({item.name: getattr(value, item.name) for item in value.__dataclass_fields__.values()})
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable(item) for item in value), key=str)
    return value


def _publish_replaced_interactions(
    *,
    detector: InteractionDetector,
    registry: LocationRegistry,
    current: Aggregate,
    available5: Sequence[Aggregate],
    published_records: set[tuple[str, int]],
    facts: list[dict[str, Any]],
) -> None:
    """End active episodes when a newer location generation is published."""
    def source_aggregate(record: Any) -> Aggregate:
        matches = tuple(
            item for item in available5
            if nifty_label(item.time) == record.source_aggregate_id
        )
        if not matches:
            raise ValueError(
                f"location source aggregate is unavailable: {record.source_aggregate_id}"
            )
        return matches[0]

    available = tuple(registry.available_as_of(current.available_at))
    for record in available:
        if record.alias not in {"SESSION_HIGH", "SESSION_LOW"}:
            continue
        key = (record.location_id, record.generation)
        if key in published_records:
            continue
        previous = max(
            (
                item
                for item in registry.records.values()
                if item.alias == record.alias
                and item.generation != record.generation
                and (item.location_id, item.generation) in published_records
            ),
            key=lambda item: (item.available_at, item.generation),
            default=None,
        )
        published_records.add(key)
        if previous is not None and previous.price != record.price:
            aggregate = source_aggregate(record)
            for interaction in tuple(detector.interactions.values()):
                if (
                    interaction.status.value == "ACTIVE"
                    and str(interaction.location) == record.alias
                ):
                    ended = detector.end(
                        interaction.interaction_id,
                        available_at=record.available_at,
                        close_reason="REFERENCE_REPLACED",
                    )
                    snapshot = ended.snapshot()
                    facts.append(_fact(
                        "interaction_ended", ended.interaction_id,
                        ended.interaction_id,
                        {
                            "interaction_id": ended.interaction_id,
                            "location": str(ended.location),
                            "timeframe": ended.timeframe,
                            "reference_generation": ended.reference_generation,
                            "reason": snapshot["close_reason"],
                            "source_observation_id": interaction.reference_provenance.get(
                                "source_observation_id"
                            ),
                            "terminal_candle_id": snapshot["terminal_candle_id"],
                        },
                        observed_at=aggregate.end_time,
                        available_at=aggregate.available_at,
                    ))
def replay_interaction_response(
    aggregates: Mapping[str, Sequence[Aggregate]], context: ContextLocationReplay,
    registry: LocationRegistry, *, templates: Mapping[tuple[str, str], TradeTemplate] = TRADE_TEMPLATES,
    replay_mode: str = "FULL",
    fill_permission_margin: float | None = 0.05,
    raw_rows: Sequence[Mapping[str, Any]] = (),
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]] | None = None,
    atr_by_session: Mapping[str, float | None] | None = None,
    prior_value: ValuePublication | None = None,
) -> InteractionResponseReplay:
    """Advance independent 1m/5m detectors in one availability-ordered clock."""
    one = tuple(aggregates.get("1m", ()))
    five = tuple(aggregates.get("5m", ()))
    all_bars = tuple(sorted(one, key=lambda item: (item.available_at, item.time)))
    observations: dict[str, tuple[LocationObservation, ...]] = {}
    for fact in context.context_location:
        values = fact.get("values", {})
        source = str(values.get("source_observation_id", ""))
        locations = values.get("locations", ())
        parsed: list[LocationObservation] = []
        for item in locations if isinstance(locations, Sequence) else ():
            if not isinstance(item, Mapping):
                continue
            parsed.append(LocationObservation(
                location=str(item["location"]), location_id=item.get("location_id"),
                reference_generation=int(item["reference_generation"]), reference=float(item["reference"]),
                band_low=float(item["band_low"]), band_high=float(item["band_high"]),
                open=float(item["open"]), high=float(item["high"]), low=float(item["low"]), close=float(item["close"]),
                close_relation=str(item["close_relation"]), band_contact=bool(item["band_contact"]),
                source_aggregate_id=str(item["source_aggregate_id"]), observed_at=str(item["observed_at"]),
                available_at=str(item["available_at"]), method=str(item["method"]),
                observation_id=str(item.get("observation_id", "")), timeframe=str(item.get("timeframe", "")),
                child_observation_ids=tuple(str(value) for value in item.get("child_observation_ids", ())),
                source_observation_id=item.get("source_observation_id"),
                reference_source_aggregate_id=item.get("reference_source_aggregate_id"),
                reference_source_timeframe=item.get("reference_source_timeframe"),
            ))
        for item in parsed:
            target = source
            if item.timeframe == "1m" and item.source_aggregate_id:
                target = f"aggregate:1m:{item.source_aggregate_id}"
            existing = {
                value.observation_id: value
                for value in observations.get(target, ())
            }
            # Context facts are a cumulative stream.  Repeated observation IDs
            # retain their original point-in-time availability; later facts may
            # contain the same observation after it has already been published.
            existing.setdefault(item.observation_id, item)
            observations[target] = tuple(existing.values())
    # The final evidence registry contains the complete VWAP history, but
    # Canonical publishes that history only when the first 5m aggregate is
    # available. Keep the replay registry at that same causal boundary.
    published_registry = LocationRegistry(registry.stream_id)
    deferred_vwap = []
    for record in registry.records.values():
        if record.alias == "VWAP":
            deferred_vwap.append(record)
        else:
            published_registry.register(record)
    vwap_registered: set[tuple[str, int]] = set()
    detector = InteractionDetector(published_registry)
    facts: list[dict[str, Any]] = []
    published_records: set[tuple[str, int]] = set()
    vwap_response_published: set[str] = set()
    opening_gap_policy = OpeningGapPolicy()
    regime_publications: list[tuple[str, Any]] = []
    context_publications: list[dict[str, Any]] = []
    gap_direction: str | None = None
    opening_gap_state: str | None = None
    opening_gap_reference: float | None = None
    opening_gap_bars = 0
    prior_value_low = (
        prior_value.reference.value_area_low
        if prior_value is not None and prior_value.reference is not None else None
    )
    prior_value_high = (
        prior_value.reference.value_area_high
        if prior_value is not None and prior_value.reference is not None else None
    )
    gap_last_1m_available: datetime | None = None
    transition_owner = OrderflowTransitionOwner(
        published_registry,
        rows_by_date=rows_by_date or {},
        atr_by_session=atr_by_session,
    )
    transition_owner.context.rows = [dict(item) for item in raw_rows]
    transition_owner.context.prior_session_rows = tuple(
        tuple(dict(item) for item in values)
        for date, values in sorted((rows_by_date or {}).items())
        if one and date < one[0].session_date
    )
    observed5_available: datetime | None = None
    transition_owner.context.execution_constraints = replace(
        transition_owner.context.execution_constraints,
        fill_permission_margin=fill_permission_margin,
    )
    opportunity = build_opportunity_coordinator(
        transition_owner,
        detector=detector,
        templates=DEFAULT_TEMPLATES,
    )

    def apply_regime_publications(as_of: datetime) -> None:
        eligible = [
            publication
            for publication in regime_publications
            if parse_timestamp(publication[0]) <= as_of
        ]
        if eligible:
            transition_owner.context.regime = eligible[-1][1]

    for bar_index, current in enumerate(all_bars):
        source = f"aggregate:1m:{nifty_label(current.time)}"
        current_available = current.available_at
        # Match canonical process_bar: next decision uses the next row time label.
        next_time = (
            nifty_label(all_bars[bar_index + 1].time)
            if bar_index + 1 < len(all_bars)
            else nifty_label(current.time)
        )
        available1 = [item for item in one if item.available_at <= current_available]
        available5 = [item for item in five if item.available_at <= current_available]
        atr_source = available1[-1] if available1 else (available5[-1] if available5 else None)
        atr = (
            transition_owner.context.atr_by_session.get(atr_source.session_date)
            if atr_source is not None else None
        )
        transition_owner.context.aggregates["1m"] = list(available1)
        transition_owner.context.aggregates["5m"] = list(available5)
        bar_events: list[dict[str, Any]] = []
        converted_bar_events = 0
        if not regime_publications:
            prior_low = published_registry.resolve_as_of("PD_LOW", current.available_at)
            prior_high = published_registry.resolve_as_of("PD_HIGH", current.available_at)
            opening = opening_gap_policy.classify(
                opening=current.to_dict(),
                prior_low=prior_low.price if prior_low is not None else None,
                prior_high=prior_high.price if prior_high is not None else None,
                prior_value_low=prior_value_low,
                prior_value_high=prior_value_high,
                available_at=current.available_at,
            )
            gap_direction = str(opening.get("direction"))
            opening_gap_state = str(opening.get("state")) if gap_direction in {"up", "down"} else None
            opening_gap_reference = float(opening["reference_level"]) if gap_direction in {"up", "down"} else None
            regime_publications.append((
                serialize_utc(current.available_at),
                opening_gap_policy.regime_for(
                    state=opening_gap_state,
                    direction=gap_direction if gap_direction in {"up", "down"} else None,
                    version=f"opening-gap-permission-{serialize_utc(current.available_at)}",
                    context="opening_gap", restrictions=("causal_opening_gap",),
                ),
            ))
        apply_regime_publications(current_available)
        session_five = tuple(
            item for item in five if item.session_date == current.session_date
        )
        first_five = min(session_five, key=lambda item: item.time, default=None)
        latest5 = available5[-1] if available5 else None
        new5 = latest5 is not None and latest5.available_at != observed5_available
        if new5 and latest5 is not None:
            observed5_available = latest5.available_at
            if first_five is not None and latest5.time == first_five.time:
                regime_publications.append((
                    serialize_utc(current.available_at),
                    opening_gap_policy.regime_for(
                        state=opening_gap_state,
                        direction=gap_direction if gap_direction in {"up", "down"} else None,
                        version=f"opening-gap-permission-{serialize_utc(current.available_at)}",
                        context="opening_gap",
                        restrictions=("causal_opening_gap",),
                    ),
                ))
                apply_regime_publications(current_available)
        if current.timeframe == "1m" and gap_direction in {"up", "down"} and opening_gap_reference is not None:
            if gap_last_1m_available is None or current.available_at > gap_last_1m_available:
                gap_last_1m_available = current.available_at
                opening_gap_bars += 1
                transitioned = opening_gap_policy.transition(
                    direction=gap_direction,
                    completed_bars=opening_gap_bars,
                    close=current.close,
                    reference_level=opening_gap_reference,
                    prior_value_low=prior_value_low,
                    prior_value_high=prior_value_high,
                )
                if transitioned is not None and transitioned != opening_gap_state:
                    opening_gap_state = transitioned
                    regime_publications.append((
                        serialize_utc(current.available_at),
                        opening_gap_policy.regime_for(
                            state=opening_gap_state, direction=gap_direction,
                            version=f"opening-gap-permission-{serialize_utc(current.available_at)}",
                            context="opening_gap", restrictions=("causal_opening_gap",),
                        ),
                    ))
                    apply_regime_publications(current_available)
        if current.timeframe == "1m" and available5:
            for record in deferred_vwap:
                key = (record.location_id, record.generation)
                if record.available_at > current_available or key in vwap_registered:
                    continue
                published_registry.register(record)
                vwap_registered.add(key)
        # Permission is a state-machine boundary: publish the ordinary context
        # from mutable market state before any interaction detector observes
        # this clock. Phase 3 evidence never supplies these contexts.
        if new5 and latest5 is not None:
            scores = build_context_scores(
                latest5,
                raw_rows=raw_rows,
                aggregates={"1m": list(available1), "5m": list(available5)},
                atr=atr,
            )
            long_score, short_score = scores or (None, None)
            context_id = (
                f"5m-context-{latest5.symbol}-{latest5.session_date}-1m_5m"
            )
            versions = transition_owner.context.market_contexts.versions.get(context_id, ())
            permission_context = MarketContext(
                context_id=context_id,
                version=f"v{len(versions) + 1}",
                timestamp=latest5.time,
                available_at=latest5.available_at,
                location=None,
                market_state=transition_owner.context.market_state,
                long_score=long_score,
                short_score=short_score,
                evidence=({
                    "completed_bar": True,
                    "open": latest5.open,
                    "high": latest5.high,
                    "low": latest5.low,
                    "close": latest5.close,
                },),
                source_timeframe="5m",
                definition_version="1m_5m",
                source_provenance=({
                    "source_candle_id": latest5.time,
                    "available_at": latest5.available_at,
                },),
            )
            reference = transition_owner.context.publish_context(permission_context)
            transition_owner.context.market_contexts.ordinary_context = reference
            publication = {
                "kind": "context_observed",
                "context_id": permission_context.context_id,
                "version": permission_context.version,
                "market_state": permission_context.market_state,
                "long_score": long_score,
                "short_score": short_score,
                "observed_at": serialize_utc(latest5.end_time),
                "available_at": serialize_utc(latest5.available_at),
                "context": _jsonable(permission_context),
            }
            context_publications.append(publication)
            transition_owner.context.emit({
                **publication,
            }, bar_events)
            transition_owner.context.emit({
                "kind": "permission",
                "level": "5m",
                "time": serialize_utc(latest5.end_time),
                "decision_at": serialize_utc(current.end_time),
                "observed_at": serialize_utc(latest5.end_time),
                "aggregate_time": serialize_utc(latest5.time),
                "available_at": serialize_utc(latest5.available_at),
                "status": "allowed",
                "owner": "5m",
                "evidence_timeframe": "5m",
                "context_id": permission_context.context_id,
                "market_state": permission_context.market_state,
                "long_score": long_score,
                "short_score": short_score,
            }, bar_events)
        _publish_replaced_interactions(
            detector=detector,
            registry=published_registry,
            current=current,
            available5=available5,
            published_records=published_records,
            facts=facts,
        )
        typed = tuple(
            item for item in observations.get(source, ())
            if item.available_at <= current_available
        )
        if atr is None and new5 and latest5 is not None:
            bar_events.append({
                "kind": "atr_unavailable",
                "available_at": latest5.available_at,
                "observed_at": latest5.end_time,
                "decision": "interaction_creation",
                "reason": "insufficient_daily_history",
            })
        transitions = [
            (transition, current)
            for transition in detector.observe_1m(
                current=current, available=available1, atr=atr, observations=typed,
            )
        ]
        if new5 and latest5 is not None:
            source5 = f"aggregate:5m:{nifty_label(latest5.time)}"
            one_observations = tuple(
                item for key, values in observations.items()
                if key.startswith("aggregate:1m:")
                for item in values if item.available_at <= current_available
            )
            available_ids = {item.observation_id for item in one_observations}
            typed5 = tuple(
                item for item in observations.get(source5, ())
                if item.available_at <= current_available
                and all(child_id in available_ids for child_id in item.child_observation_ids)
            )
            transitions.extend(
                (transition, latest5)
                for transition in detector.observe(
                    current=latest5, available5=available5, available1=available1,
                    atr=atr, observations5=typed5, observations1=one_observations,
                )
            )
            observed5_available = latest5.available_at
        for transition, event_current in transitions:
            snap = transition.interaction.snapshot()
            iid = transition.interaction.interaction_id
            transition_owner.context.interactions[iid] = (
                detector.interactions[iid].evaluation_snapshot()
            )
            if (
                str(transition.interaction.location) == "VWAP"
                and iid not in vwap_response_published
                and transition.interaction.pre_contact_side is not None
            ):
                for item in transition.interaction.candles[1:]:
                    close = item.candle.close
                    confirmed = (
                        transition.interaction.pre_contact_side.value == "BELOW"
                        and close >= transition.interaction.band_low
                    ) or (
                        transition.interaction.pre_contact_side.value == "ABOVE"
                        and close <= transition.interaction.band_high
                    )
                    if not confirmed:
                        continue
                    source = detector.interactions.get(iid)
                    source_id = (
                        source.reference_provenance.get("source_observation_id")
                        if source is not None else None
                    )
                    transition_owner.context.emit({
                        "kind": "response_confirmed",
                        "interaction_id": iid,
                        "location": "VWAP",
                        "timeframe": transition.interaction.timeframe,
                        "reference_generation": transition.interaction.reference_generation,
                        "response_status": "CONFIRMED",
                        "response_reason": "causal_vwap_rejection",
                        "source_observation_id": source_id,
                        "evidence_ids": [nifty_label(item.candle.time)],
                        "observed_at": item.candle.end_time,
                        "available_at": item.candle.available_at,
                        "current_available_at": event_current.available_at,
                    }, bar_events)
                    vwap_response_published.add(iid)
                    break
            opportunity.publish_interaction_context(
                transition.interaction, bar_events,
            )
            facts.extend(bar_events_to_facts(
                bar_events[converted_bar_events:],
                session_date=current.session_date,
            ))
            converted_bar_events = len(bar_events)
            observation = transition.observation
            if (
                replay_mode == "FULL"
                and observation is not None
                and transition.interaction.status.value == "ACTIVE"
            ):
                interaction_key = f"{observation.location.upper()}:{observation.timeframe}:{observation.reference_generation}"
                decision_id = hashlib.sha256(
                    f"opportunity|{iid}|{observation.observation_id}".encode()
                ).hexdigest()[:24]
                observation_values = {
                    "interaction_id": iid,
                    "decision_stage": "opportunity",
                    "decision_id": decision_id,
                    "location": observation.location,
                    "timeframe": observation.timeframe,
                    "reference_generation": observation.reference_generation,
                    "source_observation_ids": [observation.observation_id],
                    "source_fingerprint": event_current.source_fingerprint,
                    "reason": "location_interaction_observed",
                    "outcome": "observed",
                    "interaction_key": interaction_key,
                    "feature_values": {
                        "open": observation.open,
                        "high": observation.high,
                        "low": observation.low,
                        "close": observation.close,
                    },
                }
                facts.append(_fact(
                    # An interaction can publish one observation per completed
                    # candle. Keep each causal observation distinct while
                    # retaining the interaction ID as its parent.
                    "location_interaction_observed",
                    f"{iid}:{observation.observation_id}",
                    f"aggregate:{event_current.timeframe}:{nifty_label(event_current.time)}", observation_values,
                    observed_at=observation.observed_at,
                    available_at=observation.available_at,
                ))
            if transition.created:
                facts.append(_fact("interaction_created", iid, f"aggregate:{event_current.timeframe}:{nifty_label(event_current.time)}", {
                    "interaction_id": iid, "location": str(transition.interaction.location),
                    "timeframe": transition.interaction.timeframe,
                    "reference_generation": transition.interaction.reference_generation,
                    "reference_level": transition.interaction.reference_level,
                    "band_low": transition.interaction.band_low, "band_high": transition.interaction.band_high,
                    "source_aggregate_id": None,
                    "source_observation_id": observation.source_observation_id if observation is not None else None,
                    "stage": "OBSERVING_LOCATION", "evidence": [event_current.to_dict()],
                }, observed_at=event_current.end_time, available_at=event_current.available_at))
            if transition.interaction.status.value == "ENDED" and transition.departed:
                facts.append(_fact("interaction_ended", iid, iid, {
                    "interaction_id": iid, "location": str(transition.interaction.location),
                    "timeframe": transition.interaction.timeframe,
                    "reference_generation": transition.interaction.reference_generation,
                    "reason": snap["close_reason"],
                    "source_observation_id": (
                        detector.interactions[iid].reference_provenance.get("source_observation_id")
                        if iid in detector.interactions else None
                    ),
                    "terminal_candle_id": snap["terminal_candle_id"],
                    "terminal_close_side": snap["terminal_close_side"],
                }, observed_at=snap["candles"][-1]["end_time"],
                    available_at=snap["candles"][-1]["available_at"]))
        transition_owner.context.interactions = {
            iid: state.evaluation_snapshot()
            for iid, state in detector.interactions.items()
        }
        if current.timeframe == "1m":
            timestamp = serialize_utc(current.end_time)
            transition_owner.advance_opportunity_timeouts(
                timestamp, available5, bar_events,
            )
            if available5:
                opportunity.handle_structure(
                    timestamp,
                    next_time,
                    current.to_dict(),
                    list(available5),
                    bar_events,
                )
            opportunity.handle_opening_interaction(
                current.to_dict(),
                timestamp,
                next_time,
                bar_events,
            )
            transition_owner.context.pending_bar_events.extend(bar_events)
            facts.extend(
                bar_events_to_facts(
                    bar_events[converted_bar_events:],
                    session_date=current.session_date,
                )
            )
            transition_owner.advance_replay_bar(
                (),
                index=one.index(current),
                timestamp=nifty_label(current.time),
                next_time=next_time,
                row=current.to_dict(),
                available5=available5,
            )
    parity_streams = transition_owner.context.parity_snapshot()
    interaction_facts = tuple(facts)
    opportunity_facts = tuple(parity_streams.get("opportunity_lifecycle", ()))
    return InteractionResponseReplay(
        facts=interaction_facts,
        opportunity_facts=opportunity_facts,
        formations=(),
        regime_publications=tuple(regime_publications),
        context_publications=tuple(context_publications),
        execution_phase_facts={
            name: tuple(values)
            for name, values in transition_owner.context.parity_snapshot().items()
        },
        parity_layers={
            name: tuple(values)
            for name, values in transition_owner.context.parity_layers.items()
        },
        execution_diagnostics=tuple(transition_owner.context.execution_diagnostics),
    )


__all__ = ["InteractionResponseReplay", "replay_interaction_response"]
