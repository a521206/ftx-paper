"""Runtime-neutral causal interaction and response replay boundary."""

from __future__ import annotations
# pyright: reportArgumentType=false

import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from .context_location_replay import ContextLocationReplay
from .interaction_detector import InteractionDetector
from .interactions import candle_identity
from .location_observation import LocationObservation
from .orderflow_common import Aggregate, parse_timestamp, serialize_timestamp
from .timestamps import NIFTY_ZONE, nifty_label, serialize_utc
from .state_machine_types import LocationRegistry
from .templates import Direction, OpeningInteractionKind, TRADE_TEMPLATES, TradeTemplate
from .value_publication import ValuePublication
from .policies import PDPolicy, StandardLocationPolicy
from .opportunity_coordinator import OpportunityCoordinator
from .opening_gap_policy import OpeningGapPolicy
from .transition import OrderflowTransitionOwner


@dataclass(frozen=True, slots=True)
class InteractionResponseReplay:
    facts: tuple[dict[str, Any], ...]
    opportunity_facts: tuple[dict[str, Any], ...] = ()
    formations: tuple[Any, ...] = ()
    regime_publications: tuple[tuple[str, Any], ...] = ()
    execution_phase_facts: dict[str, tuple[dict[str, Any], ...]] = field(default_factory=dict)
    execution_diagnostics: tuple[dict[str, Any], ...] = ()

    def parity_phase_facts(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "interaction_response": [dict(item) for item in self.facts],
            "opportunity_lifecycle": [dict(item) for item in self.opportunity_facts],
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
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _handle_opening_interaction(
    context: Any,
    row: Mapping[str, Any],
    facts: list[dict[str, Any]],
    *,
    timestamp: str,
    next_time: str,
) -> None:
    """Attach the first completed 1m evidence to provisional opening seeds."""
    for life in tuple(context.opportunities.values()):
        if (
            not life.active
            or life.opening_policy_version is None
            or life.opening_interaction_kind in {
                OpeningInteractionKind.OPENING_BREAKOUT.value,
                OpeningInteractionKind.OPENING_RETEST.value,
            }
            or life.formation_available_at >= parse_timestamp(timestamp)
        ):
            continue
        boundary = life.reference_level
        if boundary is None or life.context_candle is None:
            continue
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        direction = Direction(life.direction)
        crossed = close >= boundary if direction is Direction.LONG else close <= boundary
        touched = low <= boundary <= high
        if not (crossed or touched):
            continue
        policy = life.opening_policy_version
        if policy.startswith(OpeningInteractionKind.OPENING_BREAKOUT.value):
            if not crossed:
                continue
            kind = OpeningInteractionKind.OPENING_BREAKOUT
        elif not life.opening_interaction_evidence:
            kind = OpeningInteractionKind.OPENING_REJECTION
        else:
            has_recovery = any(
                item.get("kind") == "opening_recovery"
                for item in life.opening_interaction_evidence
            )
            recovered = close > boundary if direction is Direction.SHORT else close < boundary
            if not has_recovery and recovered:
                kind = OpeningInteractionKind.OPENING_REJECTION
            elif has_recovery and crossed:
                kind = OpeningInteractionKind.OPENING_RETEST
            else:
                continue
        evidence = {
            "time": str(row["time"]), "observed_at": timestamp,
            "available_at": next_time, "open": float(row["open"]),
            "high": high, "low": low, "close": close,
            "reference_level": boundary,
            "kind": (
                "opening_recovery"
                if kind is OpeningInteractionKind.OPENING_REJECTION
                and life.opening_interaction_evidence
                and (close > boundary if direction is Direction.SHORT else close < boundary)
                else kind.value
            ),
        }
        life.opening_interaction_kind = kind.value
        life.opening_interaction_evidence.append(evidence)
        event = {
            "kind": "opening_interaction",
            "lifecycle_id": life.lifecycle_id,
            "interaction_kind": kind.value,
            "location_interaction_id": None,
            "evidence": list(life.opening_interaction_evidence),
            "policy_version": life.opening_policy_version,
            "observed_at": timestamp,
            "available_at": next_time,
        }
        context.record_event(event)
        pending_bar_events = getattr(context, "pending_bar_events", None)
        if isinstance(pending_bar_events, list):
            pending_bar_events.append(event)


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
    replay_mode: str = "FULL", atr: float | None = None,
    fill_permission_margin: float | None = 0.05,
    raw_rows: Sequence[Mapping[str, Any]] = (),
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]] | None = None,
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
    vwap_emitted: set[str] = set()
    published_records: set[tuple[str, int]] = set()
    resolved_response_emitted: set[tuple[str, str, tuple[str, ...]]] = set()
    opportunity_coordinator = OpportunityCoordinator(
        fill_permission_margin=fill_permission_margin,
    )
    opening_gap_policy = OpeningGapPolicy()
    regime_publications: list[tuple[str, Any]] = []
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
    )
    transition_owner.context.rows = [dict(item) for item in raw_rows]
    transition_owner.context.prior_session_rows = tuple(
        tuple(dict(item) for item in values)
        for date, values in sorted((rows_by_date or {}).items())
        if one and date < one[0].session_date
    )
    processed_formations = 0
    observed5_available: datetime | None = None

    def apply_regime_publications(as_of: datetime) -> None:
        eligible = [
            publication
            for publication in regime_publications
            if parse_timestamp(publication[0]) <= as_of
        ]
        if eligible:
            transition_owner.context.regime = eligible[-1][1]

    for current in all_bars:
        source = f"aggregate:1m:{nifty_label(current.time)}"
        current_available = current.available_at
        next_time = serialize_utc(current.available_at)
        available1 = [item for item in one if item.available_at <= current_available]
        available5 = [item for item in five if item.available_at <= current_available]
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
        if new5 and first_five is not None and latest5 is not None and latest5.time == first_five.time:
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
            opening_seed = opportunity_coordinator.form_opening_seed(
                candle=latest5,
                registry=published_registry,
                templates=templates,
                aggregates={
                    timeframe: list(values)
                    for timeframe, values in (
                        ("1m", available1), ("5m", available5),
                    )
                },
                raw_rows=raw_rows,
                rows_by_date=rows_by_date or {},
                regime_version=(
                    transition_owner.context.regime.version
                    if transition_owner.context.regime is not None
                    else "unresolved"
                ),
            )
            if opening_seed is not None:
                outcome_values, formation = opening_seed
                transition_owner.attach_setup(formation)
                facts.append(_fact(
                    "or_directional_seed",
                    formation.identity,
                    formation.lifecycle_id,
                    {
                        "lifecycle_id": formation.lifecycle_id,
                        "causal_identity": formation.identity,
                        "location": outcome_values["location"],
                        "direction": outcome_values["direction"],
                        "interaction_id": None,
                        "opening_interaction_kind": None,
                        "opening_policy_version": formation.values["opening_policy_version"],
                        "candidate_provenance": "opening_hypothesis",
                        "reference_generation": formation.lifecycle.reference_generation,
                        "long_score": formation.values["long_score"],
                        "short_score": formation.values["short_score"],
                    },
                    observed_at=formation.observed_at,
                    available_at=formation.available_at,
                ))
                facts.append(_fact(
                    "opportunity_outcome",
                    formation.identity,
                    str(outcome_values["interaction_id"]),
                    outcome_values,
                    observed_at=formation.observed_at,
                    available_at=formation.available_at,
                ))
                facts.append(_fact(
                    "lifecycle_formed",
                    formation.identity,
                    formation.lifecycle_id,
                    formation.values,
                    observed_at=formation.observed_at,
                    available_at=formation.available_at,
                ))
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
        transitions = [
            (transition, current)
            for transition in detector.observe_1m(
                current=current, available=available1, atr=None, observations=typed,
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
                    atr=None, observations5=typed5, observations1=one_observations,
                )
            )
            observed5_available = latest5.available_at
        for transition, event_current in transitions:
            snap = transition.interaction.snapshot()
            iid = transition.interaction.interaction_id
            observation = transition.observation
            source = detector.interactions.get(iid)
            source_observation_id = (
                source.reference_provenance.get("source_observation_id")
                if source is not None else None
            )
            if snap["location"] == "VWAP" and iid not in vwap_emitted and snap["pre_contact_side"]:
                for candle in snap["candles"][1:]:
                    confirmed = (
                        snap["pre_contact_side"] == "BELOW" and candle["close"] >= snap["band_low"]
                    ) or (
                        snap["pre_contact_side"] == "ABOVE" and candle["close"] <= snap["band_high"]
                    )
                    if confirmed:
                        vwap_emitted.add(iid)
                        facts.append(_fact("response_confirmed", f"{iid}:VWAP", iid, {
                            "interaction_id": iid, "location": "VWAP",
                            "timeframe": snap["timeframe"],
                            "reference_generation": snap["reference_generation"],
                            "response_status": "CONFIRMED", "response_reason": "causal_vwap_rejection",
                            "source_observation_id": source_observation_id,
                            "evidence_ids": [candle["candle_id"]],
                            "current_available_at": event_current.available_at,
                        }, observed_at=candle["end_time"], available_at=candle["available_at"]))
                        break
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
                    "source_aggregate_id": observation.source_aggregate_id if observation is not None else None,
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
        if new5 and latest5 is not None:
            for life in tuple(transition_owner.context.opportunities.values()):
                if (
                    not life.active
                    or life.candidate_invalidated
                    or not getattr(life, "episode_confirmed", True)
                    or not transition_owner.execution_owner.setup_invalidated_by_close(
                        life, close=latest5.close
                    )
                ):
                    continue
                invalidation_events: list[dict[str, Any]] = []
                transition_owner.execution_owner.invalidate_opportunity(
                    life,
                    timestamp=serialize_utc(current.end_time),
                    observed_at=latest5.end_time,
                    available_at=latest5.available_at,
                    bar_events=invalidation_events,
                )
                facts.append(_fact(
                    "setup_invalidated",
                    life.lifecycle_id,
                    life.lifecycle_id,
                    {
                        "lifecycle_id": life.lifecycle_id,
                        "reason": "setup_invalidated",
                        "boundary": life.failure_boundary,
                    },
                    observed_at=latest5.end_time,
                    available_at=latest5.available_at,
                ))
        if available5:
            new_confirmation_context = (
                latest5 is not None
                and candle_identity(latest5)
                != transition_owner.context.last_confirmation_context_time
            )
            _dispatch_structure(
                detector=detector,
                templates=templates,
                available5=available5,
                available1=available1,
                as_of=available5[-1].available_at,
                new_confirmation_context=new_confirmation_context,
                facts=facts,
                resolved_response_emitted=resolved_response_emitted,
                published_resolved_responses=transition_owner.context.published_resolved_responses,
                opportunity_coordinator=opportunity_coordinator,
                raw_rows=raw_rows,
                atr=atr,
                regime_version=(
                    transition_owner.context.regime.version
                    if transition_owner.context.regime is not None
                    else "unresolved"
                ),
            )
            if new_confirmation_context and latest5 is not None:
                transition_owner.context.last_confirmation_context_time = candle_identity(
                    latest5
                )
            transition_owner.context.published_resolved_responses.update(
                (interaction_id, template_id)
                for interaction_id, template_id, _evidence_ids
                in resolved_response_emitted
            )
        transition_owner.context.interactions = {
            iid: state.evaluation_snapshot()
            for iid, state in detector.interactions.items()
        }
        if current.timeframe == "1m":
            _handle_opening_interaction(
                transition_owner.context,
                current.to_dict(),
                facts,
                timestamp=serialize_utc(current.end_time),
                next_time=next_time,
            )
        new_formations = tuple(opportunity_coordinator.formations[processed_formations:])
        if new_formations:
            scores = {
                item.lifecycle.lifecycle_id: (
                    item.values.get("long_score"), item.values.get("short_score"),
                )
                for item in new_formations if item.lifecycle is not None
            }
            transition_owner.advance_execution(
                new_formations,
                regime_publications=tuple(regime_publications),
                decision_scores=scores,
                decision_at=next_time,
            )
            processed_formations += len(new_formations)
        if current.timeframe == "1m" and opportunity_coordinator.formations:
            # Preserve the completed evidence window for position-management
            # policies that consume causal 1m/5m history.
            transition_owner.context.aggregates["1m"] = list(available1)
            transition_owner.context.aggregates["5m"] = list(available5)
            transition_owner.advance_replay_bar(
                tuple(opportunity_coordinator.formations),
                index=one.index(current),
                timestamp=serialize_utc(current.end_time),
                next_time=next_time,
                row=current.to_dict(),
                available5=available5,
            )
    lifecycle_kinds = {
        "or_directional_seed",
        "opportunity_outcome", "lifecycle_formed", "setup_invalidated",
        "setup_expired", "setup_superseded", "opportunity_timeout",
    }
    interaction_facts = tuple(item for item in facts if item["kind"] not in lifecycle_kinds)
    opportunity_facts = tuple(
        item for item in facts if item["kind"] in lifecycle_kinds
    )
    return InteractionResponseReplay(
        interaction_facts,
        opportunity_facts,
        tuple(opportunity_coordinator.formations),
        tuple(regime_publications),
        {name: tuple(values) for name, values in transition_owner.context.parity_snapshot().items()},
        tuple(transition_owner.context.execution_diagnostics),
    )


def _dispatch_structure(
    *,
    detector: InteractionDetector,
    templates: Mapping[tuple[str, str], TradeTemplate],
    available5: Sequence[Aggregate],
    available1: Sequence[Aggregate],
    as_of: datetime,
    new_confirmation_context: bool,
    facts: list[dict[str, Any]],
    resolved_response_emitted: set[tuple[str, str, tuple[str, ...]]],
    published_resolved_responses: set[tuple[str, str]] | None = None,
    opportunity_coordinator: OpportunityCoordinator,
    raw_rows: Sequence[Mapping[str, Any]],
    atr: float | None,
    regime_version: str = "unresolved",
) -> None:
    """Run the canonical structure phase after all interaction publications."""
    dispatch_start = len(facts)

    def emission_identity(kind: str, identity: str) -> str:
        count = sum(
            1 for item in facts[:dispatch_start]
            if item["kind"] == kind
            and item["identity"] == identity
        )
        return identity if count == 0 else f"{identity}:evaluation:{count}"

    def append_policy_fact(kind: str, identity: str, parent: str, values: Mapping[str, Any], *,
                           observed_at: str, available_at: str) -> None:
        fact = _fact(
            kind, identity, parent, values,
            observed_at=observed_at, available_at=available_at,
        )
        fact["_publication_available_at"] = serialize_utc(as_of)
        facts.append(fact)

    latest = available5[-1]
    for state in reversed(tuple(detector.interactions.values())):
        interaction = state.evaluation_snapshot()
        if interaction.location == "VWAP":
            continue
        iid = interaction.interaction_id
        is_pd = interaction.location in {"PD_LOW", "PD_HIGH"}
        response_already_published = any(
            interaction_id == iid
            for interaction_id, _template_id, _evidence_ids
            in resolved_response_emitted
        ) or any(
            interaction_id == iid
            for interaction_id, _template_id
            in (published_resolved_responses or ())
        )
        if interaction.creation_candle.available_at > latest.available_at:
            continue
        if interaction.status.value != "ACTIVE" and (
            is_pd
            or interaction.terminal_candle_id != candle_identity(latest)
            or not new_confirmation_context
            or response_already_published
        ):
            continue
        if interaction.status.value not in {"ACTIVE", "ENDED"}:
            continue
        # Canonical re-evaluates standard locations only when a new completed
        # 5m confirmation context becomes available. PD role evaluation is the
        # intentional exception and may continue on repeated clocks.
        if not new_confirmation_context and not is_pd:
            continue
        policy = PDPolicy() if is_pd else StandardLocationPolicy()
        selected = policy.templates_for(tuple(templates.values()), interaction)
        if not selected:
            continue
        role_events: list[dict[str, Any]] = []
        evaluations = (
            PDPolicy().evaluate_hypotheses(
                selected, interaction, available5, as_of=as_of,
                state=state, emit=role_events.append,
            )
            if is_pd else StandardLocationPolicy().arbitrate(
                selected, interaction, available5, as_of=as_of,
            )
        )
        for role_event in role_events:
            kind = str(role_event.get("kind", "role_transition"))
            # PD role availability is internal policy state, not a Canonical
            # interaction/response parity publication.
            if kind == "role_context_available":
                continue
            facts.append(_fact(
                kind, iid, iid, role_event,
                observed_at=str(role_event["observed_at"]),
                available_at=str(role_event["available_at"]),
            ))
        for evaluation in evaluations:
            outcome = evaluation.outcome
            base_identity = evaluation.identity or f"{iid}:{evaluation.template.template_id}:{outcome.status.value}"
            identity = emission_identity("opportunity_outcome", base_identity)
            response = outcome.response
            response_key = (
                iid,
                evaluation.template.template_id,
                tuple(response.evidence_ids),
            ) if response is not None else None
            response_to_publish = (
                response
                if response is not None and response_key not in resolved_response_emitted
                else None
            )
            if response_to_publish is not None:
                assert response_key is not None
                resolved_response_emitted.add(response_key)
                append_policy_fact("hypothesis_arbitrated", emission_identity("hypothesis_arbitrated", identity), iid, {
                    "interaction_id": iid, "location": interaction.location,
                    "template_id": evaluation.template.template_id,
                    "response_type": evaluation.template.response_type.value,
                    "direction": response_to_publish.direction.value,
                    "arbitration_selected": evaluation.selected,
                    "arbitration_reason": evaluation.arbitration_reason,
                }, observed_at=response_to_publish.observed_at, available_at=response_to_publish.available_at)
            facts.append(_fact("opportunity_outcome", identity, iid, {
                "interaction_id": iid,
                "location": interaction.location,
                "template_id": evaluation.template.template_id,
                "response_type": evaluation.template.response_type.value,
                "direction": response.direction.value if response is not None else evaluation.template.direction.value,
                "status": outcome.status.value,
                "reason": outcome.reason,
                "arbitration_selected": evaluation.selected,
                "arbitration_reason": evaluation.arbitration_reason,
                "as_of": outcome.as_of,
                "evidence_ids": [item.time for item in outcome.evidence],
                "evidence_lineage": [dataclasses.asdict(item) for item in outcome.evidence_lineage],
            }, observed_at=outcome.observed_at, available_at=outcome.available_at))
            formation = opportunity_coordinator.form_resolved(
                interaction=interaction,
                evaluation=evaluation,
                outcome=outcome,
                as_of=as_of,
                aggregates={"1m": list(available1), "5m": list(available5)},
                raw_rows=raw_rows,
                atr=atr,
                regime_version=regime_version,
            )
            if formation is not None:
                facts.append(_fact(
                    "lifecycle_formed",
                    formation.identity,
                    formation.lifecycle_id,
                    formation.values,
                    observed_at=formation.observed_at,
                    available_at=formation.available_at,
                ))
            if response_to_publish is not None and not is_pd:
                append_policy_fact("response_confirmed", emission_identity("response_confirmed", identity), iid, {
                    "interaction_id": iid, "location": interaction.location,
                    "template_id": evaluation.template.template_id,
                    "response_type": evaluation.template.response_type.value,
                    "direction": response_to_publish.direction.value,
                    "evidence_ids": list(response_to_publish.evidence_ids),
                }, observed_at=response_to_publish.observed_at, available_at=response_to_publish.available_at)
__all__ = ["InteractionResponseReplay", "replay_interaction_response"]
