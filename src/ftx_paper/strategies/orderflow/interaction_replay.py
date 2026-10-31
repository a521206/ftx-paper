"""Runtime-neutral causal interaction and response replay boundary."""

from __future__ import annotations
# pyright: reportArgumentType=false

import dataclasses
from dataclasses import dataclass
from datetime import datetime
import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from .context_location_replay import ContextLocationReplay
from .interaction_detector import InteractionDetector
from .location_observation import LocationObservation
from .orderflow_common import Aggregate, parse_timestamp, serialize_timestamp
from .timestamps import nifty_label, serialize_utc
from .state_machine_types import LocationRegistry
from .templates import TRADE_TEMPLATES, TradeTemplate
from .policies import PDPolicy, StandardLocationPolicy


@dataclass(frozen=True, slots=True)
class InteractionResponseReplay:
    facts: tuple[dict[str, Any], ...]

    def parity_phase_facts(self) -> dict[str, list[dict[str, Any]]]:
        return {"interaction_response": [dict(item) for item in self.facts]}


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
    return {"kind": kind, "occurred_at": observed_timestamp,
            "observed_at": observed_timestamp, "available_at": available_timestamp,
            "identity": identity, "causal_parent_id": parent, "values": dict(snapshot),
            "unavailable": list(unavailable)}


def replay_interaction_response(
    aggregates: Mapping[str, Sequence[Aggregate]], context: ContextLocationReplay,
    registry: LocationRegistry, *, templates: Mapping[tuple[str, str], TradeTemplate] = TRADE_TEMPLATES,
    replay_mode: str = "FULL",
) -> InteractionResponseReplay:
    """Advance independent 1m/5m detectors in one availability-ordered clock."""
    one = tuple(aggregates.get("1m", ()))
    five = tuple(aggregates.get("5m", ()))
    all_bars = tuple(sorted((*one, *five), key=lambda item: (item.available_at, item.timeframe, item.time)))
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
    vwap_published = False
    detector = InteractionDetector(published_registry)
    facts: list[dict[str, Any]] = []
    vwap_emitted: set[str] = set()
    role_context_emitted: set[str] = set()
    for current in all_bars:
        source = f"aggregate:{current.timeframe}:{nifty_label(current.time)}"
        current_available = current.available_at
        available1 = [item for item in one if item.available_at <= current_available]
        available5 = [item for item in five if item.available_at <= current_available]
        if available5 and not vwap_published:
            for record in deferred_vwap:
                published_registry.register(record)
            vwap_published = True
        typed = tuple(
            item for item in observations.get(source, ())
            if item.available_at <= current_available
        )
        if current.timeframe == "1m":
            transitions = detector.observe_1m(current=current, available=available1, atr=None, observations=typed)
        else:
            one_observations = tuple(item for key, values in observations.items() if key.startswith("aggregate:1m:") for item in values if item.available_at <= current_available)
            available_ids = {item.observation_id for item in one_observations}
            typed = tuple(
                item for item in typed
                if all(child_id in available_ids for child_id in item.child_observation_ids)
            )
            transitions = detector.observe(current=current, available5=available5, available1=available1, atr=None, observations5=typed, observations1=one_observations)
        for transition in transitions:
            snap = transition.interaction.snapshot()
            iid = transition.interaction.interaction_id
            observation = transition.observation
            if replay_mode == "FULL" and observation is not None and transition.interaction.status.value == "ACTIVE":
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
                    "source_fingerprint": current.source_fingerprint,
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
                    "location_interaction_observed", iid,
                    f"aggregate:{current.timeframe}:{current.time}", observation_values,
                    observed_at=observation.observed_at,
                    available_at=observation.available_at,
                ))
            if transition.created:
                facts.append(_fact("interaction_created", iid, f"aggregate:{current.timeframe}:{current.time}", {
                    "interaction_id": iid, "location": str(transition.interaction.location),
                    "timeframe": transition.interaction.timeframe,
                    "reference_generation": transition.interaction.reference_generation,
                    "reference_level": transition.interaction.reference_level,
                    "band_low": transition.interaction.band_low, "band_high": transition.interaction.band_high,
                    "source_aggregate_id": observation.source_aggregate_id if observation is not None else None,
                    "source_observation_id": observation.source_observation_id if observation is not None else None,
                    "stage": "OBSERVING_LOCATION", "evidence": [current.to_dict()],
                }, observed_at=current.end_time, available_at=current.available_at))
            if transition.interaction.status.value == "ENDED" and transition.departed:
                facts.append(_fact("interaction_ended", iid, iid, {
                    "interaction_id": iid, "location": str(transition.interaction.location),
                    "timeframe": transition.interaction.timeframe,
                    "reference_generation": transition.interaction.reference_generation,
                    "reason": snap["close_reason"],
                    "source_observation_id": observation.source_observation_id if observation is not None else None,
                    "terminal_candle_id": snap["terminal_candle_id"],
                    "terminal_close_side": snap["terminal_close_side"],
                }, observed_at=snap["candles"][-1]["end_time"],
                   available_at=snap["candles"][-1]["available_at"]))
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
                            "source_observation_id": observation.source_observation_id if observation else None,
                            "evidence_ids": [candle["candle_id"]],
                            "current_available_at": current.available_at,
                        }, observed_at=candle["end_time"], available_at=candle["available_at"]))
                        break
            if current.timeframe != "5m" or snap["location"] == "VWAP":
                continue
            is_pd = snap["location"] in {"PD_LOW", "PD_HIGH"}
            policy = PDPolicy() if is_pd else StandardLocationPolicy()
            selected = policy.templates_for(tuple(templates.values()), transition.interaction)
            state = detector.interactions.get(iid)
            role_events: list[dict[str, Any]] = []
            if not selected:
                continue
            if is_pd:
                evaluations = PDPolicy().evaluate_hypotheses(
                    selected, transition.interaction, available5,
                    as_of=current.available_at, state=state,
                    emit=role_events.append,
                )
            else:
                evaluations = StandardLocationPolicy().arbitrate(
                    selected, transition.interaction, available5,
                    as_of=current.available_at,
                )
            for role_event in role_events:
                kind = str(role_event.get("kind", "role_transition"))
                # Policy evaluation may revisit the same causal prefix on each
                # clock tick; publish the interaction context only once in the
                # replay stream while retaining every role transition.
                if kind == "role_context_available" and iid in role_context_emitted:
                    continue
                if kind == "role_context_available":
                    role_context_emitted.add(iid)
                facts.append(_fact(
                    kind, iid, iid, role_event,
                    observed_at=str(role_event["observed_at"]),
                    available_at=str(role_event["available_at"]),
                ))
            for evaluation in evaluations:
                outcome = evaluation.outcome
                identity = evaluation.identity or f"{iid}:{evaluation.template.template_id}:{outcome.status.value}"
                response = outcome.response
                facts.append(_fact("opportunity_outcome", identity, iid, {
                    "interaction_id": iid,
                    "location": snap["location"],
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
                if response is not None:
                    facts.append(_fact("hypothesis_arbitrated", identity, iid, {
                        "interaction_id": iid, "location": snap["location"],
                        "template_id": evaluation.template.template_id,
                        "response_type": evaluation.template.response_type.value,
                        "direction": response.direction.value,
                        "arbitration_selected": evaluation.selected,
                        "arbitration_reason": evaluation.arbitration_reason,
                    }, observed_at=response.observed_at, available_at=response.available_at))
                    if not is_pd:
                        facts.append(_fact("response_confirmed", identity, iid, {
                            "interaction_id": iid, "location": snap["location"],
                            "template_id": evaluation.template.template_id,
                            "response_type": evaluation.template.response_type.value,
                            "direction": response.direction.value,
                            "evidence_ids": list(response.evidence_ids),
                        }, observed_at=response.observed_at, available_at=response.available_at))
    return InteractionResponseReplay(tuple(facts))


__all__ = ["InteractionResponseReplay", "replay_interaction_response"]
