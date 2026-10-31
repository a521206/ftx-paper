"""Runtime-neutral causal interaction and response replay boundary."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from .context_location_replay import ContextLocationReplay
from .interaction_detector import InteractionDetector
from .location_observation import LocationObservation
from .orderflow_common import Aggregate
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
    if not isinstance(observed, str) or not isinstance(available, str):
        raise ValueError("interaction parity fact timestamps are required")
    return {"kind": kind, "occurred_at": observed, "observed_at": observed, "available_at": available,
            "identity": identity, "causal_parent_id": parent, "values": dict(snapshot), "unavailable": list(unavailable)}


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
        observations[source] = tuple(parsed)
    detector = InteractionDetector(registry)
    facts: list[dict[str, Any]] = []
    response_emitted: set[tuple[str, str]] = set()
    role_emitted: set[str] = set()
    vwap_emitted: set[str] = set()
    for current in all_bars:
        source = f"aggregate:{current.timeframe}:{current.time}"
        available1 = [item for item in one if item.available_at <= current.available_at]
        available5 = [item for item in five if item.available_at <= current.available_at]
        typed = tuple(
            item for item in observations.get(source, ())
            if item.available_at <= current.available_at
        )
        if current.timeframe == "1m":
            transitions = detector.observe_1m(current=current, available=available1, atr=None, observations=typed)
        else:
            one_observations = tuple(item for key, values in observations.items() if key.startswith("aggregate:1m:") for item in values if item.available_at <= current.available_at)
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
            policy = PDPolicy() if snap["location"] in {"PD_LOW", "PD_HIGH"} else StandardLocationPolicy()
            selected = tuple(item for item in templates.values() if snap["location"] in {loc.value for loc in item.locations})
            if selected and current.timeframe == "5m":
                if isinstance(policy, PDPolicy) and iid not in role_emitted:
                    role_emitted.add(iid)
                    facts.append(_fact("role_context_available", iid, iid, {
                        "interaction_id": iid, "location": snap["location"],
                        "role": "SUPPORT" if snap["location"] == "PD_LOW" else "RESISTANCE",
                    }, observed_at=transition.interaction.interaction_started_at,
                       available_at=transition.interaction.available_at))
                role_events: list[dict[str, Any]] = []
                evaluations = (policy.evaluate_hypotheses(selected, transition.interaction, available5,
                                                          as_of=current.available_at,
                                                          emit=role_events.append)
                               if isinstance(policy, PDPolicy) else
                               policy.arbitrate(selected, transition.interaction, available5,
                                                as_of=current.available_at))
                for role_event in role_events:
                    facts.append(_fact(
                        "role_transition", iid, iid, role_event,
                        observed_at=str(role_event["observed_at"]),
                        available_at=str(role_event["available_at"]),
                    ))
                for evaluation in evaluations:
                    outcome = evaluation.outcome
                    if outcome.status.value == "PENDING":
                        continue
                    identity = evaluation.identity or f"{iid}:{evaluation.template.template_id}:{outcome.status.value}"
                    if (identity, outcome.status.value) in response_emitted:
                        continue
                    response_emitted.add((identity, outcome.status.value))
                    response = outcome.response
                    if response is not None:
                        facts.append(_fact("hypothesis_arbitrated", identity, iid, {
                            "interaction_id": iid, "location": snap["location"],
                            "template_id": evaluation.template.template_id,
                            "response_type": evaluation.template.response_type.value,
                            "direction": response.direction.value,
                            "arbitration_selected": evaluation.selected,
                            "arbitration_reason": evaluation.arbitration_reason,
                        }, observed_at=response.observed_at, available_at=response.available_at))
                        if snap["location"] not in {"PD_LOW", "PD_HIGH"}:
                            facts.append(_fact("response_confirmed", identity, iid, {
                                "interaction_id": iid, "location": snap["location"],
                                "template_id": evaluation.template.template_id,
                                "response_type": evaluation.template.response_type.value,
                                "direction": response.direction.value,
                                "evidence_ids": list(response.evidence_ids),
                            }, observed_at=response.observed_at, available_at=response.available_at))
                    facts.append(_fact("opportunity_outcome", identity, iid, {
                        "interaction_id": iid, "location": snap["location"],
                        "template_id": evaluation.template.template_id,
                        "response_type": evaluation.template.response_type.value,
                        "direction": response.direction.value if response is not None else evaluation.template.direction.value,
                        "status": outcome.status.value, "reason": outcome.reason,
                        "arbitration_selected": evaluation.selected,
                        "arbitration_reason": evaluation.arbitration_reason,
                        "as_of": outcome.as_of,
                        "evidence_ids": list(response.evidence_ids) if response is not None else [item.evidence_id for item in outcome.evidence_lineage],
                        "evidence_lineage": [dataclasses.asdict(item) for item in outcome.evidence_lineage],
                    }, observed_at=outcome.observed_at, available_at=outcome.available_at))
    return InteractionResponseReplay(tuple(facts))


__all__ = ["InteractionResponseReplay", "replay_interaction_response"]
