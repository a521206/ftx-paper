"""Runtime-neutral causal interaction and response replay boundary."""

from __future__ import annotations

from dataclasses import dataclass
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
    emitted: set[tuple[str, str]] = set()
    response_emitted: set[tuple[str, str]] = set()
    for current in all_bars:
        source = f"aggregate:{current.timeframe}:{current.time}"
        typed = observations.get(source, ())
        available1 = [item for item in one if item.available_at <= current.available_at]
        available5 = [item for item in five if item.available_at <= current.available_at]
        if current.timeframe == "1m":
            transitions = detector.observe_1m(current=current, available=available1, atr=None, observations=typed)
        else:
            one_observations = tuple(item for key, values in observations.items() if key.startswith("aggregate:1m:") for item in values if item.available_at <= current.available_at)
            transitions = detector.observe(current=current, available5=available5, available1=available1, atr=None, observations5=typed, observations1=one_observations)
        for transition in transitions:
            snap = transition.interaction.snapshot()
            iid = transition.interaction.interaction_id
            kind = "interaction_created" if transition.created else "interaction_departure" if transition.departed else "interaction_observed"
            key = (kind, f"{iid}:{current.time}")
            if key not in emitted:
                facts.append(_fact(kind, iid, f"aggregate:{current.timeframe}:{current.time}", snap))
                emitted.add(key)
            if transition.departed:
                facts.append(_fact("interaction_closed", iid, iid, snap))
            policy = PDPolicy() if snap["location"] in {"PD_LOW", "PD_HIGH"} else StandardLocationPolicy()
            selected = tuple(item for item in templates.values() if snap["location"] in {loc.value for loc in item.locations})
            if selected and current.timeframe == "5m":
                evaluations = policy.evaluate_hypotheses(selected, transition.interaction, available5, as_of=current.available_at) if isinstance(policy, PDPolicy) else policy.arbitrate(selected, transition.interaction, available5, as_of=current.available_at)
                for evaluation in evaluations:
                    outcome = evaluation.outcome
                    if outcome.status.value == "PENDING":
                        continue
                    identity = evaluation.identity or f"{iid}:{evaluation.template.template_id}:{outcome.status.value}"
                    if (identity, outcome.status.value) in response_emitted:
                        continue
                    response_emitted.add((identity, outcome.status.value))
                    facts.append(_fact("response_confirmed" if outcome.status.value == "RESOLVED" else "response_invalidated" if outcome.status.value == "CONTRADICTED" else "response_expired", identity, iid, {"template_id": evaluation.template.template_id, "status": outcome.status.value, "reason": outcome.reason, "selected": evaluation.selected, "arbitration_reason": evaluation.arbitration_reason, "evidence_ids": list(outcome.evidence_lineage)}, observed_at=outcome.observed_at, available_at=outcome.available_at))
    return InteractionResponseReplay(tuple(facts))


__all__ = ["InteractionResponseReplay", "replay_interaction_response"]
