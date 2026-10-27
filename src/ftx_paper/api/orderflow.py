"""Trader-facing orderflow projection built from canonical persisted views."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first(*values: Any) -> Any:
    return next((value for value in values if value is not None and value != ""), None)


def _direction(value: Any) -> str | None:
    if value is None or value == "":
        return None
    text = str(value).upper()
    return {"BUY": "LONG", "SELL": "SHORT", "BULLISH": "LONG", "BEARISH": "SHORT"}.get(text, text)


def build_orderflow_projection(
    decisions: Iterable[Mapping[str, Any]],
    events: Iterable[Mapping[str, Any]],
    positions: Iterable[Mapping[str, Any]],
    trades: Iterable[Mapping[str, Any]],
    *,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Project the latest orderflow decision into stable trader concepts."""
    canonical = [row for row in decisions if isinstance(row.get("decision_view"), Mapping)]
    canonical.sort(key=lambda row: str(row["decision_view"].get("at") or row.get("created_at") or ""), reverse=True)
    latest = canonical[0] if canonical else {}
    view = _mapping(latest.get("decision_view"))
    subject = _mapping(view.get("subject"))
    policy = _mapping(view.get("policy"))
    evidence = _mapping(view.get("evidence"))
    metrics = _mapping(evidence.get("metrics"))
    provenance = _mapping(evidence.get("provenance"))
    state = _mapping(evidence.get("state"))
    direction = _direction(_first(view.get("direction"), subject.get("direction")))
    entry = _first(metrics.get("entry_price"), metrics.get("fill_price"))
    stop = _first(metrics.get("stop"), metrics.get("stop_price"))
    target = _first(metrics.get("target_price"), metrics.get("target"))
    risk_reward = None
    if entry is not None and stop is not None and target is not None:
        risk = abs(float(entry) - float(stop))
        risk_reward = round(abs(float(target) - float(entry)) / risk, 2) if risk else None
    raw_outcome = _first(view.get("display_outcome"), view.get("outcome"), policy.get("outcome"))
    outcome = str(raw_outcome) if raw_outcome is not None else None
    reason = _first(policy.get("message"), policy.get("reason_code"), view.get("detail"))
    action_name = "Exit / manage position" if str(view.get("event_type", "")).upper() == "EXITDECISION" else (
        "Trade now" if outcome in {"accepted", "triggered", "filled"} else
        "Wait for confirmation" if str(state.get("response_state", "")).upper() in {"PENDING_RESPONSE", "CONFIRMED"} and outcome == "rejected" else
        "No trade" if outcome is not None else None
    )
    location_keys: dict[str, dict[str, Any]] = {}
    for row in canonical:
        row_view = _mapping(row.get("decision_view"))
        row_evidence = _mapping(row_view.get("evidence"))
        row_provenance = _mapping(row_evidence.get("provenance"))
        row_state = _mapping(row_evidence.get("state"))
        location = _first(row_view.get("context"), _mapping(row_view.get("subject")).get("location"), row_provenance.get("cell"))
        if location is None:
            continue
        location_keys[str(location)] = {
            "name": str(location).replace("_", " ").title(),
            "location": location,
            "reference": _first(row_provenance.get("reference_level"), _mapping(row_evidence.get("metrics")).get("entry_price")),
            "distance": _first(row_state.get("current_price_distance"), row_provenance.get("distance")),
            "interaction_status": row_state.get("interaction_state"),
            "response_status": row_state.get("response_state"),
            "implication": _direction(_first(row_view.get("direction"), _mapping(row_view.get("subject")).get("direction"))),
        }
    sequence = []
    views_by_id = {str(_mapping(row.get("decision_view")).get("decision_id")): _mapping(row.get("decision_view")) for row in canonical}
    for row in sorted(events, key=lambda item: str(item.get("created_at") or item.get("payload", {}).get("decision_at") or "")):
        payload = _mapping(row.get("payload"))
        row_view = _mapping(row.get("decision_view")) or views_by_id.get(str(payload.get("decision_id")), {})
        if not row_view:
            row_view = _mapping(payload.get("decision_view"))
        if not row_view and payload.get("strategy_id") != "orderflow-research":
            continue
        if row_view:
            row_evidence = _mapping(row_view.get("evidence"))
            row_state = _mapping(row_evidence.get("state"))
            row_policy = _mapping(row_view.get("policy"))
            event_type = str(row.get("event_type") or "").upper()
            lifecycle_transition = {"FILL": "entry accepted", "EXECUTEDDECISION": "entry accepted", "ORDER_SUPPRESSED": "entry rejected", "RISK_REJECTED": "entry rejected", "EXITDECISION": "exit / manage"}.get(event_type)
            sequence.append({
                "time": _first(row.get("created_at"), row_view.get("at"), payload.get("decision_at")),
                "location": _first(row_view.get("context"), _mapping(row_view.get("subject")).get("location")),
                "transition": _first(lifecycle_transition, row_state.get("response_state"), row_state.get("interaction_state"), row_policy.get("reason_code"), row_view.get("stage")),
                "direction": _direction(_first(row_view.get("direction"), _mapping(row_view.get("subject")).get("direction"))),
                "implication": _first(row_policy.get("message"), row_policy.get("reason_code"), row_view.get("display_outcome")),
                "evidence": row,
            })
    return {
        "as_of": _first(view.get("at"), latest.get("created_at")),
        "updated_at": as_of,
        "bias": direction,
        "market_state": _first(state.get("response_state"), state.get("interaction_state"), view.get("stage"), "Not reported"),
        "setup": {"name": _first(subject.get("setup"), view.get("context")), "location": view.get("context"), "stage": view.get("stage")},
        "action": {"name": action_name, "direction": direction, "outcome": outcome, "entry_trigger": _first(provenance.get("trigger"), policy.get("message")), "entry_price": entry, "trigger_zone": {"lower": provenance.get("lower_band"), "upper": provenance.get("upper_band")}, "stop_basis": _first(metrics.get("stop_basis"), policy.get("reason_code")), "stop_price": stop, "target": target, "exit_condition": _first(policy.get("exit_mode"), metrics.get("exit_mode")), "risk_reward": risk_reward, "reason": reason},
        "locations": list(location_keys.values()),
        "sequence": sequence[-100:],
        "position": {"open": list(positions)},
        "trades": list(trades),
        "provenance": {"decision_id": view.get("decision_id"), "decision_at": view.get("at"), "source": provenance},
    }
