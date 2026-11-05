"""Presentation read model for the orderflow market-context screen."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


_STAGES = ("interaction", "opportunity", "candidate", "admission", "trigger", "execution", "exit")


def _clean(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value).replace("_", " ")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _view_item(row: Mapping[str, Any]) -> dict[str, Any]:
    view = _mapping(row.get("decision_view"))
    subject = _mapping(view.get("subject"))
    policy = _mapping(view.get("policy"))
    evidence = _mapping(view.get("evidence"))
    provenance = _mapping(evidence.get("provenance"))
    state = _mapping(evidence.get("state"))
    stage = _clean(view.get("stage")) or "transition"
    outcome = _clean(view.get("outcome") or policy.get("outcome")) or "recorded"
    location = subject.get("location")
    title = location or subject.get("setup") or subject.get("instrument") or view.get("event_type") or "Market state"
    detail = policy.get("message") or policy.get("reason_code") or view.get("detail") or view.get("event_type") or "State transition recorded"
    return {
        "at": view.get("at") or row.get("created_at"),
        "title": str(title),
        "detail": str(detail),
        "stage": stage,
        "outcome": outcome,
        "location": str(location) if location else None,
        "direction": subject.get("direction") or state.get("direction"),
        "instrument": provenance.get("instrument") or subject.get("instrument"),
        "timeframe": provenance.get("timeframe"),
        "reference_level": provenance.get("reference_level"),
        "decision_id": view.get("decision_id"),
        "evidence": dict(evidence),
    }


def build_market_context(decisions: Sequence[Mapping[str, Any]], session_date: str) -> dict[str, Any]:
    """Build the trader-facing orderflow context projection from canonical decisions."""
    items = sorted((_view_item(row) for row in decisions), key=lambda item: str(item.get("at") or ""), reverse=True)
    latest = items[0] if items else None
    locations: dict[str, dict[str, Any]] = {}
    for item in items:
        location = item.get("location")
        if location and location not in locations:
            locations[location] = {
                key: item[key]
                for key in ("location", "detail", "stage", "outcome", "direction", "instrument", "timeframe", "reference_level", "at")
            }
    stage_counts = {stage: sum(stage in str(item["stage"]).lower() for item in items) for stage in _STAGES}
    return {
        "date": session_date,
        "as_of": latest.get("at") if latest else None,
        "interpretation": latest.get("detail") if latest else "No structural market read has been recorded",
        "state": latest.get("stage") if latest else "awaiting read",
        "state_outcome": latest.get("outcome") if latest else None,
        "locations": list(locations.values()),
        "lifecycle": {
            "stage_counts": stage_counts,
            "transition_count": len(items),
            "transitions": [
                {key: item[key] for key in ("at", "title", "detail", "stage", "outcome", "location", "direction", "decision_id")}
                for item in items[:12]
            ],
        },
    }


__all__ = ["build_market_context"]
