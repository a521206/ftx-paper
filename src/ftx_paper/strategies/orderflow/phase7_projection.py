"""Projection of explicit execution facts for Phase 7 parity."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

PHASE7_STREAMS = (
    "intent_admission",
    "trigger_exit",
    "execution_fills",
    "execution_transitions",
    "ledger_mutations",
)


def _record(stream: str, raw: Mapping[str, Any], sequence: int) -> dict[str, Any]:
    values = dict(raw.get("values", raw))
    identity = raw.get("identity")
    if identity is None:
        identity = next((f"{name}:{raw[name]}" for name in
                         ("event_id", "fill_id", "reservation_id", "intent_id", "position_id", "opportunity_id")
                         if raw.get(name) is not None), None)
    if identity is None:
        identity = f"{stream}:{sequence}"
    elif raw.get("identity") is None:
        identity = f"{identity}:{sequence}"
    occurred = (raw.get("occurred_at") or raw.get("observed_at") or raw.get("filled_at")
                or raw.get("reserved_at") or raw.get("released_at") or raw.get("decided_at"))
    observed = raw.get("observed_at") or occurred
    available = raw.get("available_at") or occurred
    if occurred is None or observed is None or available is None:
        raise ValueError(f"{stream} fact is missing identity or causal timestamps")
    return {
        "kind": str(raw.get("kind", stream)),
        "occurred_at": occurred,
        "observed_at": observed,
        "available_at": available,
        "sequence": sequence,
        "identity": str(identity),
        "causal_parent_id": raw.get("causal_parent_id") or raw.get("opportunity_id") or raw.get("lifecycle_id"),
        "values": values,
        "unavailable": list(raw.get("unavailable", [])),
    }


def build_phase7_projection(payload: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    facts = payload.get("parity_phase_facts")
    facts = facts if isinstance(facts, Mapping) else {}
    result: dict[str, list[dict[str, Any]]] = {}
    for stream in PHASE7_STREAMS:
        raw = facts.get(stream)
        if raw is None:
            raw = payload.get(stream, [])
        if not isinstance(raw, list):
            raise ValueError(f"{stream} facts must be a list")
        result[stream] = [_record(stream, item, index) for index, item in enumerate(raw) if isinstance(item, Mapping)]
        if len(result[stream]) != len(raw):
            raise ValueError(f"{stream} facts must contain objects")
    return result


__all__ = ["PHASE7_STREAMS", "build_phase7_projection"]
