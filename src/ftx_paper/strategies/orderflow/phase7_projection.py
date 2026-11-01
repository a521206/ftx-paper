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
    required = {"identity", "occurred_at", "observed_at", "available_at", "causal_parent_id", "values", "unavailable"}
    missing = required - set(raw)
    if missing:
        raise ValueError(f"{stream} fact missing {sorted(missing)}")
    return {
        "kind": str(raw.get("kind", stream)),
        "occurred_at": raw["occurred_at"],
        "observed_at": raw["observed_at"],
        "available_at": raw["available_at"],
        "sequence": sequence,
        "identity": str(raw["identity"]),
        "causal_parent_id": raw["causal_parent_id"],
        "values": dict(raw["values"]),
        "unavailable": list(raw["unavailable"]),
    }


def build_phase7_projection(payload: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    facts = payload.get("parity_phase_facts")
    facts = facts if isinstance(facts, Mapping) else {}
    result: dict[str, list[dict[str, Any]]] = {}
    for stream in PHASE7_STREAMS:
        raw = facts.get(stream, [])
        if not isinstance(raw, list):
            raise ValueError(f"{stream} facts must be a list")
        result[stream] = [_record(stream, item, index) for index, item in enumerate(raw) if isinstance(item, Mapping)]
        if len(result[stream]) != len(raw):
            raise ValueError(f"{stream} facts must contain objects")
    return result


__all__ = ["PHASE7_STREAMS", "build_phase7_projection"]
