"""Projection of producer-owned replay facts.

This module deliberately does not derive execution evidence from trades,
terminal state, or presentation reports.  The execution owner must publish
facts into ``parity_streams`` before they can appear in a replay payload.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, cast

from .phase7_projection import PHASE7_STREAMS, build_phase7_projection


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(cast(Any, value)))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted((_json_safe(item) for item in value), key=str)
    return value


def build_replay_payload(
    machine: Any,
    records: list[dict[str, Any]],
    replay_records: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    capture_mode: str = "full",
) -> dict[str, Any]:
    """Build a bounded payload from facts explicitly emitted by the machine."""
    context = getattr(machine, "context", machine)
    streams = getattr(context, "parity_streams", {})
    if not isinstance(streams, dict):
        streams = {}
    phase_facts = {
        name: _json_safe(list(streams[name]))
        for name in PHASE7_STREAMS
        if name in streams
    }
    payload: dict[str, Any] = {
        "artifact_type": "orderflow_state_machine_trace",
        "schema_version": 7,
        "capture_mode": capture_mode,
        "replay_mode": capture_mode,
        "date": rows[0].get("session_date") if rows else None,
        "causal": True,
        "source_bar_count": len(rows),
        "records": _json_safe(records),
        "replay_records": _json_safe(replay_records),
        "parity_phase_facts": phase_facts,
    }
    # Validate the producer-owned shape without manufacturing missing streams.
    if phase_facts:
        payload["phase7_projection"] = build_phase7_projection(
            {"parity_phase_facts": phase_facts}
        )
    return payload


__all__ = ["build_replay_payload"]
