"""Projection of producer-owned replay facts.

This module deliberately does not derive execution evidence from trades,
terminal state, or presentation reports.  The execution owner must publish
facts into ``parity_streams`` before they can appear in a replay payload.
"""

from __future__ import annotations

from collections import Counter
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


def _execution_diagnostic_summary(records: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, Any]:
    """Publish compact causal coverage alongside the raw diagnostic trace."""
    by_kind = Counter(str(record.get("kind", "")) for record in records)
    by_lifecycle = Counter(
        str(record.get("lifecycle_id"))
        for record in records
        if record.get("lifecycle_id") is not None
    )
    return {
        "record_count": len(records),
        "by_kind": dict(sorted(by_kind.items())),
        "lifecycle_count": len(by_lifecycle),
        "records_by_lifecycle": dict(sorted(by_lifecycle.items())),
        "first_sequence": records[0].get("sequence") if records else None,
        "last_sequence": records[-1].get("sequence") if records else None,
    }


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
    execution_states = (
        tuple(context.execution_engine.snapshot().values())
        if getattr(context, "execution_engine", None) is not None else ()
    )
    execution_fills = [
        asdict(fill) for state in execution_states for fill in state.fills
    ]
    execution_transitions = list(streams.get("execution_transitions", ()))
    ledger_mutations = list(streams.get("ledger_mutations", ()))
    if not ledger_mutations:
        for state in execution_states:
            parent = state.opportunity_id
            for reservation in state.reservation_history:
                ledger_mutations.append({
                    "kind": "reservation",
                    "opportunity_id": parent,
                    **asdict(reservation),
                })
                if reservation.released_at:
                    ledger_mutations.append({
                        "kind": "reservation_release",
                        "opportunity_id": parent,
                        "reservation_id": reservation.reservation_id,
                        "occurred_at": reservation.released_at,
                        "reason": reservation.release_reason,
                    })
            for admission in state.admission_history:
                if admission.status.value != "NOT_EVALUATED":
                    ledger_mutations.append({
                        "kind": "authorization",
                        "opportunity_id": parent,
                        **asdict(admission),
                    })
            for fill in state.fills:
                ledger_mutations.append({
                    "kind": (
                        "exit_settlement"
                        if fill.kind in {"exit", "partial_exit"}
                        else "fill"
                    ),
                    "opportunity_id": parent,
                    **asdict(fill),
                })
    phase7_input = {
        "parity_phase_facts": streams,
        "execution_fills": execution_fills,
        "execution_transitions": execution_transitions,
        "ledger_mutations": ledger_mutations,
    }
    phase_facts = {
        name: _json_safe(list(streams[name]))
        for name in PHASE7_STREAMS
        if name in streams
    }
    execution_diagnostics = tuple(getattr(context, "execution_diagnostics", ()))
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
        "parity_phase_facts": {
            **phase_facts,
            **build_phase7_projection(phase7_input),
        },
        "execution_fills": _json_safe(execution_fills),
        "execution_transitions": _json_safe(execution_transitions),
        "ledger_mutations": _json_safe(ledger_mutations),
        "execution_diagnostics": _json_safe(execution_diagnostics),
        "execution_diagnostic_summary": _execution_diagnostic_summary(execution_diagnostics),
    }
    # Validate the producer-owned shape without manufacturing missing streams.
    if phase_facts or execution_states:
        payload["phase7_projection"] = build_phase7_projection(
            phase7_input
        )
    return payload


__all__ = ["build_replay_payload"]
