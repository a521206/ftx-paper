"""Projection of producer-owned replay facts.

This module deliberately does not derive execution evidence from trades,
terminal state, or presentation reports.  The execution owner must publish
facts into ``parity_streams`` before they can appear in a replay payload.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, cast
from zoneinfo import ZoneInfo

from .parity_artifacts import LAYERS, PHASE_STREAMS


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


def _transport_parity_facts(raw: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    """Copy producer facts and normalize causal timestamps to UTC."""
    result: list[dict[str, Any]] = []
    for sequence, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise ValueError("parity facts must contain objects")
        fact = _json_safe(dict(item))
        fact.setdefault("sequence", sequence)
        for field in ("occurred_at", "observed_at", "available_at"):
            value = fact.get(field)
            if value is None:
                continue
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
            fact[field] = parsed.astimezone(timezone.utc).isoformat(
                timespec="microseconds"
            ).replace("+00:00", "Z")
        result.append(fact)
    return result


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
    layers = getattr(context, "parity_layers", {})
    if not isinstance(streams, Mapping) or not isinstance(layers, Mapping):
        raise ValueError("replay context parity facts must be mappings")
    # Parity evidence is producer-owned.  Terminal execution snapshots and
    # presentation records are deliberately not evidence sources.
    observed = getattr(context, "parity_observed", None)
    observed_streams = set(observed) if isinstance(observed, set) else set(streams)
    phase_facts = {
        name: _transport_parity_facts(streams[name]) if name in streams and name in observed_streams else []
        for name in PHASE_STREAMS
    }
    observed_layers = set(layers)
    parity_layers = {
        name: _transport_parity_facts(layers[name]) if name in layers and name in observed_layers else []
        for name in LAYERS
    }
    coverage = {
        "phase_streams": {
            name: {
                "status": "MATCHED" if name in streams and name in observed_streams else "UNAVAILABLE",
                "observed": name in streams and name in observed_streams,
                "coverage": "observed_empty" if name in streams and name in observed_streams and not phase_facts[name] else (
                    "observed" if name in streams and name in observed_streams else "unavailable"
                ),
            }
            for name in PHASE_STREAMS
        },
        "layers": {
            name: {
                "status": "MATCHED" if name in layers and name in observed_layers else "UNAVAILABLE",
                "observed": name in layers and name in observed_layers,
                "coverage": "observed_empty" if name in layers and name in observed_layers and not parity_layers[name] else (
                    "observed" if name in layers and name in observed_layers else "unavailable"
                ),
            }
            for name in LAYERS
        },
    }
    execution_fills = _transport_parity_facts(streams["execution_fills"]) if "execution_fills" in streams and "execution_fills" in observed_streams else []
    execution_transitions = _transport_parity_facts(streams["execution_transitions"]) if "execution_transitions" in streams and "execution_transitions" in observed_streams else []
    ledger_mutations = _transport_parity_facts(streams["ledger_mutations"]) if "ledger_mutations" in streams and "ledger_mutations" in observed_streams else []
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
        "scope": {"timezone": "Asia/Kolkata"},
        "parity_phase_facts": phase_facts,
        "parity_layers": parity_layers,
        "parity_coverage": coverage,
        "execution_diagnostics": _json_safe(execution_diagnostics),
        "execution_diagnostic_summary": _execution_diagnostic_summary(execution_diagnostics),
        "input_diagnostics": _json_safe(tuple(getattr(context, "input_diagnostics", ()))),
    }
    # Validate the producer-owned shape without manufacturing missing streams.
    for name, values in (("execution_fills", execution_fills),
                         ("execution_transitions", execution_transitions),
                         ("ledger_mutations", ledger_mutations)):
        if name in streams and name in observed_streams:
            payload[name] = values
    return payload


__all__ = ["build_replay_payload"]
