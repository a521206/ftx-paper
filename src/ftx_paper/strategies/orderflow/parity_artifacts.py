"""Producer-neutral Phase 4 parity artifacts.

This module accepts only facts emitted by the orderflow runtime.  It never
reconstructs context, locations, permissions, or replacements from bars or
terminal state.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

PHASE_STREAMS = ("source_observation", "context_location", "interaction_response", "opportunity_lifecycle", "intent_admission", "trigger_exit")
UTC = timezone.utc


def _json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str).encode()


def canonical_hash(value: object) -> str:
    return hashlib.sha256(_json(value)).hexdigest()


def _timestamp(value: object) -> str:
    if value is None:
        raise ValueError("parity timestamps are required")
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("parity timestamps must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("parity timestamps must be timezone-aware")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _copy(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_copy(item) for item in value]
    return value


def normalize_phase_record(record: Mapping[str, Any], *, sequence: int) -> dict[str, Any]:
    required = {"kind", "occurred_at", "observed_at", "available_at", "identity", "causal_parent_id", "values", "unavailable"}
    missing = required - set(record)
    if missing:
        raise ValueError(f"phase record missing {sorted(missing)}")
    result = _copy(record)
    result["occurred_at"] = _timestamp(result["occurred_at"])
    result["observed_at"] = _timestamp(result["observed_at"])
    result["available_at"] = _timestamp(result["available_at"])
    if result["available_at"] < result["observed_at"]:
        raise ValueError("phase record availability precedes observation")
    result["sequence"] = sequence
    if not isinstance(result["identity"], str) or not result["identity"]:
        raise ValueError("phase record identity is required")
    if not isinstance(result["values"], Mapping):
        raise ValueError("phase record values must be an object")
    if not isinstance(result["unavailable"], list) or any(not isinstance(item, str) for item in result["unavailable"]):
        raise ValueError("phase record unavailable must be a list of strings")
    parent = result["causal_parent_id"]
    if parent is not None and not isinstance(parent, str):
        raise ValueError("phase record causal_parent_id must be a string or null")
    return result


def _manifest(records: Sequence[Mapping[str, Any]], *, status: str, observed: bool) -> dict[str, Any]:
    return {"status": status, "observed": observed, "count": len(records), "sha256": canonical_hash(records)}


def phase_facts_from_payload(payload: Mapping[str, Any], stream: str = "context_location") -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
    """Read one explicit runtime phase stream; missing facts stay unavailable."""
    if stream not in PHASE_STREAMS:
        raise ValueError(f"unknown phase stream: {stream}")
    raw = payload.get("parity_phase_facts")
    if not isinstance(raw, Mapping):
        direct = payload.get(stream)
        raw = {stream: direct} if direct is not None else None
    if not isinstance(raw, Mapping) or stream not in raw:
        return (), {"status": "UNAVAILABLE", "observed": False, "reason": f"missing_{stream}_facts"}
    records = raw[stream]
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        return (), {"status": "INCOMPLETE", "observed": False, "reason": f"invalid_{stream}_facts"}
    try:
        normalized = tuple(normalize_phase_record(item, sequence=index) for index, item in enumerate(records) if isinstance(item, Mapping))
    except ValueError as exc:
        return (), {"status": "INCOMPLETE", "observed": True, "reason": str(exc)}
    if len(normalized) != len(records):
        return (), {"status": "INCOMPLETE", "observed": True, "reason": f"invalid_{stream}_record"}
    return normalized, {"status": "MATCHED", "observed": True, "coverage": "observed_empty" if not normalized else "observed"}


def build_phase4_artifact(*, run_id: str, producer: Mapping[str, Any], scope: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
    records, coverage = phase_facts_from_payload(payload)
    phase_streams: dict[str, list[dict[str, Any]]] = {}
    manifests: dict[str, dict[str, Any]] = {}
    for stream in PHASE_STREAMS:
        if stream == "context_location":
            current = list(records)
            current_coverage = coverage
        else:
            current, current_coverage = phase_facts_from_payload(payload, stream)
            current = list(current)
        phase_streams[stream] = current
        manifests[stream] = _manifest(current, status=str(current_coverage["status"]), observed=bool(current_coverage["observed"]))
        manifests[stream].update({key: value for key, value in current_coverage.items() if key in {"reason", "coverage"}})
    return {
        "artifact_type": "orderflow_phase4_parity",
        "schema_version": 1,
        "contract": {"id": "orderflow_phase_streams", "version": 1, "streams": list(PHASE_STREAMS)},
        "producer": _copy(producer), "run_id": run_id, "scope": _copy(scope),
        "phase_streams": phase_streams,
        "manifest": {"version": 1, "hash_algorithm": "sha256", "run_id": run_id, "streams": manifests},
    }


def compare_phase4_manifests(canonical: Mapping[str, Any], paper: Mapping[str, Any], *, canonical_records: Any = None, paper_records: Any = None, streams: Sequence[str] = PHASE_STREAMS) -> dict[str, Any]:
    """Compare streams in order, fetching records only after hash mismatch."""
    left_streams = canonical.get("manifest", {}).get("streams", {})
    right_streams = paper.get("manifest", {}).get("streams", {})
    unknown = set(streams) - set(PHASE_STREAMS)
    if unknown:
        raise ValueError(f"unknown phase streams: {sorted(unknown)}")
    for stream in streams:
        left = left_streams.get(stream, {})
        right = right_streams.get(stream, {})
        if left.get("status") != "MATCHED" or right.get("status") != "MATCHED":
            status = "INCOMPLETE" if "INCOMPLETE" in {left.get("status"), right.get("status")} else "UNAVAILABLE"
            return {"status": status, "stream": stream, "canonical": left, "paper": right}
        if (left.get("count"), left.get("sha256")) == (right.get("count"), right.get("sha256")):
            continue
        if canonical_records is None or paper_records is None:
            return {"status": "MISMATCHED", "stream": stream, "reason": "manifest", "canonical": left, "paper": right}
        canonical_values = canonical_records(stream)
        paper_values = paper_records(stream)
        for index, (canonical_value, paper_value) in enumerate(zip(canonical_values, paper_values)):
            if canonical_value != paper_value:
                path, left_value, right_value = _first_mismatch(canonical_value, paper_value)
                return {
                    "status": "MISMATCHED", "stream": stream, "index": index,
                    "field": path, "canonical_value": left_value,
                    "paper_value": right_value,
                    "canonical_record": canonical_value, "paper_record": paper_value,
                }
        return {"status": "MISMATCHED", "stream": stream, "reason": "count", "canonical_count": len(canonical_values), "paper_count": len(paper_values)}
    return {"status": "MATCHED"}


def _first_mismatch(left: Any, right: Any, path: str = "") -> tuple[str, Any, Any]:
    """Return the deterministic first differing field in two normalized facts."""
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        for key in sorted(set(left) | set(right), key=str):
            child = f"{path}.{key}" if path else str(key)
            if key not in left or key not in right:
                return child, left.get(key), right.get(key)
            if left[key] != right[key]:
                return _first_mismatch(left[key], right[key], child)
        return path, left, right
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            if left_item != right_item:
                return _first_mismatch(left_item, right_item, f"{path}[{index}]")
        return f"{path}[{min(len(left), len(right))}]", left, right
    return path, left, right


__all__ = ["PHASE_STREAMS", "build_phase4_artifact", "canonical_hash", "compare_phase4_manifests", "normalize_phase_record", "phase_facts_from_payload"]
