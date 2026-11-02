"""Canonical producer parity contract primitives.

This module accepts only facts emitted by the orderflow runtime.  It never
reconstructs context, locations, permissions, or replacements from bars or
terminal state.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
import hashlib
import json
import re
from typing import Any

from .timestamps import parse_causal_timestamp, parse_session_timestamp, parse_utc, serialize_utc
from .phase7_projection import PHASE7_STREAMS

ARTIFACT_TYPE = "producer_parity"
SCHEMA_VERSION = 1
CONTRACT_ID = "orderflow_producer_parity"
CONTRACT_VERSION = 1
FILENAME = "parity_artifact.json"

PHASE_STREAMS = ("source_observation", "context_location", "interaction_response", "opportunity_lifecycle", "intent_admission", "trigger_exit")
LAYERS = ("run_scope", "decision", "execution_settlement", "portfolio_ledger")
DECISION_STAGES = ("interaction", "candidate", "admission", "trigger", "exit")
EXECUTION_KINDS = (
    "reservation", "authorization", "submission", "order_intent", "fill",
    "pending_exit", "exit", "exit_fill", "cancellation", "settlement",
    "synthetic_settlement",
)
LEDGER_KINDS = (
    "reservation", "reservation_release", "fill", "partial_fill",
    "cancellation", "exit_settlement", "daily_reset", "closeout",
)
PARITY_PAYLOAD_KEYS = ("parity_phase_facts", "parity_layers", "parity_coverage")
_EXECUTION_GROUP_ID = re.compile(
    r"^(execution-group:(?P<session_date>\d{4}-\d{2}-\d{2}:[^:]+:[^:]+:)(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?))$"
)


def validate_contract_declarations(
    phase_streams: Mapping[str, Any], layers: Mapping[str, Any]
) -> None:
    """Require the complete canonical stream and layer declaration surface."""
    if set(phase_streams) != set(PHASE_STREAMS):
        raise ValueError("all phase streams must be declared")
    if set(layers) != set(LAYERS):
        raise ValueError("all final layers must be declared")


def _json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str).encode()


def canonical_hash(value: object) -> str:
    return hashlib.sha256(_json(value)).hexdigest()


def _records_hash(records: Sequence[Mapping[str, Any]]) -> str:
    return canonical_hash([
        {key: value for key, value in record.items() if key != "producer_extensions"}
        for record in records
    ])


def _coverage(status: str, observed: bool, count: int, coverage: str | None = None) -> dict[str, Any]:
    if status not in {"MATCHED", "MISMATCHED", "INCOMPLETE", "UNAVAILABLE"}:
        raise ValueError(f"invalid parity status: {status}")
    state = coverage or ("observed" if observed else "unavailable")
    if state not in {"observed", "observed_empty", "unavailable"}:
        raise ValueError(f"invalid parity coverage: {state}")
    if state == "observed_empty" and (not observed or count != 0):
        raise ValueError("observed_empty requires observed zero-row coverage")
    return {"status": status, "observed": observed, "coverage": state, "count": count}


def _timestamp(value: object) -> str:
    if value is None:
        raise ValueError("parity timestamps are required")
    try:
        if not isinstance(value, (str, datetime)):
            raise TypeError("parity timestamp must be a datetime or string")
        return serialize_utc(parse_causal_timestamp(value, field="parity timestamp"))
    except (TypeError, ValueError) as exc:
        if "timezone-aware" in str(exc):
            raise ValueError("parity timestamps must be timezone-aware") from exc
        raise ValueError("parity timestamps must be ISO-8601") from exc


def _copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _copy(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_copy(item) for item in value]
    return value


def _normalize_embedded_ids(value: Any, *, key: str | None = None) -> Any:
    if isinstance(value, Mapping):
        return {
            str(item_key): _normalize_embedded_ids(item, key=str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, list):
        return [_normalize_embedded_ids(item, key=key) for item in value]
    if key != "execution_group_id" or not isinstance(value, str):
        return value
    match = _EXECUTION_GROUP_ID.fullmatch(value)
    if match is None:
        return value
    session_date = match.group("session_date").split(":", 1)[0]
    timestamp = match.group("timestamp")
    try:
        parsed = parse_session_timestamp(session_date, timestamp)
    except (TypeError, ValueError):
        return value
    prefix = value[: match.start("timestamp")]
    return f"{prefix}{serialize_utc(parsed)}"


def normalize_phase_record(record: Mapping[str, Any], *, sequence: int) -> dict[str, Any]:
    required = {"kind", "occurred_at", "observed_at", "available_at", "identity", "causal_parent_id", "values", "unavailable"}
    missing = required - set(record)
    if missing:
        raise ValueError(f"phase record missing {sorted(missing)}")
    result = _normalize_embedded_ids(_copy(record))
    result["occurred_at"] = _timestamp(result["occurred_at"])
    result["observed_at"] = _timestamp(result["observed_at"])
    result["available_at"] = _timestamp(result["available_at"])
    if parse_utc(result["available_at"]) < parse_utc(result["observed_at"]):
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


def _normalize_record(
    record: Mapping[str, Any], *, stream: str, sequence: int, layer: str | None = None,
) -> dict[str, Any]:
    required = {
        "kind", "occurred_at", "observed_at", "available_at", "sequence",
        "identity", "causal_parent_id", "values", "unavailable",
    }
    missing = required - set(record)
    if missing:
        raise ValueError(f"{stream} record missing {sorted(missing)}")
    result = _normalize_embedded_ids(_copy(record))
    result["occurred_at"] = _timestamp(result["occurred_at"])
    result["observed_at"] = _timestamp(result["observed_at"])
    result["available_at"] = _timestamp(result["available_at"])
    if result["sequence"] != sequence:
        raise ValueError(f"{stream} record sequence must be contiguous")
    if not isinstance(result["identity"], str) or not result["identity"]:
        raise ValueError(f"{stream} record identity is required")
    if not isinstance(result["values"], Mapping) or not isinstance(result["unavailable"], list):
        raise ValueError(f"{stream} record values and unavailable are required")
    if any(not isinstance(item, str) for item in result["unavailable"]):
        raise ValueError(f"{stream} unavailable values must be named strings")
    if result["causal_parent_id"] is not None and not isinstance(result["causal_parent_id"], str):
        raise ValueError(f"{stream} causal_parent_id must be a string or null")
    if layer == "run_scope":
        if result["kind"] != "run_scope":
            raise ValueError("run_scope records must use kind run_scope")
        required_counts = {
            "selected_source_rows", "filtered_source_rows", "completed_source_rows",
            "gap_rows", "processed_decision_bars",
        }
        if not required_counts <= set(result["values"]):
            raise ValueError("run_scope records require source and decision-bar counts")
    elif layer == "decision":
        if result["kind"] != "decision" or result.get("stage") not in DECISION_STAGES:
            raise ValueError("decision records require a valid decision stage")
    elif layer == "execution_settlement" and result["kind"] not in EXECUTION_KINDS:
        raise ValueError("invalid execution_settlement kind")
    elif layer == "portfolio_ledger" and result["kind"] not in LEDGER_KINDS:
        raise ValueError("invalid portfolio_ledger kind")
    return result


def _stream_manifest(
    records: Sequence[Mapping[str, Any]], *, status: str, observed: bool,
    coverage: str | None = None,
) -> dict[str, Any]:
    return {
        **_coverage(status, observed, len(records), coverage),
        "sha256": _records_hash(records),
    }


def build_parity_artifact(
    *, run_id: str, producer: Mapping[str, Any], scope: Mapping[str, Any],
    phase_streams: Mapping[str, Sequence[Mapping[str, Any]]],
    layers: Mapping[str, Sequence[Mapping[str, Any]]], coverage: Mapping[str, Any],
) -> dict[str, Any]:
    """Build and validate the unified producer parity artifact."""
    validate_contract_declarations(phase_streams, layers)
    normalized_phases = {
        name: [_normalize_record(item, stream=name, sequence=index)
               for index, item in enumerate(records)]
        for name, records in phase_streams.items()
    }
    normalized_layers = {
        name: [_normalize_record(item, stream=name, sequence=index, layer=name)
               for index, item in enumerate(records)]
        for name, records in layers.items()
    }
    coverage_phases = coverage.get("phase_streams") if isinstance(coverage, Mapping) else None
    coverage_layers = coverage.get("layers") if isinstance(coverage, Mapping) else None
    if not isinstance(coverage_phases, Mapping) or not isinstance(coverage_layers, Mapping):
        raise ValueError("coverage must declare phase_streams and layers")
    if set(coverage_phases) != set(PHASE_STREAMS) or set(coverage_layers) != set(LAYERS):
        raise ValueError("coverage must declare every phase stream and final layer")
    for group, names in ((coverage_phases, PHASE_STREAMS), (coverage_layers, LAYERS)):
        for name in names:
            entry = group[name]
            if not isinstance(entry, Mapping):
                raise ValueError(f"{name} coverage entry must be an object")
            _coverage(str(entry.get("status")), bool(entry.get("observed")),
                      len(normalized_phases[name] if name in PHASE_STREAMS else normalized_layers[name]),
                      entry.get("coverage"))
    if any(coverage_phases[name]["status"] != "MATCHED" for name in PHASE_STREAMS):
        raise ValueError("final parity layers require complete phase streams")
    manifest = {
        "version": 1, "hash_algorithm": "sha256", "run_id": run_id,
        "phase_streams": {
            name: _stream_manifest(normalized_phases[name],
                                   status=str(coverage_phases[name]["status"]),
                                   observed=bool(coverage_phases[name]["observed"]),
                                   coverage=coverage_phases[name].get("coverage"))
            for name in PHASE_STREAMS
        },
        "layers": {
            name: _stream_manifest(normalized_layers[name],
                                   status=str(coverage_layers[name]["status"]),
                                   observed=bool(coverage_layers[name]["observed"]),
                                   coverage=coverage_layers[name].get("coverage"))
            for name in LAYERS
        },
    }
    artifact = {
        "artifact_type": ARTIFACT_TYPE, "schema_version": SCHEMA_VERSION,
        "contract": {"id": CONTRACT_ID, "version": CONTRACT_VERSION},
        "producer": _copy(producer), "run_id": run_id, "scope": _copy(scope),
        "coverage": _copy(coverage), "decision_substreams": normalized_phases,
        "layers": normalized_layers, "manifest": manifest,
    }
    validate_parity_artifact(artifact)
    return artifact


def build_parity_artifact_from_payload(
    payload: Mapping[str, Any], *, run_id: str, producer: Mapping[str, Any],
) -> dict[str, Any]:
    """Build from explicit producer facts attached to a replay payload."""
    phase_streams = payload.get("parity_phase_facts")
    layers = payload.get("parity_layers")
    coverage = payload.get("parity_coverage")
    if not isinstance(phase_streams, Mapping) or not isinstance(layers, Mapping) or not isinstance(coverage, Mapping):
        raise ValueError("replay payload is missing explicit parity facts and coverage")
    return build_parity_artifact(
        run_id=run_id, producer=producer, scope=payload.get("scope", {}),
        phase_streams=phase_streams, layers=layers, coverage=coverage,
    )


def validate_parity_artifact(payload: Mapping[str, Any]) -> None:
    """Validate the complete canonical artifact envelope and manifests."""
    required = {"artifact_type", "schema_version", "contract", "producer", "run_id", "scope", "coverage", "decision_substreams", "layers", "manifest"}
    if set(payload) != required:
        raise ValueError("invalid producer parity artifact envelope")
    if payload["artifact_type"] != ARTIFACT_TYPE or payload["schema_version"] != SCHEMA_VERSION:
        raise ValueError("invalid producer parity artifact version")
    if payload["contract"] != {"id": CONTRACT_ID, "version": CONTRACT_VERSION}:
        raise ValueError("invalid producer parity contract")
    scope, producer = payload["scope"], payload["producer"]
    if not isinstance(scope, Mapping) or not isinstance(scope.get("timezone"), str):
        raise ValueError("parity scope timezone is required")
    if not isinstance(producer, Mapping) or not producer.get("name"):
        raise ValueError("parity producer identity is required")
    manifest = payload["manifest"]
    if not isinstance(manifest, Mapping) or manifest.get("version") != 1 or manifest.get("hash_algorithm") != "sha256" or manifest.get("run_id") != payload["run_id"]:
        raise ValueError("parity manifest identity is invalid")
    coverage = payload["coverage"]
    if not isinstance(coverage, Mapping):
        raise ValueError("parity coverage is required")
    for collection_name, names in (("decision_substreams", PHASE_STREAMS), ("layers", LAYERS)):
        collection = payload[collection_name]
        manifest_name = "phase_streams" if collection_name == "decision_substreams" else "layers"
        group = coverage.get(manifest_name)
        manifest_group = manifest.get(manifest_name)
        if not isinstance(collection, Mapping) or set(collection) != set(names):
            raise ValueError(f"{collection_name} must declare every stream")
        if not isinstance(group, Mapping) or set(group) != set(names) or not isinstance(manifest_group, Mapping):
            raise ValueError(f"coverage must declare every {manifest_name} entry")
        for name in names:
            records = collection[name]
            if not isinstance(records, list):
                raise ValueError(f"{name} must be a list")
            identities: set[str] = set()
            for index, item in enumerate(records):
                if not isinstance(item, Mapping) or item.get("sequence") != index:
                    raise ValueError(f"{name} sequence is not contiguous")
                identity = item.get("identity")
                if not isinstance(identity, str) or identity in identities:
                    raise ValueError(f"{name} identity is invalid or duplicated")
                identities.add(identity)
                _normalize_record(item, stream=name, sequence=index, layer=name if collection_name == "layers" else None)
            entry, manifest_entry = group[name], manifest_group.get(name)
            if not isinstance(entry, Mapping) or not isinstance(manifest_entry, Mapping):
                raise ValueError(f"invalid coverage entry: {name}")
            if not isinstance(entry.get("observed"), bool):
                raise ValueError(f"{name} coverage observed must be boolean")
            if entry.get("status") not in {"MATCHED", "MISMATCHED", "INCOMPLETE", "UNAVAILABLE"}:
                raise ValueError(f"{name} coverage status is invalid")
            if entry.get("coverage") not in {"observed", "observed_empty", "unavailable"}:
                raise ValueError(f"{name} coverage value is invalid")
            for field in ("status", "observed", "coverage"):
                if entry.get(field) != manifest_entry.get(field):
                    raise ValueError(f"coverage mismatch: {name}.{field}")
            if manifest_entry.get("count") != len(records) or manifest_entry.get("sha256") != _records_hash(records):
                raise ValueError(f"{name} manifest count or hash mismatch")
            status = manifest_entry.get("status")
            if status == "MATCHED" and not manifest_entry.get("observed"):
                raise ValueError(f"{name} matched stream is not observed")
            if not records and status == "MATCHED" and manifest_entry.get("coverage") != "observed_empty":
                raise ValueError(f"{name} empty stream is not observed")
            if status == "UNAVAILABLE" and records:
                raise ValueError(f"{name} unavailable stream contains records")


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


def build_phase7_artifact(*, run_id: str, producer: Mapping[str, Any], scope: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
    from .phase7_projection import build_phase7_projection
    facts = payload.get("parity_phase_facts")
    facts = facts if isinstance(facts, Mapping) else {}
    streams = build_phase7_projection({"parity_phase_facts": facts})
    manifest: dict[str, dict[str, Any]] = {}
    for name, records in streams.items():
        # Only parity_phase_facts is producer-owned. Top-level projection
        # fields are projections and must not turn an empty stream into an
        # observed execution fact.
        present = name in facts
        status = "MATCHED" if present else "UNAVAILABLE"
        entry = _manifest(records, status=status, observed=present)
        entry["coverage"] = "observed_empty" if present and not records else ("observed" if present else "unavailable")
        if not present:
            entry["reason"] = f"missing_{name}_facts"
        manifest[name] = entry
    return {
        "artifact_type": "orderflow_phase7_parity", "schema_version": 1,
        "contract": {"id": "orderflow_phase7_streams", "version": 1, "streams": list(PHASE7_STREAMS)},
        "producer": _copy(producer), "run_id": run_id, "scope": _copy(scope),
        "phase_streams": streams,
        "manifest": {"version": 1, "hash_algorithm": "sha256", "run_id": run_id, "streams": manifest},
    }


def compare_phase7_manifests(canonical: Mapping[str, Any], paper: Mapping[str, Any], *, canonical_records: Any = None, paper_records: Any = None) -> dict[str, Any]:
    left = canonical.get("manifest", {}).get("streams", {})
    right = paper.get("manifest", {}).get("streams", {})
    for stream in PHASE7_STREAMS:
        lm, rm = left.get(stream), right.get(stream)
        if not isinstance(lm, Mapping) or not isinstance(rm, Mapping):
            return {"status": "INCOMPLETE", "stream": stream, "reason": "manifest_missing"}
        if (lm.get("status"), rm.get("status")) != ("MATCHED", "MATCHED"):
            return {"status": "INCOMPLETE", "stream": stream, "canonical": dict(lm), "paper": dict(rm)}
        if (lm.get("count"), lm.get("sha256")) == (rm.get("count"), rm.get("sha256")):
            continue
        if canonical_records is None or paper_records is None:
            return {"status": "MISMATCHED", "stream": stream, "reason": "manifest", "canonical": dict(lm), "paper": dict(rm)}
        return {"status": "MISMATCHED", "stream": stream, **compare_chronological_streams(canonical_records(stream), paper_records(stream))}
    return {"status": "MATCHED"}


def compare_chronological_streams(
    canonical: Sequence[Mapping[str, Any]], paper: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compare causal facts in timestamp/sequence order and report the first gap."""
    left = [normalize_phase_record(item, sequence=index) for index, item in enumerate(canonical)]
    right = [normalize_phase_record(item, sequence=index) for index, item in enumerate(paper)]
    for index, (left_item, right_item) in enumerate(zip(left, right)):
        if left_item != right_item:
            field, left_value, right_value = _first_mismatch(left_item, right_item)
            return {"status": "MISMATCHED", "index": index, "field": field,
                    "canonical_value": left_value, "paper_value": right_value,
                    "canonical_record": left_item, "paper_record": right_item}
    if len(left) != len(right):
        return {"status": "MISMATCHED", "index": min(len(left), len(right)),
                "reason": "count", "canonical_count": len(left), "paper_count": len(right)}
    return {"status": "MATCHED", "count": len(left)}


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


__all__ = [
    "ARTIFACT_TYPE", "CONTRACT_ID", "CONTRACT_VERSION", "DECISION_STAGES",
    "EXECUTION_KINDS", "FILENAME", "LAYERS", "LEDGER_KINDS", "PARITY_PAYLOAD_KEYS",
    "PHASE_STREAMS", "PHASE7_STREAMS", "SCHEMA_VERSION",
    "build_parity_artifact", "build_parity_artifact_from_payload",
    "build_phase4_artifact", "build_phase7_artifact", "canonical_hash",
    "compare_chronological_streams", "compare_phase4_manifests",
    "compare_phase7_manifests", "normalize_phase_record", "phase_facts_from_payload",
    "validate_contract_declarations", "validate_parity_artifact",
]
