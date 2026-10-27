"""Minimal producer-neutral parity artifact for Paper replays."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from typing import Any


PARITY_ARTIFACT_SCHEMA_VERSION = 2
PARITY_CONTRACT_ID = "producer_replay_parity"
PARITY_MANIFEST_VERSION = 1
PARITY_LAYERS = ("run_scope", "decision", "execution_settlement", "portfolio_ledger")


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("parity timestamps must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def strategy_payload_hash(strategy: Mapping[str, Any] | None) -> tuple[dict[str, Any], str]:
    payload = {str(key): _jsonable(value) for key, value in (strategy or {}).items()}
    return payload, hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _timestamp(record: Mapping[str, Any]) -> str:
    value = (record.get("occurred_at") or record.get("timestamp") or record.get("decision_at")
             or record.get("fill_timestamp"))
    if value is None:
        raise ValueError("parity records require occurred_at")
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("parity timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _record(record: Mapping[str, Any], *, kind: str, sequence: int) -> dict[str, Any]:
    values = record.get("comparable_values", record.get("values", record))
    values = dict(values) if isinstance(values, Mapping) else {}
    if "comparable_values" not in record and "values" not in record:
        values = {key: value for key, value in values.items() if key not in {
            "event_type", "decision_id", "client_order_id", "producer_local_id",
            "comparison_identity", "source", "session_date", "sequence", "stream_sequence",
            "account_revision",
        }}
    occurred_at = _timestamp(record)
    identity_basis = {
        "kind": kind,
        "minute": occurred_at[:16],
        "dimensions": {key: values.get(key) for key in (
            "cell", "vehicle", "direction", "side", "instrument", "status", "quantity",
        ) if values.get(key) is not None},
    }
    identity = hashlib.sha256(_canonical_json(identity_basis).encode("utf-8")).hexdigest()
    return {
        "kind": kind,
        "occurred_at": occurred_at,
        "sequence": sequence,
        "identity": identity,
        "values": _jsonable(values),
        "unavailable": [key for key, value in values.items() if value is None],
    }


def _coverage(
    decisions: Sequence[Mapping[str, Any]], bars_seen: int, unavailable: Sequence[str],
    *, execution_observed: bool, ledger_observed: bool, input_identity: Any,
) -> dict[str, Any]:
    issues = []
    if bars_seen <= 0:
        issues.append("no_completed_bars")
    if not input_identity:
        issues.append("missing_input_identity")
    return {"complete": not issues, "issues": issues, "bars_seen": bars_seen,
            "record_count": len(decisions),
            "observed_layers": {"run_scope": True, "decision": bars_seen > 0,
                                 "execution_settlement": execution_observed,
                                 "portfolio_ledger": ledger_observed},
            "field_availability": {"decision": bool(decisions), "execution": execution_observed,
                                    "settlement": False, "ledger": ledger_observed},
            "unavailable": sorted({str(item) for item in unavailable})}


def build_parity_artifact(
    result: Mapping[str, Any], *, request: Mapping[str, Any] | None = None,
    layer_records: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Build the four-layer artifact from producer-owned replay facts."""
    request = request or {}
    layer_records = layer_records or {}
    source_decisions = list(layer_records.get("decisions", ()))
    source_decisions = [event for event in source_decisions
                        if str(event.get("event_type", "")).upper().endswith("DECISION")
                        and str(event.get("event_type", "")).upper() != "EXECUTEDDECISION"]
    decision_kinds = {"CANDIDATEDECISION": "candidate", "ACCEPTEDDECISION": "accepted",
                      "REJECTEDDECISION": "rejected", "EXITDECISION": "exit"}
    decisions = [_record(event, kind=decision_kinds.get(str(event.get("event_type", "")).upper(), "candidate"), sequence=index)
                 for index, event in enumerate(source_decisions)]
    execution_types = {"ORDER_INTENT", "ORDER_AUTHORIZED", "ORDER_ACK", "FILL", "ORDER_UNFILLED", "ORDER_SUPPRESSED", "RISK_REJECTED"}
    source_execution = [event for event in layer_records.get("execution", ())
                        if str(event.get("event_type", "")).upper() in execution_types]
    source_exits = [event for event in layer_records.get("execution", ())
                    if str(event.get("event_type", "")).upper() in {"EXIT", "CLOSE"}]
    execution_kinds = {"ORDER_INTENT": "order_intent", "FILL": "fill", "EXIT": "exit",
                       "CLOSE": "exit"}
    execution = [_record(event, kind=execution_kinds.get(str(event.get("event_type", "")).upper(), "order_intent"), sequence=index)
                 for index, event in enumerate([*source_execution, *source_exits])]
    settlements = [_record(event, kind="synthetic_settlement", sequence=index)
                   for index, event in enumerate(result.get("synthetic_settlements", ()))]
    ledger = result.get("ledger", {})
    ledger_records: list[dict[str, Any]] = []
    if isinstance(ledger, Mapping):
        for date, row in sorted(ledger.items()):
            if not isinstance(row, Mapping):
                continue
            occurred_at = f"{str(date)[:10]}T00:00:00Z"
            reservations = row.get("reservations")
            if isinstance(reservations, Mapping):
                for reservation in reservations.values():
                    if isinstance(reservation, Mapping):
                        ledger_records.append(_record({"occurred_at": occurred_at, "values": reservation},
                                                      kind="reservation", sequence=len(ledger_records)))
            ledger_records.append(_record({"occurred_at": occurred_at, "values": row},
                                          kind="closeout", sequence=len(ledger_records)))
    strategy, strategy_hash = strategy_payload_hash(result.get("strategy"))
    timezone_name = str(request.get("timezone") or result.get("metadata", {}).get("timezone") or "UTC")
    session_date = str(request.get("session_date", ""))[:10]
    scope = {"requested_date_from": session_date, "requested_date_to": session_date,
             "effective_date_from": session_date, "effective_date_to": session_date,
             "timezone": timezone_name,
             "applicable_partitions": list(request.get("partitions", request.get("vehicles", ()))),
             "replay_input": {"identity": result.get("metadata", {}).get("input_fingerprint"),
                              "provenance": result.get("metadata", {}).get("input_provenance", {})},
             "processed": {"bar_count": int(result.get("bars_seen", 0)),
                           "record_count": len(source_decisions)}}
    input_identity = result.get("metadata", {}).get("input_fingerprint")
    coverage = _coverage(decisions, int(result.get("bars_seen", 0)), result.get("unavailable_fields", ()),
                         execution_observed=True, ledger_observed=True, input_identity=input_identity)
    layers = {
        "run_scope": [_record({"occurred_at": f"{session_date}T00:00:00Z", "values": scope}, kind="run_scope", sequence=0)],
        "decision": decisions,
        "execution_settlement": [*execution, *settlements],
        "portfolio_ledger": ledger_records,
    }
    artifact = {"artifact_type": "producer_parity", "schema_version": PARITY_ARTIFACT_SCHEMA_VERSION,
                "contract": {"id": PARITY_CONTRACT_ID, "version": 1, "layers": list(PARITY_LAYERS)},
                 "producer": {"name": "ftx-paper", "version": str(strategy.get("version", "unknown"))},
                "run_id": str(request.get("run_id") or result.get("run_id") or "paper-replay"),
                "scope": scope, "coverage": coverage,
                "producer_configuration": {"payload": strategy, "payload_hash": strategy_hash},
                "layers": layers}
    artifact["manifest"] = build_parity_manifest(artifact)
    return _jsonable(artifact)


def parity_layer_records(artifact: Mapping[str, Any], layer: str) -> list[dict[str, Any]]:
    if layer not in PARITY_LAYERS:
        raise ValueError(f"unsupported parity layer: {layer}")
    layers = artifact.get("layers")
    if not isinstance(layers, Mapping) or not isinstance(layers.get(layer), list):
        raise ValueError(f"persisted parity layer is unavailable: {layer}")
    return list(layers[layer])


def build_parity_manifest(artifact: Mapping[str, Any]) -> dict[str, Any]:
    layers: dict[str, dict[str, Any]] = {}
    for layer in PARITY_LAYERS:
        records = parity_layer_records(artifact, layer)
        if any(not isinstance(record, Mapping) for record in records):
            raise ValueError(f"invalid {layer} record")
        coverage = artifact.get("coverage", {})
        observed = coverage.get("observed_layers", {}).get(layer, False)
        status = "INCOMPLETE" if not coverage.get("complete", False) else ("MATCHED" if observed else "UNAVAILABLE")
        layers[layer] = {"status": status, "count": len(records),
                         "sha256": hashlib.sha256(_canonical_json(records).encode("utf-8")).hexdigest()}
    scope = artifact.get("scope", {})
    return {"version": PARITY_MANIFEST_VERSION, "hash_algorithm": "sha256",
            "run_id": artifact.get("run_id"), "scope": scope,
            "contract": artifact.get("contract"), "layers": layers}


def validate_parity_artifact(artifact: Mapping[str, Any]) -> dict[str, Any]:
    required = ("artifact_type", "schema_version", "contract", "producer", "run_id", "scope", "coverage", "producer_configuration", "layers", "manifest")
    if any(key not in artifact for key in required) or artifact.get("artifact_type") != "producer_parity" or artifact.get("schema_version") != 2:
        raise ValueError("invalid parity artifact envelope")
    contract = artifact["contract"]
    if not isinstance(contract, Mapping) or contract.get("id") != PARITY_CONTRACT_ID or contract.get("layers") != list(PARITY_LAYERS):
        raise ValueError("invalid parity contract")
    layers = artifact["layers"]
    if not isinstance(layers, Mapping) or set(layers) != set(PARITY_LAYERS):
        raise ValueError("parity artifact must declare four layers")
    for layer in PARITY_LAYERS:
        records = layers[layer]
        if not isinstance(records, list):
            raise ValueError(f"parity layer {layer} must be an array")
        for sequence, record in enumerate(records):
            if not isinstance(record, Mapping) or any(key not in record for key in ("kind", "occurred_at", "sequence", "identity", "values", "unavailable")):
                raise ValueError(f"invalid {layer} record")
            if record["sequence"] != sequence:
                raise ValueError(f"{layer} sequence must be contiguous")
            if not isinstance(record["occurred_at"], str):
                raise ValueError(f"{layer} record timestamp is required")
            _timestamp(record)
    if artifact["manifest"] != build_parity_manifest(artifact):
        raise ValueError("parity manifest does not match layers")
    return dict(artifact)


def read_parity_artifact(artifact: Mapping[str, Any]) -> dict[str, Any]:
    return validate_parity_artifact(artifact)


def compare_parity_manifests(left: Mapping[str, Any], right: Mapping[str, Any], *, max_context: int = 3) -> dict[str, Any]:
    if left.get("contract") != right.get("contract") or left.get("scope") != right.get("scope"):
        return {"status": "MISMATCHED", "layer": "run_scope"}
    for layer in PARITY_LAYERS:
        left_layer = left.get("layers", {}).get(layer, {})
        right_layer = right.get("layers", {}).get(layer, {})
        if left_layer.get("status") != right_layer.get("status"):
            statuses = {left_layer.get("status"), right_layer.get("status")}
            status = "INCOMPLETE" if "INCOMPLETE" in statuses else (
                "UNAVAILABLE" if "UNAVAILABLE" in statuses else "MISMATCHED")
            return {"status": status, "layer": layer}
        if (left_layer.get("count"), left_layer.get("sha256")) != (right_layer.get("count"), right_layer.get("sha256")):
            return {"status": "MISMATCHED", "layer": layer, "context_limit": max_context}
    return {"status": "MATCHED"}


def compare_parity_artifacts(left: Mapping[str, Any], right: Mapping[str, Any], *, max_context: int = 3,
                             left_records: Sequence[Mapping[str, Any]] | None = None,
                             right_records: Sequence[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    result = compare_parity_manifests(left.get("manifest", left), right.get("manifest", right), max_context=max_context)
    if result.get("status") != "MISMATCHED" or left_records is None or right_records is None:
        return result
    mismatches = [{"index": index, "left": left_record, "right": right_record}
                  for index, (left_record, right_record) in enumerate(zip(left_records, right_records))
                  if left_record != right_record][:max_context]
    return {**result, "context": mismatches}


__all__ = ["PARITY_ARTIFACT_SCHEMA_VERSION", "PARITY_CONTRACT_ID", "PARITY_LAYERS",
           "build_parity_artifact", "build_parity_manifest", "compare_parity_artifacts",
           "compare_parity_manifests", "parity_layer_records", "read_parity_artifact",
           "strategy_payload_hash", "validate_parity_artifact"]
