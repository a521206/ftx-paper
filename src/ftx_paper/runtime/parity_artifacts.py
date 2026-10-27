"""Minimal producer-neutral parity artifact for Paper replays."""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

from ftx_paper.runtime.events import is_decision_event


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


def _replay_input(metadata: Mapping[str, Any]) -> dict[str, Any]:
    """Expose selected-source identity in the canonical replay-input shape."""
    provenance = metadata.get("input_provenance")
    current = provenance.get("current") if isinstance(provenance, Mapping) else None
    if not isinstance(current, Mapping):
        current = {}
    fingerprint = current.get("source_fingerprint") or metadata.get("input_fingerprint")
    symbol = str(current.get("selected_symbol") or current.get("symbol") or "NIFTY")
    expiry = str(current.get("selected_expiry") or current.get("expiry") or "")
    instrument_type = str(current.get("instrument_type") or "FUT").upper()
    contract_type = "CONTINUOUS" if not expiry else instrument_type
    identity = {
        "symbol": symbol,
        "series": "I",
        "contract_type": contract_type,
        "expiry": expiry,
    }
    source = {
        "symbol": symbol,
        "series": "I",
        "contract_type": contract_type,
        "expiry": expiry,
        "bar_count": int(current.get("source_bar_count") or metadata.get("input_bar_count") or 0),
        "fingerprint": fingerprint,
        "identity": identity,
        "identities": [identity],
    }
    return {"identity": [fingerprint] if fingerprint else [], "provenance": [source]}


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


def decision_diagnostics(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize Paper decisions without changing the parity layer."""
    kinds = Counter(str(record.get("kind", "")) for record in records)
    stages: Counter[str] = Counter()
    outcomes: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    for record in records:
        values = record.get("values", record.get("comparable_values", record))
        values = values if isinstance(values, Mapping) else {}
        stages[str(values.get("decision_stage", "unknown"))] += 1
        outcomes[str(values.get("outcome", record.get("kind", "unknown")))] += 1
        reasons[str(values.get("reason") or values.get("reason_code") or "")] += 1
    return {
        "count": len(records),
        "kind_counts": dict(sorted(kinds.items())),
        "stage_counts": dict(sorted(stages.items())),
        "outcome_counts": dict(sorted(outcomes.items())),
        "reason_counts": dict(sorted(reasons.items())),
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
                        if is_decision_event(str(event.get("event_type", "")), event)
                        and (str(event.get("outcome", "")).lower() in {"", "accepted", "rejected"}
                             or str(event.get("decision_stage", "")).lower() == "opportunity"
                             or str(event.get("event_type", "")).upper() == "EXITDECISION")]
    decisions = [_record(event, kind=("candidate" if str(event.get("decision_stage", "")).lower() == "opportunity"
                                      else "exit" if str(event.get("event_type", "")).upper() == "EXITDECISION"
                                      else str(event.get("outcome", "rejected"))), sequence=index)
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
    metadata = result.get("metadata", {})
    metadata = metadata if isinstance(metadata, Mapping) else {}
    timezone_name = str(request.get("timezone") or metadata.get("timezone") or "Asia/Kolkata")
    session_date = str(request.get("session_date", ""))[:10]
    requested_from = str(request.get("date_from") or session_date)[:10]
    requested_to = str(request.get("date_to") or session_date)[:10]
    replay_input = _replay_input(metadata)
    scope = {
        "requested_date_from": requested_from,
        "requested_date_to": requested_to,
        "effective_date_from": session_date,
        "effective_date_to": session_date,
        "timezone": timezone_name,
        "sessions": ["orderflow"] if str(request.get("strategy_id", "")).startswith("ftx-paper-orderflow") else [],
        "vehicles": list(request.get("vehicles", ())),
        "partitions": {"source": ["orderflow"]} if str(request.get("strategy_id", "")).startswith("ftx-paper-orderflow") else {},
        "replay_input": replay_input,
        "processed": {
            "bar_count": int(result.get("bars_seen", 0)),
            "session_count": 1 if session_date else 0,
        },
    }
    input_identity = metadata.get("input_fingerprint")
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
        # A producer can establish coverage, but only a cross-producer
        # comparison can establish MATCHED.
        status = "INCOMPLETE" if not coverage.get("complete", False) else "UNAVAILABLE"
        layers[layer] = {"status": status, "observed": bool(observed), "count": len(records),
                         "sha256": hashlib.sha256(_canonical_json(records).encode("utf-8")).hexdigest()}
    scope = artifact.get("scope", {})
    return {"version": PARITY_MANIFEST_VERSION, "hash_algorithm": "sha256",
            "run_id": artifact.get("run_id"), "scope": scope,
            "contract": artifact.get("contract"), "layers": layers}


def validate_parity_artifact(artifact: Mapping[str, Any]) -> dict[str, Any]:
    required = ("artifact_type", "schema_version", "contract", "producer", "run_id", "scope", "coverage", "producer_configuration", "layers", "manifest")
    if any(key not in artifact for key in required) or artifact.get("artifact_type") != "producer_parity" or artifact.get("schema_version") != 2:
        raise ValueError("invalid parity artifact envelope")
    if not isinstance(artifact["producer"], Mapping):
        raise ValueError("invalid parity producer")
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
        if left_layer.get("observed") != right_layer.get("observed"):
            return {"status": "UNAVAILABLE", "layer": layer}
        if not left_layer.get("observed", False):
            return {"status": "UNAVAILABLE", "layer": layer}
        if (left_layer.get("count"), left_layer.get("sha256")) != (right_layer.get("count"), right_layer.get("sha256")):
            return {"status": "MISMATCHED", "layer": layer, "context_limit": max_context}
    return {"status": "MATCHED"}


def compare_parity_artifacts(left: Mapping[str, Any], right: Mapping[str, Any], *, max_context: int = 3) -> dict[str, Any]:
    try:
        left = read_parity_artifact(left)
        right = read_parity_artifact(right)
    except ValueError as exc:
        return {"status": "UNAVAILABLE", "reason": str(exc)}
    left_producer = left.get("producer", {}).get("name")
    right_producer = right.get("producer", {}).get("name")
    if not left_producer or not right_producer or left_producer == right_producer:
        return {"status": "UNAVAILABLE", "reason": "comparison requires distinct producers"}
    result = compare_parity_manifests(left["manifest"], right["manifest"], max_context=max_context)
    counts = {
        layer: {"left": len(parity_layer_records(left, layer)),
                "right": len(parity_layer_records(right, layer))}
        for layer in PARITY_LAYERS
    }
    result = {**result, "scope": {"left": left.get("scope"), "right": right.get("scope")},
              "counts": counts}
    if result.get("layer") == "run_scope" and left.get("scope") != right.get("scope"):
        result["context"] = [{"field": key, "left": left.get("scope", {}).get(key),
                               "right": right.get("scope", {}).get(key)}
                              for key in sorted(set(left.get("scope", {})) | set(right.get("scope", {})))
                              if left.get("scope", {}).get(key) != right.get("scope", {}).get(key)][:max_context]
    if result.get("status") != "MISMATCHED":
        return result
    layer = result.get("layer")
    if layer not in PARITY_LAYERS:
        return result
    left_layer = parity_layer_records(left, layer)
    right_layer = parity_layer_records(right, layer)
    context = [{"index": index, "left": a, "right": b}
               for index, (a, b) in enumerate(zip(left_layer, right_layer)) if a != b][:max_context]
    if len(left_layer) != len(right_layer) and len(context) < max_context:
        context.append({"index": min(len(left_layer), len(right_layer)),
                        "left": left_layer[len(right_layer)] if len(left_layer) > len(right_layer) else None,
                        "right": right_layer[len(left_layer)] if len(right_layer) > len(left_layer) else None})
    return {**result, "left_count": len(left_layer), "right_count": len(right_layer), "context": context}


__all__ = ["PARITY_ARTIFACT_SCHEMA_VERSION", "PARITY_CONTRACT_ID", "PARITY_LAYERS",
           "build_parity_artifact", "build_parity_manifest", "compare_parity_artifacts",
           "compare_parity_manifests", "parity_layer_records", "read_parity_artifact",
           "strategy_payload_hash", "validate_parity_artifact"]
