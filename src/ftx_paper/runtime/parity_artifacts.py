"""Paper-owned producer for the versioned parity artifact.

The module is intentionally data-only at its boundary.  P9 decision records
are kept separate from P10 execution, exit, settlement, and ledger facts.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


PARITY_ARTIFACT_SCHEMA_VERSION = 1
_REQUIRED_DECISION_FIELDS = (
    "comparison_identity", "stream_sequence", "date", "timestamp", "event_type", "stage", "outcome",
)


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
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def strategy_payload_hash(strategy: Mapping[str, Any] | None) -> tuple[dict[str, Any], str]:
    """Return the deterministic strategy-only payload and its SHA-256 hash."""
    payload = {str(key): _jsonable(value) for key, value in (strategy or {}).items()}
    encoded = _canonical_json(payload).encode("utf-8")
    return payload, hashlib.sha256(encoded).hexdigest()


def _identity(record: Mapping[str, Any]) -> str:
    identity = {key: record.get(key) for key in (
        "event_type", "date", "timestamp", "session", "vehicle", "cell",
        "direction", "stage", "outcome", "reason_code",
    )}
    return hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()


def _timestamp(record: Mapping[str, Any]) -> str | None:
    value = record.get("timestamp") or record.get("decision_at") or record.get("fill_timestamp")
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("parity timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _record(event: Mapping[str, Any], ordinal: int) -> dict[str, Any]:
    payload = dict(event.get("payload", event))
    event_type = str(event.get("event_type", payload.get("event_type", ""))).upper()
    timestamp = _timestamp(payload)
    values = {key: payload.get(key) for key in (
        "entry_price", "stop", "score", "score_factors", "requested_quantity", "effective_quantity",
    )}
    record = {
        "comparison_identity": _identity({
            "event_type": event_type, "date": str(payload.get("session_date") or payload.get("date") or (timestamp or ""))[:10],
            "timestamp": timestamp, "session": payload.get("session"), "vehicle": payload.get("vehicle"),
            "cell": payload.get("cell"), "direction": payload.get("direction"),
            "stage": payload.get("decision_stage"), "outcome": payload.get("outcome") or event_type.lower(),
            "reason_code": payload.get("reason") or payload.get("rejection_reason"),
        }),
        "producer_local_id": payload.get("decision_id") or payload.get("candidate_id"),
        "stream_sequence": ordinal,
        "date": str(payload.get("session_date") or payload.get("date") or (timestamp or "")[:10])[:10],
        "timestamp": timestamp,
        "event_type": event_type,
        "detector_ordinal": payload.get("detector_emission_ordinal"),
        "session": payload.get("session"),
        "cell": payload.get("cell"),
        "direction": payload.get("direction"),
        "stage": payload.get("decision_stage"), "outcome": payload.get("outcome") or event_type.lower(),
        "reason_code": payload.get("reason") or payload.get("rejection_reason"), "values": values,
        "field_availability": {key: "available" if value not in (None, "") else "missing" for key, value in values.items()},
    }
    return _jsonable(record)


def _coverage(records: Sequence[Mapping[str, Any]], *, bars_seen: int, unavailable: Sequence[str]) -> dict[str, Any]:
    issues: list[str] = []
    if not records:
        issues.append("no_decision_records")
    if bars_seen <= 0:
        issues.append("no_completed_bars")
    required_missing = sorted({field for record in records for field in _REQUIRED_DECISION_FIELDS if record.get(field) in (None, "")})
    if required_missing:
        issues.append("missing_required_fields:" + ",".join(required_missing))
    return {
        "complete": not issues,
        "issues": issues,
        "field_availability": {
            field: "available" if all(record.get(field) not in (None, "") for record in records) else "missing"
            for field in _REQUIRED_DECISION_FIELDS
        },
            "unavailable_fields": sorted(set(str(item) for item in unavailable)),
            "record_count": len(records),
            "bars_seen": bars_seen,
    }


def build_parity_artifact(
    result: Mapping[str, Any], *, request: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the Paper-owned schema-v1 envelope from a replay result."""
    request = request or {}
    events = list(result.get("events", ()))
    decisions = [_record(event, ordinal) for ordinal, event in enumerate(
        event for event in events
        if str(event.get("event_type", "")).upper().endswith("DECISION")
        and str(event.get("event_type", "")).upper() != "EXITDECISION"
    )]
    execution_types = {"ORDER_INTENT", "ORDER_AUTHORIZED", "ORDER_ACK", "FILL", "EXECUTEDDECISION", "ORDER_UNFILLED", "ORDER_SUPPRESSED", "RISK_REJECTED"}
    execution = [_jsonable({"ordinal": ordinal, **dict(event)}) for ordinal, event in enumerate(
        event for event in events if str(event.get("event_type", "")).upper() in execution_types
    )]
    exits = [_jsonable({"ordinal": ordinal, **dict(event)}) for ordinal, event in enumerate(
        event for event in events if str(event.get("event_type", "")).upper() in {"EXITDECISION", "EXIT", "CLOSE"}
    )]
    strategy, strategy_hash = strategy_payload_hash(result.get("strategy"))
    run_id = str(request.get("run_id") or result.get("run_id") or "paper-replay")
    generated_at = str(
        request.get("generated_at")
        or result.get("metadata", {}).get("generated_at")
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    coverage = _coverage(decisions, bars_seen=int(result.get("bars_seen", 0)), unavailable=list(result.get("unavailable_fields", ())))
    envelope = {
        "artifact_type": "ftx_producer_parity",
        "schema_version": PARITY_ARTIFACT_SCHEMA_VERSION,
        "producer": {"identifier": "ftx-paper", "source_version": str(result.get("strategy", {}).get("version", "unknown"))},
        "run_id": run_id,
        "scope": {"requested_date_from": str(request.get("session_date", ""))[:10], "requested_date_to": str(request.get("session_date", ""))[:10], "effective_date_from": str(request.get("session_date", ""))[:10], "effective_date_to": str(request.get("session_date", ""))[:10], "timezone": "Asia/Kolkata", "sessions": list(request.get("sessions", ())), "vehicles": list(result.get("vehicles", ())), "replay_input": {"identity": result.get("metadata", {}).get("input_fingerprint"), "provenance": result.get("metadata", {}).get("input_provenance", {})}, "processed": {"bar_count": int(result.get("bars_seen", 0)), "session_count": 1}, "generated_at": generated_at},
        "strategy": {"payload": strategy, "payload_hash": strategy_hash, "hash_recipe": "sha256(canonical-json(sort_keys=true,separators=(',',':'),ensure_ascii=true))"},
        "coverage": coverage,
        "p9": {"decision_records": decisions, "coverage": coverage},
        "p10": {"execution": execution, "exits": exits, "synthetic_settlements": _jsonable(result.get("synthetic_settlements", [])), "ledger": _jsonable(result.get("ledger", {}))},
    }
    return _jsonable(envelope)


__all__ = ["PARITY_ARTIFACT_SCHEMA_VERSION", "build_parity_artifact", "strategy_payload_hash"]
