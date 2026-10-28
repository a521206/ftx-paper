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
DECISION_STAGES = frozenset({"interaction", "candidate", "admission", "trigger", "exit"})
DECISION_OUTCOMES = frozenset({"accepted", "rejected", "cancelled", "invalidated", "expired", "observed", "unavailable"})
EXECUTION_KINDS = frozenset({"reservation", "authorization", "submission", "order_intent", "fill", "pending_exit", "exit", "exit_fill", "cancellation", "settlement", "synthetic_settlement"})
LEDGER_KINDS = frozenset({"reservation", "reservation_release", "authorization", "fill", "partial_fill", "cancellation", "exit_settlement", "daily_reset", "closeout"})


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


def _decision_value(record: Mapping[str, Any], name: str) -> Any:
    """Read a decision field from the producer payload or its canonical view."""
    value = record.get(name)
    if value is not None:
        return value
    view = record.get("decision_view")
    if isinstance(view, Mapping):
        value = view.get(name)
        if value is not None:
            return value
        policy = view.get("policy")
        if isinstance(policy, Mapping):
            return policy.get(name)
    return None


def _native_kind(record: Mapping[str, Any], *, kind: str) -> str:
    """Preserve Paper's native decision reason instead of its envelope type."""
    explicit = record.get("native_kind")
    if explicit:
        return str(explicit).lower()
    reason = _decision_value(record, "reason") or _decision_value(record, "reason_code")
    if reason:
        return str(reason).lower()
    stage = _decision_value(record, "decision_stage") or _decision_value(record, "stage")
    return str(stage or kind).lower()


def _is_structural_decision(record: Mapping[str, Any]) -> bool:
    """Exclude interaction reference closures from comparable decisions."""
    reason = _decision_value(record, "reason") or _decision_value(record, "reason_code")
    return str(reason).upper() != "REFERENCE_REPLACED"


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


def _neutral_stage(record: Mapping[str, Any]) -> str:
    raw = _decision_value(record, "stage") or _decision_value(record, "decision_stage")
    reason = str(_decision_value(record, "reason") or _decision_value(record, "reason_code") or "").lower()
    value = str(raw or "").lower()
    if value in DECISION_STAGES:
        return value
    if "interaction" in value or value in {"lifecycle", "location"}:
        return "interaction"
    if "admission" in value or "admission" in reason:
        return "admission"
    if "trigger" in value:
        return "trigger"
    if "exit" in value or str(record.get("event_type", "")).upper() in {"EXITDECISION", "EXIT", "CLOSE"}:
        return "exit"
    if "candidate" in value or "setup" in value or "opportunity" in value or "candidate" in reason or "setup_" in reason:
        return "candidate"
    raise ValueError("decision is missing a neutral stage")


def _neutral_outcome(record: Mapping[str, Any]) -> str:
    value = _decision_value(record, "outcome")
    reason = str(_decision_value(record, "reason") or _decision_value(record, "reason_code") or "").lower()
    if value is None:
        value = "unavailable" if record.get("unavailable") else "observed"
    outcome = str(value).lower()
    if outcome == "recorded":
        return "observed"
    if outcome not in DECISION_OUTCOMES:
        if "invalid" in reason:
            return "invalidated"
        if "expire" in reason:
            return "expired"
        if "cancel" in reason:
            return "cancelled"
        raise ValueError("decision has an unsupported outcome")
    return outcome


def _record(record: Mapping[str, Any], *, kind: str, sequence: int,
            parent_identity: str | None = None) -> dict[str, Any]:
    values = record.get("comparable_values", record.get("values", record))
    values = dict(values) if isinstance(values, Mapping) else {}
    if "comparable_values" not in record and "values" not in record:
        values = {key: value for key, value in values.items() if key not in {
            "event_type", "decision_id", "client_order_id", "producer_local_id",
            "candidate_id", "opportunity_id", "lifecycle_id", "setup_id", "causal_parent_id",
            "comparison_identity", "source", "session_date", "sequence", "stream_sequence",
            "account_revision",
            "producer_extensions",
            "decision_view",
        }}
    view = record.get("decision_view")
    if isinstance(view, Mapping):
        stage = record.get("stage") or view.get("stage")
        outcome = record.get("outcome") or view.get("outcome")
        evidence = record.get("evidence") or view.get("evidence")
    else:
        stage = record.get("stage")
        outcome = record.get("outcome", values.get("outcome"))
        evidence = record.get("evidence")
    occurred_at = _timestamp(record)
    native_kind = _native_kind(record, kind=kind)
    identity_basis = {
        "kind": kind,
        "minute": occurred_at[:16],
        "event_key": record.get("neutral_event_key") or record.get("event_key") or record.get("event_type"),
        "reason": _decision_value(record, "reason") or _decision_value(record, "reason_code"),
        "dimensions": {key: values.get(key) for key in (
            "cell", "vehicle", "direction", "side", "instrument", "status", "quantity",
        ) if values.get(key) is not None},
    }
    identity = hashlib.sha256(_canonical_json(identity_basis).encode("utf-8")).hexdigest()
    if not isinstance(evidence, Mapping):
        evidence = {}
    projected = {
        "kind": kind,
        "native_kind": native_kind,
        "stage": stage,
        "outcome": outcome,
        "comparison_identity": identity,
        "causal_parent_id": parent_identity,
        "evidence": _jsonable(dict(evidence)),
        "producer_extensions": _jsonable(record.get("producer_extensions", {})),
        "occurred_at": occurred_at,
        "sequence": sequence,
        "identity": identity,
        "values": _jsonable(values),
        "unavailable": list(record.get("unavailable", ())) + [key for key, value in values.items() if value is None],
        "identity_basis": identity_basis,
    }
    if kind == "decision":
        projected["stage"] = _neutral_stage(record)
        projected["outcome"] = _neutral_outcome(record)
        if projected["stage"] not in DECISION_STAGES or projected["outcome"] not in DECISION_OUTCOMES:
            raise ValueError("invalid neutral decision stage or outcome")
    return projected


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


_STRUCTURAL_STAGE_BY_REASON = {
    "setup_candidate_formed": "candidate_formation",
    "candidate_formed": "candidate_formation",
    "setup_expired": "setup_expiry",
    "expired": "setup_expiry",
    "opportunity_invalidated_by_source_gap": "setup_invalidation",
    "setup_invalidated": "setup_invalidation",
    "invalidated": "setup_invalidation",
    "orderflow_admission": "admission",
    "admission": "admission",
}
_TERMINAL_STRUCTURAL_STAGES = frozenset({"setup_expiry", "setup_invalidation", "admission"})


def _record_field(record: Mapping[str, Any], name: str) -> Any:
    value = record.get(name)
    if value is not None:
        return value
    values = record.get("values")
    if isinstance(values, Mapping) and values.get(name) is not None:
        return values[name]
    extensions = record.get("producer_extensions")
    if isinstance(extensions, Mapping):
        for extension in extensions.values():
            if not isinstance(extension, Mapping):
                continue
            causal_ids = extension.get("causal_ids")
            if isinstance(causal_ids, Mapping) and causal_ids.get(name) is not None:
                return causal_ids[name]
            if extension.get(name) is not None:
                return extension[name]
    return None


def _causal_opportunity_id(record: Mapping[str, Any]) -> str | None:
    for name in ("lifecycle_id", "opportunity_id", "candidate_id", "causal_parent_id"):
        value = _record_field(record, name)
        if value:
            return str(value)
    return None


def _structural_stage(record: Mapping[str, Any]) -> str | None:
    reason = _record_field(record, "reason") or _record_field(record, "reason_code")
    if reason is not None:
        stage = _STRUCTURAL_STAGE_BY_REASON.get(str(reason).lower())
        if stage is not None:
            return stage
    native = str(record.get("native_kind") or record.get("kind") or "").lower()
    return _STRUCTURAL_STAGE_BY_REASON.get(native)


def causal_lifecycle_records(
    left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compare structural lifecycle facts while retaining producer populations.

    This is diagnostic only.  It indexes every record, deduplicates only the
    structural view by causal opportunity identity, and reports repeated
    terminal transitions instead of hiding them from the parity layer.
    """
    def index(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        indexed: dict[str, list[dict[str, Any]]] = {}
        structural: dict[tuple[str, str], dict[str, Any]] = {}
        duplicates: Counter[tuple[str, str]] = Counter()
        native_kinds: Counter[str] = Counter()
        for record in records:
            native_kinds[str(record.get("native_kind") or record.get("kind") or "")] += 1
            identity = str(record.get("comparison_identity") or record.get("identity") or "")
            indexed.setdefault(identity, []).append(dict(record))
            opportunity = _causal_opportunity_id(record)
            stage = _structural_stage(record)
            if opportunity is None or stage is None:
                continue
            key = (opportunity, stage)
            if key in structural:
                duplicates[key] += 1
            else:
                structural[key] = dict(record)
        return {
            "record_count": len(records),
            "native_kind_counts": dict(sorted(native_kinds.items())),
            "identity_index": indexed,
            "structural_count": len(structural),
            "structural_index": {f"{key[0]}::{key[1]}": value for key, value in structural.items()},
            "duplicate_structural_records": [
                {"opportunity_id": key[0], "stage": key[1], "duplicates": count}
                for key, count in sorted(duplicates.items())
            ],
            "duplicate_terminal_transitions": [
                {"opportunity_id": key[0], "stage": key[1], "duplicates": count}
                for key, count in sorted(duplicates.items()) if key[1] in _TERMINAL_STRUCTURAL_STAGES
            ],
        }

    left_index = index(left)
    right_index = index(right)
    return {"left": left_index, "right": right_index}


def _deduplicate_structural_records(records: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    seen: set[tuple[str, str]] = set()
    result: list[Mapping[str, Any]] = []
    for record in records:
        opportunity = _causal_opportunity_id(record)
        stage = _structural_stage(record)
        key = (opportunity, stage) if opportunity is not None and stage is not None else None
        if key is not None and key in seen:
            continue
        if key is not None:
            seen.add(key)
        result.append(record)
    return result


def _causal_id(record: Mapping[str, Any]) -> str | None:
    values = record.get("values", record.get("comparable_values", record))
    values = values if isinstance(values, Mapping) else {}
    for key in ("candidate_id", "opportunity_id", "lifecycle_id", "setup_id", "causal_parent_id"):
        value = _record_field(record, key) or record.get(key) or values.get(key)
        if value:
            return str(value)
    return None


def _evidence_timestamp(record: Mapping[str, Any]) -> str | None:
    evidence = record.get("evidence")
    if isinstance(evidence, Mapping):
        timestamps = evidence.get("timestamps")
        if isinstance(timestamps, Sequence) and not isinstance(timestamps, (str, bytes)) and timestamps:
            return str(timestamps[0])
    values = record.get("values")
    if isinstance(values, Mapping):
        for key in ("observed_at", "available_at", "decision_at", "timestamp", "fill_timestamp"):
            if values.get(key):
                return str(values[key])
    return str(record.get("occurred_at")) if record.get("occurred_at") else None


def _provenance_gaps(artifact: Mapping[str, Any], layer: str, *, max_records: int) -> list[Mapping[str, Any]]:
    layers = artifact.get("layers", {})
    all_records = [item for values in layers.values() if isinstance(values, list)
                   for item in values if isinstance(item, Mapping)] if isinstance(layers, Mapping) else []
    known = {str(value) for item in all_records for value in (
        item.get("identity"), item.get("comparison_identity"), item.get("causal_parent_id"),
        _record_field(item, "decision_id"),
    ) if value}
    records = layers.get(layer, []) if isinstance(layers, Mapping) else []
    return [item for item in records if isinstance(item, Mapping)
            and (not item.get("causal_parent_id") or str(item["causal_parent_id"]) not in known)][:max_records]


def causal_lifecycle_diff(
    paper: Mapping[str, Any], canonical: Mapping[str, Any], *, max_records: int = 100,
) -> dict[str, Any]:
    """Report lifecycle coverage without changing the strict four-layer verdict."""
    def records(artifact: Mapping[str, Any], layer: str) -> list[Mapping[str, Any]]:
        raw = artifact.get("layers", {}).get(layer, [])
        return [item for item in raw if isinstance(item, Mapping)]

    paper_decisions = records(paper, "decision")
    canonical_decisions = records(canonical, "decision")

    def index(items: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
        indexed: dict[str, list[Mapping[str, Any]]] = {}
        for item in items:
            key = _causal_id(item) or str(item.get("comparison_identity") or item.get("identity"))
            indexed.setdefault(key, []).append(item)
        return indexed

    def indexes(items: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, list[Mapping[str, Any]]]]:
        fields = ("candidate_id", "opportunity_id", "lifecycle_id", "causal_parent_id",
                  "comparison_identity")
        result: dict[str, dict[str, list[Mapping[str, Any]]]] = {field: {} for field in fields}
        result["evidence_timestamp"] = {}
        for item in items:
            for field in fields:
                value = _record_field(item, field) or item.get(field)
                if value is not None:
                    result[field].setdefault(str(value), []).append(item)
            timestamp = _evidence_timestamp(item)
            if timestamp is not None:
                result["evidence_timestamp"].setdefault(timestamp, []).append(item)
        return result

    paper_index = index(paper_decisions)
    canonical_index = index(canonical_decisions)
    terminal_reasons = {
        "setup_expired", "setup_invalidated", "opportunity_invalidated_by_source_gap",
        "invalidated", "filled", "exit", "exit_decision", "position_closed", "hard_stop",
        "same_side_departure", "eod", "session_close",
    }

    def terminal_duplicates(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        seen: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
        for item in items:
            reason = _record_field(item, "reason") or _record_field(item, "reason_code")
            native = str(item.get("native_kind") or reason or item.get("kind") or "").lower()
            stage = _structural_stage(item) or native
            if native in terminal_reasons or stage in _TERMINAL_STRUCTURAL_STAGES:
                seen.setdefault((_causal_id(item) or "", stage), []).append(item)
        return [
            {"causal_id": key[0], "stage": key[1], "records": [
                {"identity": item.get("identity"), "native_kind": item.get("native_kind"),
                 "occurred_at": item.get("occurred_at"), "evidence_timestamp": _evidence_timestamp(item)}
                for item in values
            ]}
            for key, values in seen.items() if key[0] and len(values) > 1
        ][:max_records]

    paper_keys, canonical_keys = set(paper_index), set(canonical_index)
    return {
        "first_concrete_mismatch": next((
            {"causal_id": key, "paper": paper_index.get(key, []), "canonical": canonical_index.get(key, [])}
            for key in sorted(paper_keys | canonical_keys)
            if paper_index.get(key, [{}])[0] != canonical_index.get(key, [{}])[0]
        ), None),
        "missing_paper_records": [item for key in sorted(canonical_keys - paper_keys) for item in canonical_index[key]][:max_records],
        "extra_paper_records": [item for key in sorted(paper_keys - canonical_keys) for item in paper_index[key]][:max_records],
        "duplicate_terminal_transitions": terminal_duplicates(paper_decisions),
        "counts_by_native_kind": {
            "paper": dict(sorted(Counter(str(item.get("native_kind") or item.get("kind")) for item in paper_decisions).items())),
            "canonical": dict(sorted(Counter(str(item.get("native_kind") or item.get("kind")) for item in canonical_decisions).items())),
        },
        "indexes": {"paper": indexes(paper_decisions), "canonical": indexes(canonical_decisions)},
        "execution_provenance_gaps": _provenance_gaps(paper, "execution_settlement", max_records=max_records),
        "ledger_provenance_gaps": _provenance_gaps(paper, "portfolio_ledger", max_records=max_records),
        "remaining_canonical_coverage_blockers": canonical.get("coverage", {}).get("issues", []),
    }


def _coverage(
    decisions: Sequence[Mapping[str, Any]], bars_seen: int, unavailable: Sequence[str],
    *, execution_observed: bool, settlement_observed: bool, ledger_observed: bool,
    input_identity: Any, projection_issues: Sequence[str] = (),
) -> dict[str, Any]:
    issues = list(projection_issues)
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
                                     "settlement": settlement_observed, "ledger": ledger_observed},
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
                        if (is_decision_event(str(event.get("event_type", "")), event)
                            or event.get("decision_stage") is not None
                            or event.get("stage") is not None
                            or isinstance(event.get("decision_view"), Mapping))
                        and _is_structural_decision(event)]
    projection_issues = [
        f"missing_event_key:{index}"
        for index, event in enumerate(source_decisions)
        if not event.get("neutral_event_key") and not event.get("event_key")
    ]
    decisions = []
    local_identities: dict[str, str] = {}
    for index, event in enumerate(source_decisions):
        local_parent = event.get("causal_parent_id") or event.get("decision_id")
        try:
            projected = _record(event, kind="decision", sequence=len(decisions))
        except ValueError as exc:
            projection_issues.append(f"decision_projection_error:{index}:{exc}")
            continue
        if local_parent is not None and str(local_parent) in local_identities:
            projected["causal_parent_id"] = local_identities[str(local_parent)]
        decisions.append(projected)
        for key in ("decision_id", "candidate_id", "opportunity_id", "lifecycle_id", "setup_id"):
            value = event.get(key)
            if value is not None:
                local_identities[str(value)] = str(projected["identity"])
    execution_types = {"ORDER_INTENT", "ORDER_AUTHORIZED", "ORDER_ACK", "FILL", "ORDER_UNFILLED", "ORDER_SUPPRESSED", "RISK_REJECTED", "RESERVATION", "AUTHORIZATION", "SUBMISSION", "PENDING_EXIT", "EXIT_FILL", "CANCELLATION", "SETTLEMENT"}
    source_execution = [event for event in layer_records.get("execution", ())
                        if str(event.get("event_type", "")).upper() in execution_types
                        or str(event.get("event_type", "")).upper() in {"EXIT", "CLOSE"}]
    projection_issues.extend(
        f"missing_event_key:execution:{index}"
        for index, event in enumerate(source_execution)
        if not event.get("neutral_event_key") and not event.get("event_key")
    )
    execution_kinds = {"ORDER_INTENT": "order_intent", "ORDER_AUTHORIZED": "authorization",
                       "ORDER_ACK": "submission", "AUTHORIZATION": "authorization",
                       "SUBMISSION": "submission", "RESERVATION": "reservation", "FILL": "fill",
                       "EXIT_FILL": "exit_fill", "PENDING_EXIT": "pending_exit",
                       "CANCELLATION": "cancellation", "SETTLEMENT": "settlement",
                       "EXIT": "exit", "CLOSE": "exit", "ORDER_UNFILLED": "cancellation",
                       "ORDER_SUPPRESSED": "cancellation", "RISK_REJECTED": "cancellation"}
    execution = []
    for index, event in enumerate(source_execution):
        parent = local_identities.get(str(event.get("decision_id") or event.get("causal_parent_id")))
        if (event.get("decision_id") or event.get("causal_parent_id")) and parent is None:
            projection_issues.append(f"missing_parent_link:execution:{index}")
        execution.append(_record(
            event, kind=execution_kinds.get(str(event.get("event_type", "")).upper(), "order_intent"),
            sequence=index, parent_identity=parent,
        ))
    settlements = [_record(event, kind="synthetic_settlement", sequence=len(execution) + index)
                   for index, event in enumerate(result.get("synthetic_settlements", ()))]
    ledger_records: list[dict[str, Any]] = []
    for event in layer_records.get("ledger", ()):
        if isinstance(event, Mapping):
            if not event.get("neutral_event_key") and not event.get("event_key"):
                projection_issues.append(f"missing_event_key:ledger:{len(ledger_records)}")
            parent = local_identities.get(str(event.get("decision_id") or event.get("causal_parent_id")))
            if (event.get("decision_id") or event.get("causal_parent_id")) and parent is None:
                projection_issues.append(f"missing_parent_link:ledger:{len(ledger_records)}")
            ledger_records.append(_record(
                event,
                kind=str(event.get("kind") or event.get("event_type") or "ledger_mutation").lower(),
                sequence=len(ledger_records),
                parent_identity=parent,
            ))
    strategy, strategy_hash = strategy_payload_hash(result.get("strategy"))
    metadata = result.get("metadata", {})
    metadata = metadata if isinstance(metadata, Mapping) else {}
    timezone_name = str(request.get("timezone") or metadata.get("timezone") or "Asia/Kolkata")
    session_date = str(request.get("session_date", ""))[:10]
    requested_from = str(request.get("date_from") or session_date)[:10]
    requested_to = str(request.get("date_to") or session_date)[:10]
    replay_input = _replay_input(metadata)
    scope = {
        "source_contract": request.get("source_contract", "orderflow-replay-v1"),
        "requested_date_from": requested_from,
        "requested_date_to": requested_to,
        "effective_date_from": session_date,
        "effective_date_to": session_date,
        "timezone": timezone_name,
        "market_session": request.get("market_session", ["09:15", "15:30"]),
        "requested_sessions": [requested_from] if requested_from else [],
        "effective_sessions": [session_date] if session_date else [],
        "sessions": ["orderflow"] if str(request.get("strategy_id", "")).startswith("ftx-paper-orderflow") else [],
        "vehicles": list(request.get("vehicles", ())),
        "partitions": {"source": ["orderflow"]} if str(request.get("strategy_id", "")).startswith("ftx-paper-orderflow") else {},
        "session_end": request.get("session_end", "15:30"),
        "session_end_cutoff": request.get("session_end_cutoff", request.get("session_end", "15:30")),
        "decision_clock": request.get("decision_clock", {"cutoff": "exclusive"}),
        "capture_mode": request.get("capture_mode", metadata.get("capture_mode")),
        "capital_profile": request.get("capital_profile", result.get("configuration", {})),
        "strategy_configuration_hash": metadata.get("configuration_hash"),
        "replay_input": replay_input,
        "counts": {
            "selected_source_rows": int(metadata.get("selected_source_rows", 0)),
            "filtered_source_rows": int(metadata.get("filtered_source_rows", 0)),
            "completed_source_rows": int(metadata.get("completed_source_rows", 0)),
            "gap_rows": int(metadata.get("gap_rows", 0)),
            "completed_causal_bars": int(result.get("causal_bars_seen", 0)),
            "incomplete_causal_bars": int(metadata.get("incomplete_causal_bars", 0)),
            "processed_decision_bars": int(metadata.get("processed_decision_bars", result.get("bars_seen", 0))),
        },
        "processed": {
            "bar_count": int(metadata.get("processed_decision_bars", result.get("bars_seen", 0))),
            "session_count": 1 if session_date else 0,
        },
    }
    input_identity = metadata.get("input_fingerprint")
    coverage = _coverage(
        decisions,
        int(result.get("causal_bars_seen", result.get("bars_seen", 0))),
        result.get("unavailable_fields", ()),
        execution_observed="execution" in layer_records,
        settlement_observed="synthetic_settlements" in result or any(
            str(item.get("event_type", "")).upper() in {"SETTLEMENT", "SYNTHETIC_SETTLEMENT"}
            for item in layer_records.get("execution", ()) if isinstance(item, Mapping)
        ),
        ledger_observed="ledger" in layer_records,
        input_identity=input_identity,
        projection_issues=projection_issues,
    )
    coverage["layer_issues"] = {
        "run_scope": ["missing_input_identity"] if not input_identity else [],
        "decision": [issue for issue in projection_issues if ":execution:" not in issue and ":ledger:" not in issue],
        "execution_settlement": [issue for issue in projection_issues if ":execution:" in issue],
        "portfolio_ledger": [issue for issue in projection_issues if ":ledger:" in issue],
    }
    layers = {
        "run_scope": [_record({
            "occurred_at": f"{session_date}T00:00:00Z", "values": scope,
            "identity": hashlib.sha256(_canonical_json(scope).encode("utf-8")).hexdigest(),
            "comparison_identity": hashlib.sha256(_canonical_json(scope).encode("utf-8")).hexdigest(),
        }, kind="run_scope", sequence=0)],
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
    def canonical_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return [
            {key: value for key, value in record.items() if key != "producer_extensions"}
            for record in records
        ]

    layers: dict[str, dict[str, Any]] = {}
    for layer in PARITY_LAYERS:
        records = parity_layer_records(artifact, layer)
        if any(not isinstance(record, Mapping) for record in records):
            raise ValueError(f"invalid {layer} record")
        coverage = artifact.get("coverage", {})
        observed_layers = coverage.get("observed_layers", {})
        observed = bool(observed_layers.get(layer, False)) if isinstance(observed_layers, Mapping) else False
        layer_issues = coverage.get("layer_issues", {})
        incomplete = (
            any(issue in {"no_completed_bars", "missing_input_identity"} for issue in coverage.get("issues", ()))
            or (isinstance(layer_issues, Mapping) and bool(layer_issues.get(layer)))
        )
        status = "INCOMPLETE" if incomplete else (
            "AVAILABLE" if observed else "UNAVAILABLE"
        )
        layers[layer] = {"status": status, "observed": observed, "count": len(records),
                         "sha256": hashlib.sha256(_canonical_json(canonical_records(records)).encode("utf-8")).hexdigest()}
    scope = artifact.get("scope", {})
    return {"version": PARITY_MANIFEST_VERSION, "hash_algorithm": "sha256",
            "run_id": artifact.get("run_id"), "scope": scope,
            "contract": artifact.get("contract"), "layers": layers}


def validate_parity_artifact(artifact: Mapping[str, Any]) -> dict[str, Any]:
    required = ("artifact_type", "schema_version", "contract", "producer", "run_id", "scope", "coverage", "producer_configuration", "layers", "manifest")
    if any(key not in artifact for key in required) or artifact.get("artifact_type") != "producer_parity" or artifact.get("schema_version") != 2:
        raise ValueError("invalid parity artifact envelope")
    if not isinstance(artifact["producer"], Mapping) or not artifact["producer"].get("name"):
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
            if _timestamp(record) != record["occurred_at"]:
                raise ValueError(f"{layer} timestamps must be normalized to UTC")
            identity_basis = record.get("identity_basis")
            if not isinstance(identity_basis, Mapping):
                raise ValueError(f"{layer} record identity basis is required")
            expected_identity = hashlib.sha256(_canonical_json(identity_basis).encode("utf-8")).hexdigest()
            if record.get("identity") != expected_identity or record.get("comparison_identity") != expected_identity:
                raise ValueError(f"{layer} record identity is not canonical")
            if layer == "decision":
                if record.get("kind") != "decision" or record.get("stage") not in DECISION_STAGES:
                    raise ValueError("decision records require a neutral stage")
                if record.get("outcome") not in DECISION_OUTCOMES:
                    raise ValueError("decision records require a neutral outcome")
            elif layer == "execution_settlement" and record.get("kind") not in EXECUTION_KINDS:
                raise ValueError("unsupported execution lifecycle kind")
            elif layer == "portfolio_ledger" and record.get("kind") not in LEDGER_KINDS:
                raise ValueError("unsupported ledger mutation kind")
    identities = {
        str(record.get("identity"))
        for layer in PARITY_LAYERS
        for record in layers[layer]
        if isinstance(record, Mapping)
    }
    for layer in PARITY_LAYERS:
        for record in layers[layer]:
            parent = record.get("causal_parent_id")
            if parent is not None and str(parent) not in identities:
                raise ValueError(f"{layer} causal parent is unavailable")
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
    if result.get("status") == "UNAVAILABLE" and result.get("layer") in PARITY_LAYERS:
        layer = str(result["layer"])
        left_manifest_layer = left["manifest"]["layers"][layer]
        right_manifest_layer = right["manifest"]["layers"][layer]
        if (left_manifest_layer.get("count"), left_manifest_layer.get("sha256")) != (
            right_manifest_layer.get("count"), right_manifest_layer.get("sha256")
        ):
            result = {"status": "MISMATCHED", "layer": layer, "context_limit": max_context}
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
           "build_parity_artifact", "build_parity_manifest", "causal_lifecycle_diff",
           "causal_lifecycle_records",
           "compare_parity_artifacts",
           "compare_parity_manifests", "parity_layer_records", "read_parity_artifact",
           "strategy_payload_hash", "validate_parity_artifact"]
