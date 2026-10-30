"""Minimal producer-neutral parity artifact for Paper replays."""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import hashlib
import json
from typing import Any

from ftx_paper.contracts.replay import LEGACY_REPLAY_FIELDS


PARITY_ARTIFACT_SCHEMA_VERSION = 2
PARITY_CONTRACT_ID = "producer_replay_parity"
PARITY_MANIFEST_VERSION = 1
NORMALIZED_BAR_FINGERPRINT_RECIPE = "normalized-bars-v1"
_SESSION_ZONE = ZoneInfo("Asia/Kolkata")
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


def normalized_bar_fingerprint(rows: Sequence[Mapping[str, Any]]) -> str:
    """Hash comparable normalized bars using the canonical source recipe.

    Futures source identity is represented separately in replay provenance. The
    canonical input identity uses the continuous-series transformation so a
    dated contract selected by Paper compares with NiftyZoning's series `I`.
    """
    normalized = []
    for row in rows:
        minute = str(row.get("minute") or row.get("time") or "")
        try:
            timestamp = datetime.fromisoformat(minute.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("parity bar timestamps must be ISO timestamps") from exc
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("parity bar timestamps must be timezone-aware")
        required = ("open", "high", "low", "close")
        if any(row.get(name) is None for name in required):
            raise ValueError("parity bars require complete OHLCV values")
        try:
            open_price = float(row["open"])
            high_price = float(row["high"])
            low_price = float(row["low"])
            close_price = float(row["close"])
            volume = int(row["volume"]) if row.get("volume") is not None else None
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("parity bar OHLCV values must be numeric") from exc
        session_timestamp = timestamp.astimezone(_SESSION_ZONE)
        normalized.append({
            # Canonical source identity uses the session-local minute label,
            # not the transport timestamp or its UTC-derived calendar date.
            "time": session_timestamp.strftime("%H:%M"),
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume if volume is not None else 0,
            "oi": int(row.get("oi", row.get("open_interest")) or 0),
            "symbol": "NIFTY",
            "expiry": "",
            "contract_type": "CONTINUOUS",
            "session_date": str(row.get("session_date") or session_timestamp.date().isoformat()),
        })
    normalized.sort(key=lambda row: tuple(row[key] for key in (
        "session_date", "time", "open", "high", "low", "close", "volume", "oi",
        "symbol", "expiry", "contract_type",
    )))
    return hashlib.sha256(_canonical_json(normalized).encode("utf-8")).hexdigest()


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
    fingerprint = (
        current.get("normalized_fingerprint")
        or metadata.get("normalized_fingerprint")
    )
    symbol = str(current.get("selected_symbol") or current.get("symbol") or "NIFTY")
    expiry = str(current.get("selected_expiry") or current.get("expiry") or "")
    instrument_type = str(current.get("instrument_type") or "FUT").upper()
    source_identity = {
        "symbol": symbol,
        "series": str(current.get("series") or "").upper(),
        "contract_type": instrument_type,
        "expiry": expiry,
    }
    identity = {
        "symbol": symbol,
        "series": "I",
        "contract_type": "CONTINUOUS",
        "expiry": "",
    }
    source = {
        "symbol": symbol,
        "series": "I",
        "contract_type": "CONTINUOUS",
        "expiry": "",
        "bar_count": int(current.get("source_bar_count") or metadata.get("input_bar_count") or 0),
        "fingerprint": fingerprint,
        "identity": identity,
        "identities": [identity],
        "source_identity": source_identity,
        "fingerprint_recipe": NORMALIZED_BAR_FINGERPRINT_RECIPE,
    }
    return {
        "identity": [fingerprint] if fingerprint else [],
        "provenance": [source],
        "fingerprint_recipe": NORMALIZED_BAR_FINGERPRINT_RECIPE,
    }


def _timestamp(record: Mapping[str, Any]) -> str:
    value = (record.get("occurred_at") or record.get("observed_at") or record.get("available_at")
             or record.get("timestamp") or record.get("decision_at") or record.get("fill_timestamp"))
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
    if str(record.get("event_type", "")).upper() == "POLICY_DECISION":
        return "interaction"
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
    legacy = set(record).intersection(LEGACY_REPLAY_FIELDS)
    values_candidate = record.get("comparable_values", record.get("values"))
    if isinstance(values_candidate, Mapping):
        legacy.update(set(values_candidate).intersection(LEGACY_REPLAY_FIELDS))
    if legacy:
        raise ValueError("legacy replay fields are not accepted: " + ", ".join(sorted(legacy)))
    values = record.get("comparable_values", record.get("values", record))
    values = dict(values) if isinstance(values, Mapping) else {}
    # Producer-neutral records own the stream position.  Legacy replay
    # envelopes may carry a restarted sequence inside ``values``; retaining it
    # would wrap producer-local ordering into the parity payload.
    values.pop("sequence", None)
    values.pop("stream_sequence", None)
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
    for field in ("observed_at", "available_at", "neutral_event_key", "event_key"):
        if record.get(field) is not None:
            projected[field] = _jsonable(record[field])
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
        stages[str(record.get("stage") or values.get("decision_stage", "unknown"))] += 1
        outcomes[str(record.get("outcome") or values.get("outcome", record.get("kind", "unknown")))] += 1
        reasons[str(record.get("reason") or values.get("reason") or values.get("reason_code") or "")] += 1
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
    "admission_decided": "admission",
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


def _parent_identity(record: Mapping[str, Any], identities: Mapping[str, str]) -> str | None:
    """Resolve producer lifecycle IDs without exposing them in canonical values."""
    candidates = (
        record.get("causal_parent_id"), record.get("decision_id"),
        record.get("entry_decision_id"), record.get("opportunity_id"),
        record.get("candidate_id"), record.get("lifecycle_id"),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        value = str(candidate)
        if value in identities:
            return identities[value]
        base = value.rsplit(":", 1)[0]
        if base in identities:
            return identities[base]
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
             "field_availability": {"decision": bool(decisions), "execution_settlement": execution_observed or settlement_observed,
                                      "portfolio_ledger": ledger_observed},
            "unavailable": sorted({str(item) for item in unavailable})}


def build_parity_artifact(
    result: Mapping[str, Any], *, request: Mapping[str, Any] | None = None,
    layer_records: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Build the four-layer artifact from producer-owned replay facts."""
    request = request or {}
    layer_records = layer_records or {}
    source_decisions = list(layer_records.get("decision", ()))
    source_decisions = [event for event in source_decisions if _is_structural_decision(event)]
    projection_issues: list[str] = []
    decisions = []
    local_identities: dict[str, str] = {}
    decision_parents: list[str | None] = []
    projected_decisions: list[dict[str, Any]] = []
    for index, event in enumerate(source_decisions):
        local_parent = (
            event.get("causal_parent_id") or event.get("decision_id")
            or event.get("opportunity_id") or event.get("candidate_id")
            or event.get("lifecycle_id") or event.get("setup_id")
        )
        try:
            projected = _record(event, kind="decision", sequence=len(decisions))
        except ValueError as exc:
            raise ValueError(f"decision projection failed at {index}: {exc}") from exc
        # Sequence is an artifact-owned stream position.  Never retain a
        # producer/runtime sequence (which may restart for each bar).
        projected["sequence"] = len(projected_decisions)
        projected_decisions.append(projected)
        decision_parents.append(str(local_parent) if local_parent is not None else None)
        for key in ("decision_id", "candidate_id", "opportunity_id", "lifecycle_id", "setup_id"):
            value = event.get(key)
            if value is not None:
                local_identities.setdefault(str(value), str(projected["identity"]))
    for index, (projected, local_parent) in enumerate(zip(projected_decisions, decision_parents)):
        if local_parent is not None:
            parent = local_identities.get(local_parent)
            if parent and parent != projected["identity"]:
                projected["causal_parent_id"] = parent
            elif not parent:
                projection_issues.append(f"missing_parent_link:decision:{index}")
        projected["sequence"] = len(decisions)
        decisions.append(projected)
    execution_types = {"ORDER_INTENT", "ORDER_AUTHORIZED", "ORDER_ACK", "FILL", "ORDER_UNFILLED", "EXECUTION_PROPOSAL_SUPPRESSED", "ENTRY_BLOCKED_RISK", "EXECUTION_GROUP_CANDIDATE_DENIED", "TRIGGER_DENIED_PORTFOLIO", "RESERVATION", "AUTHORIZATION", "SUBMISSION", "PENDING_EXIT", "EXIT_FILL", "CANCELLATION", "SETTLEMENT"}
    source_execution = [event for event in layer_records.get("execution_settlement", ())
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
                       "EXECUTION_PROPOSAL_SUPPRESSED": "execution_proposal_suppressed",
                       "ENTRY_BLOCKED_RISK": "entry_blocked_risk",
                       "EXECUTION_GROUP_CANDIDATE_DENIED": "execution_group_candidate_denied",
                       "TRIGGER_DENIED_PORTFOLIO": "trigger_denied_portfolio"}
    execution = []
    for index, event in enumerate(source_execution):
        parent = _parent_identity(event, local_identities)
        if any(event.get(key) for key in ("decision_id", "causal_parent_id", "entry_decision_id", "opportunity_id", "candidate_id", "lifecycle_id")) and parent is None:
            projection_issues.append(f"missing_parent_link:execution:{index}")
        projected = _record(
            event, kind=execution_kinds.get(str(event.get("event_type", "")).upper(), "order_intent"),
            sequence=index, parent_identity=parent,
        )
        projected["sequence"] = len(execution)
        execution.append(projected)
    settlements = []
    for index, event in enumerate(result.get("synthetic_settlements", ())):
        projected = _record(event, kind="synthetic_settlement", sequence=len(execution) + index)
        projected["sequence"] = len(execution) + index
        settlements.append(projected)
    ledger_records: list[dict[str, Any]] = []
    for event in layer_records.get("portfolio_ledger", ()):
        if isinstance(event, Mapping):
            if not event.get("neutral_event_key") and not event.get("event_key"):
                projection_issues.append(f"missing_event_key:ledger:{len(ledger_records)}")
            parent = _parent_identity(event, local_identities)
            if any(event.get(key) for key in ("decision_id", "causal_parent_id", "entry_decision_id", "opportunity_id", "candidate_id", "lifecycle_id")) and parent is None:
                projection_issues.append(f"missing_parent_link:ledger:{len(ledger_records)}")
            projected = _record(
                event,
                kind=str(event.get("kind") or event.get("event_type") or "ledger_mutation").lower(),
                sequence=len(ledger_records),
                parent_identity=parent,
            )
            projected["sequence"] = len(ledger_records)
            ledger_records.append(projected)
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
        "vehicles": list(request.get("vehicles", ("futures", "synthetic"))),
        "session_end_cutoff": request.get("session_end_cutoff", "15:30"),
        "decision_clock": request.get("decision_clock", {"start": "inclusive", "end": "exclusive"}),
        "fingerprint_recipe": NORMALIZED_BAR_FINGERPRINT_RECIPE,
        "execution_scope": str(result.get("execution_scope") or request.get("mode") or "replay"),
        "ledger_scope": str(result.get("ledger_scope") or "replay"),
        "capture_mode": request.get("capture_mode", metadata.get("capture_mode", "full")),
        "capital_profile": request.get("capital_profile", {"name": "research"}),
        "strategy_configuration": request.get("strategy_configuration", {
            "candidate_states_are_actionable": False,
            "cost_basis": "gross_before_costs",
            "dynamic_session_extrema": "updated from completed 5m aggregates at their available_at",
            "entry_basis": "5m structural candidate plus causal 1m displacement or 5m recovery retest",
            "exit_basis": "owner-authorized 1m or 5m policy signal",
            "registered_locations": ["OR_LOW", "OR_HIGH", "PD_LOW", "PD_HIGH", "SESSION_HIGH", "SESSION_LOW"],
            "research_only": True,
        }),
        "replay_input": replay_input,
        "counts": {
            "selected_source_rows": int(metadata.get("selected_source_rows", 0)),
            "filtered_source_rows": int(metadata.get("filtered_source_rows", 0)),
            "completed_source_rows": int(metadata.get("completed_source_rows", 0)),
            "gap_rows": int(metadata.get("gap_rows", 0)),
            "processed_decision_bars": int(metadata.get("processed_decision_bars", result.get("bars_seen", 0))),
        },
    }
    if metadata.get("orderflow_daily_atr"):
        scope["orderflow_daily_atr"] = _jsonable(metadata["orderflow_daily_atr"])
    replay_identity = replay_input.get("identity", ())
    input_identity = replay_identity[0] if replay_identity else None
    coverage = _coverage(
        decisions,
        int(result.get("causal_bars_seen", result.get("bars_seen", 0))),
        result.get("unavailable_fields", ()),
        execution_observed="execution_settlement" in layer_records,
        settlement_observed="synthetic_settlements" in result or any(
            str(item.get("event_type", "")).upper() in {"SETTLEMENT", "SYNTHETIC_SETTLEMENT"}
            for item in layer_records.get("execution_settlement", ()) if isinstance(item, Mapping)
        ),
        ledger_observed="portfolio_ledger" in layer_records,
        input_identity=input_identity,
        projection_issues=projection_issues,
    )
    coverage["layer_issues"] = {
        "run_scope": ["missing_input_identity"] if not input_identity else [],
        "decision": [issue for issue in projection_issues if ":execution:" not in issue and ":ledger:" not in issue],
        "execution_settlement": [issue for issue in projection_issues if ":execution:" in issue],
        "portfolio_ledger": [issue for issue in projection_issues if ":ledger:" in issue],
    }
    run_scope_identity = hashlib.sha256(_canonical_json({"kind": "run_scope", "scope": scope}).encode("utf-8")).hexdigest()
    layers = {
        "run_scope": [{"kind": "run_scope", "occurred_at": f"{session_date}T00:00:00Z", "sequence": 0,
                       "identity": run_scope_identity, "comparison_identity": run_scope_identity,
                       "identity_basis": {"kind": "run_scope", "scope": scope}, "values": _jsonable(scope), "unavailable": []}],
        "decision": decisions,
        "execution_settlement": [*execution, *settlements],
        "portfolio_ledger": ledger_records,
    }
    run_id = str(request.get("run_id") or result.get("run_id") or "paper-replay")
    execution_scope = str(result.get("execution_scope") or request.get("mode") or "replay")
    for records in layers.values():
        for record in records:
            record.update({
                "schema_version": 1,
                "run_id": run_id,
                "execution_scope": execution_scope,
                "ledger_scope": "replay",
            })
    artifact = {"artifact_type": "producer_parity", "schema_version": PARITY_ARTIFACT_SCHEMA_VERSION,
                "contract": {"id": PARITY_CONTRACT_ID, "version": 1, "layers": list(PARITY_LAYERS)},
                 "producer": {"name": "ftx-paper", "version": str(strategy.get("version", "unknown"))},
                 "run_id": run_id,
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
            not bool(coverage.get("complete", True))
            or (
            any(issue in {"no_completed_bars", "missing_input_identity"} for issue in coverage.get("issues", ()))
            or (isinstance(layer_issues, Mapping) and bool(layer_issues.get(layer)))
            )
        )
        status = "UNAVAILABLE" if not observed else ("INCOMPLETE" if incomplete else "AVAILABLE")
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
            if not isinstance(record, Mapping) or any(key not in record for key in ("schema_version", "run_id", "execution_scope", "ledger_scope", "kind", "occurred_at", "sequence", "identity", "values", "unavailable")):
                raise ValueError(f"invalid {layer} record")
            if record["sequence"] != sequence:
                raise ValueError(f"{layer} sequence must be contiguous")
            if record.get("schema_version") != 1 or record.get("run_id") != artifact.get("run_id"):
                raise ValueError(f"{layer} record scope is invalid")
            if record.get("execution_scope") != "replay" or record.get("ledger_scope") != "replay":
                raise ValueError(f"{layer} record scope is invalid")
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


__all__ = ["NORMALIZED_BAR_FINGERPRINT_RECIPE", "PARITY_ARTIFACT_SCHEMA_VERSION", "PARITY_CONTRACT_ID", "PARITY_LAYERS",
           "build_parity_artifact", "build_parity_manifest", "causal_lifecycle_diff",
           "causal_lifecycle_records",
           "compare_parity_artifacts",
           "compare_parity_manifests", "parity_layer_records", "read_parity_artifact",
            "strategy_payload_hash", "normalized_bar_fingerprint", "validate_parity_artifact"]
