"""Strategy-neutral decision records shared by policy and presentation layers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


DECISION_VIEW_SCHEMA_VERSION = 1
_POLICY_OUTCOMES = frozenset({"accepted", "rejected", "cancelled", "superseded", "recorded", "triggered"})


def _value(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    return None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _set_if_missing(mapping: dict[str, Any], key: str, value: Any) -> None:
    if mapping.get(key) in (None, "") and value not in (None, ""):
        mapping[key] = value


def build_decision_view(
    payload: Mapping[str, Any],
    *,
    event_type: str,
    strategy_id: str | None = None,
) -> dict[str, Any]:
    """Build the persisted, strategy-neutral view from a decision payload.

    Strategies may provide ``subject``, ``policy`` and ``evidence`` directly.
    The flat-field fallback keeps previously persisted decisions readable while
    strategies migrate to the structured contract.
    """
    native = all(isinstance(payload.get(key), Mapping) for key in ("subject", "policy", "evidence"))
    subject = _mapping(payload.get("subject"))
    _set_if_missing(subject, "instrument", _value(payload, "instrument", "symbol", "market", "contract"))
    _set_if_missing(subject, "vehicle", _value(payload, "vehicle"))
    _set_if_missing(subject, "location", _value(payload, "location", "cell", "zone"))
    _set_if_missing(subject, "direction", _value(payload, "direction", "side", "action"))
    _set_if_missing(subject, "setup", _value(payload, "setup", "setup_type"))
    subject = {key: value for key, value in subject.items() if value is not None and value != ""}

    policy = _mapping(payload.get("policy"))
    _set_if_missing(policy, "outcome", _value(payload, "outcome") or "recorded")
    _set_if_missing(policy, "reason_code", _value(payload, "reason", "reason_code"))
    _set_if_missing(policy, "message", _value(payload, "message", "detail", "signal", "trigger"))
    policy = {key: value for key, value in policy.items() if value is not None and value != ""}

    evidence = _mapping(payload.get("evidence"))
    metrics = _mapping(evidence.get("metrics"))
    for key in (
        "score", "setup_score", "stop", "stop_basis", "stop_bp", "vix", "vix_at_event",
        "pcr", "pcr_at_event", "quantity", "bars_held", "mae_bp", "mfe_bp",
        "entry_price", "exit_price", "fill_price", "requested_quantity", "effective_quantity",
        "risk_per_trade", "risk_amount", "score_multiplier", "max_lots", "lot_size", "sequence",
        "confirmation_count", "missing_inputs", "detector_emission_ordinal",
    ):
        if key in payload and payload[key] is not None:
            metrics.setdefault(key, payload[key])
    if metrics:
        evidence["metrics"] = metrics
    provenance = _mapping(evidence.get("provenance"))
    for key in (
        "bundle_id", "decision_source", "session_date", "source_observation_id", "source_observation_ids",
        "response_evidence_ids", "feature_version", "availability_cutoff", "timeframe", "instrument",
        "available_at", "observed_at", "interval_start", "interval_end", "generation", "interaction_id",
        "opportunity_id", "replay_provenance", "template_id", "template_version", "reference_level",
        "lower_band", "upper_band",
    ):
        if key in payload and payload[key] is not None:
            provenance.setdefault(key, payload[key])
    if provenance:
        evidence["provenance"] = provenance
    features = _mapping(evidence.get("features"))
    if isinstance(payload.get("feature_values"), Mapping):
        features.update(payload["feature_values"])
    if features:
        evidence["features"] = features
    diagnostics = _mapping(evidence.get("diagnostics"))
    for key in ("score_factors", "required_input_availability", "sizing_pipeline"):
        if isinstance(payload.get(key), Mapping):
            diagnostics[key] = dict(payload[key])
    if diagnostics:
        evidence["diagnostics"] = diagnostics
    state = _mapping(evidence.get("state"))
    for key in ("response_state", "interaction_state", "generation", "trigger"):
        if key in payload and payload[key] is not None:
            state[key] = payload[key]
    if state:
        evidence["state"] = state

    decision_at = _value(payload, "decision_at", "timestamp", "created_at")
    resolved_strategy_id = strategy_id or _value(payload, "strategy_id")
    view = {
        "schema_version": DECISION_VIEW_SCHEMA_VERSION,
        "event_type": event_type,
        "at": decision_at,
        "strategy_id": resolved_strategy_id,
        "strategy_version": _value(payload, "strategy_version"),
        "config_hash": _value(payload, "config_hash"),
        "decision_id": _value(payload, "decision_id", "candidate_id"),
        "stage": _value(payload, "decision_stage", "stage"),
        "subject": subject,
        "policy": policy,
        "evidence": evidence,
        "context": subject.get("location") or subject.get("instrument") or subject.get("setup"),
        "direction": subject.get("direction"),
        "detail": policy.get("reason_code") or policy.get("message") or event_type,
        "outcome": policy.get("outcome", "recorded"),
    }
    if not native:
        view["legacy_adapted"] = True
    return view


def validate_decision_view(view: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return a canonical decision view for persistence/readers."""
    required = ("schema_version", "at", "decision_id", "subject", "policy", "evidence")
    missing = [key for key in required if view.get(key) in (None, "")]
    if missing:
        raise ValueError("decision view is missing required fields: " + ", ".join(missing))
    if view.get("schema_version") != DECISION_VIEW_SCHEMA_VERSION:
        raise ValueError("unsupported decision view schema version")
    if not isinstance(view["at"], str) or not view["at"].strip():
        raise ValueError("decision view at must be a non-empty ISO timestamp")
    for key in ("subject", "policy", "evidence"):
        if not isinstance(view[key], Mapping):
            raise ValueError(f"decision view {key} must be an object")
    outcome = view["policy"].get("outcome")
    if outcome not in _POLICY_OUTCOMES:
        raise ValueError("decision view policy outcome is invalid")
    return dict(view)


__all__ = ["DECISION_VIEW_SCHEMA_VERSION", "build_decision_view", "validate_decision_view"]
