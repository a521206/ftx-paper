"""Causal entry and exit admission policy."""

from __future__ import annotations

from .hierarchy import ContextRelation, ContextVersionRef, MarketContext
from .execution import AdmissionStatus, ExecutionAdmission
from .timestamps import coerce_timestamp


def exit_admission_reason(*, score_direction: str | None, score_margin: float | None, decision_scores: tuple[float | None, float | None] | None, rule_valid: bool = True) -> str | None:
    if not rule_valid:
        return "exit_rule_invalid"
    if score_direction is None or score_margin is None:
        return None
    if decision_scores is None or None in decision_scores:
        return "exit_context_scores_unavailable"
    own = decision_scores[0] if score_direction == "long" else decision_scores[1]
    opposing = decision_scores[1] if score_direction == "long" else decision_scores[0]
    assert own is not None and opposing is not None
    return "exit_score_mismatch" if own - opposing < score_margin else None


def evaluate_entry(*, opportunity: object, timestamp: str, constraints: object,
                   execution: object, decision_scores: tuple[float | None, float | None] | None = None,
                   permission: bool = True) -> ExecutionAdmission:
    """Apply the canonical entry permission and decision-context gates."""
    reason: str | None = None
    if not permission:
        reason = "opening_gap_permission_changed"
    required = bool(getattr(opportunity, "decision_context_required", True))
    margin = getattr(constraints, "fill_permission_margin", None)
    intent = getattr(execution, "intent", None)
    if reason is None and margin is not None and intent is not None and required and decision_scores is None:
        reason = "decision_context_unavailable"
    if reason is None and margin is not None and required and decision_scores is not None:
        long_score, short_score = decision_scores
        if long_score is None or short_score is None:
            reason = "decision_context_scores_unavailable"
        else:
            direction = str(getattr(getattr(opportunity, "direction"), "value", getattr(opportunity, "direction")))
            own, opposing = ((long_score, short_score) if direction == "long" else (short_score, long_score))
            if own - opposing < margin:
                reason = "fill_permission_mismatch"
    status = AdmissionStatus.DENIED if reason else AdmissionStatus.ALLOWED
    return ExecutionAdmission(status, reason, timestamp, intent.intent_id if intent is not None else None)


def causal_context_at(*, parent_context: MarketContext | None, timestamp: str) -> MarketContext | None:
    return parent_context if parent_context is not None and parent_context.available_at <= coerce_timestamp(timestamp) else None


__all__ = ["causal_context_at", "evaluate_entry", "exit_admission_reason", "ContextRelation", "ContextVersionRef"]
