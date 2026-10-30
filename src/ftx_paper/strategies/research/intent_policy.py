"""Causal entry/exit intent policy projections."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class IntentAdmissionStatus(StrEnum):
    ALLOWED = "ALLOWED"
    DENIED = "DENIED"


@dataclass(frozen=True, slots=True)
class ExecutionAdmission:
    status: IntentAdmissionStatus
    reason: str | None
    timestamp: str
    intent_id: str | None = None


def causal_context_at(*, parent_context: Any | None, timestamp: str) -> Any | None:
    return parent_context if parent_context is not None and getattr(parent_context, "source_timeframe", None) == "5m" and parent_context.available_at <= timestamp else None


def evaluate_entry(*, opportunity: Any, timestamp: str, fill_permission_margin: float | None = None, execution: Any | None = None, decision_scores: tuple[float | None, float | None] | None = None, permission: bool = True) -> ExecutionAdmission:
    reason = None
    if not permission:
        reason = "opening_gap_permission_changed"
    elif fill_permission_margin is not None and getattr(opportunity, "decision_context_required", False):
        if decision_scores is None or any(value is None for value in decision_scores):
            reason = "decision_context_scores_unavailable"
        else:
            own = decision_scores[0] if str(opportunity.direction) in {"long", "Direction.LONG"} else decision_scores[1]
            opposing = decision_scores[1] if str(opportunity.direction) in {"long", "Direction.LONG"} else decision_scores[0]
            if own is not None and opposing is not None and own - opposing < fill_permission_margin:
                reason = "fill_permission_mismatch"
    return ExecutionAdmission(IntentAdmissionStatus.DENIED if reason else IntentAdmissionStatus.ALLOWED, reason, timestamp, getattr(getattr(execution, "intent", None), "intent_id", None))


def exit_admission_reason(*, score_direction: str | None, score_margin: float | None, decision_scores: tuple[float | None, float | None] | None, rule_valid: bool = True) -> str | None:
    if not rule_valid:
        return "exit_rule_invalid"
    if score_direction is None or score_margin is None:
        return None
    if decision_scores is None or any(value is None for value in decision_scores):
        return "exit_context_scores_unavailable"
    own, opposing = (decision_scores if score_direction == "long" else decision_scores[::-1])
    return "exit_score_mismatch" if own is not None and opposing is not None and own - opposing < score_margin else None


__all__ = ["ExecutionAdmission", "IntentAdmissionStatus", "causal_context_at", "evaluate_entry", "exit_admission_reason"]
