"""Ordered diagnostic admission and intent contracts for orderflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from .orderflow_opportunity import SetupOpportunity


class PredicateStatus(StrEnum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class PredicateResult:
    predicate_id: str
    status: PredicateStatus
    value: object | None = None
    threshold: object | None = None
    reason: str | None = None
    units: str | None = None
    reference_window: str | None = None
    evidence_ids: tuple[str, ...] = ()
    context_version: str | None = None
    permission_version: str | None = None


@dataclass(frozen=True, slots=True)
class DiagnosticDecisionIntent:
    """Non-executable projection of a causally eligible setup."""

    intent_id: str
    opportunity_id: str
    executable: bool = False
    setup_id: str | None = None
    setup_version: str | None = None
    permission_version: str | None = None
    evidence_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    opportunity_id: str
    status: PredicateStatus
    reason: str
    predicates: tuple[PredicateResult, ...]
    intent_id: str | None = None
    diagnostic_intent: DiagnosticDecisionIntent | None = None

    @property
    def decision_id(self) -> str:
        return f"admission:{self.opportunity_id}"

    @property
    def evidence_ids(self) -> tuple[str, ...]:
        return tuple(sorted({
            evidence_id
            for predicate in self.predicates
            for evidence_id in predicate.evidence_ids
        }))


def evaluate_admission(
    opportunity: SetupOpportunity,
    *,
    context_available: bool,
    regime_allowed: bool | None,
    opening_gap_allowed: bool | None,
    active_position_allowed: bool | None,
    exposure_allowed: bool | None = None,
    allocation_allowed: bool | None = None,
    sizing_allowed: bool | None = None,
    risk_allowed: bool | None = None,
    setup_valid: bool | None = None,
    direction_matches: bool | None = None,
    confirmations_available: bool | None = None,
    current_at: datetime | None = None,
    evidence_ids: tuple[str, ...] = (),
    context_version: str | None = None,
    context_relation: str | None = None,
    permission_version: str | None = None,
    setup_version: str | None = None,
    expected_setup_version: str | None = None,
    expected_context_version: str | None = None,
    reference_window: str | None = None,
    diagnostic: bool = True,
) -> AdmissionDecision:
    """Apply gates in canonical order; diagnostic mode never creates an intent."""
    if not diagnostic:
        raise ValueError("executable orderflow admission is deferred")
    if opportunity.status.value != "ACTIVE":
        predicate = PredicateResult(
            "setup_lifecycle", PredicateStatus.FAILED, value=False,
            reason=f"setup_{opportunity.status.value.lower()}",
            evidence_ids=evidence_ids, context_version=context_version,
            permission_version=permission_version, reference_window=reference_window,
        )
        return AdmissionDecision(opportunity.lifecycle_id, PredicateStatus.FAILED,
                                 predicate.reason or "setup_not_active", (predicate,))
    if expected_setup_version is not None and setup_version != expected_setup_version:
        predicate = PredicateResult(
            "setup_version", PredicateStatus.FAILED, value=False,
            reason="setup_version_mismatch", evidence_ids=evidence_ids,
            context_version=context_version, permission_version=permission_version,
            reference_window=reference_window,
        )
        return AdmissionDecision(opportunity.lifecycle_id, PredicateStatus.FAILED, predicate.reason or "setup_version_mismatch", (predicate,))
    if expected_context_version is not None and context_version != expected_context_version:
        predicate = PredicateResult(
            "context_version", PredicateStatus.FAILED, value=False,
            reason="context_version_mismatch", evidence_ids=evidence_ids,
            context_version=context_version, permission_version=permission_version,
            reference_window=reference_window,
        )
        return AdmissionDecision(opportunity.lifecycle_id, PredicateStatus.FAILED, predicate.reason or "context_version_mismatch", (predicate,))
    if context_relation is not None and context_relation not in {
        "confirmed", "conflicted", "neutral", "unavailable",
    }:
        raise ValueError("invalid context relation")
    relation_allowed = (
        context_available
        and context_relation not in {"conflicted", "unavailable"}
    ) if context_relation is not None else context_available
    checks = [
        ("context_relation", relation_allowed),
        ("regime_permission", regime_allowed),
        ("opening_gap_permission", opening_gap_allowed),
        ("active_position_gate", active_position_allowed),
    ]
    optional_checks = (
        ("exposure_gate", exposure_allowed), ("allocation_gate", allocation_allowed),
        ("sizing_gate", sizing_allowed), ("risk_gate", risk_allowed),
    )
    checks.extend(optional_checks)
    if setup_valid is not None:
        checks.append(("setup_validity", setup_valid))
    if direction_matches is not None:
        checks.append(("direction_match", direction_matches))
    if confirmations_available is not None:
        checks.append(("confirmation_availability", confirmations_available))
    if current_at is not None:
        checks.append(("setup_expiry", current_at < opportunity.setup.expires_at))
    predicates: list[PredicateResult] = []
    for index, (predicate_id, value) in enumerate(checks):
        if value is None:
            result = PredicateResult(
                predicate_id, PredicateStatus.UNAVAILABLE, reason="evidence_unavailable",
                evidence_ids=evidence_ids, context_version=context_version,
                permission_version=permission_version, reference_window=reference_window,
            )
        elif value:
            result = PredicateResult(
                predicate_id, PredicateStatus.PASSED, value=True,
                evidence_ids=evidence_ids, context_version=context_version,
                permission_version=permission_version, reference_window=reference_window,
            )
        else:
            result = PredicateResult(
                predicate_id, PredicateStatus.FAILED, value=False, reason="gate_denied",
                evidence_ids=evidence_ids, context_version=context_version,
                permission_version=permission_version, reference_window=reference_window,
            )
        predicates.append(result)
        if result.status is not PredicateStatus.PASSED:
            predicates.extend(
                PredicateResult(
                    later_id, PredicateStatus.UNAVAILABLE, value=None,
                    reason=f"blocked_by:{predicate_id}", evidence_ids=evidence_ids,
                    context_version=context_version, permission_version=permission_version,
                    reference_window=reference_window,
                )
                for later_id, _ in checks[index + 1:]
            )
            return AdmissionDecision(opportunity.lifecycle_id, result.status, result.reason or result.status.value, tuple(predicates))
    intent = DiagnosticDecisionIntent(
        intent_id=f"diagnostic:{opportunity.lifecycle_id}",
        opportunity_id=opportunity.lifecycle_id,
        setup_id=opportunity.setup.setup_id,
        setup_version=opportunity.setup.setup_id,
        evidence_ids=opportunity.setup.evidence_ids,
    )
    return AdmissionDecision(
        opportunity.lifecycle_id, PredicateStatus.PASSED,
        "diagnostic_eligible", tuple(predicates), None, intent,
    )


def can_requalify(*, cancellation_reason: str, allowed_reasons: frozenset[str] = frozenset({"expired_trigger", "cancelled_before_submission"})) -> bool:
    """Requalification is explicit and never applies to terminal denials."""
    return cancellation_reason in allowed_reasons


__all__ = [
    "AdmissionDecision", "DiagnosticDecisionIntent", "PredicateResult",
    "PredicateStatus", "can_requalify", "evaluate_admission",
]
