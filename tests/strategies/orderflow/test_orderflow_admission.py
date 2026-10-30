from datetime import datetime, timedelta, timezone

from ftx_paper.strategy import PredicateStatus, SetupOpportunity, evaluate_admission
from ftx_paper.strategy.orderflow import _admission_projection


def _opportunity() -> SetupOpportunity:
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=timezone.utc)
    return SetupOpportunity.create(
        interaction_id="interaction-1", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=15), entry_lower=99,
        entry_upper=100, failure_boundary=98, evidence_ids=("e1",),
    )


def test_diagnostic_admission_records_ordered_predicates_without_intent() -> None:
    decision = evaluate_admission(_opportunity(), context_available=True,
                                  opening_gap_allowed=True, active_position_allowed=True,
                                  exposure_allowed=True, allocation_allowed=True,
                                  sizing_allowed=True, risk_allowed=True)

    assert decision.status is PredicateStatus.PASSED
    assert decision.diagnostic_intent is not None
    assert decision.intent_id == decision.diagnostic_intent.intent_id
    assert [item.predicate_id for item in decision.predicates] == [
        "context_relation", "opening_gap_permission", "active_position_gate",
        "exposure_gate", "allocation_gate", "sizing_gate", "risk_gate",
    ]


def test_admission_preserves_unavailable_gate_reason() -> None:
    decision = evaluate_admission(_opportunity(), context_available=True,
                                  opening_gap_allowed=None, active_position_allowed=True)

    assert decision.status is PredicateStatus.UNAVAILABLE
    assert decision.reason == "evidence_unavailable"
    assert len(decision.predicates) == 7
    assert all(item.status is PredicateStatus.UNAVAILABLE for item in decision.predicates[1:])

    projection = _admission_projection(decision)
    assert projection["admission_outcome"] == "unavailable"
    assert projection["admission_first_failed_predicate"] == "opening_gap_permission"
    assert projection["admission_blocked_predicates"] == [
        "active_position_gate", "exposure_gate", "allocation_gate",
        "sizing_gate", "risk_gate",
    ]


def test_admission_projection_distinguishes_passed_and_failed_causes() -> None:
    passed = evaluate_admission(
        _opportunity(), context_available=True,
        opening_gap_allowed=True, active_position_allowed=True,
        evaluate_portfolio_gates=False,
    )
    failed = evaluate_admission(
        _opportunity(), context_available=True,
        opening_gap_allowed=False, active_position_allowed=True,
        evaluate_portfolio_gates=False,
    )

    assert _admission_projection(passed)["admission_outcome"] == "accepted"
    assert _admission_projection(failed) == {
        "admission_status": "FAILED",
        "admission_reason": "gate_denied",
        "admission_causal_reason": "gate_denied",
        "admission_first_failed_predicate": "opening_gap_permission",
        "admission_blocked_predicates": ["active_position_gate"],
        "admission_outcome": "rejected",
    }


def test_admission_rejects_stale_setup_and_context_versions() -> None:
    opportunity = _opportunity()
    setup = evaluate_admission(
        opportunity, context_available=True,
        opening_gap_allowed=True, active_position_allowed=True,
        setup_version="setup-old", expected_setup_version="setup-new",
    )
    assert setup.status is PredicateStatus.FAILED
    assert setup.reason == "setup_version_mismatch"

    context = evaluate_admission(
        opportunity, context_available=True,
        opening_gap_allowed=True, active_position_allowed=True,
        context_version="context-old", expected_context_version="context-new",
    )
    assert context.status is PredicateStatus.FAILED
    assert context.reason == "context_version_mismatch"


def test_executable_policy_keeps_admission_as_diagnostic_projection() -> None:
    decision = evaluate_admission(
        _opportunity(), context_available=True,
        opening_gap_allowed=True, active_position_allowed=True,
        exposure_allowed=True, allocation_allowed=True,
        sizing_allowed=True, risk_allowed=True,
    )

    assert decision.status is PredicateStatus.PASSED
    assert decision.diagnostic_intent is not None
    assert decision.diagnostic_intent.executable is False
