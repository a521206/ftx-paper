from datetime import datetime, timedelta, timezone

from ftx_paper.strategy import PredicateStatus, SetupOpportunity, evaluate_admission


def _opportunity() -> SetupOpportunity:
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=timezone.utc)
    return SetupOpportunity.create(
        interaction_id="interaction-1", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=15), entry_lower=99,
        entry_upper=100, failure_boundary=98, evidence_ids=("e1",),
    )


def test_diagnostic_admission_records_ordered_predicates_without_intent() -> None:
    decision = evaluate_admission(_opportunity(), context_available=True, regime_allowed=True,
                                  opening_gap_allowed=True, active_position_allowed=True,
                                  exposure_allowed=True, allocation_allowed=True,
                                  sizing_allowed=True, risk_allowed=True)

    assert decision.status is PredicateStatus.PASSED
    assert decision.intent_id is None
    assert [item.predicate_id for item in decision.predicates] == [
        "context_relation", "regime_permission", "opening_gap_permission", "active_position_gate",
        "exposure_gate", "allocation_gate", "sizing_gate", "risk_gate",
    ]


def test_admission_preserves_unavailable_gate_reason() -> None:
    decision = evaluate_admission(_opportunity(), context_available=True, regime_allowed=None,
                                  opening_gap_allowed=True, active_position_allowed=True)

    assert decision.status is PredicateStatus.UNAVAILABLE
    assert decision.reason == "evidence_unavailable"
    assert len(decision.predicates) == 8
    assert all(item.status is PredicateStatus.UNAVAILABLE for item in decision.predicates[2:])


def test_admission_rejects_stale_setup_and_context_versions() -> None:
    opportunity = _opportunity()
    setup = evaluate_admission(
        opportunity, context_available=True, regime_allowed=True,
        opening_gap_allowed=True, active_position_allowed=True,
        setup_version="setup-old", expected_setup_version="setup-new",
    )
    assert setup.status is PredicateStatus.FAILED
    assert setup.reason == "setup_version_mismatch"

    context = evaluate_admission(
        opportunity, context_available=True, regime_allowed=True,
        opening_gap_allowed=True, active_position_allowed=True,
        context_version="context-old", expected_context_version="context-new",
    )
    assert context.status is PredicateStatus.FAILED
    assert context.reason == "context_version_mismatch"
