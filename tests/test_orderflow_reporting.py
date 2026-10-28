import pytest

from ftx_paper.strategy.orderflow_reporting import detached_replay_records
from ftx_paper.contracts import build_decision_view, validate_decision_view


def test_unavailable_and_rejected_views_keep_distinct_status_and_boundary() -> None:
    unavailable = validate_decision_view(build_decision_view({
        "decision_id": "u1", "decision_at": "2026-01-01T10:00:00+00:00",
        "outcome": "unavailable", "reason": "future_evidence",
        "expected_available_at": "2026-01-01T10:05:00+00:00",
    }, event_type="POLICY_DECISION"))
    rejected = validate_decision_view(build_decision_view({
        "decision_id": "r1", "decision_at": "2026-01-01T10:00:00+00:00",
        "outcome": "rejected", "reason": "gate_closed",
    }, event_type="POLICY_DECISION"))
    assert unavailable["policy"]["outcome"] == "unavailable"
    assert unavailable["evidence"]["provenance"]["expected_available_at"] == "2026-01-01T10:05:00+00:00"
    assert rejected["policy"]["outcome"] == "rejected"
    with pytest.raises(ValueError, match="availability boundary"):
        validate_decision_view(build_decision_view({
            "decision_id": "bad-boundary", "decision_at": "2026-01-01T10:00:00+00:00",
            "outcome": "unavailable", "reason": "future_evidence",
            "available_at": "not-a-timestamp",
        }, event_type="POLICY_DECISION"))


def test_decision_view_rejects_missing_outcome() -> None:
    with pytest.raises(ValueError, match="policy outcome"):
        validate_decision_view(build_decision_view({
            "decision_id": "missing", "decision_at": "2026-01-01T10:00:00+00:00",
        }, event_type="POLICY_DECISION"))


def test_recorded_facts_have_deterministic_same_timestamp_and_session_order() -> None:
    events = (
        {"event_type": "Z", "decision_id": "b", "decision_at": "2026-01-01T10:00:00+00:00"},
        {"event_type": "A", "decision_id": "a", "decision_at": "2026-01-01T10:00:00+00:00"},
    )
    records = detached_replay_records(events, scope={})
    assert [item["decision_id"] for item in records[0].events] == ["a", "b"]
