import pytest

from datetime import datetime, timezone

from ftx_paper.strategies.orderflow.orderflow_capture import ReplayRecord
from ftx_paper.strategies.orderflow.orderflow_reporting import (
    build_multi_session_report,
    build_session_summary,
    causal_actions,
    detached_replay_records,
    first_divergent_phase,
)
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


def test_session_reports_project_detached_facts_in_record_order() -> None:
    timestamp = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
    record = ReplayRecord(
        1,
        timestamp,
        timestamp,
        {"state": "before"},
        {"state": "after"},
        observations=("observation-1",),
        decisions=("decision-1",),
        checkpoints=({"phase": "interaction_advancement", "status": "observed"},),
        scope={},
    )

    summary = build_session_summary((record,))
    actions = causal_actions((record,))

    assert summary["record_count"] == 1
    assert summary["decision_count"] == 1
    assert summary["observation_count"] == 1
    assert summary["checkpoint_count"] == 1
    assert actions[0]["checkpoints"] == record.checkpoints
    assert actions[0]["before_state"] == {"state": "before"}


def test_multi_session_reports_sort_dates_and_find_first_checkpoint_divergence() -> None:
    first_timestamp = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
    second_timestamp = datetime(2026, 1, 2, 10, tzinfo=timezone.utc)
    left = ReplayRecord(
        1, first_timestamp, first_timestamp, {}, {},
        checkpoints=({"phase": "interaction_advancement"},),
    )
    right = ReplayRecord(
        1, second_timestamp, second_timestamp, {}, {},
        checkpoints=({"phase": "opportunity_formation"},),
    )

    report = build_multi_session_report({"2026-01-02": (right,), "2026-01-01": (left,)})

    assert [item["session_date"] for item in report["sessions"]] == ["2026-01-01", "2026-01-02"]
    assert first_divergent_phase((left,), (right,)) == "interaction_advancement"
