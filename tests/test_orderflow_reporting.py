from datetime import datetime, timezone
import pytest

from ftx_paper.strategy import ReplayRecord, build_session_summary, causal_actions, first_divergent_phase
from ftx_paper.strategy.orderflow_reporting import build_multi_session_report, detached_replay_records
from ftx_paper.contracts import build_decision_view, validate_decision_view


def test_reporting_consumes_detached_records_without_recomputation() -> None:
    record = ReplayRecord(
        1, datetime(2026, 1, 1, 9, 15, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 9, 16, tzinfo=timezone.utc), {}, {},
        ("obs-1",), ("decision-1",),
    )

    summary = build_session_summary((record,))

    assert summary["record_count"] == 1
    assert summary["decision_count"] == 1
    assert causal_actions((record,))[0]["decisions"] == ("decision-1",)
    assert causal_actions((record,))[0]["before_state"] == {}


def test_first_divergent_phase_stops_before_trade_comparison() -> None:
    left = ReplayRecord(
        1, datetime(2026, 1, 1, 9, 15, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 9, 16, tzinfo=timezone.utc), {}, {},
        checkpoints=({"phase": "source_row", "status": "observed"},),
    )
    right = ReplayRecord(
        1, datetime(2026, 1, 1, 9, 15, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 9, 16, tzinfo=timezone.utc), {}, {},
        checkpoints=({"phase": "source_row", "status": "observed"},
                     {"phase": "location_publication", "status": "unavailable"}),
    )
    assert first_divergent_phase((left,), (right,)) == "location_publication"


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
    report = build_multi_session_report({"2026-01-02": records, "2026-01-01": records})
    assert [item["session_date"] for item in report["sessions"]] == ["2026-01-01", "2026-01-02"]
