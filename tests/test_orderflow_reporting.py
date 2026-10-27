from datetime import datetime, timezone

from ftx_paper.strategy import ReplayRecord, build_session_summary, causal_actions


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
