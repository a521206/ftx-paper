from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.strategy import CaptureMode, ReplayRecord, ReplayRecorder


UTC = timezone.utc


def _record(sequence: int) -> ReplayRecord:
    timestamp = datetime(2026, 1, 1, 9, 15 + sequence - 1, tzinfo=UTC)
    return ReplayRecord(sequence, timestamp, timestamp + timedelta(minutes=1),
                        {"state": "before"}, {"state": "after"}, (f"obs-{sequence}",))


def test_replay_recorder_detaches_state_and_enforces_sequence() -> None:
    before = {"state": "before"}
    item = ReplayRecord(1, datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 0, 1, tzinfo=UTC), before, {"state": "after"})
    recorder = ReplayRecorder()
    recorder.record(item)
    before["state"] = "mutated"
    assert recorder.records[0].before_state["state"] == "before"
    with pytest.raises(ValueError, match="sequence"):
        recorder.record(_record(3))


def test_replay_recorder_detaches_nested_state() -> None:
    nested = {"values": [1]}
    item = ReplayRecord(1, datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 0, 1, tzinfo=UTC), nested, {})
    nested["values"].append(2)
    assert item.before_state["values"] == [1]


def test_summary_capture_preserves_decisions_but_reduces_observation_detail() -> None:
    recorder = ReplayRecorder(CaptureMode.SUMMARY)
    item = _record(1)
    recorder.record(ReplayRecord(
        item.sequence, item.timestamp, item.available_at,
        item.before_state, item.after_state, ("observation",), ("decision",),
    ))

    assert recorder.records[0].decisions == ("decision",)
    assert recorder.records[0].observations == ()


def test_replay_record_detaches_causal_checkpoints_and_freezes_scope() -> None:
    checkpoints = [{"phase": "source_row", "parent_identity": "source-1"}]
    scope = {"source_contract": "fixture", "input_identity": "input-1",
             "requested_dates": ["2026-01-01"], "effective_dates": ["2026-01-01"],
             "vehicles": ["futures"], "strategy_configuration": {},
             "timezone": "UTC", "decision_clock": {"cutoff": "inclusive"},
             "counts": {"selected_source_rows": 1, "filtered_source_rows": 0,
                        "completed_source_rows": 1, "gap_rows": 0,
                        "processed_decision_bars": 1}}
    record = ReplayRecord(
        1, datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        {}, {}, checkpoints=tuple(checkpoints), scope=scope,
    )
    checkpoints[0]["phase"] = "mutated"
    scope["timezone"] = "mutated"
    assert record.checkpoints[0]["phase"] == "source_row"
    assert record.scope["timezone"] == "UTC"
