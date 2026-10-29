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


def test_summary_capture_preserves_decisions_but_reduces_observation_detail() -> None:
    recorder = ReplayRecorder(CaptureMode.SUMMARY)
    item = _record(1)
    recorder.record(ReplayRecord(
        item.sequence, item.timestamp, item.available_at,
        item.before_state, item.after_state, ("observation",), ("decision",),
    ))

    assert recorder.records[0].decisions == ("decision",)
    assert recorder.records[0].observations == ()
    assert recorder.records[0].aggregates == ()
    assert recorder.records[0].unavailable == ()
