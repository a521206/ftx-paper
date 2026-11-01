from __future__ import annotations

from ftx_paper.strategies.orderflow.state_machine import ReplayMachine


def test_replay_records_decision_clock_at_five_minute_boundary() -> None:
    rows = [
        {"time": "09:29", "session_date": "2026-09-29"},
        {"time": "09:30", "session_date": "2026-09-29"},
    ]
    events = (
        (
            "2026-09-29T09:25", {},
            {"time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"},
            {"timeframe": "5m", "time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"},
        ),
        (
            "2026-09-29T09:29", {},
            {"time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"},
            {"timeframe": "1m", "time": "2026-09-29T09:28", "available_at": "2026-09-29T09:29"},
        ),
        (
            "2026-09-29T09:30", {},
            {"time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"},
            {"timeframe": "1m", "time": "2026-09-29T09:29", "available_at": "2026-09-29T09:30"},
        ),
        (
            "2026-09-29T09:30", {},
            {"time": "2026-09-29T09:25", "available_at": "2026-09-29T09:30"},
            {"timeframe": "5m", "time": "2026-09-29T09:25", "available_at": "2026-09-29T09:30"},
        ),
    )

    records, replay_state = ReplayMachine(
        rows,
        evidence_events=events,
        evidence_at_clock=lambda _clock: {"contexts": []},
        session_date="2026-09-29",
    ).run()

    assert replay_state.sequence == 1

    first_clock = records[0]["decision_clock"]
    assert first_clock["decision_at"] == "2026-09-29T09:29"
    assert first_clock["next_decision_at"] == "2026-09-29T09:30"
    assert first_clock["source_1m_time"] == "09:29"
    assert first_clock["new_5m"] is True
    assert first_clock["previous_5m_available_at"] is None
    assert first_clock["updated_5m_available_at"] == "2026-09-29T09:25"
    assert first_clock["available_1m"] == [{
        "time": "2026-09-29T09:28", "available_at": "2026-09-29T09:29",
    }]

    second_clock = records[1]["decision_clock"]
    assert second_clock["new_5m"] is True
    assert second_clock["previous_5m_available_at"] == "2026-09-29T09:25"
    assert second_clock["updated_5m_available_at"] == "2026-09-29T09:30"
    assert second_clock["available_1m"][-1] == {
        "time": "2026-09-29T09:29", "available_at": "2026-09-29T09:30",
    }
    assert second_clock["available_5m"][-1] == {
        "time": "2026-09-29T09:25", "available_at": "2026-09-29T09:30",
    }
