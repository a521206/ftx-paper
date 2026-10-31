from __future__ import annotations

from ftx_paper.strategies.orderflow.state_machine import ReplayMachine


def test_replay_machine_publishes_evidence_at_each_row_clock() -> None:
    rows = [
        {"time": "09:15", "session_date": "2026-10-30"},
        {"time": "09:20", "session_date": "2026-10-30"},
    ]
    events = (
        (
                "2026-10-30T09:20",
                {"atr": {"value": None}, "contexts": []},
                {
                    "time": "2026-10-30T09:15", "end_time": "2026-10-30T09:19",
                    "available_at": "2026-10-30T09:20", "timeframe": "5m",
                    "source_bar_count": 5, "open": 100.0, "high": 105.0,
                    "low": 99.0, "close": 101.0, "volume": 50,
                    "symbol": "NIFTY", "expiry": "", "contract_type": "FUT",
                    "session_date": "2026-10-30", "source_fingerprint": "source",
                },
        ),
    )

    machine = ReplayMachine(
        rows,
        evidence_events=events,
        evidence_at_clock=lambda clock: (
            events[0][1] if clock >= events[0][0] else {"contexts": []}
        ),
        session_date="2026-10-30",
    )

    records, state = machine.run()

    assert records[0]["ordinary_context_ref"] is None
    assert records[1]["ordinary_context_ref"] is None
    assert state.sequence == 1
    assert state.latest_5m is not None
    assert state.latest_5m["time"] == "2026-10-30T09:15"
    assert state.context_publications == ()


def test_replay_machine_emits_causal_interaction_facts_once_at_availability() -> None:
    rows = [
        {"time": "09:15", "session_date": "2026-10-30"},
        {"time": "09:20", "session_date": "2026-10-30"},
        {"time": "09:21", "session_date": "2026-10-30"},
    ]
    fact = {
        "kind": "interaction_created",
        "identity": "interaction-1",
        "available_at": "2026-10-30T09:20",
    }
    machine = ReplayMachine(
        rows,
        evidence_events=(),
        evidence_at_clock=lambda _clock: {"contexts": []},
        session_date="2026-10-30",
        interaction_response=(fact, fact),
    )
    records, state = machine.run()
    assert records[0]["events"] == []
    assert len(records[1]["events"]) == 1
    assert records[1]["events"][0]["identity"] != fact["identity"]
    assert "neutral_event_key" in records[1]["events"][0]
    assert records[2]["events"] == []
    assert len(state.interaction_response) == 1
