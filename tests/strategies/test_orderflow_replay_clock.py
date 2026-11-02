from __future__ import annotations

from ftx_paper.strategies.orderflow.state_machine import ReplayMachine
from ftx_paper.strategies.orderflow.execution import ExecutionEngine
from ftx_paper.strategies.orderflow.state_machine_types import ReplayContext


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


def test_replay_captures_execution_diagnostic_order_without_changing_events() -> None:
    context = ReplayContext(execution_engine=ExecutionEngine())
    context.record_event({
        "kind": "trigger_observation",
        "lifecycle_id": "life-1",
        "observed_at": "2026-09-29T09:21:00Z",
        "available_at": "2026-09-29T09:22:00Z",
        "eligible": True,
    })
    context.record_event({"kind": "location_interaction_observed"})

    assert [item["kind"] for item in context.execution_diagnostics] == [
        "trigger_observation",
    ]
    assert context.execution_diagnostics[0]["sequence"] == 0
    assert len(context.events) == 2


def test_replay_event_identity_is_not_based_on_global_event_ordinal() -> None:
    first = ReplayContext(execution_engine=ExecutionEngine())
    second = ReplayContext(execution_engine=ExecutionEngine())
    event = {
        "kind": "filled",
        "lifecycle_id": "life-1",
        "intent_id": "intent-1",
        "observed_at": "2026-09-29T09:21:00Z",
        "available_at": "2026-09-29T09:21:00Z",
    }
    first.record_event(event)
    second.record_event({"kind": "location_interaction_observed"})
    second.record_event(event)

    assert first.events[0]["neutral_event_key"] == second.events[1]["neutral_event_key"]


def test_replay_event_identity_includes_causal_parent() -> None:
    first = ReplayContext(execution_engine=ExecutionEngine())
    second = ReplayContext(execution_engine=ExecutionEngine())
    event = {
        "kind": "exit_signal",
        "lifecycle_id": "life-1",
        "observed_at": "2026-09-29T09:21:00Z",
        "available_at": "2026-09-29T09:22:00Z",
    }
    first.record_event({**event, "causal_parent_id": "parent-1"})
    second.record_event({**event, "causal_parent_id": "parent-2"})

    assert first.events[0]["neutral_event_key"] != second.events[0]["neutral_event_key"]
