from __future__ import annotations

import pytest

from ftx_paper.strategies.orderflow.state_machine import ReplayMachine
from ftx_paper.strategies.orderflow.state_machine_source import source_metadata


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


def test_source_metadata_rejects_missing_session_date() -> None:
    with pytest.raises(ValueError, match="session_date"):
        source_metadata([{"time": "09:15"}])


def test_replay_machine_accepts_full_timestamp_artifact_rows_without_rewriting_bar() -> None:
    rows = [
        {"time": "2026-10-30T09:15", "session_date": "2026-10-30"},
        {"time": "2026-10-30T09:20", "session_date": "2026-10-30"},
    ]
    machine = ReplayMachine(
        rows,
        evidence_events=(),
        evidence_at_clock=lambda _clock: {"contexts": []},
        session_date="2026-10-30",
    )

    records, _ = machine.run()

    assert records[0]["bar"]["time"] == "2026-10-30T09:15"
    assert records[0]["timestamp"] == "2026-10-30T09:15"


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


def test_replay_machine_preserves_later_publication_of_repeated_causal_fact() -> None:
    fact = {
        "kind": "response_confirmed",
        "identity": "response-1:evaluation:1",
        "observed_at": "2026-10-30T09:19",
        "available_at": "2026-10-30T09:20",
        "_publication_available_at": "2026-10-30T09:25",
    }
    machine = ReplayMachine(
        [
            {"time": "09:20", "session_date": "2026-10-30"},
            {"time": "09:25", "session_date": "2026-10-30"},
        ],
        evidence_events=(),
        evidence_at_clock=lambda _clock: {"contexts": []},
        session_date="2026-10-30",
        interaction_response=(fact,),
    )

    records, _ = machine.run()

    assert records[0]["events"] == []
    assert records[1]["events"][0]["available_at"] == "2026-10-30T09:20"
    assert "_publication_available_at" not in records[1]["events"][0]


def test_replay_machine_gates_utc_fact_against_local_replay_clock() -> None:
    rows = [
        {"time": "09:15", "session_date": "2026-10-30"},
        {"time": "09:20", "session_date": "2026-10-30"},
    ]
    # 03:47Z is 09:17 Asia/Kolkata and must not be visible at 09:15 local.
    fact = {
        "kind": "interaction_created",
        "identity": "interaction-utc",
        "available_at": "2026-10-30T03:47:00Z",
    }
    machine = ReplayMachine(
        rows,
        evidence_events=(),
        evidence_at_clock=lambda _clock: {"contexts": []},
        session_date="2026-10-30",
        interaction_response=(fact,),
    )

    records, _ = machine.run()

    assert records[0]["events"] == []
    assert len(records[1]["events"]) == 1


def test_interaction_parent_does_not_invent_context_publication_identity() -> None:
    interaction_id = "NIFTY:2026-10-30:PD_LOW:1m:2026-10-30T09:16"
    fact = {
        "kind": "location_interaction_observed",
        "identity": "interaction-1",
        "causal_parent_id": "aggregate:1m:2026-10-30T09:16",
        "observed_at": "2026-10-30T09:16",
        "available_at": "2026-10-30T09:17",
        "values": {
            "interaction_id": interaction_id,
            "source_fingerprint": "source",
        },
    }
    machine = ReplayMachine(
        [{"time": "09:17", "session_date": "2026-10-30"}],
        evidence_events=(),
        evidence_at_clock=lambda _clock: {"contexts": []},
        session_date="2026-10-30",
        interaction_response=(fact,),
    )

    records, _ = machine.run()

    event = records[0]["events"][0]
    assert event["causal_parent_id"] == "aggregate:1m:2026-10-30T09:16"
    assert event["values"]["causal_parent_key"] == event["causal_parent_key"]


def test_normalization_preserves_producer_parent_id_alongside_neutral_key() -> None:
    fact = {
        "kind": "phase_fact",
        "identity": "fact-1",
        "causal_parent_id": "aggregate:1m:2026-10-30T09:16",
        "observed_at": "2026-10-30T09:16",
        "available_at": "2026-10-30T09:17",
        "values": {"source_fingerprint": "source"},
    }
    machine = ReplayMachine(
        [{"time": "09:17", "session_date": "2026-10-30"}],
        evidence_events=(),
        evidence_at_clock=lambda _clock: {"contexts": []},
        session_date="2026-10-30",
        interaction_response=(fact,),
    )

    records, _ = machine.run()

    event = records[0]["events"][0]
    assert event["causal_parent_id"] == "aggregate:1m:2026-10-30T09:16"
    assert event["causal_parent_key"].startswith("niftyzoning:derived:")
    assert event["values"]["causal_parent_key"] == event["causal_parent_key"]
