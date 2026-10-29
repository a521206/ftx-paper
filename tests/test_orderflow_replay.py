from datetime import datetime, timezone
import pytest

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.strategy import (
    CANONICAL_PHASE_ORDER, ReplayMachine, ReplayPhase,
    observe_market, prepare_replay_input,
)
from tests.test_orderflow_observation import _rows


UTC = timezone.utc


def test_replay_machine_records_available_evidence_last() -> None:
    observation = observe_market(prepare_replay_input(_rows(6), "2026-01-01", session_partition="09:15-09:21"))
    machine = ReplayMachine(observation)
    timestamp = datetime(2026, 1, 1, 3, 50, tzinfo=UTC)

    result = machine.process_bar(timestamp, before_state={"position": None}, after_state={"position": None})

    assert result.phases == CANONICAL_PHASE_ORDER
    assert result.phases[-1] is ReplayPhase.RECORDING
    assert len(result.available_observation_ids) == 6
    assert machine.recorder.records[0].observations == result.available_observation_ids


def test_replay_machine_uses_requested_capture_mode() -> None:
    observation = observe_market(prepare_replay_input(_rows(1), "2026-01-01", session_partition="09:15-09:16"))
    machine = ReplayMachine(observation, capture_mode="summary")
    machine.process_bar(datetime(2026, 1, 1, 3, 46, tzinfo=UTC), before_state={}, after_state={})
    assert machine.recorder.capture_mode.value == "summary"
    assert machine.recorder.records[0].observations == ()


def test_replay_machine_rejects_future_or_reversed_bars() -> None:
    observation = observe_market(prepare_replay_input(_rows(1), "2026-01-01", session_partition="09:15-09:16"))
    machine = ReplayMachine(observation)
    first = datetime(2026, 1, 1, 3, 46, tzinfo=UTC)
    machine.process_bar(first, before_state={}, after_state={})
    with pytest.raises(ValueError, match="chronological"):
        machine.process_bar(first, before_state={}, after_state={})


def test_replay_machine_suspends_admission_after_source_gap() -> None:
    rows = _rows(6)
    rows.pop(2)
    observation = observe_market(prepare_replay_input(rows, "2026-01-01", session_partition="09:15-09:21"))
    machine = ReplayMachine(observation)

    result = machine.process_bar(datetime(2026, 1, 1, 3, 48, tzinfo=UTC), before_state={}, after_state={})

    assert result.admission_suspended
    assert result.available_observation_ids


def test_replay_machine_resumes_admission_after_complete_recovery_parent() -> None:
    rows = _rows(11)
    rows.pop(2)
    observation = observe_market(prepare_replay_input(rows, "2026-01-01", session_partition="09:15-09:26"))
    machine = ReplayMachine(observation)

    suspended = machine.process_bar(datetime(2026, 1, 1, 3, 48, tzinfo=UTC), before_state={}, after_state={})
    recovered = machine.process_bar(datetime(2026, 1, 1, 3, 55, tzinfo=UTC), before_state={}, after_state={})

    assert suspended.admission_suspended
    assert not recovered.admission_suspended


def test_orderflow_replay_is_isolated_from_live_execution_state(tmp_path) -> None:
    bar = MarketBar(
        Instrument("NIFTY26SEP", "NFO", "FUTURES", "2026-09-29"),
        datetime(2026, 9, 29, 3, 45, tzinfo=UTC),
        22600, 22610, 22590, 22600,
    )
    store = RuntimeStore(tmp_path)
    worker = ReplayWorker(store)
    worker._bars = lambda session_date, **_kwargs: (bar,)
    before_status = store.read_status()
    before_events = store.read_events(1000)

    result = worker.execute_detached({
        "session_date": "2026-09-29",
        "strategy_id": "ftx-paper-orderflow",
        "vehicles": ["futures", "synthetic"],
        "mode": "replay",
        "include_parity_layers": False,
    })

    assert result["execution_scope"] == "replay"
    assert result["shadow_trades"] == []
    assert any(item["event_type"] == "DAILY_RESET" for item in result["shadow_ledger"])
    assert result["execution_scope"] == "replay"
    assert result["ledger_scope"] == "replay"
    assert result["replay_records"]
    assert result["replay_records"][0]["checkpoints"]
    assert not any(str(event.get("values", {}).get("event_type", "")).upper() in {"FILL", "RESERVATION", "SETTLEMENT"}
                   for event in result["events"])
    assert store.read_status() == before_status
    assert store.read_events(1000) == before_events


def test_paired_replay_compares_decisions_and_keeps_execution_projection_separate() -> None:
    decisions = [{
        "decision_id": "decision-1", "decision_stage": "admission",
        "outcome": "unavailable", "reason": "future_evidence",
        "source_observation_ids": ["obs-1"], "available_at": "2026-09-29T03:50:00+00:00",
    }]
    comparison = ReplayWorker.compare_paired_replay(
        {"events": decisions, "shadow_trades": [], "shadow_ledger": []},
        {"events": decisions, "shadow_trades": [{"status": "filled"}], "shadow_ledger": [{"net_pnl": 0}]},
    )

    assert comparison == {"matched": True, "mismatches": [], "diagnostic_mutations": False}
