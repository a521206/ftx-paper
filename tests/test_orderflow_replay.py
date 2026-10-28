from datetime import datetime, timezone
from threading import Event
import pytest

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.strategy import (
    CANONICAL_PHASE_ORDER, ReplayMachine, ReplayPhase,
    observe_market, prepare_replay_input,
)
from ftx_paper.strategy.factory import ConfiguredStrategyFactory
from ftx_paper.strategy.orderflow import OrderflowConfig, OrderflowStrategy
from ftx_paper.strategy.policy_contract import ExecutionMode
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


def test_explicit_orderflow_dispatch_is_executable_and_not_vwap() -> None:
    strategy = ConfiguredStrategyFactory().create(
        strategy="ftx-paper-orderflow",
        config=OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE),
    )
    assert isinstance(strategy, OrderflowStrategy)
    assert strategy.metadata.name == "ftx-paper-orderflow"
    assert strategy.policy.execution_mode is ExecutionMode.EXECUTABLE


def test_orderflow_replay_defaults_to_diagnostic_without_execution_mutations(tmp_path) -> None:
    bar = MarketBar(
        Instrument("NIFTY26SEP", "NFO", "FUTURES", "2026-09-29"),
        datetime(2026, 9, 29, 3, 45, tzinfo=UTC),
        22600, 22610, 22590, 22600,
    )
    worker = ReplayWorker(RuntimeStore(tmp_path))
    worker._bars = lambda session_date, **_kwargs: (bar,)

    result = worker._execute({
        "session_date": "2026-09-29",
        "strategy_id": "ftx-paper-orderflow",
        "vehicles": ["futures", "synthetic"],
    }, Event())

    assert result["metadata"]["execution_mode"] == "diagnostic"
    assert result["trades"] == []
    assert result["ledger"] == {}
    assert not any(str(event.get("event_type", "")).upper() in {"FILL", "RESERVATION", "SETTLEMENT"}
                   for event in result["events"])
