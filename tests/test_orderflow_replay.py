from datetime import datetime, timezone
import pytest

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
