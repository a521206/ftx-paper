from __future__ import annotations

import pytest

from ftx_paper.execution.lifecycle import (
    EXECUTION_PHASES,
    ExecutionLifecycleCoordinator,
    ExecutionPhase,
    ExecutionTurn,
)
from ftx_paper.contracts import Instrument, OrderIntent, OrderRole, OrderSide


def _entry(order_id: str = "entry-1") -> OrderIntent:
    return OrderIntent(
        client_order_id=order_id,
        instrument=Instrument("NIFTY", "NFO", "FUTURES", "2026-10-28"),
        side=OrderSide.BUY,
        quantity=1,
        role=OrderRole.ENTRY,
        setup_availability="2026-10-28T09:15:00+00:00",
    )


def test_lifecycle_runs_all_phases_once_in_order() -> None:
    seen: list[str] = []
    operations = {
        phase: lambda _turn, phase=phase: seen.append(phase) or {}
        for phase in EXECUTION_PHASES
    }
    turn = ExecutionTurn(
        "live:1", "live", "2026-10-28T09:15:00+05:30",
        "2026-10-28T09:16:00+05:30", "2026-10-28T09:16:00+05:30",
    )

    events = ExecutionLifecycleCoordinator().run(turn, operations)

    assert tuple(event.phase for event in events) == EXECUTION_PHASES
    assert seen == list(EXECUTION_PHASES)


def test_turn_requires_causal_timestamp_order() -> None:
    with pytest.raises(ValueError, match="timestamps"):
        ExecutionTurn(
            "invalid", "replay", "2026-10-28T09:20:00+05:30",
            "2026-10-28T09:19:00+05:30", "2026-10-28T09:19:00+05:30",
        )


def test_phase_names_are_explicit() -> None:
    assert ExecutionPhase.EXECUTION_SETTLEMENT == "execution_settlement"


def test_missing_phase_operation_is_rejected() -> None:
    with pytest.raises(ValueError, match="explicitly cover all phases"):
        ExecutionLifecycleCoordinator().run(
            ExecutionTurn(
                "replay:1", "replay", "2026-10-28T09:15:00+05:30",
                "2026-10-28T09:15:00+05:30", "2026-10-28T09:15:00+05:30",
            ),
            {},
        )


def test_pending_entry_requires_strictly_later_observation() -> None:
    coordinator = ExecutionLifecycleCoordinator()
    order = _entry()
    coordinator.defer_entry(order, available_at="2026-10-28T09:15:00+00:00", reason="setup")

    assert coordinator.due_entries("2026-10-28T09:15:00+00:00") == ()
    assert coordinator.due_entries("2026-10-28T09:16:00+00:00") == (order,)


def test_pending_entries_are_idempotent_and_conflicts_are_rejected() -> None:
    coordinator = ExecutionLifecycleCoordinator()
    order = _entry()
    coordinator.defer_entry(order, available_at="2026-10-28T09:15:00+00:00", reason="setup")
    coordinator.defer_entry(order, available_at="2026-10-28T09:15:00+00:00", reason="setup")
    with pytest.raises(ValueError, match="pending order identity"):
        coordinator.defer_entry(order, available_at="2026-10-28T09:16:00+00:00", reason="other")
