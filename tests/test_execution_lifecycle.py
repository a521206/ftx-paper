from __future__ import annotations

import pytest

from ftx_paper.execution.lifecycle import (
    EXECUTION_PHASES,
    ExecutionLifecycleCoordinator,
    ExecutionPhase,
    ExecutionTurn,
    FunctionLifecycleAdapter,
)


def test_lifecycle_runs_all_phases_once_in_order() -> None:
    seen: list[str] = []
    adapter = FunctionLifecycleAdapter({
        phase: lambda _turn, phase=phase: seen.append(phase) or {}
        for phase in EXECUTION_PHASES
    })
    turn = ExecutionTurn(
        "live:1", "live", "2026-10-28T09:15:00+05:30",
        "2026-10-28T09:16:00+05:30", "2026-10-28T09:16:00+05:30",
    )

    events = ExecutionLifecycleCoordinator().run(turn, adapter)

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
