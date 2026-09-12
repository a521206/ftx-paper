"""Focused replay tests for the independent paper risk gate."""

from __future__ import annotations

from ftx_paper.core.risk_state import RiskGateState


def test_net_directional_exposure_is_signed_and_persists_across_segments() -> None:
    gate = RiskGateState(max_net_directional_lots=8)
    gate.record_entry(cell="low", direction="long", quantity=6, date="2026-09-10")
    assert gate.can_enter(cell="high", direction="long", bar=20, quantity=3, date="2026-09-10") is False
    assert gate.can_enter(cell="high", direction="short", bar=20, quantity=2, date="2026-09-10") is True
    gate.record_entry(cell="high", direction="short", quantity=2, date="2026-09-10")
    assert gate.net_directional_lots == 4


def test_segment_reset_clears_cell_cooldown_but_preserves_exposure() -> None:
    gate = RiskGateState(max_net_directional_lots=8)
    gate.record_entry(cell="high", direction="long", quantity=2, date="2026-09-11")
    gate.record_exit(cell="high", reason="target", entry_bar=284, exit_bar=285, date="2026-09-11")
    assert gate.can_enter(cell="high", direction="long", bar=286, quantity=2, date="2026-09-11") is False
    gate.reset_segment()
    assert gate.net_directional_lots == 2
    assert gate.can_enter(cell="high", direction="long", bar=286, quantity=2, date="2026-09-11") is True


def test_thesis_failure_requires_cooldown_and_locks_after_two_stops() -> None:
    gate = RiskGateState(thesis_cooldown_bars=3, cell_cooldown_bars=0)
    gate.record_entry(cell="low", direction="long", quantity=1, date="2026-09-10")
    gate.record_exit(cell="low", reason="hard_stop", entry_bar=10, exit_bar=12, date="2026-09-10")
    assert gate.rejection_reason(cell="low", direction="long", bar=14, quantity=1, date="2026-09-10") == "thesis_cooldown"
    assert gate.rejection_reason(cell="low", direction="long", bar=16, quantity=1, date="2026-09-10") is None
    gate.record_entry(cell="low", direction="long", quantity=1, date="2026-09-10")
    gate.record_exit(cell="low", reason="stop", entry_bar=16, exit_bar=18, date="2026-09-10")
    assert gate.rejection_reason(cell="low", direction="long", bar=30, quantity=1, date="2026-09-10") == "thesis_failed"


def test_post_exit_cell_cooldown_is_independent_of_thesis_result() -> None:
    gate = RiskGateState(thesis_cooldown_bars=0, cell_cooldown_bars=2)
    gate.record_entry(cell="low", direction="long", quantity=1, date="2026-09-10")
    gate.record_exit(cell="low", reason="target", entry_bar=10, exit_bar=12, date="2026-09-10")
    assert gate.can_enter(cell="low", direction="long", bar=14, quantity=1, date="2026-09-10") is False
    assert gate.can_enter(cell="low", direction="long", bar=15, quantity=1, date="2026-09-10") is True


def test_snapshot_restore_preserves_gate_state_and_day_reset_clears_it() -> None:
    original = RiskGateState(max_net_directional_lots=8)
    original.record_entry(cell="low", direction="long", quantity=4, date="2026-09-10")
    original.record_exit(cell="low", reason="target", entry_bar=10, exit_bar=12, date="2026-09-10")

    restored = RiskGateState(max_net_directional_lots=8)
    restored.restore(original.snapshot())
    assert restored.net_directional_lots == 4
    assert restored.can_enter(cell="low", direction="long", bar=13, quantity=1, date="2026-09-10") is False
    assert restored.can_enter(cell="low", direction="long", bar=13, quantity=1, date="2026-09-11") is True
    assert restored.net_directional_lots == 0
