from types import SimpleNamespace

from ftx_paper.strategies.research.active_position_gate import ActivePositionGate


def _bar(available_at: str, close: float, volume: float = 10.0) -> SimpleNamespace:
    return SimpleNamespace(
        session_date="2026-09-28", available_at=available_at,
        end_time=available_at, high=close, low=close, close=close, volume=volume,
    )


def test_session_extreme_block_is_causal_idempotent_and_resets_after_close() -> None:
    active = SimpleNamespace(position_id="position-or-low", lifecycle_id="or-low", direction="short")
    candidate = SimpleNamespace(
        location="SESSION_LOW", direction="long", session_date="2026-09-28",
        lifecycle_id="session-low",
    )
    context = SimpleNamespace(
        aggregates={"5m": [_bar("2026-09-28T10:40", 99.0)]},
        execution_engine=SimpleNamespace(open_positions=lambda: [active]),
        opportunities={"or-low": SimpleNamespace(location="OR_LOW")},
        session_extreme_gate_blocked_lifecycles=set(),
    )
    gate = ActivePositionGate()

    evidence = gate.check(context, candidate, "2026-09-28T10:47")
    assert evidence is not None
    assert evidence["block_reason"] == "active_position_session_extreme_gate"
    assert evidence["vwap"] == 99.0
    assert context.session_extreme_gate_blocked_lifecycles == {"session-low"}
    assert gate.check(context, candidate, "2026-09-28T10:47") == evidence

    context.execution_engine.open_positions = lambda: []
    gate.update(context, "2026-09-28T10:50")
    assert not gate.blocks
    assert not gate.is_blocked("session-low")
    assert not context.session_extreme_gate_blocked_lifecycles


def test_non_extreme_candidate_is_not_blocked() -> None:
    context = SimpleNamespace(
        aggregates={"5m": [_bar("2026-09-28T10:40", 99.0)]},
        execution_engine=SimpleNamespace(open_positions=lambda: [SimpleNamespace(position_id="p")]),
        opportunities={},
        session_extreme_gate_blocked_lifecycles=set(),
    )
    candidate = SimpleNamespace(
        location="OR_LOW", direction="long", session_date="2026-09-28", lifecycle_id="candidate",
    )
    assert ActivePositionGate().check(context, candidate, "2026-09-28T10:47") is None
