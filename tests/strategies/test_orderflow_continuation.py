from types import SimpleNamespace

from ftx_paper.strategies.orderflow.execution import (
    ExecutionEngine,
    RequalificationStatus,
)
from ftx_paper.strategies.orderflow.hierarchy import TriggerObservation
from ftx_paper.strategies.orderflow.lifecycle import SetupOpportunity
from ftx_paper.strategies.orderflow.state_machine_types import ReplayContext
from ftx_paper.strategies.orderflow.templates import Direction


def _life(lifecycle_id: str, *, key: tuple[str, ...]) -> SetupOpportunity:
    return SetupOpportunity(
        lifecycle_id,
        Direction.LONG,
        "2026-10-01",
        "NIFTY",
        "orderflow",
        "2026-10-01T09:20:00Z",
        "2026-10-01T09:21:00Z",
        lifecycle_id,
        None,
        "OR_HIGH",
        100.0,
        None,
        101.0,
        99.0,
        continuation_key=key,
    )


def test_continuation_owner_is_claimed_once() -> None:
    engine = ExecutionEngine()
    context = ReplayContext(execution_engine=engine)
    owner = _life("candidate-9", key=("2026-10-01", "NIFTY", "OR_HIGH", "long"))
    later = _life("candidate-10", key=owner.continuation_key or ())

    assert context.register_opportunity(owner) is True
    assert context.continuation_owner(later.continuation_key) == owner.lifecycle_id
    assert context.register_opportunity(later) is False
    assert later.superseded_by_lifecycle_id == owner.lifecycle_id


def test_requalification_updates_one_queue_item_and_requires_new_context() -> None:
    engine = ExecutionEngine()
    life = _life("candidate-9", key=("2026-10-01", "NIFTY", "OR_HIGH", "long"))
    engine.register(life.lifecycle_id)
    engine.state_for(life.lifecycle_id)  # assert registration is usable
    engine._states[life.lifecycle_id].setup = SimpleNamespace(setup_id="setup-9")
    observation = TriggerObservation(
        "intent-9", "2026-10-01T09:30:00Z", "2026-10-01T09:31:00Z",
        True, True, 101.0, "crossed",
    )

    queue = engine.queue_requalification(
        life,
        intent_id="intent-9",
        observation=observation,
        reason="active_position_session_extreme_gate",
        denied_at="2026-10-01T09:30:00Z",
        denied_available_at="2026-10-01T09:31:00Z",
        blocking_position_id="position-1",
    )
    assert queue.status is RequalificationStatus.PENDING
    assert engine.prepare_requalification(
        life, context_available_at="2026-10-01T09:31:00Z"
    ) is None
    ready = engine.prepare_requalification(
        life, context_available_at="2026-10-01T09:36:00Z"
    )
    assert ready is queue
    assert ready.status is RequalificationStatus.REQUALIFICATION_READY
    assert ready.attempt_number == 2

    updated = engine.queue_requalification(
        life,
        intent_id="intent-9:attempt-1",
        observation=observation,
        reason="active_position_session_extreme_gate",
        denied_at="2026-10-01T09:37:00Z",
        denied_available_at="2026-10-01T09:38:00Z",
        blocking_position_id="position-1",
    )
    assert updated.requalification_id == queue.requalification_id
    assert updated.attempt_number == 2
    assert updated.status is RequalificationStatus.PENDING
