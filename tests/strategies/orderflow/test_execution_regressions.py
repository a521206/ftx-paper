from datetime import datetime, timezone
from types import SimpleNamespace

from ftx_paper.strategies.orderflow.execution import EntryOrder, EntryReservation
from ftx_paper.strategies.orderflow.hierarchy import (
    IntentStatus,
    ResolvedExitRule,
    TradeIntent,
)
from ftx_paper.strategies.orderflow.lifecycle import SetupOpportunity
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry
from ftx_paper.strategies.orderflow.templates import Direction
from ftx_paper.strategies.orderflow.templates import StopPolicy
from ftx_paper.strategies.orderflow.transition import OrderflowTransitionOwner


UTC = timezone.utc


def _life() -> SetupOpportunity:
    now = datetime(2026, 9, 29, 3, 50, tzinfo=UTC)
    return SetupOpportunity(
        lifecycle_id="life-1", direction=Direction.LONG, session_date="2026-09-29",
        instrument="NIFTY", strategy="orderflow-1m_5m", formation_at=now,
        formation_available_at=now, candidate_id="candidate-1", parent_context=None,
        location="OR_LOW", reference_level=100.0, context_candle=None,
        breakout_level=101.0, failure_boundary=99.0,
    )


def _triggered(owner: OrderflowTransitionOwner, life: SetupOpportunity, setup: object) -> None:
    owner.engine.register(life.lifecycle_id)
    state = owner.engine._state(life.lifecycle_id)
    intent = TradeIntent(
        "intent-1", "setup-1", "regime-1", "long", IntentStatus.TRIGGERED,
        "2026-09-29T03:50:00Z", 101.0, 99.0, "2026-09-29T04:30:00Z",
        triggered_at="2026-09-29T03:51:00Z", lifecycle_id=life.lifecycle_id,
    )
    state.intent = intent
    state.pending_entry = EntryOrder(intent, "2026-09-29T03:52:00Z", 101.0)
    reservation = EntryReservation(
        "reservation:life-1", life.lifecycle_id, "long", life.session_date,
        "2026-09-29T03:52:00Z",
    )
    state.reservation = reservation
    state.reservation_history = (reservation,)
    state.setup = setup


def test_replay_transition_fills_triggered_entry_and_records_fact() -> None:
    owner = OrderflowTransitionOwner(LocationRegistry("test"))
    life = _life()
    setup = SimpleNamespace(exit_plan=(), template=SimpleNamespace(stop_policy=StopPolicy.OR_VWAP))
    _triggered(owner, life, setup)
    formation = SimpleNamespace(lifecycle=life, setup=setup)

    owner.process_execution_bar(
        (formation,), index=0, timestamp="2026-09-29T03:53:00Z",
        next_time="2026-09-29T03:54:00Z",
        row={"open": 101.0, "high": 102.0, "low": 100.0, "close": 101.5},
    )

    state = owner.engine.state_for(life.lifecycle_id)
    assert state.position is not None
    assert state.reservation is None
    assert owner.parity_streams["execution_fills"]


def test_required_exit_plan_rejection_cancels_entry_and_releases_reservation() -> None:
    owner = OrderflowTransitionOwner(LocationRegistry("test"))
    life = _life()
    required = ResolvedExitRule("target", "SESSION_HIGH", "fixed_target", 100.0, None, required=True)
    setup = SimpleNamespace(exit_plan=(required,))
    _triggered(owner, life, setup)

    owner.execution_owner.fill_triggered(
        (SimpleNamespace(lifecycle=life, setup=setup),),
        bar_open={life.lifecycle_id: 101.0}, fill_at="2026-09-29T03:53:00Z",
    )

    state = owner.engine.state_for(life.lifecycle_id)
    assert state.position is None
    assert state.pending_entry is None
    assert state.reservation is None
    assert state.intent is not None
    assert state.intent.status is IntentStatus.CANCELLED
