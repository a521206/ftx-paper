"""Regression tests for canonical market-state publication and 3R exit preemption."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from ftx_paper.strategies.orderflow.execution import (
    EntryOrder,
    ExecutionCoordinator,
    ExecutionEngine,
    ExitOrder,
)
from ftx_paper.strategies.orderflow.hierarchy import IntentStatus, ResolvedExitRule, TradeIntent
from ftx_paper.strategies.orderflow.interactions import (
    CandleClassification,
    InteractionCandle,
    InteractionSnapshot,
    InteractionStatus,
)
from ftx_paper.strategies.orderflow.lifecycle import SetupOpportunity
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.orderflow_common import LIQUIDITY_PROBE
from ftx_paper.strategies.orderflow.state_machine_types import (
    BALANCE,
    EXHAUSTION_RISK,
    OrderflowState,
)
from ftx_paper.strategies.orderflow.templates import Direction, ExitExecutionTiming

TIMESTAMP = "2026-10-01T03:49:00Z"
NEXT_TIME = "2026-10-01T03:50:00Z"


def test_market_event_keeps_context_state_in_sync_with_event() -> None:
    context = OrderflowState(execution_engine=ExecutionEngine())
    events: list[dict[str, Any]] = []

    context.emit_market_event(
        {"kind": "exit_signal", "owner": "position_management"},
        EXHAUSTION_RISK,
        events,
    )
    context.emit_market_event(
        {"kind": "setup_candidate", "owner": "5m"},
        LIQUIDITY_PROBE,
        events,
    )

    assert context.market_state == LIQUIDITY_PROBE
    assert events[-1]["market_state"] == LIQUIDITY_PROBE


def _opportunity() -> SetupOpportunity:
    formation_at = datetime.fromisoformat("2026-10-01T03:45:00+00:00")
    return SetupOpportunity(
        lifecycle_id="life-1",
        direction=Direction.LONG,
        session_date="2026-10-01",
        instrument="NIFTY",
        strategy="orderflow",
        formation_at=formation_at,
        formation_available_at=formation_at,
        candidate_id="life-1",
        parent_context=None,
        location=None,
        reference_level=100.0,
        context_candle=Aggregate(
            "2026-10-01T09:15",
            "2026-10-01T09:19",
            "2026-10-01T09:20",
            "1m",
            1,
            100.0,
            101.0,
            99.0,
            100.5,
            100,
            "NIFTY",
            "",
            "2026-10-01",
            "replay",
        ),
        breakout_level=101.0,
        failure_boundary=99.0,
        opening_policy_version="opening_breakout-1m-v1",
    )


def _source_candle(time_label: str) -> Aggregate:
    return Aggregate(
        time_label,
        time_label,
        time_label,
        "1m",
        1,
        100.0,
        104.0,
        99.0,
        103.8,
        100,
        "NIFTY",
        "",
        "2026-10-01",
        "replay",
    )


def _interaction() -> InteractionSnapshot:
    return InteractionSnapshot(
        interaction_id="ix-1",
        instrument="NIFTY",
        session_date="2026-10-01",
        location="TEST",
        timeframe="1m",
        template_version="tv-1",
        creation_candle=_source_candle("2026-10-01T09:19"),
        reference_level=101.0,
        band=LocationBand(low=98.5, high=99.5),
        pre_contact_side=None,
        interaction_started_at=datetime.fromisoformat(
            "2026-10-01T03:49:00+00:00"
        ),
        available_at=datetime.fromisoformat("2026-10-01T03:49:00+00:00"),
        reference_generation=1,
        sessions_carried=0,
        status=InteractionStatus.ACTIVE,
        departure_direction=None,
        terminal_close_side=None,
        terminal_candle_id=None,
        close_reason=None,
        candles=(
            InteractionCandle(
                _source_candle("2026-10-01T09:19"),
                CandleClassification.CONTAINED,
            ),
        ),
        synthetic_snapshots=(),
    )


def _open_lifecycle() -> tuple[OrderflowState, ExecutionEngine, ExecutionCoordinator, SetupOpportunity]:
    engine = ExecutionEngine()
    context = OrderflowState(execution_engine=engine)
    context.interactions["ix-1"] = _interaction()
    life = _opportunity()
    context.opportunities[life.lifecycle_id] = life
    engine.register(life.lifecycle_id)
    intent = TradeIntent(
        intent_id="intent-1",
        setup_id="setup-1",
        regime_version="v1",
        direction="long",
        status=IntentStatus.TRIGGERED,
        created_at=TIMESTAMP,
        trigger_price=99.6,
        failure_boundary=99.0,
        expiry=NEXT_TIME,
        triggered_at=TIMESTAMP,
        lifecycle_id=life.lifecycle_id,
    )
    engine.update_intent(life, intent)
    engine.select_entry(
        life,
        EntryOrder(intent=intent, available_at=TIMESTAMP, fill_price=100.0),
    )
    engine.fill_entry(
        life,
        price=100.0,
        quantity=1.0,
        filled_at=TIMESTAMP,
        protective_stop=99.0,
    )
    coordinator = ExecutionCoordinator(
        host=SimpleNamespace(context=context), engine=engine
    )
    return context, engine, coordinator, life


def _row() -> dict[str, Any]:
    return {
        "time": "09:49",
        "session_date": "2026-10-01",
        "open": 103.0,
        "high": 104.0,
        "low": 103.5,
        "close": 103.8,
        "volume": 100.0,
    }


def test_partial_3r_fires_latches_exhaustion_risk_and_settles_as_partial_exit_order() -> None:
    context, engine, coordinator, life = _open_lifecycle()
    first_bar: list[dict[str, Any]] = []
    coordinator.handle_management(0, TIMESTAMP, NEXT_TIME, _row(), [], first_bar, life)

    pending = engine.state_for(life.lifecycle_id).pending_exit
    assert pending is not None
    assert pending.reason == "partial_3r"
    assert context.market_state == EXHAUSTION_RISK
    signals = [item for item in first_bar if item["kind"] == "exit_signal"]
    assert [item["reason"] for item in signals] == ["partial_3r"]
    assert signals[0]["market_state"] == EXHAUSTION_RISK
    assert signals[0]["level"] == "3R"
    assert signals[0]["quantity"] == 0.5
    assert [
        item["kind"]
        for item in first_bar
        if item["kind"] in {"exit_order", "position_closed", "partial_exit_order"}
    ] == []

    second_bar: list[dict[str, Any]] = []
    coordinator.handle_management(1, NEXT_TIME, NEXT_TIME, _row(), [], second_bar, life)
    position = engine.state_for(life.lifecycle_id).position
    assert position is not None
    assert position.status.value == "open"
    assert position.partial_exit_taken is True
    assert position.quantity == 0.5
    assert engine.state_for(life.lifecycle_id).pending_exit is None
    assert [item["kind"] for item in second_bar] == ["partial_exit_order"]
    partial = second_bar[0]
    assert partial["reason"] == "partial_3r"
    assert partial["quantity"] == 0.5
    assert partial["remaining_quantity"] == 0.5
    assert partial["risk_reward"] == 3.0
    assert context.market_state == EXHAUSTION_RISK


def test_pending_conditional_exit_at_entry_blocks_partial_3r() -> None:
    context, engine, coordinator, life = _open_lifecycle()
    assert engine.queue_exit(life, ExitOrder("conditional_exit", NEXT_TIME))
    bar_events: list[dict[str, Any]] = []

    coordinator.handle_management(0, TIMESTAMP, NEXT_TIME, _row(), [], bar_events, life)

    pending = engine.state_for(life.lifecycle_id).pending_exit
    assert pending is not None
    assert pending.reason == "conditional_exit"
    assert context.market_state == BALANCE
    kinds = {item["kind"] for item in bar_events}
    assert "exit_signal" not in kinds
    assert "partial_exit_order" not in kinds


def _filled_lifecycle(
    context: OrderflowState, engine: ExecutionEngine, life: SetupOpportunity
) -> ExecutionEngine:
    engine.register(life.lifecycle_id)
    intent = TradeIntent(
        intent_id="intent-1",
        setup_id="setup-1",
        regime_version="v1",
        direction="long",
        status=IntentStatus.TRIGGERED,
        created_at=TIMESTAMP,
        trigger_price=99.6,
        failure_boundary=99.0,
        expiry=NEXT_TIME,
        triggered_at=TIMESTAMP,
        lifecycle_id=life.lifecycle_id,
    )
    engine.update_intent(life, intent)
    engine.select_entry(
        life,
        EntryOrder(intent=intent, available_at=TIMESTAMP, fill_price=100.0),
    )
    engine.fill_entry(
        life,
        price=100.0,
        quantity=1.0,
        filled_at=TIMESTAMP,
        protective_stop=99.0,
        exit_plan=(
            ResolvedExitRule(
                rule_id="rule-1",
                location="TEST",
                mode="fixed_target",
                level=None,
                source_context=None,
                execution=ExitExecutionTiming(),
            ),
        ),
    )
    return engine


def test_same_call_activation_reflects_same_call_fixed_target_exit() -> None:
    engine = ExecutionEngine()
    context = OrderflowState(execution_engine=engine)
    context.interactions["ix-1"] = _interaction()
    life = _opportunity()
    context.opportunities[life.lifecycle_id] = life
    _filled_lifecycle(context, engine, life)
    coordinator = ExecutionCoordinator(
        host=SimpleNamespace(context=context), engine=engine
    )
    context.market_state = LIQUIDITY_PROBE
    bar_events: list[dict[str, Any]] = []

    coordinator.handle_management(0, TIMESTAMP, NEXT_TIME, _row(), [], bar_events, life)

    pending = engine.state_for(life.lifecycle_id).pending_exit
    assert pending is not None
    assert pending.reason == "fixed_target"
    assert context.market_state == LIQUIDITY_PROBE
    assert [item["kind"] for item in bar_events if item["kind"] in {
        "exit_signal", "partial_exit_order", "exit_order", "position_closed",
    }] == []


def test_activated_3r_preempts_same_call_fixed_target_exit() -> None:
    engine = ExecutionEngine()
    context = OrderflowState(execution_engine=engine)
    context.interactions["ix-1"] = _interaction()
    life = _opportunity()
    context.opportunities[life.lifecycle_id] = life
    _filled_lifecycle(context, engine, life)
    position = engine.state_for(life.lifecycle_id).position
    assert position is not None
    engine.update_position(
        life, replace(position, post_3r_activated=True)
    )
    coordinator = ExecutionCoordinator(
        host=SimpleNamespace(context=context), engine=engine
    )
    context.market_state = LIQUIDITY_PROBE
    bar_events: list[dict[str, Any]] = []

    coordinator.handle_management(0, TIMESTAMP, NEXT_TIME, _row(), [], bar_events, life)

    pending = engine.state_for(life.lifecycle_id).pending_exit
    assert pending is not None
    assert pending.reason == "partial_3r"
    assert context.market_state == EXHAUSTION_RISK
    reasons = [
        item["reason"] for item in bar_events if item["kind"] == "exit_signal"
    ]
    assert reasons == ["partial_3r"]
