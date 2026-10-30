from datetime import datetime, timezone
from types import SimpleNamespace

from ftx_paper.contracts import Instrument, MarketBar, OrderIntent, OrderRole, OrderSide
from ftx_paper.strategies.shared.response import ResponseCandle, build_response_spec, evaluate_response_window
import pytest

from ftx_paper.strategies.shared.templates import (
    Direction,
    ExitExecutionTiming,
    ExitMode,
    ExitResponseConfig,
    LocationTemplate,
    ResponseType,
    TradeExit,
    TRADE_TEMPLATES,
)
from ftx_paper.strategies.orderflow.orderflow_exit import evaluate_exit_plan_rule, exit_plan_from_template
from ftx_paper.strategies.orderflow.orderflow_config import OrderflowConfig
from ftx_paper.strategies.orderflow.orderflow_position_manager import OrderflowPositionManager


def test_template_registry_derives_direction_and_response_geometry() -> None:
    template = TRADE_TEMPLATES[("5m_recovery_breakout", "OR_HIGH")]
    assert template.direction is Direction.SHORT
    assert template.response_type is ResponseType.REJECTION
    spec = build_response_spec(template, band_lower=99.0, band_upper=101.0)
    result = evaluate_response_window(
        (ResponseCandle(102.0, 103.0, 99.5, 100.0), ResponseCandle(100.0, 100.5, 98.0, 98.5)),
        to_candle=lambda item: item,
        spec_for=lambda _item: spec,
        armed=False,
    )
    assert result.status is not None
    assert result.direction is Direction.SHORT


def test_template_exits_and_vwap_stops_match_canonical_locations() -> None:
    exits = {
        key: TRADE_TEMPLATES[key].exits[0]
        for key in (
            ("5m_recovery_breakout", "PD_HIGH"),
            ("5m_recovery_breakout", "PD_LOW"),
            ("5m_recovery_breakout", "SESSION_HIGH"),
            ("5m_acceptance_breakout", "SESSION_LOW"),
        )
    }
    assert exits[("5m_recovery_breakout", "PD_HIGH")].location is LocationTemplate.PD_LOW
    assert exits[("5m_recovery_breakout", "PD_LOW")].location is LocationTemplate.PD_HIGH
    assert exits[("5m_recovery_breakout", "SESSION_HIGH")].location is LocationTemplate.OR_LOW
    assert exits[("5m_acceptance_breakout", "SESSION_LOW")].location is LocationTemplate.OR_HIGH
    assert all(exit_rule.mode is ExitMode.CONDITIONAL for exit_rule in exits.values())
    assert all(exit_rule.execution == ExitExecutionTiming() for exit_rule in exits.values())
    assert TRADE_TEMPLATES[("5m_recovery_breakout", "OR_HIGH")].exits == ()
    assert TRADE_TEMPLATES[("5m_acceptance_breakout", "OR_HIGH")].stop_policy.value == "or_vwap"
    assert TRADE_TEMPLATES[("5m_acceptance_breakout", "OR_LOW")].stop_policy.value == "or_vwap"


def test_template_locations_do_not_use_vwap_as_a_location() -> None:
    assert all(
        LocationTemplate.VWAP not in template.locations
        for template in TRADE_TEMPLATES.values()
    )


def test_exit_execution_timing_rejects_unsupported_contracts() -> None:
    with pytest.raises(ValueError, match="same-bar exits"):
        ExitExecutionTiming(order="same_bar")
    with pytest.raises(ValueError, match="completed-close exits"):
        ExitExecutionTiming(evidence="completed_close", order="same_bar")


def test_exit_response_normalizes_enums_and_rejects_unstructured_execution_values() -> None:
    response = ExitResponseConfig("long", "rejection")
    assert response.direction is Direction.LONG
    assert response.response_type is ResponseType.REJECTION

    with pytest.raises(TypeError, match="structured execution timing"):
        TradeExit(
            rule_id="unstructured",
            location=LocationTemplate.PD_LOW,
            mode=ExitMode.CONDITIONAL,
            required=False,
            execution=object(),  # type: ignore[arg-type]
            trigger="touch_after_confirmation",
        )


def test_template_exit_becomes_frozen_plan_and_uses_next_bar_response() -> None:
    template = TRADE_TEMPLATES[("5m_recovery_breakout", "PD_LOW")]
    plan = exit_plan_from_template(template.exits, levels={"PD_HIGH": 105.0})
    assert plan[0].rule_id == "pd_low-long-exit"
    assert plan[0].level == 105.0
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    bars = (
        MarketBar(instrument, datetime(2026, 1, 1, 10, tzinfo=timezone.utc), 100, 101, 99, 100),
        MarketBar(instrument, datetime(2026, 1, 1, 10, 1, tzinfo=timezone.utc), 104, 106, 104, 105),
        MarketBar(instrument, datetime(2026, 1, 1, 10, 2, tzinfo=timezone.utc), 104, 106, 103, 104),
    )
    result = evaluate_exit_plan_rule(
        plan[0], position_side=OrderSide.BUY, bars=bars, entry_bar_index=0,
        decision_scores=(0.0, 1.0),
    )
    assert result.triggered is True
    assert result.reason == "exit_triggered"


def test_frozen_trade_exit_flows_through_position_manager_to_next_bar_open() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    plan = exit_plan_from_template(
        TRADE_TEMPLATES[("5m_recovery_breakout", "PD_LOW")].exits,
        levels={"PD_HIGH": 105.0}, bands={"PD_HIGH": (104.5, 105.5)},
    )
    manager = OrderflowPositionManager(OrderflowConfig())
    manager.update_exit_scores("PD_HIGH", (0.0, 1.0))
    manager.update_exit_interactions({
        "PD_HIGH": (SimpleNamespace(
            interaction_id="pd-high-after-entry",
            created_at=datetime(2026, 1, 1, 9, 1, tzinfo=timezone.utc),
            available_at=datetime(2026, 1, 1, 9, 1, tzinfo=timezone.utc),
            band_lower=104.5,
            band_upper=105.5,
            candles=(SimpleNamespace(
                available_at=datetime(2026, 1, 1, 9, 1, tzinfo=timezone.utc),
            ),),
        ),),
    })
    manager.register_entry(
        OrderIntent(
            "entry-canonical-exit", instrument, OrderSide.BUY, 1,
            stop_price=95.0, role=OrderRole.ENTRY, exit_plan=plan,
        ),
        fill_price=100.0,
            entry_fill_time=datetime(2026, 1, 1, 9, tzinfo=timezone.utc),
    )
    assert manager.on_closed_bar(
        MarketBar(instrument, datetime(2026, 1, 1, 9, 1, tzinfo=timezone.utc), 100, 106, 99, 105),
    ) == ()
    actions = manager.on_closed_bar(
        MarketBar(instrument, datetime(2026, 1, 1, 9, 2, tzinfo=timezone.utc), 104, 106, 103, 104),
    )
    assert len(actions) == 1
    assert actions[0].reason == "conditional_exit:pd_low-long-exit"
    assert actions[0].price == 104


def test_exit_binding_requires_post_entry_interaction_creation() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    plan = exit_plan_from_template(
        TRADE_TEMPLATES[("5m_recovery_breakout", "PD_LOW")].exits,
        levels={}, bands={},
    )
    manager = OrderflowPositionManager(OrderflowConfig())
    manager.update_exit_interactions({
        "PD_HIGH": (SimpleNamespace(
            interaction_id="pre-entry",
            created_at=datetime(2026, 1, 1, 8, 59, tzinfo=timezone.utc),
            available_at=datetime(2026, 1, 1, 9, tzinfo=timezone.utc),
            band_lower=104.5, band_upper=105.5,
        ),),
    })
    manager.register_entry(
        OrderIntent("entry-no-pre-entry-bind", instrument, OrderSide.BUY, 1,
                    stop_price=95.0, role=OrderRole.ENTRY, exit_plan=plan),
        fill_price=100.0,
        entry_fill_time=datetime(2026, 1, 1, 9, tzinfo=timezone.utc),
    )
    manager.on_closed_bar(
        MarketBar(instrument, datetime(2026, 1, 1, 9, 1, tzinfo=timezone.utc), 100, 101, 99, 100),
    )
    position, _ = manager.positions["entry-no-pre-entry-bind"]
    assert position.exit_plan[0].interaction_id is None
