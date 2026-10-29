from ftx_paper.strategy.response import ResponseCandle, build_response_spec, evaluate_response_window
import pytest

from ftx_paper.strategy.templates import (
    Direction,
    ExitExecutionTiming,
    ExitMode,
    ExitResponseConfig,
    LocationTemplate,
    ResponseType,
    TradeExit,
    TRADE_TEMPLATES,
)


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
