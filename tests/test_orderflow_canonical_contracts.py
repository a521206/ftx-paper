from ftx_paper.strategy.response import ResponseCandle, build_response_spec, evaluate_response_window
from ftx_paper.strategy.templates import Direction, ResponseType, TRADE_TEMPLATES


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


def test_non_vwap_template_exits_match_canonical_locations() -> None:
    assert TRADE_TEMPLATES[("5m_recovery_breakout", "PD_HIGH")].exits[0].location.value == "PD_LOW"
    assert TRADE_TEMPLATES[("5m_recovery_breakout", "PD_LOW")].exits[0].location.value == "PD_HIGH"
    assert TRADE_TEMPLATES[("5m_recovery_breakout", "SESSION_HIGH")].exits[0].location.value == "OR_LOW"
    assert TRADE_TEMPLATES[("5m_acceptance_breakout", "SESSION_LOW")].exits[0].location.value == "OR_HIGH"
    assert TRADE_TEMPLATES[("5m_recovery_breakout", "OR_HIGH")].exits == ()
    assert ("5m_acceptance_breakout", "OR_HIGH") not in TRADE_TEMPLATES
    assert ("5m_acceptance_breakout", "OR_LOW") not in TRADE_TEMPLATES
