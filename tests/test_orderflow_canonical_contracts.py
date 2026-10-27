from ftx_paper.strategy.opportunity import opportunity_identity
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


def test_opportunity_identity_is_stable_across_runner_adapters() -> None:
    values = dict(
        session_date="2026-01-01", instrument="NIFTYFUT", direction=Direction.LONG,
        interaction_id="interaction-1", reference_generation=2,
        response_evidence_ids=("bar-1", "bar-2"),
        formation_at="2026-01-01T09:30:00+00:00",
        formation_available_at="2026-01-01T09:35:00+00:00",
        template_id="or_low-long-recovery_breakout",
    )
    assert opportunity_identity(**values) == opportunity_identity(**values)
