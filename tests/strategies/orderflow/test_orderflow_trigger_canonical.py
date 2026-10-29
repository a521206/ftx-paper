from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.strategy import (
    EntryPolicy,
    OrderflowObservation,
    OrderflowObservationContext,
    observe_trigger,
)


UTC = timezone.utc


def _observation(*, observation_id: str = "obs", observed_at: datetime | None = None, **features: float) -> OrderflowObservation:
    observed_at = observed_at or datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    return OrderflowObservation(
        observation_id, "bar", "v1", "1m", "NIFTYFUT", observed_at,
        observed_at + timedelta(minutes=1), features, observed_at,
        observed_at + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )


def test_intrabar_geometry_is_directional_and_does_not_require_previous_close() -> None:
    long_trigger = observe_trigger(
        _observation(open=99.0, high=100.5, low=98.5, close=99.5),
        intent_id="long", direction="long", boundary=100.0,
        entry_policy=EntryPolicy.INTRABAR_CROSS,
    )
    short_trigger = observe_trigger(
        _observation(open=101.0, high=101.5, low=99.5, close=100.5),
        intent_id="short", direction="short", boundary=100.0,
        entry_policy=EntryPolicy.INTRABAR_CROSS,
    )
    assert long_trigger.crossed and long_trigger.price == 100.0
    assert short_trigger.crossed and short_trigger.price == 100.0


def test_close_confirmation_ignores_wicks() -> None:
    result = observe_trigger(
        _observation(open=99.0, high=101.0, low=98.5, close=99.5),
        intent_id="intent", direction="long", boundary=100.0,
        entry_policy=EntryPolicy.CLOSE_CONFIRMATION,
    )
    assert not result.crossed
    assert result.price is None
    assert result.ineligible_reason == "entry boundary not crossed"


def test_immediate_trigger_is_available_without_price_transition() -> None:
    result = observe_trigger(
        _observation(open=99.0, high=99.5, low=98.5, close=99.0),
        intent_id="intent", direction="long", boundary=100.0,
        trigger_type="immediate",
    )
    assert result.crossed and result.eligible
    assert result.price == 100.0


def test_setup_availability_precedes_trigger_eligibility() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    result = observe_trigger(
        _observation(observed_at=observed, open=99.0, high=101.0, low=99.0, close=100.5),
        intent_id="intent", direction="long", boundary=100.0,
        setup_available_at=(observed + timedelta(minutes=1)).isoformat(),
    )
    assert result.crossed
    assert not result.eligible
    assert result.ineligible_reason == "before_setup_available_at"


def test_trigger_evidence_requires_causal_availability() -> None:
    with pytest.raises(ValueError, match="causally available"):
        from ftx_paper.strategy.orderflow_trigger import TriggerObservation

        TriggerObservation(
            intent_id="intent", observed_at="2026-01-01T10:01:00+00:00",
            available_at="2026-01-01T10:00:00+00:00", crossed=False,
            eligible=False, price=None, reason="entry boundary not crossed",
        )
