from datetime import datetime, timedelta, timezone

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy import (
    OrderflowObservation, OrderflowObservationContext, CausalOrderflowProducer,
    observe_trigger,
)


UTC = timezone.utc


def _bar(at: datetime, close: float = 100.0) -> MarketBar:
    return MarketBar(Instrument("NIFTYFUT", "NSE", "FUTURES"), at, 99.0, 101.0, 98.0, close, 10.0)


def test_live_producer_marks_forward_gap_and_resets_vwap_prefix() -> None:
    producer = CausalOrderflowProducer()
    start = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    producer.ingest(_bar(start, 100.0))
    producer.ingest(_bar(start + timedelta(minutes=1), 101.0))
    producer.ingest(_bar(start + timedelta(minutes=3), 110.0))

    context = producer.context("2026-01-01", "09:19")
    assert context is not None
    observations = context.observations_for("1m")
    assert observations[-1].context is not None
    assert observations[-1].context.replay_provenance["gap_status"] == "source_gap"


def test_trigger_expiry_is_canonical_and_uses_observed_time() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    available = observed + timedelta(minutes=1)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, available,
        {"open": 99.0, "high": 102.0, "low": 99.0, "close": 102.0}, observed, available,
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )

    expired = observe_trigger(
        observation, intent_id="intent", direction="long", boundary=100.0,
        expires_at=observed.isoformat(), setup_version="old", permission_version="p1",
        expected_setup_version="new",
    )
    assert not expired.eligible
    assert expired.ineligible_reason == "intent_expired"


def test_trigger_pullback_records_touch_then_confirms_rejection() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, observed + timedelta(minutes=1),
        {"open": 100.0, "high": 101.0, "low": 99.8, "close": 100.0},
        observed, observed + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )
    result = observe_trigger(
        observation, intent_id="intent", direction="long", boundary=100.0,
        trigger_type="pullback",
    )
    assert not result.eligible
    assert result.ineligible_reason == "entry boundary touched; awaiting rejection"
    assert result.retest_touched
    assert result.trigger_type == "pullback"
    assert result.boundary == 100.0
    rejection = OrderflowObservation(
        "obs-2", "bar", "v1", "1m", "NIFTYFUT", observed + timedelta(minutes=1), observed + timedelta(minutes=2),
        {"open": 100.0, "high": 101.2, "low": 99.9, "close": 100.8}, observed + timedelta(minutes=1), observed + timedelta(minutes=2),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )
    confirmed = observe_trigger(
        rejection, intent_id="intent", direction="long", boundary=100.0,
        trigger_type="pullback", retest_touched=True,
    )
    assert confirmed.eligible
    assert confirmed.retest_confirmed


def test_trigger_rejects_causal_timing_and_version_mismatches() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    available = observed + timedelta(minutes=1)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, available,
        {"open": 99.0, "high": 102.0, "low": 99.0, "close": 102.0},
        observed, available,
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )
    cases = (
        ({"effective_at": (available + timedelta(minutes=1)).isoformat()}, "before_effective_at"),
        ({"setup_version": "old", "expected_setup_version": "new"}, "setup_no_longer_valid"),
        ({"permission_version": "old", "expected_permission_version": "new"}, "parent_permission_blocked"),
        ({"permission_allowed": False}, "parent_permission_blocked"),
    )
    for kwargs, reason in cases:
        result = observe_trigger(
            observation, intent_id="intent", direction="long", boundary=100.0, **kwargs,
        )
        assert not result.eligible
        assert result.ineligible_reason == reason


def test_trigger_requires_a_causal_price() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, observed + timedelta(minutes=1),
        {"open": 99.0, "high": 99.5, "low": 98.5}, observed, observed + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )
    result = observe_trigger(observation, intent_id="intent", direction="long", boundary=100.0)
    assert not result.eligible
    assert result.price is None
    assert result.ineligible_reason == "missing_trigger_price"


def test_trigger_accepts_intrabar_crossing_from_ohlc() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, observed + timedelta(minutes=1),
        {"open": 99.0, "high": 102.0, "low": 99.0, "close": 102.0},
        observed, observed + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )

    result = observe_trigger(observation, intent_id="intent", direction="long", boundary=100.0)

    assert result.crossed
    assert result.eligible
    assert result.ineligible_reason is None


def test_trigger_observation_is_not_an_execution_authorization() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, observed + timedelta(minutes=1),
        {"open": 99.0, "high": 102.0, "low": 99.0, "close": 102.0},
        observed, observed + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )

    result = observe_trigger(observation, intent_id="intent", direction="long", boundary=100.0)

    assert result.crossed
    assert result.eligible
