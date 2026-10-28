from datetime import datetime, timedelta, timezone

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy import (
    OrderflowObservation, OrderflowObservationContext, CausalOrderflowProducer,
    VwapLocationProvider, observe_trigger,
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


def test_live_producer_does_not_recover_vwap_before_five_completed_minutes() -> None:
    producer = CausalOrderflowProducer()
    start = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    producer.ingest(_bar(start, 100.0))
    producer.ingest(_bar(start + timedelta(minutes=2), 110.0))
    for index in range(3, 10):
        producer.ingest(_bar(start + timedelta(minutes=index), 110.0 + index))

    context = producer.context("2026-01-01", "09:24")
    assert context is not None
    measurements = VwapLocationProvider().publish(
        context.observations, cutoff=context.cutoff,
    )
    one_minute = tuple(item for item in measurements if item.timeframe == "1m")
    assert len(one_minute) == 5
    assert one_minute[0].observed_at == start
    assert one_minute[1].observed_at == start + timedelta(minutes=6)


def test_trigger_expiry_uses_available_evidence_and_versions() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    available = observed + timedelta(minutes=1)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, available,
        {"close": 102.0, "previous_close": 99.0, "armed": True}, observed, available,
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )

    expired = observe_trigger(
        observation, boundary=100.0, execution_mode="enabled",
        expires_at=observed.isoformat(), setup_version="old", permission_version="p1",
        expected_setup_version="new",
    )
    assert not expired.eligible
    assert expired.ineligible_reason == "expired_setup"


def test_trigger_retest_requires_both_confirmation_flags() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, observed + timedelta(minutes=1),
        {"close": 102.0, "previous_close": 99.0, "armed": True, "retest_touch": True},
        observed, observed + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )
    result = observe_trigger(observation, boundary=100.0, execution_mode="enabled", trigger_type="retest")
    assert not result.eligible
    assert result.ineligible_reason == "retest_confirmation_required"


def test_trigger_rejects_causal_timing_and_version_mismatches() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    available = observed + timedelta(minutes=1)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, available,
        {"close": 102.0, "previous_close": 99.0, "armed": True},
        observed, available,
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )
    cases = (
        ({"effective_at": (available + timedelta(minutes=1)).isoformat()}, "before_effective_time"),
        ({"setup_version": "old", "expected_setup_version": "new"}, "setup_version_mismatch"),
        ({"permission_version": "old", "expected_permission_version": "new"}, "permission_version_mismatch"),
        ({"next_bar_open_at": (available + timedelta(minutes=1)).isoformat()}, "next_bar_open_unavailable"),
        ({"trigger_type": "unknown"}, "unsupported_trigger:unknown"),
    )
    for kwargs, reason in cases:
        result = observe_trigger(observation, boundary=100.0, execution_mode="enabled", **kwargs)
        assert not result.eligible
        assert result.ineligible_reason == reason


def test_trigger_requires_a_causal_price() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, observed + timedelta(minutes=1),
        {"previous_close": 99.0, "armed": True}, observed, observed + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )
    result = observe_trigger(observation, boundary=100.0, execution_mode="enabled")
    assert not result.eligible
    assert result.trigger_price is None
    assert result.ineligible_reason == "missing_trigger_price"


def test_trigger_accepts_causal_displacement_only_after_crossing() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, observed + timedelta(minutes=1),
        {"close": 102.0, "previous_close": 99.0, "armed": True},
        observed, observed + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )

    result = observe_trigger(observation, boundary=100.0, execution_mode="enabled")

    assert result.crossed
    assert result.eligible
    assert result.ineligible_reason is None


def test_diagnostic_trigger_preserves_crossing_without_authorizing_execution() -> None:
    observed = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    observation = OrderflowObservation(
        "obs", "bar", "v1", "1m", "NIFTYFUT", observed, observed + timedelta(minutes=1),
        {"close": 102.0, "previous_close": 99.0, "armed": True},
        observed, observed + timedelta(minutes=1),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {}, 1),
    )

    result = observe_trigger(observation, boundary=100.0, execution_mode="diagnostic")

    assert result.crossed
    assert not result.eligible
    assert result.ineligible_reason == "diagnostic_only"
