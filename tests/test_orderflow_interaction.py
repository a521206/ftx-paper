from datetime import datetime, timedelta, timezone

from ftx_paper.strategy import (
    InteractionCloseReason, InteractionDetector, InteractionStatus,
    LocationBand, LocationRecord, OrderflowObservation, OrderflowObservationContext,
)


UTC = timezone.utc


def _observation(number: int, close: float) -> OrderflowObservation:
    observed = datetime(2026, 1, 1, 9, 15 + number, tzinfo=UTC)
    return OrderflowObservation(
        f"obs-{number}", f"bar-{number}", "v1", "1m", "NIFTYFUT", observed,
        observed + timedelta(minutes=1), {"close": close}, observed,
        observed + timedelta(minutes=1), context=OrderflowObservationContext(
            "2026-01-01", "complete", "available", {"source": "test"}, 1,
        ),
    )


def test_interaction_freezes_reference_and_closes_on_departure() -> None:
    location = LocationRecord("loc-1", "opening_range", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")

    created = detector.observe(location, _observation(0, 100))
    ended = detector.observe(location, _observation(1, 103))

    assert created is not None and created.reference_generation == 1
    assert ended is not None
    assert ended.status is InteractionStatus.ENDED
    assert ended.close_reason is InteractionCloseReason.DEPARTED
    assert len(ended.contact_candles) == 2


def test_interaction_detector_closes_all_for_gap() -> None:
    location = LocationRecord("loc-1", "opening_range", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")
    detector.observe(location, _observation(0, 100))

    ended = detector.close_all(InteractionCloseReason.DATA_GAP, _observation(1, 100).observed_at)

    assert ended[0].close_reason is InteractionCloseReason.DATA_GAP
