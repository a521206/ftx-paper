from datetime import datetime, timedelta, timezone

from ftx_paper.strategy import (
    InteractionDetector, LocationBand, LocationRecord,
    OrderflowObservation, OrderflowObservationContext,
    ResponseOutcomeStatus, evaluate_response_window,
)


UTC = timezone.utc


def _observation(number: int, close: float, previous: float = 99.0) -> OrderflowObservation:
    observed = datetime(2026, 1, 1, 9, 15 + number, tzinfo=UTC)
    return OrderflowObservation(
        f"obs-{number}", f"bar-{number}", "v1", "5m", "NIFTYFUT", observed,
        observed + timedelta(minutes=5), {"open": previous, "close": close, "previous_close": previous},
        observed, observed + timedelta(minutes=5), evidence_ids=(f"ev-{number}",),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {"source": "test"}, 1),
    )


def _interaction():
    location = LocationRecord("loc-1", "opening_range", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("5m")
    return detector.observe(location, _observation(0, 100))


def test_response_confirms_from_completed_candle_evidence() -> None:
    interaction = _interaction()
    assert interaction is not None
    evaluation = evaluate_response_window(interaction, (_observation(1, 101),))
    assert evaluation.status is ResponseOutcomeStatus.CONFIRMED
    assert evaluation.confirmation_observation_id == "obs-1"
    assert evaluation.evidence_ids == ("ev-1",)


def test_response_expires_without_confirmation() -> None:
    interaction = _interaction()
    assert interaction is not None
    observations = tuple(_observation(index, 99) for index in range(1, 4))
    evaluation = evaluate_response_window(interaction, observations)
    assert evaluation.status is ResponseOutcomeStatus.EXPIRED
