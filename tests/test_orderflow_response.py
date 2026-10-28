from datetime import datetime, timedelta, timezone

from ftx_paper.strategy import (
    InteractionDetector, LocationBand, LocationRecord,
    OrderflowObservation, OrderflowObservationContext,
)
from ftx_paper.strategy.orderflow_response import evaluate_policy_responses
from ftx_paper.strategy.orderflow_contract import FeatureValue


UTC = timezone.utc


def _observation(
    number: int,
    close: float,
    previous: float = 99.0,
    *,
    features: dict[str, FeatureValue] | None = None,
) -> OrderflowObservation:
    observed = datetime(2026, 1, 1, 9, 15, tzinfo=UTC) + timedelta(minutes=5 * number)
    candle_features = features or {"open": previous, "close": close, "previous_close": previous}
    return OrderflowObservation(
        f"obs-{number}", f"bar-{number}", "v1", "5m", "NIFTYFUT", observed,
        observed + timedelta(minutes=5), candle_features,
        observed, observed + timedelta(minutes=5), evidence_ids=(f"ev-{number}",),
        context=OrderflowObservationContext("2026-01-01", "complete", "available", {"source": "test"}, 1),
    )


def _interaction():
    location = LocationRecord("loc-1", "OR_LOW", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("5m")
    return detector.observe(location, _observation(0, 100))


def test_policy_response_evaluates_acceptance_and_rejection_independently() -> None:
    interaction = _interaction()
    assert interaction is not None
    evaluations = evaluate_policy_responses(
        interaction, (_observation(1, 101, features={"open": 100, "high": 102, "low": 99, "close": 101}),),
        max_candles=3, cutoff=datetime(2026, 1, 1, 9, 25, tzinfo=UTC),
    )
    assert {template.response_type.value for template, _ in evaluations} == {"acceptance", "rejection"}
    assert all(evaluation.evidence_ids == ("ev-1",) for _, evaluation in evaluations)


def test_policy_response_includes_interaction_contact_candle() -> None:
    interaction = _interaction()
    assert interaction is not None
    evaluations = evaluate_policy_responses(
        interaction, (_observation(0, 101, features={
            "open": 100, "high": 102, "low": 99, "close": 101,
        }),),
        max_candles=3, cutoff=datetime(2026, 1, 1, 9, 20, tzinfo=UTC),
    )

    assert evaluations
    assert any(
        evaluation.status is not None and evaluation.status.value == "CONFIRMED"
        for _, evaluation in evaluations
    )
    assert any(evaluation.confirmation_observation_id == "obs-0" for _, evaluation in evaluations)
