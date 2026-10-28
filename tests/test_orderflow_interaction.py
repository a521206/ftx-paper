from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.strategy import (
    EvidenceLineage, HypothesisState, InteractionCloseReason, InteractionDetector, InteractionStatus,
    LocationBand, LocationObservation, LocationRecord, OrderflowObservation, OrderflowObservationContext,
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


def test_interaction_detector_snapshot_restores_generation_state() -> None:
    location = LocationRecord("loc-1", "opening_range", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")
    created = detector.observe(location, _observation(0, 100))
    assert created is not None
    restored = InteractionDetector.from_snapshot(detector.snapshot())
    advanced = restored.observe(location, _observation(1, 100))
    assert advanced is not None
    assert advanced.interaction_id == created.interaction_id
    assert len(advanced.contact_candles) == 2


def test_interaction_detector_deduplicates_by_location_observation() -> None:
    first = LocationRecord("loc-1", "opening_range", 100, LocationBand(99, 101), 1,
                           datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    second = LocationRecord("loc-2", "session_high", 100, LocationBand(99, 101), 1,
                            datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")
    observation = _observation(0, 100)
    assert detector.observe(first, observation, observation_id="location-1") is not None
    assert detector.observe(second, observation, observation_id="location-2") is not None
    assert len(detector.active_interactions) == 2


def test_wick_intersection_ends_interaction_when_close_is_outside() -> None:
    location = LocationRecord("loc-wick", "opening_range", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")
    detector.observe(location, _observation(0, 100))
    wick = _observation(1, 103)
    wick = OrderflowObservation(
        wick.observation_id, wick.source_observation_id, wick.feature_version, wick.timeframe,
        wick.instrument, wick.observed_at, wick.available_at,
        {"open": 103, "high": 103.5, "low": 100.5, "close": 103},
        wick.interval_start, wick.interval_end, context=wick.context,
    )
    advanced = detector.observe(location, wick)
    assert advanced is not None
    assert advanced.status is InteractionStatus.ENDED
    assert advanced.close_reason is InteractionCloseReason.DEPARTED
    assert advanced.contact_candles[-1].low == 100.5


def test_five_minute_location_observation_requires_child_lineage() -> None:
    with pytest.raises(ValueError, match="exactly five"):
        LocationObservation(
            "loc", "OR_HIGH", 1, 100.0, 99.0, 101.0,
            100.0, 101.0, 99.0, 100.0, True, "parent",
            datetime(2026, 1, 1, 9, 15, tzinfo=UTC),
            datetime(2026, 1, 1, 9, 20, tzinfo=UTC), "location-obs",
            timeframe="5m", child_observation_ids=("child-1",),
        )


def test_location_observation_rejects_empty_child_ids() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        LocationObservation(
            "loc", "OR_HIGH", 1, 100.0, 99.0, 101.0,
            100.0, 101.0, 99.0, 100.0, True, "parent",
            datetime(2026, 1, 1, 9, 15, tzinfo=UTC),
            datetime(2026, 1, 1, 9, 20, tzinfo=UTC), "location-obs",
            timeframe="5m", child_observation_ids=("", "b", "c", "d", "e"),
        )


def test_vwap_location_observation_requires_source_lineage() -> None:
    with pytest.raises(ValueError, match="source observation ID"):
        LocationObservation(
            "loc", "VWAP", 1, 100.0, 99.0, 101.0,
            100.0, 101.0, 99.0, 100.0, True, "parent",
            datetime(2026, 1, 1, 9, 15, tzinfo=UTC),
            datetime(2026, 1, 1, 9, 20, tzinfo=UTC), "location-obs",
        )


def test_hypothesis_state_snapshot_preserves_causal_lineage() -> None:
    location = LocationRecord("loc-state", "opening_range", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")
    interaction = detector.observe(location, _observation(0, 100))
    assert interaction is not None
    state = HypothesisState("acceptance", "v2", "PENDING_RESPONSE", True, "long", ("e1",), expected_available_at=_observation(1, 100).available_at)
    detector.update_response(interaction.interaction_id, state.status, state.hypothesis_id, state=state)
    restored = InteractionDetector.from_snapshot(detector.snapshot())
    assert restored.active_interactions[0].hypothesis_states == (state,)


def test_hypothesis_snapshot_preserves_observation_lineage() -> None:
    location = LocationRecord("loc-lineage", "opening_range", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")
    interaction = detector.observe(location, _observation(0, 100))
    assert interaction is not None
    observation = _observation(1, 101)
    lineage = EvidenceLineage(
        observation.observation_id, observation.source_observation_id, observation.timeframe,
        observation.observed_at, observation.available_at, "fingerprint-1",
    )
    state = HypothesisState(
        "acceptance", "v2", "CONFIRMED", True, "long", ("e1",),
        confirmation_observation_id=observation.observation_id,
        confirmation_available_at=observation.available_at,
        evidence_lineage=(lineage,),
    )
    detector.update_response(interaction.interaction_id, state.status, state.hypothesis_id, state=state)
    restored = InteractionDetector.from_snapshot(detector.snapshot())
    assert restored.active_interactions[0].hypothesis_states[0].evidence_lineage == (lineage,)


def test_terminal_hypothesis_cannot_be_reopened_by_later_evidence() -> None:
    location = LocationRecord("loc-terminal", "opening_range", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")
    interaction = detector.observe(location, _observation(0, 100))
    assert interaction is not None
    confirmed = HypothesisState("rejection", "v1", "CONFIRMED", True, "long", ("e1",))
    detector.update_response(interaction.interaction_id, confirmed.status, confirmed.hypothesis_id, state=confirmed)
    reopened = HypothesisState("rejection", "v1", "PENDING_RESPONSE", True, "long", ("e1", "e2"))
    updated = detector.update_response(
        interaction.interaction_id, reopened.status, reopened.hypothesis_id, state=reopened,
    )
    assert updated is not None
    assert updated.hypothesis_states == (confirmed,)


def test_prior_day_role_transition_is_persisted_with_causal_candle() -> None:
    location = LocationRecord("loc-pd", "PD_HIGH", 100, LocationBand(99, 101), 1,
                              datetime(2026, 1, 1, 9, 15, tzinfo=UTC), "agg-1")
    detector = InteractionDetector("1m")
    interaction = detector.observe(location, _observation(0, 100))
    assert interaction is not None
    updated = detector.update_prior_day_role(
        interaction.interaction_id, "SUPPORT", "obs-2", _observation(1, 100).available_at,
    )
    assert updated is not None
    restored = InteractionDetector.from_snapshot(detector.snapshot())
    persisted = restored.active_interactions[0]
    assert persisted.pd_role == "SUPPORT"
    assert persisted.pd_role_transition_candle_id == "obs-2"
