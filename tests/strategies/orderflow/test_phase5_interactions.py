from dataclasses import replace

import pytest

from ftx_paper.strategies.orderflow.interaction_detector import InteractionDetector
from ftx_paper.strategies.orderflow.interaction_detector import InteractionTransition
from ftx_paper.strategies.orderflow.interaction_replay import replay_interaction_response
from ftx_paper.strategies.orderflow.context_location_replay import ContextLocationReplay
from ftx_paper.strategies.orderflow.interactions import (
    CloseSide,
    InteractionCloseReason,
    InteractionStatus,
    LocationInteraction,
)
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.location_observation import LocationObservation
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.response import ResponseOutcomeStatus, ResponseSpec
from ftx_paper.strategies.orderflow.state_machine_types import LocationRecord, LocationRegistry
from ftx_paper.strategies.orderflow.templates import Direction, ExitExecutionTiming, ResponseType


def _bar(time: str, *, timeframe: str = "5m", close: float = 100.0, available: str | None = None) -> Aggregate:
    end_minute = int(time[-2:]) + (4 if timeframe == "5m" else 0)
    end = f"{time[:14]}{end_minute:02d}"
    return Aggregate(time, end, available or f"{time[:14]}{end_minute + 1:02d}", timeframe, 5 if timeframe == "5m" else 1,
                     100.0, 101.0, 99.0, close, 10, "NIFTY", "", "FUT", time[:10], f"fp-{time}")


def _interaction(candle: Aggregate | None = None) -> LocationInteraction:
    candle = candle or _bar("2026-09-29T09:20")
    result = LocationInteraction.create(
        instrument="NIFTY", session_date=candle.session_date, location="PD_LOW",
        template_version="1", candle=candle, reference_level=100.0, atr=10.0,
        band=LocationBand(99.0, 101.0), created_at=candle.available_at,
        available_at=candle.available_at, reference_generation=4,
        pre_contact_side=CloseSide.BELOW,
        reference_provenance={"source_observation_id": "loc-4"},
    )
    assert result is not None
    return result


def test_interaction_identity_lineage_and_duplicate_observation_are_frozen() -> None:
    interaction = _interaction()
    assert interaction.interaction_id.endswith(":5m:2026-09-29T09:20")
    assert interaction.reference_generation == 4
    assert interaction.reference_provenance["source_observation_id"] == "loc-4"
    next_bar = replace(interaction.creation_candle, time="2026-09-29T09:25", end_time="2026-09-29T09:29", available_at="2026-09-29T09:30")
    assert interaction.observe(next_bar) is InteractionStatus.ACTIVE
    assert interaction.observe(next_bar) is InteractionStatus.ACTIVE
    assert len(interaction.candles) == 2


def test_execution_policy_rejects_noncausal_completed_close_same_bar() -> None:
    with pytest.raises(ValueError, match="completed-close exits require next-bar execution"):
        ExitExecutionTiming(evidence="completed_close", order="same_bar")


def test_detector_compares_utc_observation_availability_to_local_aggregate_clock() -> None:
    current = _bar("2026-09-29T09:20", timeframe="1m")
    observation = LocationObservation(
        location="PD_LOW", location_id="pd-low", reference_generation=1,
        reference=100.0, band_low=99.0, band_high=101.0,
        open=100.0, high=101.0, low=99.0, close=100.0,
        close_relation="INSIDE_BAND", band_contact=True,
        source_aggregate_id=current.time, observed_at="2026-09-29T03:55:00Z",
        available_at="2026-09-29T04:00:00Z", method="test", observation_id="obs",
        timeframe="1m",
    )

    with pytest.raises(ValueError, match="not causally available"):
        InteractionDetector(LocationRegistry("locations")).observe_1m(
            current=current, available=[current], atr=1.0,
            observations=(observation,),
        )


def test_interaction_projects_and_freezes_across_sessions() -> None:
    interaction = _interaction()
    interaction.seed_projections({"5m": [interaction.creation_candle]})
    next_contact = replace(
        interaction.creation_candle,
        session_date="2026-09-30",
        time="2026-09-30T09:15",
        end_time="2026-09-30T09:19",
        available_at="2026-09-30T09:20",
    )
    assert interaction.observe(next_contact) is InteractionStatus.ACTIVE
    departure = replace(
        next_contact,
        time="2026-09-30T09:20",
        end_time="2026-09-30T09:24",
        available_at="2026-09-30T09:25",
        open=104.0,
        high=106.0,
        low=103.0,
        close=105.0,
    )
    assert interaction.observe(departure) is InteractionStatus.ENDED
    assert interaction.frozen_projections["5m"].source_candle_ids == (
        "2026-09-29T09:20", "2026-09-30T09:15", "2026-09-30T09:20",
    )
    with pytest.raises(ValueError, match="ending must be causal"):
        interaction.end(InteractionStatus.ENDED, available_at="2026-09-28T15:00")


def test_inside_contact_crosses_band_only_with_prior_side_context() -> None:
    contact = replace(
        _bar("2026-09-01T13:00"),
        open=102.0,
        high=103.0,
        low=99.0,
        close=100.0,
        session_date="2026-09-01",
    )
    interaction = LocationInteraction.create(
        instrument="NIFTY", session_date="2026-09-01", location="OR_HIGH",
        template_version="1", candle=contact, reference_level=100.0, atr=10.0,
        band=LocationBand(99.0, 101.0), created_at=contact.available_at,
        available_at=contact.available_at, pre_contact_side=CloseSide.ABOVE,
    )
    assert interaction is not None
    departure = replace(
        contact, time="2026-09-01T13:05", end_time="2026-09-01T13:09",
        available_at="2026-09-01T13:10", open=100.0, high=100.5,
        low=95.0, close=98.0,
    )
    assert interaction.observe(departure) is InteractionStatus.ENDED
    assert interaction.close_reason is InteractionCloseReason.CROSS_BAND_DEPARTURE


def test_interaction_departure_reason_and_future_evidence_rejection() -> None:
    interaction = _interaction()
    departure = replace(interaction.creation_candle, time="2026-09-29T09:25", end_time="2026-09-29T09:29", available_at="2026-09-29T09:30", close=105.0, high=106.0)
    assert interaction.observe(departure) is InteractionStatus.ENDED
    assert interaction.close_reason is InteractionCloseReason.CROSS_BAND_DEPARTURE
    with pytest.raises(ValueError, match="unavailable at interaction creation"):
        _interaction().observe(replace(departure, end_time="2026-09-29T09:18", available_at="2026-09-29T09:19"))


def test_response_arms_then_confirms_and_can_expire() -> None:
    spec = ResponseSpec(Direction.LONG, ResponseType.ACCEPTANCE, LocationBand(99.0, 101.0), "10:00")
    armed, status = spec.advance(open=98.0, high=100.0, low=98.0, close=98.5, armed=False)
    assert armed is True and status is None
    armed, status = spec.advance(open=100.0, high=102.0, low=99.5, close=101.5, armed=armed)
    assert status is ResponseOutcomeStatus.CONFIRMED
    assert spec.expired(session_date="2026-09-29", as_of="2026-09-29T10:00")


def test_response_rejection_invalidation_and_cross_band_confirmation() -> None:
    spec = ResponseSpec(Direction.SHORT, ResponseType.REJECTION, LocationBand(99.0, 101.0))
    _, status = spec.advance(open=100.0, high=101.0, low=99.0, close=102.0, armed=True)
    assert status is ResponseOutcomeStatus.INVALIDATED
    assert spec.cross_band_confirms(close=98.0, pre_contact_side=CloseSide.ABOVE)


def test_detector_keeps_one_episode_per_timeframe() -> None:
    registry = LocationRegistry("stream")
    record = LocationRecord("PD_LOW:session:1", "PD_LOW", 100.0, "session", "2026-09-29T09:15", "2026-09-29T09:15", "stream", LocationBand(99.0, 101.0), frozen_atr=10.0)
    registry.register(record)
    detector = InteractionDetector(registry)
    one = _bar("2026-09-29T09:20", timeframe="1m")
    five = _bar("2026-09-29T09:20", timeframe="5m")
    assert detector.observe_1m(current=one, available=[one], atr=10.0, observations=()) == []
    transitions = detector.advance_projections([one], atr=10.0)
    assert transitions and transitions[0].interaction.timeframe == "1m"
    transitions = detector.observe(current=five, available5=[five], available1=[one], atr=10.0, observations5=(), observations1=())
    assert transitions == []
    assert set(detector.interactions_for("1m"))
    assert detector.interactions_for("1m") is not detector.interactions_for("5m")


def test_replay_publishes_same_clock_interactions_before_response_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    creation = _bar("2026-09-29T09:10")
    current = _bar("2026-09-29T09:15")
    pd = _interaction(creation)
    session_high = LocationInteraction.create(
        instrument="NIFTY", session_date=creation.session_date, location="SESSION_HIGH",
        template_version="1", candle=creation, reference_level=100.0, atr=10.0,
        band=LocationBand(99.0, 101.0), created_at=creation.available_at,
        available_at=creation.available_at, reference_generation=1,
        pre_contact_side=CloseSide.BELOW,
    )
    assert session_high is not None
    pd_observation = LocationObservation(
        location="PD_LOW", location_id="pd", reference_generation=4,
        reference=100.0, band_low=99.0, band_high=101.0, open=100.0,
        high=101.0, low=99.0, close=100.0, close_relation="INSIDE_BAND",
        band_contact=True, source_aggregate_id=current.time, source_observation_id=None,
        observed_at=current.end_time, available_at=current.available_at, method="test",
        observation_id="pd-observation", timeframe="5m",
        child_observation_ids=("1", "2", "3", "4", "5"),
    )
    high_observation = replace(pd_observation, location="SESSION_HIGH", location_id="high", observation_id="high-observation")
    transitions = (
        InteractionTransition(pd.evaluation_snapshot(), pd_observation),
        InteractionTransition(session_high.evaluation_snapshot(), high_observation),
    )

    class FakeDetector:
        def __init__(self, _registry: LocationRegistry) -> None:
            self.interactions = {pd.interaction_id: pd, session_high.interaction_id: session_high}
            self._returned = False

        def observe_1m(self, **_kwargs: object) -> list[InteractionTransition]:
            return []

        def observe(self, **_kwargs: object) -> tuple[InteractionTransition, ...]:
            if self._returned:
                return ()
            self._returned = True
            return transitions

    monkeypatch.setattr("ftx_paper.strategies.orderflow.interaction_replay.InteractionDetector", FakeDetector)
    replay = replay_interaction_response(
        {"1m": (replace(current, timeframe="1m", source_bar_count=1, available_at=current.available_at),), "5m": (current,)},
        ContextLocationReplay((), ()), LocationRegistry("stream"),
    )

    assert [item["kind"] for item in replay.facts[:2]] == [
        "location_interaction_observed", "location_interaction_observed",
    ]
    policy_locations = [
        item["values"]["location"] for item in replay.opportunity_facts
    ]
    assert policy_locations.index("SESSION_HIGH") < policy_locations.index("PD_LOW")
    assert all(item["kind"] != "opportunity_outcome" for item in replay.facts)
    assert all(item["kind"] == "opportunity_outcome" for item in replay.opportunity_facts)
    assert {
        item["identity"] for item in replay.opportunity_facts
    } >= {
        item["identity"] for item in replay.facts
        if item["kind"] == "hypothesis_arbitrated"
    }
