from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ftx_paper.strategies.orderflow.host_adapter import (
    HOST_ADAPTER_CONTRACT_ID,
    CanonicalInputAdapter,
    CanonicalOutputAdapter,
    LiveHostInputAdapter,
    MarketEvidence,
    ReplayHostInputAdapter,
)
from ftx_paper.strategies.orderflow.providers import (
    CanonicalFormationProvider,
    CanonicalInputProvider,
)
from ftx_paper.strategies.orderflow.coordinator import OrderflowClockInput, OrderflowCoordinator
from ftx_paper.strategies.orderflow.evidence_publication import AtrPublication
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.interaction_detector import InteractionDetector
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry
from ftx_paper.strategies.orderflow.state_machine_reporting import build_replay_payload
from ftx_paper.strategies.orderflow.transition import OrderflowTransitionOwner
from ftx_paper.strategies.orderflow.vwap_location import VwapMeasurement


UTC = timezone.utc


def _evidence() -> tuple[MarketEvidence, ...]:
    start = datetime(2026, 10, 1, 3, 45, tzinfo=UTC)
    return tuple(
        MarketEvidence(
            source_id=f"bar-{index}",
            observed_at=start + timedelta(minutes=index),
            available_at=start + timedelta(minutes=index),
            values={"close": 100 + index},
            lineage=(f"parent-{index}",),
        )
        for index in range(2)
    )


def test_live_and_bounded_replay_use_identical_canonical_input_envelopes() -> None:
    evidence = _evidence()
    live = LiveHostInputAdapter()
    for item in evidence:
        live.publish(item)
    replay = ReplayHostInputAdapter()
    replay.process(evidence)

    assert live.boundary.state.snapshot() == replay.boundary.state.snapshot()
    assert CanonicalInputAdapter.observation(evidence[0]).identity == evidence[0].source_id


def test_input_adapter_preserves_unavailable_evidence_without_repair() -> None:
    evidence = MarketEvidence(
        source_id="bar-incomplete",
        observed_at=datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        available_at=datetime(2026, 10, 1, 3, 46, tzinfo=UTC),
        unavailable=("source_gap",),
    )

    observation = CanonicalInputAdapter.observation(evidence)

    assert observation.unavailable == ("source_gap",)
    assert observation.values == {}


def test_input_adapter_rejects_naive_or_out_of_order_evidence() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        MarketEvidence(
            source_id="naive",
            observed_at=datetime(2026, 10, 1, 3, 45),
            available_at=datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        )

    evidence = _evidence()
    with pytest.raises(ValueError, match="timestamp ordered"):
        ReplayHostInputAdapter().process((evidence[1], evidence[0]))


def test_output_envelope_is_versioned_and_contains_only_supplied_facts() -> None:
    event = LiveHostInputAdapter().publish(_evidence()[0]).events[0]
    envelope = CanonicalOutputAdapter.envelope(
        events=(event,),
        proposals=({"identity": "proposal-1"},),
        snapshot={"schema_version": 1},
        artifact={"status": "UNAVAILABLE"},
    )

    payload = envelope.to_dict()

    assert payload["contract"] == {"id": HOST_ADAPTER_CONTRACT_ID, "version": 1}
    assert payload["events"][0]["identity"] == "bar-0"
    assert payload["artifact"] == {"status": "UNAVAILABLE"}


def test_output_adapter_rejects_non_json_host_metadata() -> None:
    with pytest.raises(ValueError, match="snapshot must be a JSON object"):
        CanonicalOutputAdapter.envelope(snapshot={"timestamp": object()})


def _rows() -> list[dict[str, object]]:
    return [
        {
            "time": f"2026-10-01T09:{15 + index:02d}",
            "open": 100 + index,
            "high": 101 + index,
            "low": 99 + index,
            "close": 100 + index,
            "volume": 10,
            "oi": 1,
            "symbol": "NIFTY",
            "expiry": "",
            "contract_type": "CONTINUOUS",
            "session_date": "2026-10-01",
        }
        for index in range(5)
    ]


def test_bounded_provider_publishes_causal_aggregates_and_lineage() -> None:
    provider = CanonicalInputProvider.from_rows(_rows())

    assert len(provider.envelopes) == 5
    first = provider.envelopes[0]
    assert first.input.one_minute.timeframe == "1m"
    assert first.source_ids[0].startswith("aggregate:1m:")
    assert first.input.atr is not None
    assert first.input.atr.value is None
    assert "insufficient_daily_atr_history" in first.unavailable
    assert provider.envelopes[-1].input.five_minute is not None


def test_replay_provider_does_not_derive_pd_levels_from_prior_bars() -> None:
    prior_day = [
        SimpleNamespace(
            timestamp=datetime(2026, 9, 30, 3, 45 + index, tzinfo=UTC),
            open=100.0,
            high=110.0 + index,
            low=90.0 - index,
            close=100.0,
            volume=10,
            open_interest=1,
            instrument=SimpleNamespace(
                instrument_type="FUT", symbol="NIFTY", expiry="2026-10-29",
            ),
        )
        for index in range(5)
    ]
    current_day = [
        SimpleNamespace(
            timestamp=datetime(2026, 10, 1, 3, 45 + index, tzinfo=UTC),
            open=100.0,
            high=102.0 + index,
            low=98.0 - index,
            close=100.0,
            volume=10,
            open_interest=1,
            instrument=SimpleNamespace(
                instrument_type="FUT", symbol="NIFTY", expiry="2026-10-29",
            ),
        )
        for index in range(5)
    ]

    provider = CanonicalInputProvider.from_market_bars(
        current_day,
        feature_history_bars=prior_day,
        prior_session_levels={"2026-10-01": {}},
    )

    assert len(provider.envelopes) == len(current_day)
    first_input = provider.envelopes[0].input
    assert first_input.one_minute.session_date == "2026-10-01"
    assert first_input.session_levels is not None
    assert first_input.session_levels.prior_day_low is None
    assert first_input.session_levels.prior_day_high is None
    assert first_input.prior_session == "2026-09-30"

    materialized_provider = CanonicalInputProvider.from_market_bars(
        current_day,
        feature_history_bars=prior_day,
        prior_session_levels={
            "2026-10-01": {"high": 114.0, "low": 86.0},
        },
    )
    materialized_levels = materialized_provider.envelopes[0].input.session_levels
    assert materialized_levels is not None
    assert materialized_levels.prior_day_high == 114.0
    assert materialized_levels.prior_day_low == 86.0


def test_coordinator_limits_1m_observations_to_current_clock(monkeypatch) -> None:
    current = Aggregate(
        "2026-10-01T09:20", "2026-10-01T09:20", "2026-10-01T09:21",
        "1m", 1, 100.0, 101.0, 99.0, 100.5, 100,
        "NIFTY", "", "FUT", "2026-10-01", "clock",
    )
    observations = (
        SimpleNamespace(timeframe="1m", source_aggregate_id="2026-10-01T09:19"),
        SimpleNamespace(timeframe="1m", source_aggregate_id="2026-10-01T09:20"),
        SimpleNamespace(timeframe="5m", source_aggregate_id="2026-10-01T09:20"),
    )
    captured: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        InteractionDetector,
        "observe_1m",
        lambda _self, **kwargs: captured.append(kwargs["observations"]) or [],
    )
    coordinator = OrderflowCoordinator(LocationRegistry("test"))

    coordinator.advance(OrderflowClockInput(one_minute=current, observations_1m=observations))

    assert captured == [(observations[1],)]


def test_coordinator_limits_5m_observations_to_current_aggregate(monkeypatch) -> None:
    current = Aggregate(
        "2026-10-01T09:20", "2026-10-01T09:24", "2026-10-01T09:25",
        "5m", 5, 100.0, 101.0, 99.0, 100.5, 500,
        "NIFTY", "", "FUT", "2026-10-01", "clock",
    )
    one = Aggregate(
        "2026-10-01T09:20", "2026-10-01T09:20", "2026-10-01T09:21",
        "1m", 1, 100.0, 101.0, 99.0, 100.5, 100,
        "NIFTY", "", "FUT", "2026-10-01", "clock",
    )
    observations = (
        SimpleNamespace(timeframe="5m", source_aggregate_id="2026-10-01T09:15", location_id=None, reference_generation=0),
        SimpleNamespace(timeframe="5m", source_aggregate_id="2026-10-01T09:20", location_id=None, reference_generation=0),
    )
    captured: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        InteractionDetector,
        "observe",
        lambda _self, **kwargs: captured.append(kwargs["observations5"]) or [],
    )
    coordinator = OrderflowCoordinator(LocationRegistry("test"))

    coordinator.advance(OrderflowClockInput(
        one_minute=one, five_minute=current, observations_5m=observations,
    ))

    assert captured == [(observations[1],)]


def test_coordinator_evaluates_each_new_5m_aggregate_once(monkeypatch) -> None:
    five = Aggregate(
        "2026-10-01T09:20", "2026-10-01T09:24", "2026-10-01T09:25",
        "5m", 5, 100.0, 101.0, 99.0, 100.5, 500,
        "NIFTY", "", "FUT", "2026-10-01", "clock",
    )
    captured: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        InteractionDetector,
        "observe",
        lambda _self, **kwargs: captured.append(kwargs["observations5"]) or [],
    )
    coordinator = OrderflowCoordinator(LocationRegistry("test"))

    for minute in (20, 21):
        one = Aggregate(
            f"2026-10-01T09:{minute:02d}", f"2026-10-01T09:{minute:02d}",
            f"2026-10-01T09:{minute + 1:02d}", "1m", 1, 100.0, 101.0, 99.0,
            100.5, 100, "NIFTY", "", "FUT", "2026-10-01", "clock",
        )
        coordinator.advance(OrderflowClockInput(
            one_minute=one, five_minute=five,
            observations_5m=(SimpleNamespace(
                timeframe="5m", source_aggregate_id="2026-10-01T09:20",
                location_id=None, reference_generation=0,
            ),),
        ))

    assert len(captured) == 1


def test_coordinator_defers_vwap_publication_until_first_completed_5m() -> None:
    registry = LocationRegistry("test")
    coordinator = OrderflowCoordinator(registry)
    atr = AtrPublication(
        100.0,
        datetime(2026, 10, 1, 3, 40, tzinfo=UTC),
        datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        "atr-source",
        ("atr-source",),
    )
    measurement = VwapMeasurement(
        "1m", "2026-10-01", "2026-10-01T09:15",
        datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        datetime(2026, 10, 1, 3, 46, tzinfo=UTC),
        100.0, "vwap-source", "vwap_typical_price_volume_v1",
    )
    opening = Aggregate(
        "2026-10-01T09:15", "2026-10-01T09:15", "2026-10-01T09:16",
        "1m", 1, 100.0, 101.0, 99.0, 100.0, 100,
        "NIFTY", "", "CONTINUOUS", "2026-10-01", "one-15",
    )

    result_before_5m = coordinator.advance(OrderflowClockInput(
        one_minute=opening, atr=atr, vwap_measurements=(measurement,),
    ))

    assert registry.resolve_as_of(
        "VWAP", opening.available_at, timeframe="1m",
    ) is None
    assert not any(
        event["kind"] in {"interaction_created", "location_interaction_observed"}
        and event.get("values", {}).get("location") == "VWAP"
        for event in result_before_5m.events
    )

    last_minute = Aggregate(
        "2026-10-01T09:19", "2026-10-01T09:19", "2026-10-01T09:20",
        "1m", 1, 100.0, 101.0, 99.0, 100.0, 100,
        "NIFTY", "", "CONTINUOUS", "2026-10-01", "one-19",
    )
    first_five = Aggregate(
        "2026-10-01T09:15", "2026-10-01T09:19", "2026-10-01T09:20",
        "5m", 5, 100.0, 101.0, 99.0, 100.0, 500,
        "NIFTY", "", "CONTINUOUS", "2026-10-01", "five-15",
    )
    coordinator.advance(OrderflowClockInput(
        one_minute=last_minute, five_minute=first_five, atr=atr,
        vwap_measurements=(measurement,),
    ))

    assert registry.resolve_as_of(
        "VWAP", first_five.available_at, timeframe="1m",
    ) is not None


def test_first_opening_interaction_matches_canonical_location_priority() -> None:
    atr = AtrPublication(
        100.0,
        datetime(2026, 10, 1, 3, 40, tzinfo=UTC),
        datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        "atr-source",
        ("atr-source",),
    )
    rows = _rows()
    for row, prices in zip(
        rows,
        (
            (100.0, 104.0, 90.0, 100.0),
            (100.0, 102.0, 99.0, 101.0),
            (101.0, 103.0, 100.0, 102.0),
            (102.0, 104.0, 101.0, 103.0),
            (103.0, 105.0, 102.0, 104.0),
        ),
        strict=True,
    ):
        row.update(dict(zip(("open", "high", "low", "close"), prices, strict=True)))
    provider = CanonicalInputProvider.from_rows(
        rows, atr_values={"2026-10-01": atr},
    )
    coordinator = OrderflowCoordinator(LocationRegistry("test"))
    interactions = []
    for envelope in provider.envelopes:
        advance = coordinator.advance(envelope.input)
        interactions.extend(
            event for event in advance.events
            if event["kind"] == "location_interaction_observed"
        )

    assert interactions
    assert interactions[0]["values"]["location"] == "OR_HIGH"
    assert interactions[0]["observed_at"] == datetime(
        2026, 10, 1, 3, 49, tzinfo=UTC,
    )


def test_transition_owner_records_5m_source_fact_with_1m_lineage() -> None:
    provider = CanonicalInputProvider.from_rows(_rows())
    owner = OrderflowTransitionOwner(LocationRegistry("test"))

    for envelope in provider.envelopes:
        owner.advance(envelope.input)

    source_facts = owner.context.parity_streams["source_observation"]
    five_fact = next(
        fact for fact in source_facts
        if fact["identity"] == "aggregate:5m:2026-10-01T09:15"
    )
    assert five_fact["available_at"] == "2026-10-01T03:50:00.000000Z"
    assert five_fact["causal_parent_id"] == "aggregate:1m:2026-10-01T09:19"
    assert five_fact["values"]["aggregate"]["source_bar_count"] == 5
    assert five_fact["lineage"] == [
        f"aggregate:1m:2026-10-01T09:{minute:02d}"
        for minute in range(15, 20)
    ]
    assert [fact["identity"] for fact in source_facts] == [
        *(f"aggregate:1m:2026-10-01T09:{minute:02d}" for minute in range(15, 20)),
        "aggregate:5m:2026-10-01T09:15",
    ]
    payload = build_replay_payload(owner, [], [], _rows(), capture_mode="replay")
    assert payload["parity_phase_facts"]["source_observation"] == source_facts


def test_coordinator_emits_gap_for_missing_5m_location_observation() -> None:
    registry = LocationRegistry("test")
    registry.register_opening_range(
        99.0, 101.0, datetime(2026, 10, 1, 3, 45, tzinfo=UTC), atr=1.0,
    )
    five = Aggregate(
        "2026-10-01T09:20", "2026-10-01T09:24", "2026-10-01T09:25",
        "5m", 5, 100.0, 101.0, 99.0, 100.5, 500,
        "NIFTY", "", "FUT", "2026-10-01", "clock",
    )
    one = Aggregate(
        "2026-10-01T09:20", "2026-10-01T09:20", "2026-10-01T09:21",
        "1m", 1, 100.0, 101.0, 99.0, 100.5, 100,
        "NIFTY", "", "FUT", "2026-10-01", "clock",
    )

    result = OrderflowCoordinator(registry).advance(OrderflowClockInput(
        one_minute=one, five_minute=five,
    ))

    gaps = [event for event in result.events if event["kind"] == "location_observation_gap"]
    assert {event["location"] for event in gaps} == {"OR_LOW", "OR_HIGH"}
    assert all(event["reason"] == "incomplete_or_misaligned_1m_references" for event in gaps)


def test_live_provider_rejects_malformed_explicit_input_without_bar_fallback() -> None:
    provider = CanonicalInputProvider()
    evidence = MarketEvidence(
        source_id="bar-1",
        observed_at=datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        available_at=datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        values={"canonical_input": object()},
    )

    with pytest.raises(ValueError, match="explicit canonical orderflow input"):
        provider.from_bundle(evidence)


def test_live_provider_accepts_mapping_with_explicit_canonical_input() -> None:
    clock = CanonicalInputProvider.from_rows(_rows()).envelopes[0].input

    envelope = CanonicalInputProvider().from_bundle({"orderflow_clock_input": clock})
    supporting = CanonicalInputProvider().from_bundle(
        {"supporting_inputs": {"orderflow_clock_input": clock}},
    )

    assert envelope.input is clock
    assert envelope.source_ids == ()
    assert supporting.input is clock


def test_formation_provider_is_causal_and_retry_idempotent() -> None:
    formation = type(
        "Formation",
        (),
        {
            "identity": "setup-1",
            "lifecycle_id": "life-1",
            "values": {},
            "observed_at": "2026-10-01T03:50:00Z",
            "available_at": "2026-10-01T03:50:00Z",
        },
    )()
    provider = CanonicalFormationProvider(formations=(formation,))
    clock = CanonicalInputProvider.from_rows(_rows()).envelopes[-1].input

    assert provider(clock) == (formation,)
    assert provider(clock) == ()
