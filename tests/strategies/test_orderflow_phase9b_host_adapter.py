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
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.interaction_detector import InteractionDetector
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry


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
