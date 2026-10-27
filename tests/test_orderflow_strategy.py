from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import cast

import pytest

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.market import DecisionBundle
from ftx_paper.runtime.engine import MultiStrategyEngine, PaperEngine
from ftx_paper.runtime.parity_artifacts import build_parity_artifact
from ftx_paper.strategy import (
    LocationObservation,
    OrderflowContext,
    OrderflowObservation,
    OrderflowObservationContext,
    OrderflowStrategy,
)


UTC = timezone.utc
OBSERVED = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)


def _context(
    *,
    close: float = 101.0,
    observation_id: str = "obs-5m",
    generation: int = 1,
    contact: bool = True,
    response_close: float | None = None,
    gap_status: str = "none",
) -> tuple[DecisionBundle, DecisionContext]:
    available = OBSERVED + timedelta(minutes=5)
    location = LocationObservation(
        location_id=f"session-g{generation}",
        location_alias="session_high",
        reference_generation=generation,
        reference=100.0,
        band_lower=99.5,
        band_upper=100.5,
        open=99.0,
        high=101.0 if contact else 110.0,
        low=99.0 if contact else 109.0,
        close=close,
        band_contact=contact,
        source_aggregate_id=f"parent-{observation_id}",
        observed_at=OBSERVED,
        available_at=available,
        observation_id=f"location-{observation_id}-g{generation}",
    )
    children = tuple(
        OrderflowObservation(
            observation_id=f"obs-1m-{index}",
            source_observation_id=f"bar-1m-{index}",
            feature_version="typical_price_volume_v1",
            timeframe="1m",
            instrument="NIFTYFUT",
            observed_at=OBSERVED + timedelta(minutes=index),
            available_at=OBSERVED + timedelta(minutes=index, seconds=1),
            features={"close": 100.0, "vwap": 100.0, "volume": 5.0},
            interval_start=OBSERVED + timedelta(minutes=index),
            interval_end=OBSERVED + timedelta(minutes=index + 1),
            context=OrderflowObservationContext(
                session_id="2026-01-01",
                completeness="complete",
                availability_status="available",
                replay_provenance={"source": "test"},
                generation=generation,
                causal_sequence=1,
            ),
        )
        for index in range(5)
    )
    observation = OrderflowObservation(
        observation_id=observation_id,
        source_observation_id=f"bar-{observation_id}",
        feature_version="typical_price_volume_v1",
        timeframe="5m",
        instrument="NIFTYFUT",
        observed_at=OBSERVED,
        available_at=available,
        features={"open": 99.0, "high": 101.0, "low": 99.0,
                  "close": close, "previous_close": 99.0, "vwap": 100.0,
                  "volume": 25.0},
        interval_start=OBSERVED,
        interval_end=available,
        context=OrderflowObservationContext(
            session_id="2026-01-01",
            completeness="complete",
            availability_status="available",
            replay_provenance={"source": "test", "gap_status": gap_status},
            generation=generation,
            causal_sequence=1,
            child_observation_ids=tuple(item.observation_id for item in children),
            child_intervals=tuple(
                cast(tuple[datetime, datetime], (item.interval_start, item.interval_end))
                for item in children
            ),
        ),
        location_observations=(location,),
    )
    response_observations: tuple[OrderflowObservation, ...] = ()
    if response_close is not None:
        response_time = OBSERVED + timedelta(minutes=5)
        response_available = response_time + timedelta(minutes=5)
        response_observations = (OrderflowObservation(
            observation_id=f"response-{observation_id}",
            source_observation_id=f"response-bar-{observation_id}",
            feature_version="typical_price_volume_v1",
            timeframe="5m",
            instrument="NIFTYFUT",
            observed_at=response_time,
            available_at=response_available,
            features={"open": 100.0, "high": 102.0, "low": 99.0,
                      "close": response_close, "previous_close": 99.0},
            interval_start=response_time,
            interval_end=response_available,
            evidence_ids=(f"response-evidence-{observation_id}",),
            context=OrderflowObservationContext(
                session_id="2026-01-01",
                completeness="complete",
                availability_status="available",
                replay_provenance={"source": "test", "gap_status": "none"},
                generation=generation,
                causal_sequence=2,
            ),
        ),)
    cutoff = response_observations[-1].available_at if response_observations else available
    orderflow = OrderflowContext(
        "2026-01-01",
        cutoff,
        (observation, *response_observations, *children),
        {"5m": (observation, *response_observations), "1m": children},
    )
    bundle = DecisionBundle(
        "2026-01-01:10:00", "2026-01-01", "10:00", {}, (),
        orderflow_context=orderflow,
    )
    account = DecisionContext(
        as_of=cutoff,
        account_revision=0,
        capital=CapitalSnapshot(1000.0, 1000.0, 0.0, 0.0),
        margin=MarginSnapshot(0.0, 1000.0, 0.0),
        exposure=ExposureSnapshot(0.0, 0),
    )
    return bundle, account


def test_producer_published_location_observation_starts_diagnostic_interaction() -> None:
    bundle, account = _context()

    events = OrderflowStrategy().evaluate(bundle, account)

    assert len(events) == 1
    event = events[0]
    assert event.payload["reason"] == "location_interaction_observed"
    assert event.payload["decision_stage"] == "opportunity"
    assert event.payload["location"] == "session_high"
    assert event.payload["reference_generation"] == 1
    assert event.payload["source_observation_ids"] == ["location-obs-5m-g1"]
    assert event.order is None


def test_public_strategy_snapshot_restores_processed_location_lifecycle() -> None:
    bundle, account = _context()
    strategy = OrderflowStrategy()
    first = strategy.evaluate(bundle, account)

    restored = OrderflowStrategy.from_snapshot(strategy.snapshot())

    assert first[0].payload["interaction_id"] == "session-g1:5m"
    assert restored.evaluate(bundle, account) == ()


def test_snapshot_restore_rejects_invalid_location_detector_state() -> None:
    strategy = OrderflowStrategy()
    bundle, account = _context()
    strategy.evaluate(bundle, account)
    snapshot = strategy.snapshot()
    payload = snapshot["payload"]
    assert isinstance(payload, dict)
    payload["location_detector"] = []

    with pytest.raises(ValueError, match="detector"):
        OrderflowStrategy.from_snapshot(snapshot)


def test_generation_change_closes_old_interaction_and_opens_new_generation() -> None:
    strategy = OrderflowStrategy()
    bundle, account = _context()
    strategy.evaluate(bundle, account)

    replacement, replacement_account = _context(
        observation_id="replacement", generation=2,
    )
    events = strategy.evaluate(replacement, replacement_account)

    assert [event.payload["reason"] for event in events] == [
        "REFERENCE_REPLACED", "location_interaction_observed",
    ]
    assert events[0].payload["interaction_id"] == "session-g1:5m"
    assert events[1].payload["interaction_id"] == "session-g2:5m"


def test_non_contact_location_observation_does_not_start_interaction() -> None:
    bundle, account = _context(contact=False, observation_id="no-contact")

    assert OrderflowStrategy().evaluate(bundle, account) == ()


def test_response_and_admission_use_completed_causal_evidence() -> None:
    bundle, account = _context(response_close=101.0)

    events = OrderflowStrategy().evaluate(bundle, account)

    response = next(event for event in events if event.payload.get("decision_stage") == "response")
    admission = next(event for event in events if event.payload.get("decision_stage") == "admission")
    assert response.payload["response_state"] == "CONFIRMED"
    assert response.payload["response_evidence_ids"] == ["response-evidence-obs-5m"]
    assert admission.payload["reason"] == "orderflow_admission"
    assert admission.payload["outcome"] == "accepted"
    assert admission.order is None


def test_gap_closes_active_interaction_without_order() -> None:
    strategy = OrderflowStrategy()
    active, active_context = _context()
    strategy.evaluate(active, active_context)

    gap_time = OBSERVED + timedelta(minutes=5)
    gap_observation = OrderflowObservation(
        observation_id="gap", source_observation_id="gap-source",
        feature_version="typical_price_volume_v1", timeframe="5m",
        instrument="NIFTYFUT", observed_at=gap_time,
        available_at=gap_time + timedelta(minutes=5),
        features={"close": 100.0}, interval_start=gap_time,
        interval_end=gap_time + timedelta(minutes=5),
        context=OrderflowObservationContext(
            session_id="2026-01-01", completeness="complete",
            availability_status="available",
            replay_provenance={"source": "test", "gap_status": "source_gap"},
            generation=1, causal_sequence=2,
        ),
    )
    orderflow = OrderflowContext(
        "2026-01-01", gap_observation.available_at, (gap_observation,),
        {"5m": (gap_observation,), "1m": ()},
    )
    gap = DecisionBundle("gap", "2026-01-01", "10:05", {}, (), orderflow_context=orderflow)
    gap_context = replace(active_context, as_of=gap_observation.available_at)
    events = strategy.evaluate(gap, gap_context)

    closure = next(event for event in events if event.payload.get("reason") == "DATA_GAP")
    assert closure.payload["interaction_id"] == "session-g1:5m"
    assert all(event.order is None for event in events)


def test_orderflow_without_available_observation_emits_nothing() -> None:
    bundle, account = _context()
    account = replace(account, as_of=account.as_of - timedelta(seconds=1))

    assert OrderflowStrategy().evaluate(bundle, account) == ()


def test_orderflow_on_bundle_reports_missing_context() -> None:
    bundle = DecisionBundle("bundle", "2026-01-01", "10:00", {}, ())

    events = OrderflowStrategy().on_bundle(bundle)

    assert events[0].payload["reason"] == "orderflow_context_unavailable"


def test_orderflow_diagnostic_is_attributed_in_multi_strategy_engine() -> None:
    bundle, account = _context()
    engine = MultiStrategyEngine({"orderflow-research": OrderflowStrategy()})

    result = engine.on_bundle(bundle, context=account)

    assert result.events[0]["strategy_id"] == "orderflow-research"
    assert result.orders == ()


def test_non_executable_location_decision_is_persisted_in_parity_stream() -> None:
    bundle, context = _context()
    result = PaperEngine(OrderflowStrategy()).on_bundle(bundle, context=context)

    assert result.orders == ()
    assert result.events[0]["decision_stage"] == "opportunity"
    artifact = build_parity_artifact(
        {"strategy": {"name": "ftx-paper-orderflow"}, "bars_seen": 1,
         "metadata": {"input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-01"},
        layer_records={"decisions": list(result.events)},
    )
    record = artifact["layers"]["decision"][0]
    assert record["values"]["decision_stage"] == "opportunity"


def test_location_observation_has_public_causal_identity_contract() -> None:
    bundle, _ = _context()
    assert bundle.orderflow_context is not None
    observation = bundle.orderflow_context.observations_for("5m")[0]
    location = observation.location_observations[0]

    assert isinstance(location, LocationObservation)
    assert location.available_at >= location.observed_at
    assert location.source_aggregate_id == f"parent-{observation.observation_id}"
