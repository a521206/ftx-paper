from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.market import DecisionBundle
from ftx_paper.runtime.engine import MultiStrategyEngine
from ftx_paper.strategy import (
    OrderflowContext,
    OrderflowObservation,
    OrderflowObservationContext,
    OrderflowStrategy,
    observe_trigger,
)


UTC = timezone.utc


def _context(*, close: float = 101.0, response_status: str = "confirmed_rejection", observation_id: str = "obs-5m") -> tuple[DecisionBundle, DecisionContext]:
    observed = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    observation = OrderflowObservation(
        observation_id=observation_id,
        source_observation_id="bar-5m",
        feature_version="typical_price_volume_v1",
        timeframe="5m",
        instrument="NIFTYFUT",
        observed_at=observed,
        available_at=observed + timedelta(minutes=5),
        features={"close": close, "previous_close": 99.0, "vwap": 100.0, "volume": 25.0},
        interval_start=observed,
        interval_end=observed + timedelta(minutes=5),
        response_status=response_status,
        evidence_ids=("evidence-1",),
        context=OrderflowObservationContext(
            session_id="2026-01-01",
            completeness="complete",
            availability_status="available",
            replay_provenance={"source": "test"},
            generation=1,
            causal_sequence=1,
        ),
    )
    children = tuple(
        OrderflowObservation(
            observation_id=f"obs-1m-{index}",
            source_observation_id=f"bar-1m-{index}",
            feature_version="typical_price_volume_v1",
            timeframe="1m",
            instrument="NIFTYFUT",
            observed_at=observed + timedelta(minutes=index),
            available_at=observed + timedelta(minutes=index, seconds=1),
            features={"close": 100.0, "vwap": 100.0, "volume": 5.0},
            interval_start=observed + timedelta(minutes=index),
            interval_end=observed + timedelta(minutes=index + 1),
            context=OrderflowObservationContext(
                session_id="2026-01-01", completeness="complete",
                availability_status="available", replay_provenance={"source": "test"},
                generation=1, causal_sequence=1,
            ),
        ) for index in range(5)
    )
    observation = OrderflowObservation(
        observation_id=observation.observation_id,
        source_observation_id=observation.source_observation_id,
        feature_version=observation.feature_version,
        timeframe=observation.timeframe,
        instrument=observation.instrument,
        observed_at=observation.observed_at,
        available_at=observation.available_at,
        features=observation.features,
        interval_start=observation.interval_start,
        interval_end=observation.interval_end,
        response_status=observation.response_status,
        evidence_ids=observation.evidence_ids,
        context=OrderflowObservationContext(
            session_id="2026-01-01", completeness="complete",
            availability_status="available", replay_provenance={"source": "test"},
            generation=1, causal_sequence=1,
            child_observation_ids=tuple(item.observation_id for item in children),
            child_intervals=tuple((item.interval_start, item.interval_end) for item in children),
        ),
    )
    orderflow = OrderflowContext(
        "2026-01-01", observation.available_at,
        (observation, *children), {"5m": (observation,), "1m": children},
    )
    bundle = DecisionBundle("2026-01-01:10:00", "2026-01-01", "10:00", {}, (), orderflow_context=orderflow)
    account = DecisionContext(
        as_of=observation.available_at,
        account_revision=0,
        capital=CapitalSnapshot(1000.0, 1000.0, 0.0, 0.0),
        margin=MarginSnapshot(0.0, 1000.0, 0.0),
        exposure=ExposureSnapshot(0.0, 0),
    )
    return bundle, account


def test_orderflow_strategy_emits_diagnostic_candidate_without_order() -> None:
    bundle, account = _context()
    events = OrderflowStrategy().evaluate(bundle, account)
    assert len(events) == 1
    assert events[0].payload["reason"] == "diagnostic_vwap_interaction"
    assert events[0].order is None
    assert events[0].payload["source_observation_ids"] == ["obs-5m"]


def test_orderflow_snapshot_restores_deterministic_policy_state() -> None:
    bundle, account = _context()
    strategy = OrderflowStrategy()
    first = strategy.evaluate(bundle, account)
    restored = OrderflowStrategy.from_snapshot(strategy.snapshot())
    second = restored.evaluate(bundle, account)
    assert first[0].payload["interaction_id"]
    assert second == ()


def test_orderflow_on_bundle_reports_missing_context() -> None:
    strategy = OrderflowStrategy()
    bundle = DecisionBundle("bundle", "2026-01-01", "10:00", {}, ())

    events = strategy.on_bundle(bundle)

    assert events[0].payload["reason"] == "orderflow_context_unavailable"


def test_diagnostic_trigger_is_never_eligible() -> None:
    bundle, _ = _context()
    observation = bundle.orderflow_context.observations_for("5m")[0]

    trigger = observe_trigger(observation, boundary=100.0, execution_mode="diagnostic")

    assert trigger.crossed
    assert not trigger.eligible
    assert trigger.ineligible_reason == "diagnostic_only"


def test_orderflow_snapshot_rejects_duplicate_interactions() -> None:
    bundle, account = _context()
    strategy = OrderflowStrategy()
    strategy.evaluate(bundle, account)
    snapshot = strategy.snapshot()
    payload = snapshot["payload"]
    assert isinstance(payload, dict)
    interactions = payload["interactions"]
    assert isinstance(interactions, list) and interactions
    payload["interactions"] = [interactions[0], interactions[0]]

    with pytest.raises(ValueError, match="duplicate"):
        OrderflowStrategy.from_snapshot(snapshot)


def test_interaction_episode_is_reused_then_ended() -> None:
    strategy = OrderflowStrategy()
    first_bundle, account = _context()
    strategy.evaluate(first_bundle, account)
    repeated_bundle, repeated_account = _context()
    repeated = strategy.evaluate(repeated_bundle, repeated_account)
    outside_bundle, outside_account = _context(close=110.0, response_status="", observation_id="obs-5m-next")
    ended = strategy.evaluate(outside_bundle, outside_account)[0]

    assert repeated == ()
    assert ended.payload["reason"] == "interaction_ended"
    assert ended.payload["interaction_state"] == "ENDED"


def test_orderflow_diagnostic_is_attributed_in_multi_strategy_engine() -> None:
    bundle, account = _context()
    engine = MultiStrategyEngine({"orderflow-research": OrderflowStrategy()})

    result = engine.on_bundle(bundle, context=account)

    assert result.events[0]["strategy_id"] == "orderflow-research"
