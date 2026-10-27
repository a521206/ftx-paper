from datetime import datetime, timedelta, timezone
from dataclasses import replace
from typing import cast

import pytest

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.market import DecisionBundle
from ftx_paper.runtime.engine import MultiStrategyEngine
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.runtime.parity_artifacts import build_parity_artifact
from ftx_paper.strategy.factory import ConfiguredStrategyFactory
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
            child_intervals=tuple(
                cast(tuple[datetime, datetime], (item.interval_start, item.interval_end))
                for item in children
            ),
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
    assert events[0].payload["reason"] == "orderflow_admission"
    assert events[0].order is None
    assert events[0].payload["decision_stage"] == "admission"
    assert events[0].payload["cell"] == "orderflow"
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


def test_orderflow_without_available_observation_emits_no_vwap_fallback() -> None:
    bundle, account = _context()
    account = replace(account, as_of=account.as_of - timedelta(seconds=1))

    events = OrderflowStrategy().evaluate(bundle, account)

    assert events == ()


def test_diagnostic_trigger_is_never_eligible() -> None:
    bundle, _ = _context()
    assert bundle.orderflow_context is not None
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


def test_response_status_field_does_not_override_causal_candle_evidence() -> None:
    strategy = OrderflowStrategy()
    bundle, account = _context(response_status="recovery")

    events = strategy.evaluate(bundle, account)

    assert events[0].payload["response_state"] == "CONFIRMED"
    assert events[0].payload["response_evidence_ids"] == ["evidence-1"]


def test_response_window_expires_without_confirmation() -> None:
    strategy = OrderflowStrategy()
    for index in range(2):
        bundle, account = _context(close=99.0, response_status="confirmed_rejection", observation_id=f"obs-{index}")
        assert strategy.evaluate(bundle, account)[0].payload["response_state"] == "PENDING_RESPONSE"
    bundle, account = _context(close=99.0, response_status="confirmed_rejection", observation_id="obs-2")

    expired = strategy.evaluate(bundle, account)[0]

    assert expired.payload["reason"] == "response_expired"
    assert expired.payload["response_state"] == "EXPIRED"


def test_executable_trigger_requires_arming_and_displacement() -> None:
    bundle, _ = _context()
    assert bundle.orderflow_context is not None
    observation = bundle.orderflow_context.observations_for("5m")[0]

    unarmed = observe_trigger(observation, boundary=100.0, execution_mode="executable")
    no_displacement = observe_trigger(
        replace_observation(observation, {"trigger_armed": True, "displacement": 0.0}),
        boundary=100.0,
        execution_mode="executable",
    )
    armed = observe_trigger(
        replace_observation(observation, {"trigger_armed": True}),
        boundary=100.0,
        execution_mode="executable",
    )

    assert not unarmed.eligible
    assert unarmed.ineligible_reason == "not_armed"
    assert not no_displacement.eligible
    assert no_displacement.ineligible_reason == "insufficient_displacement"
    assert armed.eligible


def replace_observation(observation, feature_updates):
    return replace(observation, features=dict(observation.features) | feature_updates)


def test_orderflow_diagnostic_is_attributed_in_multi_strategy_engine() -> None:
    bundle, account = _context()
    engine = MultiStrategyEngine({"orderflow-research": OrderflowStrategy()})

    result = engine.on_bundle(bundle, context=account)

    assert result.events[0]["strategy_id"] == "orderflow-research"


def test_factory_dispatches_orderflow_without_using_generic_strategy() -> None:
    factory = ConfiguredStrategyFactory()

    orderflow = factory.create(strategy_name="ftx-paper-orderflow")
    generic = factory.create(strategy_name="ftx-paper-research")

    assert orderflow.metadata.name == "ftx-paper-orderflow"
    assert generic.metadata.name != orderflow.metadata.name
    assert hasattr(orderflow, "policy")
    orderflow_policy = getattr(orderflow, "policy", None)
    assert orderflow_policy is not None
    assert orderflow_policy.__class__.__name__ == "OrderflowPolicy"
    assert orderflow_policy.config.name == "ftx-paper-orderflow"


def test_orderflow_emits_trigger_opportunity_lifecycle_and_admission_stages() -> None:
    trigger_bundle, trigger_context = _context(close=120.0, observation_id="trigger")
    pending_bundle, pending_context = _context(close=99.0, observation_id="pending")
    admitted_bundle, admitted_context = _context(close=101.0, observation_id="admitted")
    ended_bundle, ended_context = _context(close=120.0, observation_id="ended")

    trigger = OrderflowStrategy()
    assert trigger.evaluate(trigger_bundle, trigger_context)[0].payload["decision_stage"] == "trigger"

    opportunity = OrderflowStrategy()
    assert opportunity.evaluate(pending_bundle, pending_context)[0].payload["decision_stage"] == "opportunity"

    lifecycle = OrderflowStrategy()
    lifecycle.evaluate(pending_bundle, pending_context)
    assert lifecycle.evaluate(ended_bundle, ended_context)[0].payload["decision_stage"] == "lifecycle"

    admission = OrderflowStrategy()
    event = admission.evaluate(admitted_bundle, admitted_context)[0]
    assert event.payload["decision_stage"] == "admission"
    assert event.order is None


def test_non_executable_orderflow_decision_is_persisted_in_parity_stream() -> None:
    bundle, context = _context()
    result = PaperEngine(OrderflowStrategy()).on_bundle(bundle, context=context)

    assert result.orders == ()
    assert result.events[0]["decision_stage"] == "admission"
    assert result.events[0]["cell"] == "orderflow"
    artifact = build_parity_artifact(
        {"strategy": {"name": "ftx-paper-orderflow"}, "bars_seen": 1,
         "metadata": {"input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-01"},
        layer_records={"decisions": list(result.events)},
    )
    record = artifact["layers"]["decision"][0]
    assert record["values"]["decision_stage"] == "admission"
    assert record["values"]["cell"] == "orderflow"


def test_orderflow_stage_order_and_causal_timestamps_are_deterministic() -> None:
    strategy = OrderflowStrategy()
    first_bundle, first_context = _context(close=99.0, observation_id="first")
    second_bundle, second_context = _context(close=101.0, observation_id="second")

    first = strategy.evaluate(first_bundle, first_context)[0].payload
    second = strategy.evaluate(second_bundle, second_context)[0].payload

    assert str(first["decision_at"]) <= str(second["decision_at"])
    assert str(first["observed_at"]) <= str(first["available_at"])
    assert str(second["observed_at"]) <= str(second["available_at"])
    assert first["source_observation_ids"] == ["first"]
    assert second["source_observation_ids"] == ["second"]
