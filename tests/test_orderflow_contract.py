from datetime import datetime, timedelta, timezone
from dataclasses import replace

import pytest

from ftx_paper.market import AggregatorConfig, DecisionBundle
from ftx_paper.contracts import MarketRole
from ftx_paper.strategy import (
    ExecutionMode,
    FeatureRequirement,
    OrderflowContext,
    OrderflowObservation,
    OrderflowObservationContext,
    PolicyDecision,
    SharedDecisionCoordinator,
    feature_union,
)


UTC = timezone.utc


def _observation(observation_id: str, minute: int, timeframe: str = "1m") -> OrderflowObservation:
    observed = datetime(2026, 1, 1, 10, minute, tzinfo=UTC)
    return OrderflowObservation(
        observation_id=observation_id,
        source_observation_id=f"bar-{observation_id}",
        feature_version="typical_price_volume_v1",
        timeframe=timeframe,
        instrument="NIFTYFUT",
        observed_at=observed,
        available_at=observed + timedelta(seconds=1),
        features={"vwap": 100.0},
        interval_start=observed,
        interval_end=observed + timedelta(minutes=1 if timeframe == "1m" else 5),
        context=OrderflowObservationContext(
            session_id="2026-01-01",
            completeness="complete",
            availability_status="available",
            replay_provenance={"source": "test"},
            generation=1,
            causal_sequence=1,
        ),
    )


def test_orderflow_context_rejects_unavailable_observations() -> None:
    observation = _observation("one", 0)
    with pytest.raises(ValueError, match="unavailable"):
        OrderflowContext("2026-01-01", observation.observed_at, (observation,))


def test_complete_parent_requires_exact_ordered_children() -> None:
    children = tuple(_observation(f"child-{index}", index) for index in range(5))
    parent = OrderflowObservation(
        observation_id="parent",
        source_observation_id="bar-parent",
        feature_version="v1",
        timeframe="5m",
        instrument="NIFTYFUT",
        observed_at=children[0].observed_at,
        available_at=children[0].observed_at + timedelta(minutes=5),
        features={"vwap": 100.0},
        interval_start=children[0].observed_at,
        interval_end=children[0].observed_at + timedelta(minutes=5),
        context=OrderflowObservationContext(
            session_id="2026-01-01",
            completeness="complete",
            availability_status="available",
            replay_provenance={"source": "test"},
            child_observation_ids=tuple(item.observation_id for item in children),
            generation=1,
            causal_sequence=1,
            child_intervals=tuple(
                (children[0].observed_at + timedelta(minutes=index),
                 children[0].observed_at + timedelta(minutes=index + 1))
                for index in range(5)
            ),
        ),
    )
    context = OrderflowContext(
        "2026-01-01", parent.available_at, (parent, *children), {"1m": children}
    )
    bundle = DecisionBundle("bundle", "2026-01-01", "10:00", {}, ())
    assert bundle.orderflow_context is None
    assert context.validate_complete_parent(parent)


def test_complete_parent_rejects_interval_not_matching_child() -> None:
    children = tuple(_observation(f"child-{index}", index) for index in range(5))
    malformed = replace(children[2], interval_end=children[2].interval_end + timedelta(seconds=1))
    children = (*children[:2], malformed, *children[3:])
    parent = replace(
        _observation("parent", 0, "5m"),
        available_at=children[0].observed_at + timedelta(minutes=5),
        context=OrderflowObservationContext(
            session_id="2026-01-01", completeness="complete", availability_status="available",
            replay_provenance={"source": "test"}, generation=1, causal_sequence=1,
            child_observation_ids=tuple(item.observation_id for item in children),
            child_intervals=tuple((item.interval_start, item.interval_end) for item in children),
        ),
        interval_start=children[0].interval_start,
        interval_end=children[0].interval_start + timedelta(minutes=5),
    )
    context = OrderflowContext("2026-01-01", parent.available_at, (parent, *children), {"1m": children})

    assert not context.validate_complete_parent(parent)


def test_feature_union_is_stable_and_preserves_required_flag() -> None:
    class Policy:
        execution_mode = ExecutionMode.DIAGNOSTIC
        required_features = (
            FeatureRequirement("vwap", "5m", required=False),
            FeatureRequirement("delta", "1m"),
        )

    class SecondPolicy:
        execution_mode = ExecutionMode.DIAGNOSTIC
        required_features = (FeatureRequirement("vwap", "5m"),)

    assert feature_union((Policy(), SecondPolicy())) == (
        FeatureRequirement("vwap", "5m"),
        FeatureRequirement("delta", "1m"),
    )
    config = AggregatorConfig.from_policies((Policy(), SecondPolicy()), required_roles=(MarketRole.FUTURES,))
    assert config.feature_requirements == feature_union((Policy(), SecondPolicy()))


def test_diagnostic_coordinator_rejects_accidental_order_payload() -> None:
    class Policy:
        execution_mode = ExecutionMode.DIAGNOSTIC
        required_features: tuple[FeatureRequirement, ...] = ()

        def evaluate(self, bundle, context):
            return (PolicyDecision({"candidate_id": "candidate", "outcome": "accepted", "order": "bad"}),)

        def on_execution_event(self, event):
            return None

        def snapshot(self):
            return {}

    context = type("Context", (), {"as_of": datetime(2026, 1, 1, tzinfo=UTC)})()
    event = SharedDecisionCoordinator(Policy()).evaluate(
        DecisionBundle("bundle", "2026-01-01", "10:00", {}, ()), context
    )[0]
    assert event.payload["outcome"] == "rejected"
    assert event.payload["reason"] == "diagnostic_policy_order_forbidden"
