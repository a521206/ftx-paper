"""Adapter exposing the existing research rules through the policy boundary."""

from __future__ import annotations

from collections.abc import Mapping

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.market.bundles import DecisionBundle

from .decision import DecisionEvent, IndependentLiveDecisionEngine
from .policy_contract import ExecutionMode, FeatureRequirement


class ResearchPolicyAdapter:
    """Preserve research decisions while exposing shared policy composition."""

    execution_mode = ExecutionMode.EXECUTABLE
    required_features: tuple[FeatureRequirement, ...] = ()

    def __init__(self, engine: IndependentLiveDecisionEngine) -> None:
        self.engine = engine

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext) -> tuple[DecisionEvent, ...]:
        del context
        return self.engine.evaluate(bundle)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        del event

    def snapshot(self) -> Mapping[str, object]:
        return self.engine.risk_snapshot()


__all__ = ["ResearchPolicyAdapter"]
