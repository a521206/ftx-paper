"""Shared typed policy boundary for strategy implementations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.contracts import OrderIntent
from ftx_paper.market.requirements import FeatureRequirement, feature_union

if TYPE_CHECKING:
    from ftx_paper.market.bundles import DecisionBundle
    from ftx_paper.strategy.decision import DecisionEvent


class ExecutionMode(StrEnum):
    DIAGNOSTIC = "diagnostic"
    EXECUTABLE = "executable"


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """Policy output before shared coordination and order authorization."""

    payload: Mapping[str, object]
    event_type: str = "CANDIDATEDECISION"
    order: OrderIntent | None = None

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise ValueError("policy decision event type is required")


class StrategyPolicy(Protocol):
    @property
    def execution_mode(self) -> ExecutionMode: ...

    @property
    def required_features(self) -> tuple[FeatureRequirement, ...]: ...

    def evaluate(
        self,
        bundle: DecisionBundle,
        context: DecisionContext,
    ) -> tuple[PolicyDecision | DecisionEvent, ...]: ...

    def on_execution_event(self, event: ExecutionNotification) -> None: ...

    def snapshot(self) -> Mapping[str, object]: ...


__all__ = [
    "ExecutionMode",
    "FeatureRequirement",
    "PolicyDecision",
    "StrategyPolicy",
    "feature_union",
]
