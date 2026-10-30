"""Shared typed policy boundary for strategy implementations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.contracts import OrderIntent

if TYPE_CHECKING:
    from ftx_paper.market.bundles import DecisionBundle


class ExecutionMode(StrEnum):
    DIAGNOSTIC = "diagnostic"
    EXECUTABLE = "executable"


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """Policy output before shared coordination and order authorization."""

    payload: Mapping[str, object]
    event_type: str = "POLICY_DECISION"
    order: OrderIntent | None = None

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise ValueError("policy decision event type is required")


class PolicyOutput(Protocol):
    """Strategy-local policy output consumed by the shared coordinator."""

    payload: Mapping[str, object]
    event_type: str
    order: OrderIntent | None


class StrategyPolicy(Protocol):
    @property
    def execution_mode(self) -> ExecutionMode: ...

    def evaluate(
        self,
        bundle: DecisionBundle,
        context: DecisionContext,
    ) -> tuple[PolicyOutput, ...]: ...

    def on_execution_event(self, event: ExecutionNotification) -> None: ...

    def snapshot(self) -> Mapping[str, object]: ...


__all__ = [
    "ExecutionMode",
    "PolicyDecision",
    "PolicyOutput",
    "StrategyPolicy",
]
