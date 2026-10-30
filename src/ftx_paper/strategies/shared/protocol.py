from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from ftx_paper.market.bundles import DecisionBundle
from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.contracts import OrderIntent


class StrategyDecision(Protocol):
    """Neutral decision fact emitted by any strategy implementation."""

    event_type: str
    payload: Mapping[str, object]
    order: OrderIntent | None


@dataclass(frozen=True, slots=True)
class StrategyMetadata:
    """Stable identity recorded with every live decision."""

    name: str
    version: str
    config_hash: str


class Strategy(Protocol):
    """Pure production boundary for normalized completed market bars.

    Implementations may keep only deterministic strategy state.  Persistence,
    broker execution, and clock access belong to the runtime layer.
    """

    @property
    def metadata(self) -> StrategyMetadata: ...

    def on_bundle(self, bundle: DecisionBundle) -> tuple[object, ...]: ...

    def evaluate(
        self, bundle: DecisionBundle, context: DecisionContext,
    ) -> tuple[object, ...]: ...

    def on_execution_event(self, event: ExecutionNotification) -> None: ...

    def snapshot(self) -> Mapping[str, object]: ...


__all__ = ["Strategy", "StrategyDecision", "StrategyMetadata"]
