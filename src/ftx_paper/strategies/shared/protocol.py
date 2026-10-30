from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from ftx_paper.market.bundles import DecisionBundle
from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification

if TYPE_CHECKING:
    from ftx_paper.strategies.research.decision import DecisionEvent


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

    def on_bundle(self, bundle: DecisionBundle) -> tuple[DecisionEvent, ...]: ...

    def evaluate(
        self, bundle: DecisionBundle, context: DecisionContext,
    ) -> tuple[DecisionEvent, ...]: ...

    def on_execution_event(self, event: ExecutionNotification) -> None: ...

    def snapshot(self) -> Mapping[str, object]: ...
