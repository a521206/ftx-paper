"""Paper host owner for one canonical orderflow transition machine."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution.service import ExecutionService

from .boundary.orderflow import OrderflowStrategy
from .boundary.orderflow_config import OrderflowConfig
from .coordinator import OrderflowClockInput
from .execution import ExecutionCoordinator
from .execution_adapter import OrderflowExecutionAdapter
from .opportunity_coordinator import LifecycleFormation
from .state_machine_types import LocationRegistry
from .transition import OrderflowTransitionOwner


class PaperOrderflowRuntime:
    """Own one canonical orderflow machine for a Paper runtime instance.

    ``RuntimeSession`` remains responsible for transport, account state,
    lifecycle, persistence, and notifications. This class owns only the
    strategy-specific transition state and its Paper execution adapter.
    """

    def __init__(
        self,
        *,
        config: OrderflowConfig | None = None,
        transition_owner: OrderflowTransitionOwner | None = None,
        execution_adapter: OrderflowExecutionAdapter | None = None,
        clock_input_provider: Callable[[object], OrderflowClockInput] | None = None,
        formation_provider: Callable[[OrderflowClockInput], Sequence[LifecycleFormation]] | None = None,
    ) -> None:
        self.execution_adapter = execution_adapter or OrderflowExecutionAdapter()
        self.transition_owner = transition_owner or OrderflowTransitionOwner(
            LocationRegistry("orderflow:paper"),
            execution_owner=ExecutionCoordinator(),
            execution_adapter=self.execution_adapter,
        )
        self.strategy = OrderflowStrategy(
            config=config,
            transition_owner=self.transition_owner,
            execution_adapter=self.execution_adapter,
            clock_input_provider=clock_input_provider,
            formation_provider=formation_provider,
        )

    @property
    def metadata(self):
        return self.strategy.metadata

    def on_bundle(self, bundle: object) -> tuple[object, ...]:
        return self.strategy.on_bundle(bundle)

    def evaluate(self, bundle: object, context: DecisionContext) -> tuple[object, ...]:
        return self.strategy.evaluate(bundle, context)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.strategy.on_execution_event(event)

    def bind_execution_service(self, service: ExecutionService, *, diagnostic: bool = False) -> None:
        self.strategy.bind_execution_service(service, diagnostic=diagnostic)

    def replay(
        self,
        bundles: Iterable[object],
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Process a bounded replay through the same strategy boundary as live.

        The caller supplies already loaded, chronologically ordered bundles.
        This method does not touch feeds, stores, brokers, or live callbacks;
        a retry must construct a new ``PaperOrderflowRuntime`` instance.
        """
        results: list[tuple[object, ...]] = []
        for bundle in bundles:
            if cancelled is not None and cancelled():
                break
            results.append(self.on_bundle(bundle))
        return tuple(results)

    def snapshot(self) -> Mapping[str, Any]:
        """Expose canonical facts without reconstructing policy state."""
        snapshot = dict(self.strategy.snapshot())
        snapshot["runtime_schema_version"] = 1
        return snapshot


__all__ = ["PaperOrderflowRuntime"]
