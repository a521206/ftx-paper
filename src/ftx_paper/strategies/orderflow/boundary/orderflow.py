"""Live strategy boundary for the canonical orderflow owner."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution.service import ExecutionService
from ftx_paper.strategies.shared.protocol import StrategyDecision, StrategyMetadata
from ftx_paper.contracts import MarketBar, MarketRole
from ftx_paper.strategies.orderflow.coordinator import OrderflowClockInput, OrderflowCoordinator
from ftx_paper.strategies.orderflow.execution_adapter import OrderflowExecutionAdapter
from ftx_paper.strategies.orderflow.execution import ExecutionCoordinator
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry
from ftx_paper.strategies.orderflow.transition import OrderflowTransitionOwner
from ftx_paper.strategies.orderflow.opportunity_coordinator import LifecycleFormation
from ftx_paper.strategies.orderflow.timestamps import serialize_utc
from ftx_paper.strategies.orderflow.providers import OrderflowInputStream
from ftx_paper.strategies.orderflow.evidence_publication import AtrPublication
from ftx_paper.strategies.orderflow.value_publication import ValuePublication

from .orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig


class OrderflowStrategy:
    """Shared live boundary for observation and execution state."""

    def __init__(self, *, config: OrderflowConfig | None = None, coordinator: OrderflowCoordinator | None = None, execution_adapter: OrderflowExecutionAdapter | None = None, execution_owner: ExecutionCoordinator | None = None, transition_owner: OrderflowTransitionOwner | None = None, clock_input_provider: Callable[[object], OrderflowClockInput] | None = None, formation_provider: Callable[[OrderflowClockInput], Sequence[LifecycleFormation]] | None = None) -> None:
        self.config = config or OrderflowConfig()
        self._transition_owner = transition_owner or OrderflowTransitionOwner(
            coordinator.location_registry if coordinator is not None else LocationRegistry("orderflow:live"),
            execution_owner=execution_owner or ExecutionCoordinator(),
            execution_adapter=execution_adapter,
        )
        if coordinator is not None:
            self._transition_owner.observation_owner = coordinator
        if execution_adapter is not None:
            self._transition_owner.execution_adapter = execution_adapter
        self._coordinator = self._transition_owner.observation_owner
        self._execution_adapter = execution_adapter
        self._execution_owner = self._transition_owner.execution_owner
        self._clock_input_provider = clock_input_provider or OrderflowInputStream()
        self._formation_provider = formation_provider

    @property
    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(ORDERFLOW_NAME, ORDERFLOW_VERSION, self.config.config_hash)

    def on_bundle(self, bundle: object) -> tuple[object, ...]:
        return self._source_observation(bundle)

    def evaluate(self, bundle: object, context: object) -> tuple[object, ...]:
        return self._source_observation(bundle)

    def append_feature_bar(self, bar: Any) -> None:
        """Append one selected completed bar through the shared input stream."""
        if not isinstance(self._clock_input_provider, OrderflowInputStream):
            raise TypeError("orderflow feature bars require the incremental input stream")
        self._clock_input_provider.append_continuous_bars((bar,))

    def publish_setup(
        self,
        session_date: str,
        *,
        atr: AtrPublication | None = None,
        prior_value: ValuePublication | None = None,
        prior_session_levels: Mapping[str, object] | None = None,
    ) -> None:
        if not isinstance(self._clock_input_provider, OrderflowInputStream):
            raise TypeError("orderflow setup requires the incremental input stream")
        self._clock_input_provider.publish_setup(
            session_date,
            atr=atr,
            prior_value=prior_value,
            prior_session_levels=prior_session_levels,
        )

    def _source_observation(self, bundle: object) -> tuple[object, ...]:
        bars = getattr(bundle, "bars", {})
        bar = bars.get(MarketRole.FUTURES) if isinstance(bars, Mapping) else None
        if not isinstance(bar, MarketBar) or self._coordinator is None:
            return ()
        clock_input = self._clock_input_provider(bundle)
        advance = self._transition_owner.advance(
            clock_input,
            formations=(self._formation_provider(clock_input) if self._formation_provider else ()),
            instrument=bar.instrument,
        )
        return tuple(
            StrategyDecision(str(item["kind"]).upper(), item)
            for item in advance.observation.events
        )

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self._transition_owner.on_execution_event(event)
        if self._execution_adapter is not None:
            self._execution_adapter.on_execution_event(event)

    def bind_execution_service(self, service: ExecutionService, *, diagnostic: bool = False) -> None:
        """Attach the existing Paper execution owner without adding policy."""
        if self._execution_adapter is None:
            self._execution_adapter = OrderflowExecutionAdapter()
            self._transition_owner.execution_adapter = self._execution_adapter
        self._execution_adapter.service = service
        self._execution_adapter.diagnostic = diagnostic

    def bind_input_provider(
        self, provider: Callable[[object], OrderflowClockInput],
    ) -> None:
        self._clock_input_provider = provider

    def snapshot(self) -> Mapping[str, object]:
        return {
            "schema_version": 1,
            "strategy": ORDERFLOW_NAME,
            "state": "observation_boundary",
            "execution_owner": self._execution_owner is not None,
            "transition_clock": (
                serialize_utc(self._transition_owner.clock)
                if self._transition_owner.clock is not None else None
            ),
            "canonical": self._transition_owner.snapshot(),
            "execution_notifications": len(self._execution_adapter.notifications) if self._execution_adapter else 0,
            "parity_streams": self._transition_owner.parity_streams,
            "parity_layers": self._transition_owner.context.parity_layers,
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object], **_: object) -> "OrderflowStrategy":
        """Restore the producer prefix without creating a second policy owner."""
        if snapshot.get("schema_version") != 1 or snapshot.get("strategy") != ORDERFLOW_NAME:
            raise ValueError("unsupported orderflow strategy snapshot")
        strategy = cls()
        canonical = snapshot.get("canonical")
        if not isinstance(canonical, Mapping):
            raise ValueError("orderflow strategy snapshot canonical state is missing")
        strategy._transition_owner.restore_snapshot(canonical)
        return strategy


__all__ = ["OrderflowConfig", "OrderflowStrategy"]
