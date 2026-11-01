"""Live strategy boundary for the canonical orderflow owner."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.strategies.shared.protocol import StrategyDecision, StrategyMetadata
from ftx_paper.contracts import MarketBar, MarketRole
from ftx_paper.strategies.orderflow.coordinator import OrderflowClockInput, OrderflowCoordinator
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.execution_adapter import OrderflowExecutionAdapter
from ftx_paper.strategies.orderflow.execution import ExecutionCoordinator
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry
from ftx_paper.strategies.orderflow.transition import OrderflowTransitionOwner
from ftx_paper.strategies.orderflow.opportunity_coordinator import LifecycleFormation
from ftx_paper.strategies.orderflow.timestamps import require_utc, nifty_label

from .orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig


class OrderflowStrategy:
    """Shared live boundary for observation and execution state."""

    def __init__(self, *, config: OrderflowConfig | None = None, coordinator: OrderflowCoordinator | None = None, execution_adapter: OrderflowExecutionAdapter | None = None, execution_owner: ExecutionCoordinator | None = None, transition_owner: OrderflowTransitionOwner | None = None, clock_input_provider: Callable[[MarketBar], OrderflowClockInput] | None = None, formation_provider: Callable[[OrderflowClockInput], Sequence[LifecycleFormation]] | None = None) -> None:
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
        self._clock_input_provider = clock_input_provider
        self._formation_provider = formation_provider

    @property
    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(ORDERFLOW_NAME, ORDERFLOW_VERSION, self.config.config_hash)

    def on_bundle(self, bundle: object) -> tuple[object, ...]:
        return self._source_observation(bundle)

    def evaluate(self, bundle: object, context: object) -> tuple[object, ...]:
        return self._source_observation(bundle)

    def _source_observation(self, bundle: object) -> tuple[object, ...]:
        bars = getattr(bundle, "bars", {})
        bar = bars.get(MarketRole.FUTURES) if isinstance(bars, Mapping) else None
        if not isinstance(bar, MarketBar) or self._coordinator is None:
            return ()
        clock_input = (
            self._clock_input_provider(bar)
            if self._clock_input_provider is not None
            else OrderflowClockInput(self._aggregate(bar))
        )
        advance = self._transition_owner.advance(
            clock_input,
            formations=(self._formation_provider(clock_input) if self._formation_provider else ()),
            instrument=bar.instrument,
        )
        decisions: list[StrategyDecision] = []
        if advance.observation.source_event is not None:
            decisions.append(StrategyDecision("SOURCE_OBSERVATION", advance.observation.source_event))
        decisions.extend(StrategyDecision(str(item["kind"]).upper(), item) for item in advance.observation.events)
        return tuple(decisions)

    @staticmethod
    def _aggregate(bar: MarketBar) -> Aggregate:
        timestamp = require_utc(bar.timestamp)
        session_date = nifty_label(timestamp)[:10]
        return Aggregate(
            timestamp, timestamp, timestamp, "1m", 1, bar.open, bar.high, bar.low,
            bar.close, int(bar.volume or 0), bar.instrument.symbol,
            bar.instrument.expiry or "", bar.instrument.instrument_type,
            session_date, f"live:{timestamp}:{bar.instrument.symbol}",
        )

    def on_execution_event(self, event: ExecutionNotification) -> None:
        if self._execution_adapter is not None:
            self._execution_adapter.on_execution_event(event)

    def snapshot(self) -> Mapping[str, object]:
        return {
            "schema_version": 1,
            "strategy": ORDERFLOW_NAME,
            "state": "observation_boundary",
            "execution_owner": self._execution_owner is not None,
            "transition_clock": self._transition_owner.clock,
            "execution_notifications": len(self._execution_adapter.notifications) if self._execution_adapter else 0,
        }


__all__ = ["OrderflowConfig", "OrderflowStrategy"]
