"""Minimal strategy entry point reserved for the canonical orderflow port."""

from __future__ import annotations

from collections.abc import Mapping

from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.strategies.shared.protocol import StrategyMetadata
from ftx_paper.strategies.research.decision import LiveDecision
from ftx_paper.strategies.orderflow.causal_events import CausalEventRegistry
from ftx_paper.contracts import MarketBar, MarketRole
from ftx_paper.strategies.orderflow.coordinator import OrderflowClockInput, OrderflowCoordinator
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate

from .orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig


class OrderflowStrategy:
    """No-op scaffold; canonical orderflow behavior is not present in Phase 1."""

    def __init__(self, *, config: OrderflowConfig | None = None, coordinator: OrderflowCoordinator | None = None) -> None:
        self.config = config or OrderflowConfig()
        self._coordinator = coordinator
        self._causal_events = coordinator.causal_events if coordinator is not None else CausalEventRegistry()

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
        if not isinstance(bar, MarketBar):
            return ()
        source_id = f"aggregate:1m:{bar.timestamp.isoformat()}"
        event = self._causal_events.publish_source_observation(
            source_id=source_id,
            session_date=str(getattr(bundle, "trading_date", "")),
            observed_at=bar.timestamp.isoformat(),
            available_at=bar.timestamp.isoformat(),
        )
        decisions: list[LiveDecision] = [LiveDecision("SOURCE_OBSERVATION", event)]
        if self._coordinator is not None and bar.volume is not None:
            advance = self._coordinator.advance(OrderflowClockInput(self._aggregate(bar)))
            decisions.extend(LiveDecision(str(item["kind"]).upper(), item) for item in advance.events)
        return tuple(decisions)

    @staticmethod
    def _aggregate(bar: MarketBar) -> Aggregate:
        timestamp = bar.timestamp.strftime("%Y-%m-%dT%H:%M")
        session_date = timestamp[:10]
        return Aggregate(
            timestamp, timestamp, timestamp, "1m", 1, bar.open, bar.high, bar.low,
            bar.close, int(bar.volume or 0), bar.instrument.symbol,
            bar.instrument.expiry or "", bar.instrument.instrument_type,
            session_date, f"live:{timestamp}:{bar.instrument.symbol}",
        )

    def on_execution_event(self, event: ExecutionNotification) -> None:
        return None

    def snapshot(self) -> Mapping[str, object]:
        return {"schema_version": 1, "strategy": ORDERFLOW_NAME, "state": "scaffold"}


__all__ = ["OrderflowConfig", "OrderflowStrategy"]
