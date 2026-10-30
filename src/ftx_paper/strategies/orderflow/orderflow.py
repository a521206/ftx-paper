"""Minimal strategy entry point reserved for the canonical orderflow port."""

from __future__ import annotations

from collections.abc import Mapping

from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.strategies.shared.protocol import StrategyMetadata

from .orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig


class OrderflowStrategy:
    """No-op scaffold; canonical orderflow behavior is not present in Phase 1."""

    def __init__(self, *, config: OrderflowConfig | None = None) -> None:
        self.config = config or OrderflowConfig()

    @property
    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(ORDERFLOW_NAME, ORDERFLOW_VERSION, self.config.config_hash)

    def on_bundle(self, bundle: object) -> tuple[object, ...]:
        return ()

    def evaluate(self, bundle: object, context: object) -> tuple[object, ...]:
        return ()

    def on_execution_event(self, event: ExecutionNotification) -> None:
        return None

    def snapshot(self) -> Mapping[str, object]:
        return {"schema_version": 1, "strategy": ORDERFLOW_NAME, "state": "scaffold"}


__all__ = ["OrderflowConfig", "OrderflowStrategy"]
