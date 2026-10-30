"""Factory for constructing the canonical orderflow strategy boundary."""

from __future__ import annotations

from ftx_paper.strategies.shared.factory import StrategyContext
from ftx_paper.strategies.shared.protocol import Strategy

from .orderflow import OrderflowStrategy
from .orderflow_config import OrderflowConfig


class OrderflowStrategyFactory:
    """Construct only the orderflow strategy."""

    def __init__(self, config: OrderflowConfig | None = None) -> None:
        self.config = config

    def create(self, context: StrategyContext) -> Strategy:
        del context
        return OrderflowStrategy(config=self.config)


__all__ = ["OrderflowStrategyFactory"]
