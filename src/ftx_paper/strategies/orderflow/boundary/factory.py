"""Factory for constructing the canonical orderflow strategy boundary."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from ftx_paper.strategies.shared.factory import StrategyContext
from ftx_paper.strategies.shared.protocol import Strategy

from .orderflow_config import OrderflowConfig
from ..execution_adapter import OrderflowExecutionAdapter
from ..paper_runtime import PaperOrderflowRuntime
from ..coordinator import OrderflowClockInput
from ..opportunity_coordinator import LifecycleFormation


class OrderflowStrategyFactory:
    """Construct only the orderflow strategy."""

    def __init__(
        self,
        config: OrderflowConfig | None = None,
        *,
        clock_input_provider: Callable[[object], OrderflowClockInput] | None = None,
        formation_provider: Callable[[OrderflowClockInput], Sequence[LifecycleFormation]] | None = None,
        execution_adapter: OrderflowExecutionAdapter | None = None,
    ) -> None:
        self.config = config
        self.clock_input_provider = clock_input_provider
        self.formation_provider = formation_provider
        self.execution_adapter = execution_adapter

    def create(self, context: StrategyContext) -> Strategy:
        del context
        return PaperOrderflowRuntime(
            config=self.config,
            clock_input_provider=self.clock_input_provider,
            formation_provider=self.formation_provider,
            execution_adapter=self.execution_adapter,
        )


__all__ = ["OrderflowStrategyFactory"]
