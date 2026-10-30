"""Application-level strategy selection and construction."""

from __future__ import annotations

from collections.abc import Mapping
from ftx_paper.domain.capital import RESEARCH_CAPITAL_PROFILE, ResearchCapitalProfile
from ftx_paper.strategies.orderflow.factory import OrderflowStrategyFactory
from ftx_paper.strategies.research.factory import ResearchStrategyFactory
from ftx_paper.strategies.shared.factory import StrategyContext, StrategyFactory
from ftx_paper.strategies.shared.protocol import Strategy


class StrategyRegistry:
    """Select a named strategy without making strategy packages know each other."""

    def __init__(
        self,
        factories: Mapping[str, StrategyFactory] | None = None,
        *,
        capital_profile: ResearchCapitalProfile = RESEARCH_CAPITAL_PROFILE,
    ) -> None:
        self._factories = dict(factories or {
            "ftx-paper-research": ResearchStrategyFactory(capital_profile=capital_profile),
            "ftx-paper-orderflow": OrderflowStrategyFactory(),
        })

    def create(self, strategy_id: str, context: StrategyContext) -> Strategy:
        try:
            factory = self._factories[strategy_id]
        except KeyError as exc:
            raise ValueError(f"unknown strategy: {strategy_id}") from exc
        return factory.create(context)


__all__ = ["StrategyFactory", "StrategyRegistry"]
