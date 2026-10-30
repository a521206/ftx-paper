"""Application-level strategy selection and construction."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from ftx_paper.domain.capital import RESEARCH_CAPITAL_PROFILE, ResearchCapitalProfile
from ftx_paper.strategies.orderflow.factory import OrderflowStrategyFactory
from ftx_paper.strategies.research.factory import ResearchStrategyFactory
from ftx_paper.strategies.shared.factory import StrategyContext, StrategyFactory
from ftx_paper.strategies.shared.protocol import Strategy


@dataclass(frozen=True, slots=True)
class StrategyDefinition:
    """Composition-owned registration for one strategy implementation."""

    strategy_id: str
    factory: StrategyFactory


class StrategyCatalog:
    """Select explicitly registered strategies at the composition boundary."""

    def __init__(
        self,
        definitions: Mapping[str, StrategyDefinition],
    ) -> None:
        self._definitions = dict(definitions)
        if any(key != definition.strategy_id for key, definition in self._definitions.items()):
            raise ValueError("strategy catalog keys must match definition IDs")

    @property
    def strategy_ids(self) -> tuple[str, ...]:
        return tuple(self._definitions)

    def create(self, strategy_id: str, context: StrategyContext) -> Strategy:
        try:
            definition = self._definitions[strategy_id]
        except KeyError as exc:
            raise ValueError(f"unknown strategy: {strategy_id}") from exc
        return definition.factory.create(context)


def build_strategy_catalog(
    capital_profile: ResearchCapitalProfile = RESEARCH_CAPITAL_PROFILE,
) -> StrategyCatalog:
    """Compose the explicitly enabled strategy definitions for the application."""
    return StrategyCatalog({
        "ftx-paper-research": StrategyDefinition(
            "ftx-paper-research", ResearchStrategyFactory(capital_profile=capital_profile),
        ),
        "ftx-paper-orderflow": StrategyDefinition(
            "ftx-paper-orderflow", OrderflowStrategyFactory(),
        ),
    })


__all__ = ["StrategyCatalog", "StrategyDefinition", "build_strategy_catalog"]
