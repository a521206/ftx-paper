"""Typed construction contracts shared by strategy implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ftx_paper.domain.capital import CapitalRuntimeContext
from ftx_paper.domain.portfolio import PortfolioState
from ftx_paper.strategies.shared.protocol import Strategy


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """Runtime-owned inputs available when a strategy instance is created."""

    enabled_vehicles: tuple[str, ...] = ("futures", "synthetic")
    portfolio: PortfolioState | None = None
    capital_context: CapitalRuntimeContext | None = None
    prior_day_high: float | None = None
    prior_day_low: float | None = None
    expiry_dates: frozenset[str] = frozenset()


class StrategyFactory(Protocol):
    """Construction port implemented by each strategy package."""

    def create(self, context: StrategyContext) -> Strategy:
        raise NotImplementedError


class StrategySelector(Protocol):
    """Runtime port for selecting a strategy assembled by the composition root."""

    def create(self, strategy_id: str, context: StrategyContext) -> Strategy: ...


__all__ = ["StrategyContext", "StrategyFactory", "StrategySelector"]
