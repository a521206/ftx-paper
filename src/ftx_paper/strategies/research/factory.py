"""Factory for constructing the FTX research strategy."""

from __future__ import annotations

from collections.abc import Callable
from ftx_paper.domain.capital import RESEARCH_CAPITAL_PROFILE, ResearchCapitalProfile
from ftx_paper.strategies.shared.factory import StrategyContext
from ftx_paper.strategies.shared.protocol import Strategy

from ftx_paper.strategies.research import ConfiguredLiveStrategy


class ResearchStrategyFactory:
    """Construct only the research strategy; selection belongs to the application layer."""

    def __init__(self, constructor: Callable[..., Strategy] = ConfiguredLiveStrategy,
                 *, capital_profile: ResearchCapitalProfile = RESEARCH_CAPITAL_PROFILE) -> None:
        self.constructor = constructor
        self.capital_profile = capital_profile

    def create(self, context: StrategyContext) -> Strategy:
        return self.constructor(
            capital_profile=self.capital_profile,
            enabled_vehicles=context.enabled_vehicles,
            portfolio=context.portfolio,
            capital_context=context.capital_context,
            prior_day_high=context.prior_day_high,
            prior_day_low=context.prior_day_low,
            expiry_dates=context.expiry_dates,
        )


__all__ = ["ResearchStrategyFactory"]
