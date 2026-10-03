"""Composition boundary for selecting replaceable strategies."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from ftx_paper.domain.capital import RESEARCH_CAPITAL_PROFILE, ResearchCapitalProfile
from ftx_paper.core.strategy import Strategy

from .live import ConfiguredLiveStrategy


class StrategyFactory(Protocol):
    def create(self, **kwargs: object) -> Strategy: ...


class ConfiguredStrategyFactory:
    """Default strategy factory; alternate strategies can implement the same port."""

    def __init__(self, constructor: Callable[..., Strategy] = ConfiguredLiveStrategy,
                 *, capital_profile: ResearchCapitalProfile = RESEARCH_CAPITAL_PROFILE) -> None:
        self.constructor = constructor
        self.capital_profile = capital_profile

    def create(self, **kwargs: object) -> Strategy:
        kwargs.setdefault("capital_profile", self.capital_profile)
        return self.constructor(**kwargs)


__all__ = ["ConfiguredStrategyFactory", "StrategyFactory"]
