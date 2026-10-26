"""Composition boundary for selecting replaceable strategies."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from ftx_paper.domain.capital import RESEARCH_CAPITAL_PROFILE, ResearchCapitalProfile
from .protocol import Strategy

from .live import ConfiguredLiveStrategy


class StrategyFactory(Protocol):
    def create(self, **kwargs: object) -> Strategy: ...


class ConfiguredStrategyFactory:
    """Named strategy composition boundary."""

    def __init__(self, constructor: Callable[..., Strategy] = ConfiguredLiveStrategy,
                 *, capital_profile: ResearchCapitalProfile = RESEARCH_CAPITAL_PROFILE) -> None:
        self.constructor = constructor
        self.capital_profile = capital_profile

    def create(self, **kwargs: object) -> Strategy:
        selected = kwargs.pop("strategy", kwargs.pop("strategy_name", "ftx-paper-research"))
        if not isinstance(selected, str):
            raise TypeError("strategy selection must be a string")
        if selected == "ftx-paper-orderflow":
            from .orderflow import OrderflowConfig, OrderflowStrategy

            config = kwargs.pop("config", None)
            for key in (
                "enabled_vehicles", "portfolio", "capital_context", "prior_day_high",
                "prior_day_low", "expiry_dates",
            ):
                kwargs.pop(key, None)
            if kwargs:
                raise TypeError(f"unsupported orderflow factory arguments: {tuple(kwargs)}")
            if config is None:
                return OrderflowStrategy()
            if not isinstance(config, OrderflowConfig):
                raise TypeError("orderflow factory config must be OrderflowConfig")
            return OrderflowStrategy(config=config)
        if selected != "ftx-paper-research":
            raise ValueError(f"unknown strategy: {selected}")
        kwargs.setdefault("capital_profile", self.capital_profile)
        return self.constructor(**kwargs)


__all__ = ["ConfiguredStrategyFactory", "StrategyFactory"]
