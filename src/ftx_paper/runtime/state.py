"""Explicit ownership containers for live and isolated replay state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ftx_paper.domain.capital import CapitalRuntimeContext, ResearchCapitalProfile
from ftx_paper.domain.portfolio import PortfolioState


@dataclass(slots=True)
class LiveRuntimeState:
    portfolio: PortfolioState
    capital_context: CapitalRuntimeContext
    engine: Any
    broker: Any | None = None


@dataclass(slots=True)
class ReplayRuntimeState:
    """All mutable execution state owned by one replay run."""

    portfolio: PortfolioState
    capital_context: CapitalRuntimeContext
    engine: Any
    coordinator: Any | None = None
    account: Any | None = None

    @classmethod
    def create(cls, profile: ResearchCapitalProfile, engine: Any) -> "ReplayRuntimeState":
        portfolio = PortfolioState(profile.initial_capital)
        return cls(portfolio, CapitalRuntimeContext(profile, environment="replay"), engine)


__all__ = ["LiveRuntimeState", "ReplayRuntimeState"]
