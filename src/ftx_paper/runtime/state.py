"""Explicit ownership containers for live and isolated replay state."""

from __future__ import annotations

from dataclasses import dataclass, field
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
    shadow_trades: list[dict[str, Any]] = field(default_factory=list)
    replay_ledger: list[dict[str, Any]] = field(default_factory=list)
    parity_records: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: {
        "decision": [], "execution_settlement": [], "portfolio_ledger": [],
    })

    @classmethod
    def create(cls, profile: ResearchCapitalProfile, engine: Any) -> "ReplayRuntimeState":
        portfolio = PortfolioState(profile.initial_capital)
        return cls(portfolio, CapitalRuntimeContext(profile, environment="replay"), engine)


__all__ = ["LiveRuntimeState", "ReplayRuntimeState"]
