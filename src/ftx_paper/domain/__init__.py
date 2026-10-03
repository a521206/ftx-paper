"""Business-domain aggregates and policies.

The domain package owns state shared by strategies and execution.  Strategies
remain replaceable decision providers and do not own the account aggregate.
"""

from .capital import (
    CapitalRuntimeContext,
    DrawdownPolicy,
    RESEARCH_CAPITAL_PROFILE,
    ResearchCapitalProfile,
    VehicleLimits,
)
from .portfolio import MarginReservation, PaperPosition, PortfolioState

__all__ = [
    "CapitalRuntimeContext",
    "DrawdownPolicy",
    "MarginReservation",
    "PaperPosition",
    "PortfolioState",
    "RESEARCH_CAPITAL_PROFILE",
    "ResearchCapitalProfile",
    "VehicleLimits",
]
