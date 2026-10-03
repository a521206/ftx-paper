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
from .portfolio import MarginReservation, PaperPosition, PortfolioState, SettlementResult
from .account import AccountAggregate
from .decision_context import (
    CapitalSnapshot, DecisionContext, ExposureSnapshot, MarginSnapshot,
    PendingOrderView, PositionView,
)

__all__ = [
    "CapitalRuntimeContext",
    "AccountAggregate",
    "CapitalSnapshot",
    "DecisionContext",
    "DrawdownPolicy",
    "MarginReservation",
    "PaperPosition",
    "PortfolioState",
    "SettlementResult",
    "RESEARCH_CAPITAL_PROFILE",
    "ResearchCapitalProfile",
    "VehicleLimits",
    "ExposureSnapshot",
    "MarginSnapshot",
    "PendingOrderView",
    "PositionView",
]
