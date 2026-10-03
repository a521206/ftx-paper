"""Immutable account context supplied to strategy decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class CapitalSnapshot:
    equity: float
    available_capital: float
    realized_pnl: float
    daily_loss: float


@dataclass(frozen=True, slots=True)
class MarginSnapshot:
    used: float
    available: float
    required_per_lot: float


@dataclass(frozen=True, slots=True)
class ExposureSnapshot:
    net_directional_lots: float
    open_positions: int


@dataclass(frozen=True, slots=True)
class PositionView:
    order_id: str
    instrument: str
    side: str
    quantity: int
    entry_price: float
    vehicle: str


@dataclass(frozen=True, slots=True)
class PendingOrderView:
    order_id: str
    state: str
    quantity: int
    vehicle: str


@dataclass(frozen=True, slots=True)
class DecisionContext:
    """Read-only point-in-time account view; never an authorization guarantee."""

    as_of: datetime
    account_revision: int
    capital: CapitalSnapshot
    margin: MarginSnapshot
    exposure: ExposureSnapshot
    positions: tuple[PositionView, ...] = ()
    pending_orders: tuple[PendingOrderView, ...] = ()
    quote_as_of: datetime | None = None


__all__ = [
    "CapitalSnapshot", "DecisionContext", "ExposureSnapshot", "MarginSnapshot",
    "PendingOrderView", "PositionView",
]
