from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from .market import Instrument


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderRole(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


@dataclass(frozen=True, slots=True)
class OrderAck:
    client_order_id: str
    broker_order_id: str
    status: str


@dataclass(frozen=True, slots=True)
class OrderIntent:
    client_order_id: str
    instrument: Instrument
    side: OrderSide
    quantity: int
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    reason: str = ""
    # Entry provenance used to reconstruct live protective exits.
    cell: str | None = None
    stop_price: float | None = None
    exit_mode: str | None = None
    target_price: float | None = None
    entry_bar: int | None = None
    role: OrderRole = field(kw_only=True)
