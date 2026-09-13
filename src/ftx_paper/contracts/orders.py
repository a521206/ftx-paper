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
    # Execution vehicle is a property of this order, never of the strategy.
    # Synthetic orders retain the futures instrument as their logical mark
    # and carry the selected CE/PE legs explicitly for the execution layer.
    vehicle: str = field(default="futures", kw_only=True)
    synthetic_legs: tuple[Instrument, Instrument] | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        vehicle = str(self.vehicle).lower()
        if vehicle not in {"futures", "synthetic"}:
            raise ValueError("order vehicle must be 'futures' or 'synthetic'")
        if vehicle == "synthetic":
            if self.synthetic_legs is None or len(self.synthetic_legs) != 2:
                raise ValueError("synthetic orders must contain a CE/PE leg pair")
            call, put = self.synthetic_legs
            if {call.instrument_type.upper(), put.instrument_type.upper()} != {"CE", "PE"}:
                raise ValueError("synthetic orders must contain one CE and one PE leg")
            if (call.expiry, call.strike) != (put.expiry, put.strike):
                raise ValueError("synthetic CE/PE legs must share expiry and strike")
        object.__setattr__(self, "vehicle", vehicle)
