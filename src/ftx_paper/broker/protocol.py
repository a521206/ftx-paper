from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol

from ftx_paper.contracts import Instrument, MarketBar, OrderAck, OrderIntent


@dataclass(frozen=True, slots=True)
class Fill:
    client_order_id: str
    instrument: Instrument
    quantity: int
    price: float
    timestamp: str


class MarketFeed(Protocol):
    def bars(self) -> Iterator[MarketBar]: ...


class Broker(Protocol):
    def submit(self, order: OrderIntent) -> OrderAck: ...

    def close(self) -> None: ...


class PaperBroker:
    """Deterministic broker for paper mode; production adapters implement Broker."""

    def __init__(self, prices: dict[str, float] | None = None) -> None:
        self.prices = prices or {}
        self.fills: list[Fill] = []

    def submit(self, order: OrderIntent) -> OrderAck:
        price = self.prices.get(order.instrument.symbol)
        if price is None:
            raise RuntimeError(f"No paper price available for {order.instrument.symbol}")
        order_ack = OrderAck(order.client_order_id, f"paper-{len(self.fills) + 1}", "FILLED")
        from datetime import datetime, timezone
        self.fills.append(Fill(order.client_order_id, order.instrument, order.quantity, price, datetime.now(timezone.utc).isoformat()))
        return order_ack

    def close(self) -> None:
        return None
