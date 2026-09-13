from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ftx_paper.contracts import Instrument, OrderAck, OrderIntent, OrderSide


@dataclass(frozen=True, slots=True)
class Fill:
    client_order_id: str
    instrument: Instrument
    quantity: int
    price: float
    timestamp: str
    vehicle: str = "futures"


class MarketFeed(Protocol):
    def start(self) -> None: ...

    def stop(self) -> None: ...

    def flush(self) -> None: ...


class Broker(Protocol):
    def submit(self, order: OrderIntent) -> OrderAck: ...

    def close(self) -> None: ...

    def poll_fill(self, order: OrderIntent, broker_order_id: str) -> Fill | None: ...


class PaperBroker:
    """Deterministic broker for paper mode; production adapters implement Broker."""

    def __init__(self, prices: dict[str, float] | None = None) -> None:
        self.prices = prices or {}
        self.fills: list[Fill] = []

    def update_price(self, symbol: str, price: float) -> None:
        """Update the simulated fill price from the latest market bar."""
        self.prices[symbol] = price

    def submit(self, order: OrderIntent) -> OrderAck:
        if order.vehicle == "synthetic":
            assert order.synthetic_legs is not None
            call, put = order.synthetic_legs
            prices = [(call, order.side), (put, OrderSide.SELL if order.side is OrderSide.BUY else OrderSide.BUY)]
            if any(leg.symbol not in self.prices for leg, _ in prices):
                missing = next(leg.symbol for leg, _ in prices if leg.symbol not in self.prices)
                raise RuntimeError(f"No paper price available for {missing}")
            from datetime import datetime, timezone
            for leg, side in prices:
                self.fills.append(Fill(order.client_order_id, leg, order.quantity,
                                       self.prices[leg.symbol], datetime.now(timezone.utc).isoformat(), order.vehicle))
            return OrderAck(order.client_order_id, f"paper-{len(self.fills)}", "FILLED")
        price = self.prices.get(order.instrument.symbol)
        if price is None:
            raise RuntimeError(f"No paper price available for {order.instrument.symbol}")
        order_ack = OrderAck(order.client_order_id, f"paper-{len(self.fills) + 1}", "FILLED")
        from datetime import datetime, timezone
        self.fills.append(Fill(
            order.client_order_id, order.instrument, order.quantity, price,
            datetime.now(timezone.utc).isoformat(), order.vehicle,
        ))
        return order_ack

    def poll_fill(self, order: OrderIntent, broker_order_id: str) -> Fill | None:
        return next(
            (fill for fill in reversed(self.fills) if fill.client_order_id == order.client_order_id),
            None,
        )

    def poll_fills(self, order: OrderIntent, broker_order_id: str) -> tuple[Fill, ...]:
        return tuple(fill for fill in self.fills if fill.client_order_id == order.client_order_id)

    def close(self) -> None:
        return None
