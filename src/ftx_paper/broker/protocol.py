from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ftx_paper.contracts import (
    Instrument, OrderAck, OrderIntent, OrderLifecycle, OrderLifecycleState, OrderSide,
)


@dataclass(frozen=True, slots=True)
class Fill:
    client_order_id: str
    instrument: Instrument
    quantity: int
    price: float
    timestamp: str
    vehicle: str = "futures"
    fill_id: str | None = None
    event_id: str | None = None
    source: str | None = None


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
        self.lifecycle: dict[str, OrderLifecycle] = {}
        self._acks: dict[str, OrderAck] = {}

    def update_price(self, symbol: str, price: float) -> None:
        """Update the simulated fill price from the latest market bar."""
        self.prices[symbol] = price

    def submit(self, order: OrderIntent) -> OrderAck:
        existing_ack = self._acks.get(order.client_order_id)
        if existing_ack is not None:
            return existing_ack
        if order.vehicle == "synthetic":
            assert order.synthetic_legs is not None
            call, put = order.synthetic_legs
            prices = [(call, order.side), (put, OrderSide.SELL if order.side is OrderSide.BUY else OrderSide.BUY)]
            if any(leg.symbol not in self.prices for leg, _ in prices):
                missing = next(leg.symbol for leg, _ in prices if leg.symbol not in self.prices)
                raise RuntimeError(f"No paper price available for {missing}")
            lifecycle = self.lifecycle.setdefault(order.client_order_id, OrderLifecycle(order.quantity))
            lifecycle.transition(OrderLifecycleState.SUBMITTING)
            from datetime import datetime, timezone
            for index, (leg, side) in enumerate(prices):
                self.fills.append(Fill(order.client_order_id, leg, order.quantity,
                                       self.prices[leg.symbol], datetime.now(timezone.utc).isoformat(),
                                       order.vehicle, f"paper-fill-{len(self.fills) + index + 1}"))
            lifecycle.apply_fill(f"paper-order-{order.client_order_id}", order.quantity)
            ack = OrderAck(order.client_order_id, f"paper-{len(self.fills)}", "FILLED")
            self._acks[order.client_order_id] = ack
            return ack
        price = self.prices.get(order.instrument.symbol)
        if price is None:
            raise RuntimeError(f"No paper price available for {order.instrument.symbol}")
        lifecycle = self.lifecycle.setdefault(order.client_order_id, OrderLifecycle(order.quantity))
        lifecycle.transition(OrderLifecycleState.SUBMITTING)
        order_ack = OrderAck(order.client_order_id, f"paper-{len(self.fills) + 1}", "FILLED")
        from datetime import datetime, timezone
        self.fills.append(Fill(
            order.client_order_id, order.instrument, order.quantity, price,
            datetime.now(timezone.utc).isoformat(), order.vehicle,
            f"paper-fill-{len(self.fills) + 1}",
        ))
        lifecycle.apply_fill(f"paper-order-{order.client_order_id}", order.quantity)
        self._acks[order.client_order_id] = order_ack
        return order_ack

    def poll_fill(self, order: OrderIntent, broker_order_id: str) -> Fill | None:
        return next(
            (fill for fill in reversed(self.fills) if fill.client_order_id == order.client_order_id),
            None,
        )

    def poll_fills(self, order: OrderIntent, broker_order_id: str) -> tuple[Fill, ...]:
        return tuple(fill for fill in self.fills if fill.client_order_id == order.client_order_id)

    def request_cancel(self, client_order_id: str) -> bool:
        lifecycle = self.lifecycle.get(client_order_id)
        return lifecycle.request_cancel() if lifecycle is not None else False

    def mark_unknown(self, client_order_id: str) -> bool:
        lifecycle = self.lifecycle.get(client_order_id)
        return lifecycle.mark_unknown() if lifecycle is not None else False

    def close(self) -> None:
        return None
