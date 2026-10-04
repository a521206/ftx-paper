"""Small capability ports for the modular-monolith scaffold.

These protocols describe boundaries only.  They do not add live execution or
broker-specific behavior.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, Protocol, runtime_checkable

from ftx_paper.broker.protocol import Fill
from ftx_paper.contracts import Instrument, MarketBar, OrderAck, OrderIntent


@runtime_checkable
class MarketDataPort(Protocol):
    def start(self) -> None: ...
    def stop(self) -> object: ...
    def subscribe(self, instruments: Iterable[Instrument]) -> None: ...
    def unsubscribe(self, instruments: Iterable[Instrument]) -> None: ...
    def request_quote(self, instrument: Instrument) -> MarketBar | None: ...
    def health(self) -> dict[str, object]: ...


@runtime_checkable
class ExecutionPort(Protocol):
    """Execution capability implemented by the deterministic PaperBroker."""

    def submit(self, order: OrderIntent) -> OrderAck: ...
    def poll_fill(self, order: OrderIntent, broker_order_id: str) -> Fill | None: ...
    def close(self) -> None: ...


@runtime_checkable
class OrderStatusPort(Protocol):
    def lifecycle(self, client_order_id: str) -> dict[str, Any] | None: ...
    def in_flight(self) -> list[dict[str, Any]]: ...


@runtime_checkable
class AccountDataPort(Protocol):
    def account_snapshot(self) -> dict[str, object]: ...


@runtime_checkable
class PnlQueryPort(Protocol):
    def pnl_snapshot(self) -> dict[str, float]: ...


MarketTickHandler = Callable[[MarketBar], None]

__all__ = [
    "AccountDataPort", "ExecutionPort", "MarketDataPort", "MarketTickHandler",
    "OrderStatusPort", "PnlQueryPort",
]
