from __future__ import annotations

from dataclasses import dataclass

from ftx_paper.broker import Fill
from ftx_paper.contracts import OrderSide


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    symbol: str
    quantity: int
    average_price: float


class PositionLedger:
    """Pure fill reconciliation; persistence is deliberately handled by runtime."""

    def __init__(self, starting_capital: float) -> None:
        self.cash = starting_capital
        self._quantities: dict[str, int] = {}
        self._costs: dict[str, float] = {}

    def apply_fill(self, fill: Fill, side: OrderSide) -> PositionSnapshot:
        signed_quantity = fill.quantity if side == OrderSide.BUY else -fill.quantity
        symbol = fill.instrument.symbol
        previous_quantity = self._quantities.get(symbol, 0)
        previous_cost = self._costs.get(symbol, 0.0)
        self.cash -= signed_quantity * fill.price
        quantity = previous_quantity + signed_quantity
        cost = previous_cost + signed_quantity * fill.price
        self._quantities[symbol] = quantity
        self._costs[symbol] = cost
        average = abs(cost / quantity) if quantity else 0.0
        return PositionSnapshot(symbol, quantity, average)

    def positions(self) -> tuple[PositionSnapshot, ...]:
        return tuple(
            PositionSnapshot(symbol, quantity, abs(self._costs[symbol] / quantity) if quantity else 0.0)
            for symbol, quantity in self._quantities.items()
            if quantity
        )
