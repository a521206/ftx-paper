from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable, Mapping

from ftx_paper.broker import Fill
from ftx_paper.contracts import OrderSide


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    vehicle: str
    symbol: str
    quantity: int
    average_price: float


class PositionLedger:
    """Pure fill reconciliation; persistence is deliberately handled by runtime."""

    def __init__(self, starting_capital: float) -> None:
        self.cash = starting_capital
        self._quantities: dict[str, int] = {}
        self._costs: dict[str, float] = {}

    def restore_state(self, *, cash: float, positions: Iterable[Mapping[str, object]]) -> None:
        """Restore persisted cash and position snapshots before live trading."""
        restored_quantities: dict[tuple[str, str], int] = {}
        restored_costs: dict[tuple[str, str], float] = {}
        for raw in positions:
            vehicle = str(raw.get("vehicle", "futures")).lower()
            if vehicle not in {"futures", "synthetic"}:
                raise ValueError("persisted position has an invalid vehicle")
            symbol = str(raw.get("symbol", "")).strip()
            quantity = int(raw.get("quantity", 0))
            average_price = float(raw.get("average_price", 0.0))
            if not symbol:
                raise ValueError("persisted position is missing a symbol")
            if quantity == 0:
                continue
            if average_price < 0:
                raise ValueError("persisted position has a negative average price")
            key = (vehicle, symbol)
            restored_quantities[key] = quantity
            restored_costs[key] = quantity * average_price
        self.cash = float(cash)
        self._quantities = restored_quantities
        self._costs = restored_costs

    def apply_fill(self, fill: Fill, side: OrderSide, *, vehicle: str | None = None) -> PositionSnapshot:
        vehicle = str(fill.vehicle if vehicle is None else vehicle).lower()
        if vehicle not in {"futures", "synthetic"}:
            raise ValueError("position vehicle must be 'futures' or 'synthetic'")
        signed_quantity = fill.quantity if side == OrderSide.BUY else -fill.quantity
        symbol = fill.instrument.symbol
        key = (vehicle, symbol)
        previous_quantity = self._quantities.get(key, 0)
        previous_cost = self._costs.get(key, 0.0)
        self.cash -= signed_quantity * fill.price
        quantity = previous_quantity + signed_quantity
        cost = previous_cost + signed_quantity * fill.price
        self._quantities[key] = quantity
        self._costs[key] = cost
        average = abs(cost / quantity) if quantity else 0.0
        return PositionSnapshot(vehicle, symbol, quantity, average)

    def positions(self) -> tuple[PositionSnapshot, ...]:
        return tuple(
            PositionSnapshot(vehicle, symbol, quantity, abs(self._costs[(vehicle, symbol)] / quantity) if quantity else 0.0)
            for (vehicle, symbol), quantity in self._quantities.items()
            if quantity
        )
