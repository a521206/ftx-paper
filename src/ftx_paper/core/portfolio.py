"""The durable, single-owner Paper portfolio state."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass, field
from typing import Mapping

from ftx_paper.config import NIFTY_LOT_SIZE
from ftx_paper.contracts import OrderIntent, OrderRole, OrderSide


@dataclass(frozen=True, slots=True)
class MarginReservation:
    order_id: str
    vehicle: str
    quantity: int
    amount: float


@dataclass(frozen=True, slots=True)
class PaperPosition:
    entry_order_id: str
    instrument: str
    side: OrderSide
    quantity: int
    entry_price: float
    vehicle: str = "futures"
    cell: str | None = None
    entry_timestamp: str | None = None
    synthetic_legs: tuple[str, str] | None = None
    synthetic_entry_prices: tuple[float, float] | None = None


@dataclass
class PortfolioState:
    """Authoritative capital, reservation, position and settlement aggregate."""
    initial_capital: float
    schema_version: int = 1
    equity: float = field(init=False)
    peak_equity: float = field(init=False)
    daily_baseline: float = field(init=False)
    realized_pnl: float = field(default=0.0, init=False)
    total_costs: float = field(default=0.0, init=False)
    reservations: dict[str, MarginReservation] = field(default_factory=dict, init=False)
    positions: dict[str, PaperPosition] = field(default_factory=dict, init=False)
    pending_orders: dict[str, str] = field(default_factory=dict, init=False)
    settled_orders: set[str] = field(default_factory=set, init=False)
    gate_snapshot: dict[str, object] = field(default_factory=dict, init=False)
    quote_provenance: dict[str, object] = field(default_factory=dict, init=False)
    state_revision: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        self.equity = self.peak_equity = self.daily_baseline = float(self.initial_capital)

    @property
    def open_margin(self) -> float:
        return sum(r.amount for r in self.reservations.values())

    @property
    def available_capital(self) -> float:
        """Equity available after all authoritative margin reservations."""
        return max(0.0, self.equity - self.open_margin)

    @property
    def drawdown(self) -> float:
        return max(0.0, self.peak_equity - self.equity)

    @property
    def total_pnl(self) -> float:
        return self.equity - self.initial_capital

    def capital_snapshot(self) -> dict[str, float]:
        return {"initial_capital": self.initial_capital, "current_equity": self.equity,
                "realized_pnl": self.realized_pnl, "total_pnl": self.total_pnl,
                "open_margin": self.open_margin, "drawdown": self.drawdown}

    def submit(self, order: OrderIntent) -> None:
        if order.client_order_id not in self.settled_orders:
            self.pending_orders.setdefault(order.client_order_id, "submitted")

    def reserve_entry(self, order: OrderIntent, *, margin_per_lot: float) -> MarginReservation:
        if order.role is not OrderRole.ENTRY:
            raise ValueError("only entry orders may reserve margin")
        existing = self.reservations.get(order.client_order_id)
        if existing is not None:
            return existing
        vehicle = str(order.vehicle).lower()
        if margin_per_lot < 0:
            raise ValueError("margin_per_lot must be non-negative")
        quantity = int(order.quantity)
        if quantity <= 0:
            raise ValueError("entry quantity must be positive")
        amount = quantity * float(margin_per_lot)
        reservation = MarginReservation(order.client_order_id, vehicle, int(order.quantity),
                                         amount)
        self.reservations[order.client_order_id] = reservation
        self.pending_orders[order.client_order_id] = "reserved"
        self.state_revision += 1
        return reservation

    def reserve_legacy(self, *, reservation_id: str, vehicle: str,
                       quantity: int, amount: float) -> MarginReservation:
        """Reserve pre-order margin for the legacy decision adapter."""
        if reservation_id in self.reservations:
            return self.reservations[reservation_id]
        quantity = int(quantity)
        amount = float(amount)
        if quantity <= 0 or amount < 0:
            raise ValueError("legacy entry has invalid quantity or amount")
        reservation = MarginReservation(reservation_id, str(vehicle).lower(), quantity, amount)
        self.reservations[reservation_id] = reservation
        self.pending_orders[reservation_id] = "reserved"
        self.state_revision += 1
        return reservation

    def fill_entry(self, order: OrderIntent, *, price: float, margin_per_lot: float,
                   timestamp: str | None = None,
                   synthetic_entry_prices: tuple[float, float] | None = None) -> PaperPosition:
        existing = self.positions.get(order.client_order_id)
        if existing is not None:
            return existing
        self.reserve_entry(order, margin_per_lot=margin_per_lot)
        position = PaperPosition(order.client_order_id, order.instrument.symbol, order.side, int(order.quantity),
                                 float(price), str(order.vehicle), order.cell, timestamp,
                                 tuple(leg.symbol for leg in order.synthetic_legs) if order.synthetic_legs else None,
                                 synthetic_entry_prices)
        self.positions[order.client_order_id] = position
        self.pending_orders[order.client_order_id] = "filled"
        self.state_revision += 1
        return position

    def cancel_entry(self, order_id: str) -> bool:
        if order_id in self.positions or order_id in self.settled_orders:
            return False
        changed = order_id in self.reservations or order_id in self.pending_orders
        self.reservations.pop(order_id, None)
        self.pending_orders[order_id] = "cancelled"
        self.settled_orders.add(order_id)
        if changed:
            self.state_revision += 1
        return changed

    def release_reservation(self, order_id: str) -> bool:
        """Release margin after a legacy/externally settled terminal fill."""
        released = self.reservations.pop(order_id, None) is not None
        if released:
            self.state_revision += 1
        return released

    def update_equity(self, equity: float, *, peak_equity: float | None = None) -> None:
        """Update externally marked equity through the portfolio owner."""
        self.equity = float(equity)
        self.peak_equity = max(
            float(peak_equity) if peak_equity is not None else self.peak_equity,
            self.equity,
        )
        self.state_revision += 1

    def settle_exit(self, order: OrderIntent, *, price: float, cost: float = 0.0) -> dict[str, float] | None:
        if order.client_order_id in self.settled_orders:
            return None
        entry_id = order.entry_order_id
        if entry_id is None or entry_id not in self.positions:
            raise ValueError("exit_without_matching_entry")
        position = self.positions.pop(entry_id)
        signed = 1.0 if position.side is OrderSide.BUY else -1.0
        gross = (float(price) - position.entry_price) * NIFTY_LOT_SIZE * position.quantity * signed
        net = gross - float(cost)
        self.realized_pnl += net
        self.total_costs += float(cost)
        self.equity += net
        self.peak_equity = max(self.peak_equity, self.equity)
        self.reservations.pop(entry_id, None)
        self.pending_orders[order.client_order_id] = "settled"
        self.settled_orders.add(order.client_order_id)
        self.state_revision += 1
        return {"entry_order_id": entry_id, "gross_pnl": gross, "costs": float(cost), "net_pnl": net}

    def reset(self) -> None:
        self.equity = self.peak_equity = self.daily_baseline = float(self.initial_capital)
        self.realized_pnl = self.total_costs = 0.0
        self.reservations.clear()
        self.positions.clear()
        self.pending_orders.clear()
        self.settled_orders.clear()
        self.gate_snapshot.clear()
        self.quote_provenance.clear()
        self.state_revision += 1

    def snapshot(self) -> dict[str, object]:
        return {"schema_version": 1, "capital": self.capital_snapshot(), "peak_equity": self.peak_equity,
                "daily_baseline": self.daily_baseline, "total_costs": self.total_costs,
                "reservations": {k: asdict(v) for k, v in self.reservations.items()},
                "positions": {k: {**asdict(v), "side": v.side.value} for k, v in self.positions.items()},
                "pending_orders": dict(self.pending_orders), "settled_orders": sorted(self.settled_orders),
                "gates": deepcopy(self.gate_snapshot), "quote_provenance": deepcopy(self.quote_provenance),
                "state_revision": self.state_revision}

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "PortfolioState":
        if snapshot.get("schema_version") != 1 or not isinstance(snapshot.get("capital"), Mapping):
            raise ValueError("unsupported portfolio snapshot schema")
        capital = snapshot["capital"]
        assert isinstance(capital, Mapping)
        portfolio = cls(float(capital["initial_capital"]))
        portfolio.equity = float(capital.get("current_equity", portfolio.initial_capital))
        portfolio.peak_equity = float(snapshot.get("peak_equity", portfolio.equity))
        portfolio.daily_baseline = float(snapshot.get("daily_baseline", portfolio.equity))
        portfolio.realized_pnl = float(capital.get("realized_pnl", 0.0))
        portfolio.total_costs = float(snapshot.get("total_costs", 0.0))
        raw = snapshot.get("reservations", {})
        if isinstance(raw, Mapping):
            portfolio.reservations = {
                str(k): MarginReservation(**dict(v))
                for k, v in raw.items() if isinstance(v, Mapping)
            }
        raw = snapshot.get("positions", {})
        if isinstance(raw, Mapping):
            portfolio.positions = {str(k): PaperPosition(**{**dict(v), "side": OrderSide(dict(v)["side"])}) for k, v in raw.items() if isinstance(v, Mapping)}
        raw = snapshot.get("pending_orders", {})
        portfolio.pending_orders = dict(raw) if isinstance(raw, Mapping) else {}
        portfolio.settled_orders = {str(v) for v in snapshot.get("settled_orders", [])}
        portfolio.state_revision = int(snapshot.get("state_revision", 0))
        raw = snapshot.get("gates", {})
        portfolio.gate_snapshot = dict(raw) if isinstance(raw, Mapping) else {}
        raw = snapshot.get("quote_provenance", {})
        portfolio.quote_provenance = dict(raw) if isinstance(raw, Mapping) else {}
        valid_in_flight_states = {"submitted", "reserved"}
        if not all(
            order_id in portfolio.positions
            or order_id.startswith("legacy:")
            or portfolio.pending_orders.get(order_id) in valid_in_flight_states
            for order_id in portfolio.reservations
        ):
            raise ValueError("portfolio snapshot contains an invalid reservation lifecycle")
        if not all(order_id in portfolio.reservations for order_id in portfolio.positions):
            raise ValueError("portfolio snapshot contains a position without a reservation")
        expected_margin = sum(item.amount for item in portfolio.reservations.values())
        if abs(expected_margin - portfolio.open_margin) > 1e-6:
            raise ValueError("portfolio snapshot margin is inconsistent")
        return portfolio

__all__ = ["MarginReservation", "PaperPosition", "PortfolioState"]
