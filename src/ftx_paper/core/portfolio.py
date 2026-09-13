"""The durable, single-owner Paper portfolio state."""
from __future__ import annotations

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
class PaperPortfolio:
    """Authoritative capital, reservation, position and settlement aggregate."""
    initial_capital: float
    margin_per_lot: Mapping[str, float] = field(default_factory=lambda: {"futures": 175000.0, "synthetic": 175000.0})
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

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        self.equity = self.peak_equity = self.daily_baseline = float(self.initial_capital)

    @property
    def open_margin(self) -> float:
        return sum(r.amount for r in self.reservations.values())

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

    def reserve_entry(self, order: OrderIntent) -> MarginReservation:
        if order.role is not OrderRole.ENTRY:
            raise ValueError("only entry orders may reserve margin")
        existing = self.reservations.get(order.client_order_id)
        if existing is not None:
            return existing
        vehicle = str(order.vehicle).lower()
        reservation = MarginReservation(order.client_order_id, vehicle, int(order.quantity),
                                         max(0, int(order.quantity)) * float(self.margin_per_lot.get(vehicle, 0)))
        self.reservations[order.client_order_id] = reservation
        self.pending_orders[order.client_order_id] = "reserved"
        return reservation

    def fill_entry(self, order: OrderIntent, *, price: float, timestamp: str | None = None,
                   synthetic_entry_prices: tuple[float, float] | None = None) -> PaperPosition:
        existing = self.positions.get(order.client_order_id)
        if existing is not None:
            return existing
        self.reserve_entry(order)
        position = PaperPosition(order.client_order_id, order.instrument.symbol, order.side, int(order.quantity),
                                 float(price), str(order.vehicle), order.cell, timestamp,
                                 tuple(leg.symbol for leg in order.synthetic_legs) if order.synthetic_legs else None,
                                 synthetic_entry_prices)
        self.positions[order.client_order_id] = position
        self.pending_orders[order.client_order_id] = "filled"
        return position

    def cancel_entry(self, order_id: str) -> bool:
        if order_id in self.positions or order_id in self.settled_orders:
            return False
        changed = order_id in self.reservations or order_id in self.pending_orders
        self.reservations.pop(order_id, None)
        self.pending_orders[order_id] = "cancelled"
        self.settled_orders.add(order_id)
        return changed

    def release_reservation(self, order_id: str) -> bool:
        """Release margin after a legacy/externally settled terminal fill."""
        return self.reservations.pop(order_id, None) is not None

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

    def snapshot(self) -> dict[str, object]:
        return {"schema_version": 1, "capital": self.capital_snapshot(), "peak_equity": self.peak_equity,
                "daily_baseline": self.daily_baseline, "total_costs": self.total_costs,
                "margin_per_lot": dict(self.margin_per_lot),
                "reservations": {k: asdict(v) for k, v in self.reservations.items()},
                "positions": {k: {**asdict(v), "side": v.side.value} for k, v in self.positions.items()},
                "pending_orders": dict(self.pending_orders), "settled_orders": sorted(self.settled_orders),
                "gates": self.gate_snapshot, "quote_provenance": self.quote_provenance}

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "PaperPortfolio":
        if snapshot.get("schema_version") != 1 or not isinstance(snapshot.get("capital"), Mapping):
            raise ValueError("unsupported portfolio snapshot schema")
        capital = snapshot["capital"]
        assert isinstance(capital, Mapping)
        raw_margin = snapshot.get("margin_per_lot", {})
        margin_per_lot = dict(raw_margin) if isinstance(raw_margin, Mapping) else None
        portfolio = cls(float(capital["initial_capital"]), margin_per_lot or {"futures": 175000.0, "synthetic": 175000.0})
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
        raw = snapshot.get("gates", {})
        portfolio.gate_snapshot = dict(raw) if isinstance(raw, Mapping) else {}
        raw = snapshot.get("quote_provenance", {})
        portfolio.quote_provenance = dict(raw) if isinstance(raw, Mapping) else {}
        return portfolio


__all__ = ["MarginReservation", "PaperPosition", "PaperPortfolio"]
