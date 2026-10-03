"""The durable, single-owner Paper portfolio state."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass, field
from math import isfinite
from typing import Any, Mapping, TypedDict

from ftx_paper.config import NIFTY_LOT_SIZE
from ftx_paper.contracts import OrderIntent, OrderRole, OrderSide


@dataclass(frozen=True, slots=True)
class MarginReservation:
    order_id: str
    vehicle: str
    quantity: int
    amount: float
    risk_scope: str = ""
    risk_allowance: float = 0.0
    reserved_risk: float = 0.0

    def __post_init__(self) -> None:
        if not self.order_id or self.vehicle not in {"futures", "synthetic"}:
            raise ValueError("invalid margin reservation identity")
        if self.quantity <= 0 or not _finite_non_negative(self.amount):
            raise ValueError("invalid margin reservation amount")
        if not self.risk_scope and (self.risk_allowance != 0.0 or self.reserved_risk != 0.0):
            raise ValueError("risk reservation requires a scope")
        if self.risk_scope and (
            not _finite_non_negative(self.risk_allowance)
            or not _finite_non_negative(self.reserved_risk)
        ):
            raise ValueError("invalid reserved risk")


@dataclass(frozen=True, slots=True)
class ScopedRiskReservation:
    order_id: str
    scope: str
    allowance: float
    amount: float

    def __post_init__(self) -> None:
        if not self.order_id or not self.scope:
            raise ValueError("invalid scoped risk reservation identity")
        if not _finite_non_negative(self.allowance) or not _finite_non_negative(self.amount):
            raise ValueError("invalid scoped risk reservation amount")
        if self.amount > self.allowance:
            raise ValueError("scoped risk reservation exceeds allowance")


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


class SettlementResult(TypedDict):
    entry_order_id: str
    gross_pnl: float
    costs: float
    net_pnl: float


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
    scoped_risk_buffers: dict[str, float] = field(default_factory=dict, init=False)
    scoped_risk_reservations: dict[str, ScopedRiskReservation] = field(default_factory=dict, init=False)
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

    def available_scoped_risk(self, scope: str, allowance: float) -> float:
        """Return a session/cell/direction risk bucket after open reservations."""
        key, cap = str(scope), float(allowance)
        if not key or not isfinite(cap) or cap < 0:
            raise ValueError("risk scope and non-negative allowance are required")
        balance = self.scoped_risk_buffers.setdefault(key, cap)
        reserved = sum(
            item.amount for item in self.scoped_risk_reservations.values()
            if item.scope == key
        ) + sum(
            item.reserved_risk for item in self.reservations.values()
            if item.risk_scope == key
        )
        return max(0.0, balance - reserved)

    def reserve_scoped_risk(
        self, order_id: str, *, scope: str, allowance: float, amount: float,
    ) -> ScopedRiskReservation:
        """Hold risk room for an accepted entry until it is settled or cancelled."""
        order_key, risk_scope = str(order_id), str(scope)
        cap, risk = float(allowance), float(amount)
        if (
            not order_key or not risk_scope or not isfinite(cap)
            or not isfinite(risk) or cap < 0 or risk < 0
        ):
            raise ValueError("scoped risk reservation fields are invalid")
        existing = self.scoped_risk_reservations.get(order_key)
        if existing is not None:
            if existing != ScopedRiskReservation(order_key, risk_scope, cap, risk):
                raise ValueError("order already has a different scoped risk reservation")
            return existing
        if risk > self.available_scoped_risk(risk_scope, cap) + 1e-7:
            raise ValueError("entry exceeds available scoped risk")
        reservation = ScopedRiskReservation(order_key, risk_scope, cap, risk)
        self.scoped_risk_reservations[order_key] = reservation
        self.state_revision += 1
        return reservation

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
        risk = self.scoped_risk_reservations.pop(order.client_order_id, None)
        reservation = MarginReservation(
            order.client_order_id, vehicle, int(order.quantity), amount,
            risk_scope=risk.scope if risk is not None else "",
            risk_allowance=risk.allowance if risk is not None else 0.0,
            reserved_risk=risk.amount if risk is not None else 0.0,
        )
        self.reservations[order.client_order_id] = reservation
        self.pending_orders[order.client_order_id] = "reserved"
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
                                  (order.synthetic_legs[0].symbol, order.synthetic_legs[1].symbol)
                                  if order.synthetic_legs and len(order.synthetic_legs) == 2 else None,
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
        self.scoped_risk_reservations.pop(order_id, None)
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

    def start_day(self) -> None:
        """Capture the current authoritative equity as the daily baseline."""
        self.daily_baseline = self.equity
        self.state_revision += 1

    def settle_exit(self, order: OrderIntent, *, price: float, cost: float = 0.0) -> SettlementResult | None:
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
        reservation = self.reservations.pop(entry_id, None)
        if reservation is not None and reservation.risk_scope:
            balance = self.scoped_risk_buffers.setdefault(
                reservation.risk_scope, reservation.risk_allowance,
            )
            self.scoped_risk_buffers[reservation.risk_scope] = min(
                reservation.risk_allowance,
                max(0.0, balance + net),
            )
        self.pending_orders[order.client_order_id] = "settled"
        self.settled_orders.add(order.client_order_id)
        self.state_revision += 1
        return {"entry_order_id": entry_id, "gross_pnl": gross, "costs": float(cost), "net_pnl": net}

    def reset(self) -> None:
        self.equity = self.peak_equity = self.daily_baseline = float(self.initial_capital)
        self.realized_pnl = self.total_costs = 0.0
        self.reservations.clear()
        self.scoped_risk_buffers.clear()
        self.scoped_risk_reservations.clear()
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
                "scoped_risk_buffers": dict(self.scoped_risk_buffers),
                "scoped_risk_reservations": {
                    k: asdict(v) for k, v in self.scoped_risk_reservations.items()
                },
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
        portfolio = cls(_as_float(capital.get("initial_capital"), "initial_capital"))
        portfolio.equity = _as_float(capital.get("current_equity", portfolio.initial_capital), "current_equity")
        portfolio.peak_equity = _as_float(snapshot.get("peak_equity", portfolio.equity), "peak_equity")
        portfolio.daily_baseline = _as_float(snapshot.get("daily_baseline", portfolio.equity), "daily_baseline")
        portfolio.realized_pnl = _as_float(capital.get("realized_pnl", 0.0), "realized_pnl")
        portfolio.total_costs = _as_float(snapshot.get("total_costs", 0.0), "total_costs")
        raw = snapshot.get("reservations", {})
        if not isinstance(raw, Mapping):
            raise ValueError("portfolio snapshot reservations must be an object")
        for key, value in raw.items():
            if not isinstance(value, Mapping):
                raise ValueError("portfolio snapshot reservation is invalid")
            try:
                portfolio.reservations[str(key)] = MarginReservation(**dict(value))
            except (TypeError, ValueError) as exc:
                raise ValueError("portfolio snapshot reservation is invalid") from exc
        raw = snapshot.get("scoped_risk_buffers", {})
        if not isinstance(raw, Mapping):
            raise ValueError("portfolio snapshot scoped risk buffers must be an object")
        portfolio.scoped_risk_buffers = {
            str(scope): _as_float(balance, "scoped risk buffer") for scope, balance in raw.items()
        }
        raw = snapshot.get("scoped_risk_reservations", {})
        if not isinstance(raw, Mapping):
            raise ValueError("portfolio snapshot scoped risk reservations must be an object")
        for order_id, values in raw.items():
            if not isinstance(values, Mapping):
                raise ValueError("portfolio snapshot scoped risk reservation is invalid")
            try:
                portfolio.scoped_risk_reservations[str(order_id)] = ScopedRiskReservation(**dict(values))
            except (TypeError, ValueError) as exc:
                raise ValueError("portfolio snapshot scoped risk reservation is invalid") from exc
        raw = snapshot.get("positions", {})
        if not isinstance(raw, Mapping):
            raise ValueError("portfolio snapshot positions must be an object")
        for key, value in raw.items():
            if not isinstance(value, Mapping):
                raise ValueError("portfolio snapshot position is invalid")
            data = dict(value)
            legs = data.get("synthetic_legs")
            if (legs is not None and (isinstance(legs, (str, bytes, bytearray))
                    or not isinstance(legs, (list, tuple)) or len(legs) != 2
                    or any(not isinstance(item, str) or not item for item in legs))):
                raise ValueError("portfolio snapshot synthetic legs are invalid")
            if legs is not None:
                data["synthetic_legs"] = (legs[0], legs[1])
            prices = data.get("synthetic_entry_prices")
            if prices is not None:
                if (isinstance(prices, (str, bytes, bytearray)) or not isinstance(prices, (list, tuple))
                        or len(prices) != 2):
                    raise ValueError("portfolio snapshot synthetic entry prices are invalid")
                data["synthetic_entry_prices"] = (
                    _as_float(prices[0], "synthetic entry price"),
                    _as_float(prices[1], "synthetic entry price"),
                )
            try:
                data["side"] = OrderSide(data["side"])
                portfolio.positions[str(key)] = PaperPosition(**data)
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("portfolio snapshot position is invalid") from exc
        raw = snapshot.get("pending_orders", {})
        if not isinstance(raw, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in raw.items()
        ):
            raise ValueError("portfolio snapshot pending_orders are invalid")
        portfolio.pending_orders = dict(raw)
        settled = snapshot.get("settled_orders", [])
        if (isinstance(settled, (str, bytes, bytearray)) or not isinstance(settled, (list, tuple, set))
                or any(not isinstance(value, str) for value in settled)):
            raise ValueError("portfolio snapshot settled_orders are invalid")
        portfolio.settled_orders = set(settled)
        revision = snapshot.get("state_revision", 0)
        if not isinstance(revision, int) or isinstance(revision, bool):
            raise ValueError("portfolio snapshot state_revision is invalid")
        portfolio.state_revision = revision
        raw = snapshot.get("gates", {})
        if not isinstance(raw, Mapping):
            raise ValueError("portfolio snapshot gates must be an object")
        portfolio.gate_snapshot = dict(raw)
        raw = snapshot.get("quote_provenance", {})
        if not isinstance(raw, Mapping):
            raise ValueError("portfolio snapshot quote_provenance must be an object")
        portfolio.quote_provenance = dict(raw)
        valid_in_flight_states = {"submitted", "reserved"}
        if not all(
            order_id in portfolio.positions
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

__all__ = ["MarginReservation", "PaperPosition", "PortfolioState", "SettlementResult"]


def _as_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
        raise ValueError(f"portfolio snapshot {name} is invalid")
    return float(value)


def _finite_non_negative(value: float) -> bool:
    return isfinite(value) and value >= 0
