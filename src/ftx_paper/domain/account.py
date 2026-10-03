"""Common account aggregate and strategy-facing decision context."""

from __future__ import annotations

from datetime import datetime, timezone
from threading import RLock

from ftx_paper.contracts import OrderIntent, OrderRole, OrderSide

from .capital import CapitalRuntimeContext
from .decision_context import (
    CapitalSnapshot, DecisionContext, ExposureSnapshot, MarginSnapshot,
    PendingOrderView, PositionView,
)
from .portfolio import PortfolioState


class AccountAggregate:
    """Single mutable owner of capital, reservations, positions, and revisions."""

    def __init__(self, portfolio: PortfolioState, capital_context: CapitalRuntimeContext) -> None:
        self.portfolio = portfolio
        self.capital_context = capital_context
        self._lock = RLock()

    @property
    def revision(self) -> int:
        return self.portfolio.state_revision

    def context(self, *, as_of: datetime | None = None) -> DecisionContext:
        with self._lock:
            now = as_of or datetime.now(timezone.utc)
            positions = tuple(
                PositionView(
                    order_id=order_id, instrument=position.instrument,
                    side=position.side.value, quantity=position.quantity,
                    entry_price=position.entry_price, vehicle=position.vehicle,
                )
                for order_id, position in self.portfolio.positions.items()
            )
            pending = tuple(
                PendingOrderView(order_id=order_id, state=state,
                                quantity=(reservation.quantity if reservation is not None else 0),
                                vehicle=(reservation.vehicle if reservation is not None else "futures"))
                for order_id, state in self.portfolio.pending_orders.items()
                if state not in {"settled", "cancelled"}
                for reservation in (self.portfolio.reservations.get(order_id),)
            )
            profile = self.capital_context.profile
            daily_loss = max(0.0, self.portfolio.daily_baseline - self.portfolio.equity)
            net_lots = sum(
                position.quantity * (1 if position.side is OrderSide.BUY else -1)
                for position in self.portfolio.positions.values()
            )
            return DecisionContext(
                as_of=now,
                account_revision=self.revision,
                capital=CapitalSnapshot(
                    equity=self.portfolio.equity,
                    available_capital=self.portfolio.available_capital,
                    realized_pnl=self.portfolio.realized_pnl,
                    daily_loss=daily_loss,
                ),
                margin=MarginSnapshot(
                    used=self.portfolio.open_margin,
                    available=self.portfolio.available_capital,
                    required_per_lot=profile.vehicle_limit("futures").margin_per_lot,
                ),
                exposure=ExposureSnapshot(
                    net_directional_lots=net_lots,
                    open_positions=len(self.portfolio.positions),
                ),
                positions=positions,
                pending_orders=pending,
            )

    def authorize_and_reserve(self, order: OrderIntent) -> None:
        """Re-check current state and reserve before an external submission."""
        with self._lock:
            if order.role is not OrderRole.ENTRY:
                self.portfolio.submit(order)
                return
            existing = self.portfolio.reservations.get(order.client_order_id)
            if existing is not None:
                return
            profile = self.capital_context.profile
            if self.portfolio.equity < self.portfolio.daily_baseline * (1 - profile.max_daily_loss):
                raise ValueError("daily_loss_limit")
            signed_lots = sum(
                position.quantity * (1 if position.side is OrderSide.BUY else -1)
                for position in self.portfolio.positions.values()
            )
            signed_lots += order.quantity * (1 if order.side is OrderSide.BUY else -1)
            if abs(signed_lots) > profile.max_net_directional_lots:
                raise ValueError("max_net_directional_lots")
            margin_per_lot = profile.vehicle_limit(order.vehicle).margin_per_lot
            if order.quantity * margin_per_lot > self.portfolio.available_capital:
                raise ValueError("insufficient_available_capital")
            self.portfolio.submit(order)
            self.portfolio.reserve_entry(
                order,
                margin_per_lot=margin_per_lot,
            )


__all__ = ["AccountAggregate"]
