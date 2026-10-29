"""One order lifecycle for live and replay."""
from __future__ import annotations

from ftx_paper.contracts import OrderIntent, OrderRole
from ftx_paper.domain.portfolio import PortfolioState, PaperPosition, SettlementResult
from ftx_paper.domain.capital import ResearchCapitalProfile, CapitalRuntimeContext


class PaperExecutionCoordinator:
    def __init__(self, portfolio: PortfolioState, capital_context: CapitalRuntimeContext | None = None) -> None:
        self.portfolio = portfolio
        self.capital_context = capital_context or CapitalRuntimeContext(ResearchCapitalProfile())
        self._entry_fill_ids: dict[str, set[str]] = {}
        self._denied: dict[str, str] = {}
        self._pending_position_limits: dict[str, int] = {}
        self._claimed_execution_groups: dict[str, str] = {}
        self._group_orders: dict[str, set[str]] = {}

    @property
    def denied_proposals(self) -> dict[str, str]:
        return dict(self._denied)

    def arbitrate(
        self, orders: tuple[OrderIntent, ...] | list[OrderIntent], *,
        execution_group: str | None = None, max_open_positions: int | None = None,
    ) -> tuple[OrderIntent, ...]:
        """Select one deterministic entry owner before any account mutation."""
        self._denied.clear()
        candidates = tuple(orders)
        ranked = sorted(
            candidates,
            key=lambda order: (
                str(getattr(order, "setup_availability", "")),
                int(getattr(order, "structural_rank", 0)),
                str(getattr(order, "lifecycle_id", order.client_order_id)),
                order.client_order_id,
            ),
        )
        accepted: list[OrderIntent] = []
        seen_ids: set[str] = set()
        claimed = {
            str(getattr(item, "execution_group_id"))
            for item in self.portfolio.positions.values()
            if getattr(item, "execution_group_id", None) is not None
        }
        claimed.update(self._claimed_execution_groups)
        for order in ranked:
            if order.client_order_id in seen_ids:
                self._denied[order.client_order_id] = "duplicate_proposal"
                continue
            seen_ids.add(order.client_order_id)
            group = execution_group or getattr(order, "execution_group_id", None)
            if order.role is not OrderRole.ENTRY:
                accepted.append(order)
            elif order.client_order_id in self.portfolio.positions:
                self._denied[order.client_order_id] = "duplicate_entry"
            elif group is not None and str(group) in claimed:
                self._denied[order.client_order_id] = "execution_group_already_claimed"
            else:
                accepted.append(order)
                if order.role is OrderRole.ENTRY and max_open_positions is not None:
                    self._pending_position_limits[order.client_order_id] = max_open_positions
        return tuple(accepted)

    def submit(self, order: OrderIntent) -> None:
        self._require_futures(order)
        self.portfolio.submit(order)
        if order.execution_group_id is not None:
            self._group_orders.setdefault(order.execution_group_id, set()).add(order.client_order_id)
        if order.role is OrderRole.ENTRY:
            self.portfolio.reserve_entry(
                order, margin_per_lot=self.capital_context.profile.vehicle_limit(order.vehicle).margin_per_lot,
            )

    def fill(self, order: OrderIntent, *, price: float, timestamp: str | None = None,
             synthetic_entry_prices: tuple[float, float] | None = None,
             cost: float = 0.0, filled_quantity: int | None = None,
             fill_id: str | None = None) -> PaperPosition | SettlementResult | None:
        self._require_futures(order)
        if order.role is OrderRole.ENTRY:
            existing = self.portfolio.positions.get(order.client_order_id)
            fill_ids = self._entry_fill_ids.setdefault(order.client_order_id, set())
            if fill_id is not None and fill_id in fill_ids:
                return existing
            group = order.execution_group_id
            if group is not None and (
                group in self._claimed_execution_groups
                or any(
                position.execution_group_id == group
                and position.entry_order_id != order.client_order_id
                for position in self.portfolio.positions.values()
                )
            ):
                self._denied[order.client_order_id] = "execution_group_already_claimed"
                return None
            position_limit = self._pending_position_limits.get(order.client_order_id)
            if position_limit is not None and len(self.portfolio.positions) >= position_limit:
                self._denied[order.client_order_id] = "portfolio_position_limit"
                return None
            result = self.portfolio.fill_entry(
                order, price=price,
                margin_per_lot=self.capital_context.profile.vehicle_limit(order.vehicle).margin_per_lot,
                timestamp=timestamp, synthetic_entry_prices=synthetic_entry_prices,
                filled_quantity=filled_quantity,
            )
            if fill_id is not None:
                fill_ids.add(fill_id)
            if result is not None and group is not None:
                self._claimed_execution_groups.setdefault(group, order.client_order_id)
                sibling_ids = set(self._group_orders.get(group, set()))
                sibling_ids.update(
                    reservation.order_id
                    for reservation in self.portfolio.reservations.values()
                    if reservation.execution_group_id == group
                )
                for sibling_id in sibling_ids:
                    if sibling_id != order.client_order_id:
                        self.portfolio.cancel_entry(sibling_id)
            return result
        return self.portfolio.settle_exit(order, price=price, cost=cost, fill_id=fill_id)

    def partial_exit(
        self, order: OrderIntent, *, price: float, quantity: int, cost: float = 0.0,
        fill_id: str | None = None,
    ) -> SettlementResult | None:
        self._require_futures(order)
        if order.role is not OrderRole.EXIT:
            raise ValueError("only exit orders may partially settle")
        return self.portfolio.settle_partial_exit(
            order, price=price, quantity=quantity, cost=cost, fill_id=fill_id,
        )

    def queue_exit(
        self, order: OrderIntent, *, available_at: str, reason: str,
    ) -> dict[str, object]:
        self._require_futures(order)
        return self.portfolio.queue_pending_exit(order, available_at=available_at, reason=reason)

    def cancel(self, order: OrderIntent) -> bool:
        return self.portfolio.cancel_entry(order.client_order_id)

    def failed_exit(self, order: OrderIntent) -> None:
        self._require_futures(order)
        self.portfolio.pending_orders[order.client_order_id] = "exit_failed"

    @staticmethod
    def _require_futures(order: OrderIntent) -> None:
        if order.vehicle != "futures":
            raise ValueError("synthetic orders are derived settlement only")


__all__ = ["PaperExecutionCoordinator"]
