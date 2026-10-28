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

    @property
    def denied_proposals(self) -> dict[str, str]:
        return dict(self._denied)

    def arbitrate(
        self, orders: tuple[OrderIntent, ...] | list[OrderIntent], *,
        execution_group: str | None = None, max_open_positions: int | None = None,
    ) -> tuple[OrderIntent, ...]:
        """Select one deterministic entry owner before any account mutation."""
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
        claimed = {
            str(getattr(item, "execution_group_id"))
            for item in self.portfolio.positions.values()
            if getattr(item, "execution_group_id", None) is not None
        }
        for order in ranked:
            group = execution_group or getattr(order, "execution_group_id", None)
            if order.role is not OrderRole.ENTRY:
                accepted.append(order)
            elif order.client_order_id in self.portfolio.positions:
                self._denied[order.client_order_id] = "duplicate_entry"
            elif group is not None and str(group) in claimed:
                self._denied[order.client_order_id] = "execution_group_already_claimed"
            elif max_open_positions is not None and len(self.portfolio.positions) + sum(
                item.role is OrderRole.ENTRY for item in accepted
            ) >= max_open_positions:
                self._denied[order.client_order_id] = "portfolio_position_limit"
            else:
                accepted.append(order)
                if group is not None:
                    claimed.add(str(group))
        return tuple(accepted)

    def submit(self, order: OrderIntent) -> None:
        self._require_futures(order)
        self.portfolio.submit(order)
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
            if fill_id is not None and fill_id in self._entry_fill_ids.setdefault(order.client_order_id, set()):
                return existing
            result = self.portfolio.fill_entry(
                order, price=price,
                margin_per_lot=self.capital_context.profile.vehicle_limit(order.vehicle).margin_per_lot,
                timestamp=timestamp, synthetic_entry_prices=synthetic_entry_prices,
                filled_quantity=filled_quantity,
            )
            if fill_id is not None:
                self._entry_fill_ids[order.client_order_id].add(fill_id)
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
