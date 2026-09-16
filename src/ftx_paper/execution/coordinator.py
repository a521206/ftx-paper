"""One order lifecycle for live and replay."""
from ftx_paper.contracts import OrderIntent, OrderRole
from ftx_paper.core.portfolio import PortfolioState
from ftx_paper.capital_config import ResearchCapitalProfile
from ftx_paper.capital_context import CapitalRuntimeContext


class PaperExecutionCoordinator:
    def __init__(self, portfolio: PortfolioState, capital_context: CapitalRuntimeContext | None = None) -> None:
        self.portfolio = portfolio
        self.capital_context = capital_context or CapitalRuntimeContext(ResearchCapitalProfile())

    def submit(self, order: OrderIntent) -> None:
        self._require_futures(order)
        self.portfolio.submit(order)
        if order.role is OrderRole.ENTRY:
            self.portfolio.reserve_entry(
                order, margin_per_lot=self.capital_context.profile.vehicle_limit(order.vehicle).margin_per_lot,
            )

    def fill(self, order: OrderIntent, *, price: float, timestamp: str | None = None,
             synthetic_entry_prices=None, cost: float = 0.0):
        self._require_futures(order)
        if order.role is OrderRole.ENTRY:
            return self.portfolio.fill_entry(
                order, price=price,
                margin_per_lot=self.capital_context.profile.vehicle_limit(order.vehicle).margin_per_lot,
                timestamp=timestamp, synthetic_entry_prices=synthetic_entry_prices,
            )
        return self.portfolio.settle_exit(order, price=price, cost=cost)

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
