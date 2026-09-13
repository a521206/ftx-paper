"""One order lifecycle for live and replay."""
from ftx_paper.contracts import OrderIntent, OrderRole
from ftx_paper.core.portfolio import PaperPortfolio


class PaperExecutionCoordinator:
    def __init__(self, portfolio: PaperPortfolio) -> None:
        self.portfolio = portfolio

    def submit(self, order: OrderIntent) -> None:
        self.portfolio.submit(order)
        if order.role is OrderRole.ENTRY:
            self.portfolio.reserve_entry(order)

    def fill(self, order: OrderIntent, *, price: float, timestamp: str | None = None,
             synthetic_entry_prices=None, cost: float = 0.0):
        if order.role is OrderRole.ENTRY:
            return self.portfolio.fill_entry(order, price=price, timestamp=timestamp, synthetic_entry_prices=synthetic_entry_prices)
        return self.portfolio.settle_exit(order, price=price, cost=cost)

    def cancel(self, order: OrderIntent) -> bool:
        return self.portfolio.cancel_entry(order.client_order_id)

    def failed_exit(self, order: OrderIntent) -> None:
        self.portfolio.pending_orders[order.client_order_id] = "exit_failed"


__all__ = ["PaperExecutionCoordinator"]
