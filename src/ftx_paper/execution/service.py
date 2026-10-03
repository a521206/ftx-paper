"""Shared order authorization and execution notification boundary."""

from __future__ import annotations

from typing import Any, Callable

from ftx_paper.contracts import OrderIntent
from ftx_paper.domain import AccountAggregate
from ftx_paper.execution.events import ExecutionNotification

from .coordinator import PaperExecutionCoordinator


class ExecutionService:
    """Common pre-submit lifecycle used by live and deterministic execution."""

    def __init__(self, account: AccountAggregate | None,
                 coordinator: PaperExecutionCoordinator | None,
                 store: Any, notify: Callable[[ExecutionNotification], None] | None = None) -> None:
        self.account = account
        self.coordinator = coordinator
        self.store = store
        self.notify = notify

    def authorize(self, order: OrderIntent, *, payload: dict[str, object], timestamp: str | None) -> None:
        """Re-check current account state and persist authorization before dispatch."""
        update_order = getattr(self.store, "update_order_lifecycle", None)
        reserved = False
        try:
            if self.account is not None:
                self.account.authorize_and_reserve(order)
                reserved = order.client_order_id in self.account.portfolio.reservations
                if callable(update_order):
                    update_order(order.client_order_id, state="AUTHORIZED", account_revision=self.account.revision)
                reservation = self.account.portfolio.reservations.get(order.client_order_id)
                self.store.append_event(
                    "ORDER_AUTHORIZED",
                    {**payload, "account_revision": self.account.revision,
                     "reservation_amount": reservation.amount if reservation is not None else 0.0},
                    f"order_authorized:{order.client_order_id}", timestamp=timestamp,
                )
            if self.coordinator is not None:
                self.coordinator.submit(order)
        except Exception:
            if reserved and self.account is not None:
                self.account.portfolio.cancel_entry(order.client_order_id)
            if callable(update_order):
                update_order(order.client_order_id, state="REJECTED")
            raise

    def publish(self, notification: ExecutionNotification) -> None:
        if self.notify is not None:
            self.notify(notification)


__all__ = ["ExecutionService"]
