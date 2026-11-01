"""Paper host boundary for canonical orderflow execution proposals.

The orderflow engine remains the lifecycle owner. This adapter only translates
an already-triggered canonical intent and selects whether the host executes it
or records a diagnostic fact without mutating account state.
"""

from __future__ import annotations

from dataclasses import dataclass
from ftx_paper.contracts import Instrument, OrderIntent, OrderRole, OrderSide
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution.service import ExecutionService

from .hierarchy import TradeIntent
from .timestamps import coerce_timestamp


@dataclass(frozen=True, slots=True)
class PaperOrderProposal:
    order: OrderIntent | None
    lifecycle_id: str
    intent_id: str
    quantity: float
    observed_at: str
    available_at: str
    diagnostic: bool


class OrderflowExecutionAdapter:
    """Translate canonical proposals into the existing Paper execution owner."""

    def __init__(self, service: ExecutionService | None = None, *, diagnostic: bool = True) -> None:
        self.service = service
        self.diagnostic = diagnostic
        self._notifications: list[ExecutionNotification] = []

    @property
    def notifications(self) -> tuple[ExecutionNotification, ...]:
        return tuple(self._notifications)

    def proposal(
        self,
        intent: TradeIntent,
        *,
        instrument: Instrument,
        quantity: float,
        observed_at: str,
        available_at: str,
        reason: str = "orderflow_entry",
    ) -> PaperOrderProposal:
        if intent.lifecycle_id is None:
            raise ValueError("orderflow intent lifecycle identity is required")
        if intent.direction not in {"long", "short"}:
            raise ValueError(f"unsupported direction: {intent.direction}")
        if intent.status.value != "triggered":
            raise ValueError("orderflow execution requires a triggered canonical intent")
        if quantity <= 0:
            raise ValueError("orderflow execution quantity must be positive")
        if coerce_timestamp(available_at, field="execution.available_at") < coerce_timestamp(intent.triggered_at or intent.created_at, field="intent.triggered_at"):
            raise ValueError("orderflow execution cannot precede the canonical trigger")
        if coerce_timestamp(available_at, field="execution.available_at") >= coerce_timestamp(intent.expiry, field="intent.expiry"):
            raise ValueError("orderflow execution intent has expired")
        if not quantity.is_integer():
            if not self.diagnostic:
                raise RuntimeError("Paper execution cannot authorize fractional canonical sizing")
            return PaperOrderProposal(None, intent.lifecycle_id, intent.intent_id, quantity, observed_at, available_at, True)
        order = OrderIntent(
            client_order_id=intent.intent_id,
            instrument=instrument,
            side=OrderSide.BUY if intent.direction == "long" else OrderSide.SELL,
            quantity=int(quantity),
            reason=reason,
            trigger_price=intent.trigger_price,
            requested_fill_price=intent.trigger_price,
            role=OrderRole.ENTRY,
            lifecycle_id=intent.lifecycle_id,
            decision_id=intent.intent_id,
            strategy_id="orderflow",
        )
        return PaperOrderProposal(order, intent.lifecycle_id, intent.intent_id, quantity, observed_at, available_at, self.diagnostic)

    def authorize(self, proposal: PaperOrderProposal, *, timestamp: str | None = None) -> None:
        """Authorize only explicit paper execution; diagnostics are read-only."""
        if self.diagnostic:
            return
        if self.service is None:
            raise RuntimeError("paper execution service is required outside diagnostic mode")
        if proposal.order is None:
            raise RuntimeError("fractional canonical sizing has no Paper order representation")
        self.service.authorize(
            proposal.order,
            payload={
                "strategy": "orderflow",
                "lifecycle_id": proposal.lifecycle_id,
                "intent_id": proposal.intent_id,
                "observed_at": proposal.observed_at,
                "available_at": proposal.available_at,
            },
            timestamp=timestamp or proposal.available_at,
        )

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self._notifications.append(event)


__all__ = ["PaperOrderProposal", "OrderflowExecutionAdapter"]
