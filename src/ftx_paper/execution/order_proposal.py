"""Execution-boundary construction of futures entry intents."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite

from ftx_paper.contracts import Instrument, OrderIntent, OrderRole, OrderSide, exit_plan_from_payload
from ftx_paper.domain.decision_context import DecisionContext



def build_entry_order(
    proposal: Mapping[str, object],
    *,
    instrument: Instrument,
    context: DecisionContext,
) -> OrderIntent | None:
    """Turn a validated policy proposal into an executable Paper intent.

    Policy stages publish setup evidence and authorization predicates. Quantity
    and executable intent construction happen here, at the shared execution
    boundary, using the current account snapshot.
    """
    if proposal.get("eligible") is not True:
        return None
    direction = proposal.get("direction")
    lifecycle_id = proposal.get("lifecycle_id")
    stop_price = proposal.get("stop_price")
    trigger_price = proposal.get("trigger_price")
    if direction not in {"long", "short"}:
        return None
    if not isinstance(lifecycle_id, str) or not lifecycle_id:
        return None
    if (
        not isinstance(stop_price, (int, float))
        or isinstance(stop_price, bool)
        or not isfinite(float(stop_price))
    ):
        return None
    if trigger_price is not None and (
        not isinstance(trigger_price, (int, float))
        or isinstance(trigger_price, bool)
        or not isfinite(float(trigger_price))
    ):
        return None
    def string(name: str) -> str | None:
        value = proposal.get(name)
        return value if isinstance(value, str) else None

    def integer(name: str) -> int | None:
        value = proposal.get(name)
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    def number(name: str) -> float | None:
        value = proposal.get(name)
        return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None

    available = context.margin.available
    required_per_lot = context.margin.required_per_lot
    available_lots = (
        int(available // required_per_lot)
        if required_per_lot > 0
        else int(available > 0)
    )
    quantity = max(0, min(available_lots, 1))
    if quantity == 0:
        return None
    exit_plan = exit_plan_from_payload(proposal.get("exit_plan"))
    if any(rule.required and rule.level is None for rule in exit_plan):
        return None
    return OrderIntent(
        client_order_id=f"orderflow-entry:{lifecycle_id}",
        instrument=instrument,
        side=OrderSide.BUY if direction == "long" else OrderSide.SELL,
        quantity=quantity,
        limit_price=float(trigger_price) if trigger_price is not None else None,
        reason="causally_accepted_orderflow_opportunity",
        cell=string("cell"),
        stop_price=float(stop_price),
        stop_policy=string("stop_policy"),
        exit_mode=string("exit_mode"),
        exit_plan=exit_plan,
        role=OrderRole.ENTRY,
        entry_bar=integer("entry_bar"),
        trigger_price=float(trigger_price) if trigger_price is not None else None,
        requested_fill_price=number("requested_fill_price") or (float(trigger_price) if trigger_price is not None else None),
        strategy_id=string("strategy_id"),
        execution_group_id=string("execution_group_id"),
        lifecycle_id=lifecycle_id,
        setup_availability=string("setup_availability"),
        structural_rank=integer("structural_rank") or 0,
    )


__all__ = ["build_entry_order"]
