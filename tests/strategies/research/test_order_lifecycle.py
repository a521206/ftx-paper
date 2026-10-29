import pytest

from ftx_paper.broker import PaperBroker
from ftx_paper.contracts import (
    Instrument, OrderIntent, OrderLifecycle, OrderLifecycleState, OrderRole, OrderSide,
)
from ftx_paper.domain import PortfolioState
from ftx_paper.execution import PaperExecutionCoordinator


def test_partial_fill_preserves_remaining_quantity_and_cancel_intent():
    lifecycle = OrderLifecycle(5, state=OrderLifecycleState.ACKNOWLEDGED)

    assert lifecycle.apply_fill("fill-1", 2) is True
    assert lifecycle.filled_quantity == 2
    assert lifecycle.remaining_quantity == 3
    assert lifecycle.state is OrderLifecycleState.PARTIALLY_FILLED

    assert lifecycle.request_cancel() is True
    assert lifecycle.state is OrderLifecycleState.CANCEL_REQUESTED
    assert lifecycle.apply_fill("fill-2", 1) is True
    assert lifecycle.state is OrderLifecycleState.CANCEL_REQUESTED
    assert lifecycle.remaining_quantity == 2
    assert lifecycle.cancel_remaining() is True
    assert lifecycle.state is OrderLifecycleState.CANCELLED


def test_duplicate_stale_and_overfill_events_are_protected():
    lifecycle = OrderLifecycle(3, state=OrderLifecycleState.ACKNOWLEDGED)

    assert lifecycle.apply_fill("fill-1", 2) is True
    assert lifecycle.apply_fill("fill-1", 2) is False
    with pytest.raises(ValueError, match="exceeds remaining"):
        lifecycle.apply_fill("fill-2", 2)
    assert lifecycle.apply_fill("fill-2", 1) is True
    assert lifecycle.state is OrderLifecycleState.FILLED
    with pytest.raises(ValueError, match="terminal"):
        lifecycle.apply_fill("fill-3", 1)


def test_unknown_outcome_does_not_release_or_resubmit():
    lifecycle = OrderLifecycle(2, state=OrderLifecycleState.SUBMITTING)

    assert lifecycle.mark_unknown() is True
    assert lifecycle.state is OrderLifecycleState.UNKNOWN
    assert lifecycle.remaining_quantity == 2
    with pytest.raises(ValueError, match="illegal order transition"):
        lifecycle.transition(OrderLifecycleState.SUBMITTING)


def test_paper_broker_reuses_retry_result_and_tracks_lifecycle():
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    order = OrderIntent("paper-order", instrument, OrderSide.BUY, 2, role=OrderRole.ENTRY)
    broker = PaperBroker({"NIFTYFUT": 100.0})

    first = broker.submit(order)
    second = broker.submit(order)

    assert second == first
    assert len(broker.fills) == 1
    assert broker.lifecycle[order.client_order_id].state is OrderLifecycleState.FILLED


def test_paper_broker_allows_retry_after_missing_price():
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    order = OrderIntent("paper-retry", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY)
    broker = PaperBroker()

    with pytest.raises(RuntimeError, match="No paper price"):
        broker.submit(order)

    broker.update_price("NIFTYFUT", 100.0)
    assert broker.submit(order).status == "FILLED"


def test_partial_entry_fills_accumulate_only_observed_position_quantity():
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    order = OrderIntent("partial-entry", instrument, OrderSide.BUY, 5, role=OrderRole.ENTRY)
    portfolio = PortfolioState(2_500_000.0)
    coordinator = PaperExecutionCoordinator(portfolio)

    coordinator.submit(order)
    first = coordinator.fill(order, price=100.0, filled_quantity=2, fill_id="fill-1")
    assert first is not None
    assert portfolio.positions[order.client_order_id].quantity == 2
    assert portfolio.pending_orders[order.client_order_id] == "partially_filled"

    second = coordinator.fill(order, price=102.0, filled_quantity=3, fill_id="fill-2")
    assert second is not None
    assert portfolio.positions[order.client_order_id].entry_price == pytest.approx(101.2)
    assert portfolio.pending_orders[order.client_order_id] == "filled"


def test_pre_submit_cancellation_is_terminal():
    lifecycle = OrderLifecycle(2, state=OrderLifecycleState.AUTHORIZED)

    assert lifecycle.request_cancel() is True
    assert lifecycle.state is OrderLifecycleState.CANCELLED
    assert lifecycle.request_cancel() is False
