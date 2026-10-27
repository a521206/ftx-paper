from ftx_paper.strategy import (
    ExecutionAuthorization, ExecutionProposal, ExecutionStatus,
    OrderflowExecutionCoordinator,
)
from ftx_paper.contracts import Instrument, OrderIntent, OrderRole, OrderSide
from ftx_paper.config import NIFTY_LOT_SIZE
from ftx_paper.domain.capital import CapitalRuntimeContext, RESEARCH_CAPITAL_PROFILE
from ftx_paper.domain.portfolio import PortfolioState
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.strategy.orderflow_execution import (
    arbitrate_execution_proposals, entry_size_against_failure_boundary, next_available_bar,
)
from datetime import datetime, timezone


def _proposal() -> ExecutionProposal:
    return ExecutionProposal("intent-1", "NIFTYFUT", "long", 1, 100, 98, 104)


def test_unapproved_execution_is_non_mutating() -> None:
    coordinator = OrderflowExecutionCoordinator(ExecutionAuthorization(False, "diagnostic"))

    assert coordinator.submit(_proposal()) is None
    assert coordinator.states == ()


def test_explicitly_authorized_paper_execution_tracks_fill() -> None:
    coordinator = OrderflowExecutionCoordinator(ExecutionAuthorization(True, "approved"))
    proposed = coordinator.submit(_proposal())
    assert proposed is not None and proposed.status is ExecutionStatus.PROPOSED

    filled = coordinator.fill("intent-1", 100.5)
    assert filled.status is ExecutionStatus.FILLED
    assert filled.filled_price == 100.5


def test_orderflow_paper_execution_settles_and_duplicate_fill_is_idempotent() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent("orderflow-entry:1", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY)
    exit_order = OrderIntent(
        "orderflow-exit:1", instrument, OrderSide.SELL, 1,
        role=OrderRole.EXIT, entry_order_id=entry.client_order_id,
    )
    portfolio = PortfolioState(RESEARCH_CAPITAL_PROFILE.initial_capital)
    coordinator = PaperExecutionCoordinator(
        portfolio, CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE),
    )

    coordinator.submit(entry)
    first = coordinator.fill(
        entry, price=100.0, timestamp="2026-01-01T10:01:00+05:30", fill_id="fill-1",
    )
    duplicate = coordinator.fill(
        entry, price=100.0, timestamp="2026-01-01T10:01:00+05:30", fill_id="fill-1",
    )
    coordinator.submit(exit_order)
    settled = coordinator.fill(
        exit_order, price=103.0, timestamp="2026-01-01T10:02:00+05:30", cost=10.0,
    )

    assert first is not None and duplicate == first
    assert settled == {
        "entry_order_id": entry.client_order_id,
        "gross_pnl": 3.0 * NIFTY_LOT_SIZE,
        "costs": 10.0,
        "net_pnl": 3.0 * NIFTY_LOT_SIZE - 10.0,
    }
    assert portfolio.positions == {}
    assert portfolio.settled_orders == {exit_order.client_order_id}


def test_orderflow_execution_uses_next_available_bar_and_failure_boundary_size() -> None:
    available = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert next_available_bar(available).minute == 1
    quantity, metrics = entry_size_against_failure_boundary(
        direction="long", entry_price=100.0, failure_boundary=98.0,
    )
    assert quantity == 0.1
    assert metrics["actual_risk_bp"] == 200.0


def test_duplicate_execution_proposals_have_stable_identity_arbitration() -> None:
    first = _proposal()
    second = ExecutionProposal("intent-1", "NIFTYFUT", "long", 1, 100, 98, 104)
    assert arbitrate_execution_proposals((second, first)) == (first,)


def test_partial_exit_realizes_pnl_and_retains_position() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent("entry-partial", instrument, OrderSide.BUY, 2, role=OrderRole.ENTRY)
    exit_order = OrderIntent(
        "exit-partial", instrument, OrderSide.SELL, 1, role=OrderRole.EXIT,
        entry_order_id=entry.client_order_id,
    )
    portfolio = PortfolioState(RESEARCH_CAPITAL_PROFILE.initial_capital)
    coordinator = PaperExecutionCoordinator(portfolio, CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE))
    coordinator.submit(entry)
    coordinator.fill(entry, price=100.0, filled_quantity=2)
    result = coordinator.partial_exit(exit_order, price=103.0, quantity=1, cost=10.0)
    assert result is not None and result["net_pnl"] == 3.0 * NIFTY_LOT_SIZE - 10.0
    assert portfolio.positions[entry.client_order_id].quantity == 1
