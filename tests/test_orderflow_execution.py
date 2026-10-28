from ftx_paper.contracts import Instrument, OrderIntent, OrderRole, OrderSide
from ftx_paper.config import NIFTY_LOT_SIZE
from ftx_paper.domain.capital import CapitalRuntimeContext, RESEARCH_CAPITAL_PROFILE
from ftx_paper.domain.portfolio import PortfolioState
from ftx_paper.execution import PaperExecutionCoordinator
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


def test_arbitration_counts_entries_only_and_persists_execution_groups() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    first = OrderIntent(
        "entry-first", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
        execution_group_id="group-1",
    )
    exit_order = OrderIntent(
        "exit-first", instrument, OrderSide.SELL, 1, role=OrderRole.EXIT,
        entry_order_id=first.client_order_id,
    )
    second = OrderIntent(
        "entry-second", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
        execution_group_id="group-1",
    )
    portfolio = PortfolioState(RESEARCH_CAPITAL_PROFILE.initial_capital)
    coordinator = PaperExecutionCoordinator(portfolio, CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE))
    coordinator.submit(first)
    coordinator.fill(first, price=100.0)

    assert coordinator.arbitrate((exit_order, second), max_open_positions=1) == (exit_order,)
    assert coordinator.denied_proposals[second.client_order_id] == "execution_group_already_claimed"
