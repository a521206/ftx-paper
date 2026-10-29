from datetime import UTC, datetime
from math import inf, nan

from ftx_paper.contracts import Instrument, OrderIntent, OrderRole, OrderSide
from ftx_paper.config import NIFTY_LOT_SIZE
from ftx_paper.domain.capital import CapitalRuntimeContext, RESEARCH_CAPITAL_PROFILE
from ftx_paper.domain.decision_context import (
    CapitalSnapshot, DecisionContext, ExposureSnapshot, MarginSnapshot,
)
from ftx_paper.domain.portfolio import PortfolioState
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.execution.order_proposal import build_entry_order


def _decision_context(*, available: float = 1000.0) -> DecisionContext:
    return DecisionContext(
        as_of=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
        account_revision=1,
        capital=CapitalSnapshot(1000.0, available, 0.0, 0.0),
        margin=MarginSnapshot(0.0, available, 1000.0),
        exposure=ExposureSnapshot(0.0, 0),
    )


def test_execution_boundary_builds_entry_from_diagnostic_proposal() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    order = build_entry_order(
        {
            "eligible": True, "direction": "long", "instrument": instrument.symbol,
            "lifecycle_id": "lifecycle-1", "stop_price": 99.0,
            "execution_group_id": "group-1", "strategy_id": "orderflow",
        },
        instrument=instrument,
        context=_decision_context(),
    )

    assert order is not None
    assert order.quantity == 1
    assert order.execution_group_id == "group-1"
    assert order.stop_price == 99.0


def test_execution_boundary_does_not_build_without_current_margin() -> None:
    order = build_entry_order(
        {"eligible": True, "direction": "long", "lifecycle_id": "lifecycle-1", "stop_price": 99.0},
        instrument=Instrument("NIFTYFUT", "NFO", "FUTURES"),
        context=_decision_context(available=0.0),
    )

    assert order is None


def test_execution_boundary_rejects_non_finite_stop_price() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    for stop_price in (nan, inf, -inf):
        assert build_entry_order(
            {
                "eligible": True, "direction": "long", "lifecycle_id": "lifecycle-1",
                "stop_price": stop_price,
            },
            instrument=instrument,
            context=_decision_context(),
        ) is None
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


def test_partial_entry_fills_accumulate_and_duplicate_fill_ids_are_idempotent() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent("entry-partial-fill", instrument, OrderSide.BUY, 2, role=OrderRole.ENTRY)
    portfolio = PortfolioState(RESEARCH_CAPITAL_PROFILE.initial_capital)
    coordinator = PaperExecutionCoordinator(portfolio, CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE))
    coordinator.submit(entry)

    coordinator.fill(entry, price=100.0, filled_quantity=1, fill_id="fill-1")
    coordinator.fill(entry, price=102.0, filled_quantity=1, fill_id="fill-2")
    coordinator.fill(entry, price=102.0, filled_quantity=1, fill_id="fill-2")

    position = portfolio.positions[entry.client_order_id]
    assert position.quantity == 2
    assert position.entry_price == 101.0


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


def test_unfilled_execution_group_candidates_do_not_claim_until_fill() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    first = OrderIntent(
        "unfilled-first", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
        execution_group_id="group-unfilled",
    )
    second = OrderIntent(
        "unfilled-second", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
        execution_group_id="group-unfilled",
    )
    portfolio = PortfolioState(RESEARCH_CAPITAL_PROFILE.initial_capital)
    coordinator = PaperExecutionCoordinator(portfolio, CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE))

    assert coordinator.arbitrate((first, second)) == (first, second)
    coordinator.submit(first)
    coordinator.submit(second)
    coordinator.fill(first, price=100.0)

    assert coordinator.fill(second, price=100.0) is None
    assert coordinator.denied_proposals[second.client_order_id] == "execution_group_already_claimed"


def test_execution_group_fill_releases_reservations_from_another_coordinator() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    first = OrderIntent("cross-owner-first", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
                        execution_group_id="cross-owner-group")
    second = OrderIntent("cross-owner-second", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
                         execution_group_id="cross-owner-group")
    portfolio = PortfolioState(RESEARCH_CAPITAL_PROFILE.initial_capital)
    owner_a = PaperExecutionCoordinator(portfolio, CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE))
    owner_b = PaperExecutionCoordinator(portfolio, CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE))
    owner_a.submit(first)
    owner_b.submit(second)

    owner_a.fill(first, price=100.0)

    assert second.client_order_id not in portfolio.reservations
    assert second.client_order_id in portfolio.settled_orders


def test_filled_execution_group_remains_claimed_after_position_closes() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    first = OrderIntent(
        "filled-group-first", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
        execution_group_id="group-terminal",
    )
    exit_order = OrderIntent(
        "filled-group-exit", instrument, OrderSide.SELL, 1, role=OrderRole.EXIT,
        entry_order_id=first.client_order_id,
    )
    second = OrderIntent(
        "filled-group-second", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
        execution_group_id="group-terminal",
    )
    coordinator = PaperExecutionCoordinator(
        PortfolioState(RESEARCH_CAPITAL_PROFILE.initial_capital),
        CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE),
    )
    coordinator.submit(first)
    coordinator.fill(first, price=100.0)
    coordinator.submit(exit_order)
    coordinator.fill(exit_order, price=101.0)

    assert coordinator.arbitrate((second,)) == ()
    assert coordinator.denied_proposals[second.client_order_id] == "execution_group_already_claimed"
