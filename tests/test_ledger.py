"""Compatibility coverage for the quarantined pre-PortfolioState ledger."""

from datetime import datetime, timezone

from ftx_paper.broker import Fill
from ftx_paper.contracts import Instrument, OrderSide
from ftx_paper.execution.ledger import PositionLedger


def test_ledger_reconciles_buy_and_sell_fills() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    ledger = PositionLedger(1000)

    ledger.apply_fill(Fill("1", instrument, 2, 100, datetime.now(timezone.utc).isoformat()), OrderSide.BUY)
    position = ledger.apply_fill(Fill("2", instrument, 1, 120, datetime.now(timezone.utc).isoformat()), OrderSide.SELL)

    assert position.quantity == 1
    assert ledger.cash == 920
    assert ledger.positions()[0].average_price == 80


def test_ledger_restores_persisted_position_before_next_fill() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    ledger = PositionLedger(1000)

    ledger.restore_state(cash=800, positions=[{"symbol": "NIFTY", "quantity": 2, "average_price": 100}])
    position = ledger.apply_fill(
        Fill("2", instrument, 1, 120, datetime.now(timezone.utc).isoformat()), OrderSide.SELL
    )

    assert position.quantity == 1
    assert ledger.cash == 920


def test_ledger_keeps_vehicle_books_separate_for_same_logical_symbol() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    ledger = PositionLedger(1000)
    timestamp = datetime.now(timezone.utc).isoformat()

    ledger.apply_fill(Fill("fut", instrument, 2, 100, timestamp, "futures"), OrderSide.BUY)
    ledger.apply_fill(Fill("syn", instrument, 1, 110, timestamp, "synthetic"), OrderSide.BUY)

    positions = {(item.vehicle, item.symbol): item.quantity for item in ledger.positions()}
    assert positions == {("futures", "NIFTYFUT"): 2, ("synthetic", "NIFTYFUT"): 1}
