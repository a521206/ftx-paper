from datetime import datetime, timezone

from ftx_paper.broker import Fill
from ftx_paper.contracts import Instrument, OrderSide
from ftx_paper.execution import PositionLedger


def test_ledger_reconciles_buy_and_sell_fills() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    ledger = PositionLedger(1000)

    ledger.apply_fill(Fill("1", instrument, 2, 100, datetime.now(timezone.utc).isoformat()), OrderSide.BUY)
    position = ledger.apply_fill(Fill("2", instrument, 1, 120, datetime.now(timezone.utc).isoformat()), OrderSide.SELL)

    assert position.quantity == 1
    assert ledger.cash == 920
    assert ledger.positions()[0].average_price == 80
