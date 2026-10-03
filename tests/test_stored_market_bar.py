from datetime import date

from ftx_paper.infrastructure.sqlite.mappers import market_bar_from_row


def test_market_bar_row_maps_to_explicit_typed_record():
    record = market_bar_from_row({
        "symbol": "NIFTYFUT", "exchange": "NFO", "minute": "2026-01-01T09:15:00+05:30",
        "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10,
        "open_interest": 20, "instrument_type": "FUT", "expiry": "2026-01-29",
        "strike": None, "option_type": None, "source": "fixture",
        "ingested_at": "2026-01-01T04:00:00+00:00",
    })
    assert record.symbol == "NIFTYFUT"
    assert record.expiry == date(2026, 1, 29)
    assert record.minute.isoformat() == "2026-01-01T09:15:00+05:30"
