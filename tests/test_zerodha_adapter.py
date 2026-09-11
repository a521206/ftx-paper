from datetime import date, datetime, timezone

from ftx_paper.broker.zerodha.adapter import discover_option_surface_contracts


class InstrumentMaster:
    def __init__(self, rows):
        self.rows = rows

    def instruments(self, exchange):
        assert exchange == "NFO"
        return self.rows


def test_discover_option_surface_selects_nearest_non_expired_expiry_and_deduplicates():
    client = InstrumentMaster([
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 1},
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 1},
        {"tradingsymbol": "NIFTY26SEP25000PE", "name": "NIFTY", "instrument_type": "PE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 2},
        {"tradingsymbol": "NIFTY26OCT25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-10-01", "strike": 25000, "instrument_token": 3},
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "BANKNIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 4},
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-03", "strike": 25000, "instrument_token": 5},
    ])

    result = discover_option_surface_contracts(client, as_of=date(2026, 9, 11))

    assert [(row["symbol"], row["instrument_token"]) for row in result] == [
        ("NIFTY26SEP25000CE", 1),
        ("NIFTY26SEP25000PE", 2),
    ]


def test_discover_option_surface_normalizes_datetime_expiry():
    client = InstrumentMaster([
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": datetime(2026, 9, 24, tzinfo=timezone.utc), "strike": 25000, "instrument_token": 1},
    ])

    result = discover_option_surface_contracts(client, as_of=date(2026, 9, 11))

    assert len(result) == 1
