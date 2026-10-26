from datetime import date, timedelta

from ftx_paper.broker.xts.adapter import (
    classify_runtime_roles,
    discover_option_surface_contracts,
    load_startup_backfill,
    resolve_instruments,
)
from ftx_paper.broker.xts.exceptions import XTSAuthenticationError


class MockXTSClient:
    def __init__(self, rows):
        self.rows = rows

    def instruments(self, exchange):
        assert exchange in {"NFO", "NSE", "NSEFO", "NSECM"}
        return self.rows

    def search(self, search_text):
        return []

    def ohlc(self, segment, token, start, end, compression=60):
        return [
            {
                "time": "2026-10-14 09:15:00",
                "timestamp": 1791959100,
                "open": 25000.0,
                "high": 25050.0,
                "low": 24980.0,
                "close": 25020.0,
                "volume": 12000.0,
                "oi": 80000.0,
            }
        ]


def test_discover_option_surface_selects_nearest_non_expired_expiry_and_deduplicates():
    client = MockXTSClient([
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 1, "segment_id": 2},
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 1, "segment_id": 2},
        {"tradingsymbol": "NIFTY26SEP25000PE", "name": "NIFTY", "instrument_type": "PE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 2, "segment_id": 2},
        {"tradingsymbol": "NIFTY26OCT25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-10-01", "strike": 25000, "instrument_token": 3, "segment_id": 2},
        {"tradingsymbol": "BANKNIFTY26SEP25000CE", "name": "BANKNIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 4, "segment_id": 2},
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-03", "strike": 25000, "instrument_token": 5, "segment_id": 2},
    ])

    result = discover_option_surface_contracts(client, as_of=date(2026, 9, 11))

    assert [(row["symbol"], row["instrument_token"]) for row in result] == [
        ("NIFTY26SEP25000CE", 1),
        ("NIFTY26SEP25000PE", 2),
    ]


def test_resolve_instruments_rolls_stale_futures_to_nearest_live_contract():
    yesterday = date.today() - timedelta(days=1)
    next_expiry = date.today() + timedelta(days=27)
    client = MockXTSClient([
        {"tradingsymbol": "NIFTY26SEPFUT", "name": "NIFTY", "instrument_type": "FUT", "expiry": yesterday, "instrument_token": 1, "segment_id": 2},
        {"tradingsymbol": "NIFTY26OCTFUT", "name": "NIFTY", "instrument_type": "FUT", "expiry": next_expiry, "instrument_token": 2, "segment_id": 2},
    ])

    specs = [{"exchange": "NSEFO", "underlying": "NIFTY", "role": "futures"}]
    resolved = resolve_instruments(client, specs)

    assert len(resolved) == 1
    assert resolved[0]["symbol"] == "NIFTY26OCTFUT"
    assert resolved[0]["instrument_token"] == 2


def test_classify_runtime_roles():
    instruments = [
        {"instrument_token": 26000, "segment_id": 1, "exchange": "NSE", "symbol": "Nifty 50", "tradingsymbol": "Nifty 50", "role": "spot", "instrument_type": "INDEX"},
        {"instrument_token": 1001, "segment_id": 2, "exchange": "NFO", "symbol": "NIFTY26OCTFUT", "tradingsymbol": "NIFTY26OCTFUT", "role": "futures", "instrument_type": "FUT"},
        {"instrument_token": 26001, "segment_id": 1, "exchange": "NSE", "symbol": "India VIX", "tradingsymbol": "India VIX", "role": "vix", "instrument_type": "INDEX"},
    ]
    roles = classify_runtime_roles(instruments)
    assert roles.spot["instrument_token"] == 26000
    assert roles.futures["instrument_token"] == 1001
    assert roles.vix["instrument_token"] == 26001


def test_load_startup_backfill():
    client = MockXTSClient([])
    instruments = [
        {"instrument_token": 26000, "segment_id": 1, "exchange": "NSE", "symbol": "Nifty 50", "tradingsymbol": "Nifty 50", "instrument_type": "INDEX"},
    ]
    bars = load_startup_backfill(client, instruments)
    assert len(bars) == 1
    b = bars[0]
    assert b.instrument.symbol == "Nifty 50"
    assert b.open == 25000.0
    assert b.close == 25020.0


def test_load_startup_backfill_aborts_on_expired_session():
    class ExpiredClient(MockXTSClient):
        def ohlc(self, segment, token, start, end, compression=60):
            raise XTSAuthenticationError("XTS session token is invalid")

    instruments = [
        {"instrument_token": 26000, "segment_id": 1, "exchange": "NSE", "symbol": "Nifty 50", "instrument_type": "INDEX"},
        {"instrument_token": 26001, "segment_id": 1, "exchange": "NSE", "symbol": "India VIX", "instrument_type": "INDEX"},
    ]

    import pytest

    with pytest.raises(RuntimeError, match="authentication expired"):
        load_startup_backfill(ExpiredClient([]), instruments)
