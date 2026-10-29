from datetime import UTC, date, datetime, timedelta
import logging

import pytest

from ftx_paper.broker.shoonya.adapter import (
    ShoonyaInstrument,
    classify_runtime_roles,
    discover_option_surface_contracts,
    instrument_expiry_iso,
    load_startup_backfill,
    resolve_instruments,
)
from ftx_paper.contracts import MarketRole


class MockShoonyaClient:
    def __init__(self, rows):
        self.rows = rows

    def instruments(self, exchange):
        assert exchange in {"NFO", "NSE"}
        return self.rows


def test_discover_option_surface_selects_nearest_non_expired_expiry_and_deduplicates():
    client = MockShoonyaClient([
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 1},
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 1},
        {"tradingsymbol": "NIFTY26SEP25000PE", "name": "NIFTY", "instrument_type": "PE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 2},
        {"tradingsymbol": "NIFTY26OCT25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-10-01", "strike": 25000, "instrument_token": 3},
        {"tradingsymbol": "BANKNIFTY26SEP25000CE", "name": "BANKNIFTY", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "instrument_token": 4},
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": "2026-09-03", "strike": 25000, "instrument_token": 5},
    ])

    result = discover_option_surface_contracts(client, as_of=date(2026, 9, 11))

    assert [(row["symbol"], row["instrument_token"]) for row in result] == [
        ("NIFTY26SEP25000CE", 1),
        ("NIFTY26SEP25000PE", 2),
    ]


def test_discover_option_surface_normalizes_datetime_expiry():
    client = MockShoonyaClient([
        {"tradingsymbol": "NIFTY26SEP25000CE", "name": "NIFTY", "instrument_type": "CE", "expiry": datetime(2026, 9, 24, tzinfo=UTC), "strike": 25000, "instrument_token": 1},
    ])

    result = discover_option_surface_contracts(client, as_of=date(2026, 9, 11))

    assert len(result) == 1
    assert result[0].get("expiry") == date(2026, 9, 24)


def test_resolve_instruments_rolls_stale_futures_to_nearest_live_contract(caplog):
    yesterday = date.today() - timedelta(days=1)
    next_expiry = date.today() + timedelta(days=27)
    client = MockShoonyaClient([
        {"tradingsymbol": "NIFTY26SEPFUT", "name": "NIFTY", "instrument_type": "FUT", "expiry": yesterday, "instrument_token": 1},
        {"tradingsymbol": "NIFTY26OCTFUT", "name": "NIFTY", "instrument_type": "FUT", "expiry": next_expiry, "instrument_token": 2},
    ])

    with caplog.at_level(logging.WARNING, logger="ftx_paper.broker.shoonya.adapter"):
        result = resolve_instruments(client, [{"exchange": "NFO", "tradingsymbol": "NIFTY26SEPFUT", "role": "futures"}])

    assert result[0]["symbol"] == "NIFTY26OCTFUT"
    assert result[0]["instrument_token"] == 2
    assert "NIFTY26SEPFUT" in caplog.text
    assert "NIFTY26OCTFUT" in caplog.text


def test_resolve_instruments_rejects_missing():
    client = MockShoonyaClient([])
    with pytest.raises(ValueError, match="Shoonya instrument not found"):
        resolve_instruments(client, [{"exchange": "NSE", "tradingsymbol": "NONEXISTENT"}])


def test_classify_runtime_roles_validates_required_roles():
    instruments: list[ShoonyaInstrument] = [
        ShoonyaInstrument(symbol="NIFTY26OCTFUT", exchange="NFO", role=MarketRole.FUTURES, instrument_token=1, tradingsymbol="NIFTY26OCTFUT", instrument_type="FUT"),
        ShoonyaInstrument(symbol="INDIA VIX", exchange="NSE", role=MarketRole.VIX, instrument_token=2, tradingsymbol="INDIA VIX", instrument_type="INDEX"),
        ShoonyaInstrument(symbol="NIFTY26OCT25000CE", exchange="NFO", role=None, instrument_token=3, tradingsymbol="NIFTY26OCT25000CE", instrument_type="CE"),
    ]
    roles = classify_runtime_roles(instruments)
    assert roles.futures["symbol"] == "NIFTY26OCTFUT"
    assert roles.vix["symbol"] == "INDIA VIX"
    assert roles.options is not None
    contracts = roles.options.get("contracts")
    assert isinstance(contracts, list)
    assert len(contracts) == 1


def test_classify_runtime_roles_raises_on_missing_futures():
    instruments: list[ShoonyaInstrument] = [
        ShoonyaInstrument(symbol="INDIA VIX", exchange="NSE", role=MarketRole.VIX, instrument_token=2, tradingsymbol="INDIA VIX", instrument_type="INDEX"),
    ]
    with pytest.raises(ValueError, match="Missing required FTX Shoonya instrument roles: futures"):
        classify_runtime_roles(instruments)


def test_load_startup_backfill_normalizes_shoonya_rows():
    class BackfillClient:
        def historical_data(self, exchange, token, start, end, interval="1"):
            return [
                {
                    "ssboe": "1728900000",
                    "into": "25000.0",
                    "inth": "25050.0",
                    "intl": "24990.0",
                    "intc": "25020.0",
                    "intv": "1000",
                    "intoi": "5000",
                }
            ]

    inst: ShoonyaInstrument = ShoonyaInstrument(
        symbol="NIFTY26OCTFUT",
        exchange="NFO",
        instrument_type="FUT",
        instrument_token=1234,
        tradingsymbol="NIFTY26OCTFUT",
        role=MarketRole.FUTURES,
        expiry="2026-10-29",
    )
    bars = load_startup_backfill(BackfillClient(), [inst])
    assert len(bars) == 1
    bar = bars[0]
    assert bar.open == 25000.0
    assert bar.high == 25050.0
    assert bar.low == 24990.0
    assert bar.close == 25020.0
    assert bar.volume == 1000.0
    assert bar.open_interest == 5000.0
    assert bar.instrument.symbol == "NIFTY26OCTFUT"


def test_instrument_expiry_iso_formats():
    assert instrument_expiry_iso(date(2026, 10, 29)) == "2026-10-29"
    assert instrument_expiry_iso("29-OCT-2026") == "2026-10-29"
    assert instrument_expiry_iso(None) is None
