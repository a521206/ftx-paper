from datetime import date, datetime, timedelta, timezone
import logging

import pytest
from requests import exceptions as requests_exceptions

from ftx_paper.broker.zerodha.adapter import (
    discover_option_surface_contracts,
    load_startup_backfill,
    resolve_instruments,
)
from ftx_paper.runtime import RuntimeStore


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


def test_resolve_instruments_rolls_stale_futures_to_nearest_live_contract(caplog):
    yesterday = date.today() - timedelta(days=1)
    next_expiry = date.today() + timedelta(days=27)
    client = InstrumentMaster([
        {"tradingsymbol": "NIFTY26SEPFUT", "name": "NIFTY", "instrument_type": "FUT", "expiry": yesterday, "instrument_token": 1},
        {"tradingsymbol": "NIFTY26OCTFUT", "name": "NIFTY", "instrument_type": "FUT", "expiry": next_expiry, "instrument_token": 2},
    ])

    with caplog.at_level(logging.WARNING, logger="ftx_paper.broker.zerodha.adapter"):
        result = resolve_instruments(client, [{"exchange": "NFO", "tradingsymbol": "NIFTY26SEPFUT", "role": "futures"}])

    assert result[0]["symbol"] == "NIFTY26OCTFUT"
    assert result[0]["instrument_token"] == 2
    assert "NIFTY26SEPFUT" in caplog.text
    assert "NIFTY26OCTFUT" in caplog.text


def test_resolve_instruments_rejects_futures_without_expiry():
    client = InstrumentMaster([
        {"tradingsymbol": "NIFTY26SEPFUT", "name": "NIFTY", "instrument_type": "FUT", "instrument_token": 1},
    ])

    with pytest.raises(ValueError, match="NIFTY26SEPFUT"):
        resolve_instruments(client, [{"exchange": "NFO", "tradingsymbol": "NIFTY26SEPFUT", "role": "futures"}])


def test_resolve_instruments_uses_persisted_underlying_contract(tmp_path):
    store = RuntimeStore(tmp_path)
    store.record_runtime_contract({
        "exchange": "NFO", "underlying": "NIFTY", "role": "futures",
        "tradingsymbol": "NIFTY26OCTFUT", "instrument_token": 2,
        "expiry": (date.today() + timedelta(days=27)).isoformat(),
    })
    client = InstrumentMaster([
        {"tradingsymbol": "NIFTY26OCTFUT", "name": "NIFTY", "instrument_type": "FUT",
         "expiry": date.today() + timedelta(days=27), "instrument_token": 2},
    ])

    result = resolve_instruments(
        client, [{"exchange": "NFO", "underlying": "NIFTY", "role": "futures"}],
        contract_store=store,
    )

    assert result[0]["symbol"] == "NIFTY26OCTFUT"


def test_load_startup_backfill_retries_transient_connection_reset(monkeypatch):
    attempts = 0

    class Client:
        def historical_data(self, *_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise requests_exceptions.ConnectionError("remote reset")
            return []

    monkeypatch.setattr(
        "ftx_paper.broker.zerodha.adapter.time.sleep", lambda _seconds: None,
    )
    monkeypatch.setattr(
        "ftx_paper.broker.zerodha.adapter._reserve_historical_request_slot",
        lambda: None,
    )

    result = load_startup_backfill(Client(), [{
        "instrument_token": 1,
        "exchange": "NSE",
        "symbol": "NIFTY",
        "tradingsymbol": "NIFTY",
        "instrument_type": "INDEX",
        "role": None,
    }])

    assert result == ()
    assert attempts == 3
