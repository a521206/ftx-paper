import os
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

from ftx_paper.contracts import Instrument, MarketBar, OptionType
from ftx_paper.runtime import RuntimeStore


def test_runtime_store_replaces_dead_lease(tmp_path):
    store = RuntimeStore(tmp_path)
    with sqlite3.connect(store.database) as connection:
        connection.execute(
            "INSERT INTO process_leases(service, pid, started_at, instance_id) VALUES (?, ?, ?, ?)",
            ("api", 999999, "2026-01-01T00:00:00+00:00", "stale"),
        )

    instance_id = store.acquire_process_lease("api")
    with sqlite3.connect(store.database) as connection:
        row = connection.execute(
            "SELECT pid, instance_id FROM process_leases WHERE service = ?", ("api",)
        ).fetchone()
    assert row == (os.getpid(), instance_id)


def test_runtime_store_release_cannot_remove_newer_lease(tmp_path):
    store = RuntimeStore(tmp_path)
    old_instance = store.acquire_process_lease("api")
    with sqlite3.connect(store.database) as connection:
        connection.execute(
            "UPDATE process_leases SET instance_id = ? WHERE service = ?",
            ("newer", "api"),
        )

    store.release_process_lease("api", old_instance)
    with sqlite3.connect(store.database) as connection:
        row = connection.execute(
            "SELECT instance_id FROM process_leases WHERE service = ?", ("api",)
        ).fetchone()
    assert row == ("newer",)


def test_runtime_store_persists_exportable_market_bars(tmp_path):
    store = RuntimeStore(tmp_path)
    bar = MarketBar(
        Instrument("NIFTY24JAN18000CE", "NFO", "CE", "2024-01-25", 18000, OptionType.CALL),
        datetime(2026, 1, 5, 4, 0, tzinfo=ZoneInfo("UTC")),
        100, 102, 99, 101, 12, 345,
    )

    store.append_market_bars((bar,), source="historical_backfill")
    rows = store.read_market_bars("2026-01-05")

    assert rows == [{
        "symbol": "NIFTY24JAN18000CE", "exchange": "NFO", "minute": "2026-01-05T09:30:00+05:30",
        "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
        "volume": 12.0, "open_interest": 345.0, "instrument_type": "CE",
        "expiry": "2024-01-25", "strike": 18000.0, "option_type": "CE",
        "source": "historical_backfill", "ingested_at": rows[0]["ingested_at"],
    }]
