# Keep this file focused on small store behavior; do not reintroduce broad
# lease/process-thread tests or artificial ReplayWorker thread stubs here.
from datetime import datetime
import sqlite3
from threading import Event, Thread
from time import sleep
from zoneinfo import ZoneInfo

import pytest

from ftx_paper.contracts import Instrument, MarketBar, OptionType
from ftx_paper.runtime import RuntimeStore


def test_runtime_store_round_trips_market_bars(tmp_path):
    store = RuntimeStore(tmp_path)
    bar = MarketBar(
        Instrument("NIFTY26JAN18000CE", "NFO", "CE", "2026-01-29", 18000, OptionType.CALL),
        datetime(2026, 1, 5, 4, 0, tzinfo=ZoneInfo("UTC")),
        100, 102, 99, 101, 12, 345,
    )

    store.append_market_bars((bar,), source="historical_backfill")
    rows = store.read_market_bars("2026-01-05")

    assert rows == [{
        "symbol": "NIFTY26JAN18000CE", "exchange": "NFO", "minute": "2026-01-05T09:30:00+05:30",
        "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
        "volume": 12.0, "open_interest": 345.0, "instrument_type": "CE",
        "expiry": "2026-01-29", "strike": 18000.0, "option_type": "CE",
        "source": "historical_backfill", "ingested_at": rows[0]["ingested_at"],
    }]
    assert store.read_market_dates() == ["2026-01-05"]


def test_runtime_store_keeps_same_minute_contract_bars_separate(tmp_path):
    store = RuntimeStore(tmp_path)
    timestamp = datetime(2026, 9, 29, 4, 0, tzinfo=ZoneInfo("UTC"))
    store.append_market_bars((
        MarketBar(Instrument("NIFTYFUT", "NFO", "FUTURES", "2026-09-29"), timestamp, 1, 2, 0, 1),
        MarketBar(Instrument("NIFTYFUT", "NFO", "FUTURES", "2026-10-27"), timestamp, 3, 4, 2, 3),
    ), source="fixture")

    rows = store.read_market_bars("2026-09-29")

    assert {(row["symbol"], row["expiry"], row["close"]) for row in rows} == {
        ("NIFTYFUT", "2026-09-29", 1.0), ("NIFTYFUT", "2026-10-27", 3.0),
    }


def test_runtime_store_clears_replay_runs_for_requested_date(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run("old-jan-5", {"session_date": "2026-01-05"})
    store.create_replay_run("old-jan-6", {"session_date": "2026-01-06"})

    assert store.clear_replay_runs_for_date("2026-01-05") == 1
    assert [run["run_id"] for run in store.read_replay_runs()] == ["old-jan-6"]


def test_runtime_store_waits_for_replay_read_lock(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run("locked-run", {"session_date": "2026-01-05"})
    lock_acquired = Event()

    def release_lock() -> None:
        lock = sqlite3.connect(store.database)
        lock.execute("BEGIN EXCLUSIVE")
        lock_acquired.set()
        sleep(0.1)
        lock.rollback()
        lock.close()

    Thread(target=release_lock).start()
    assert lock_acquired.wait(1)
    run = store.read_replay_run("locked-run")
    assert run is not None
    assert run["run_id"] == "locked-run"


def test_runtime_store_replaces_expiry_calendar_without_touching_replay_runs(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run("existing-run", {"session_date": "2026-09-03"})

    assert store.replace_expiry_dates(
        ("2026-09-08", "2026-09-15"), source="canonical_weekly_expiry_dates",
    ) is True
    assert store.read_expiry_dates() == frozenset({"2026-09-08", "2026-09-15"})
    assert store.read_replay_run("existing-run") is not None
    assert store.replace_expiry_dates(
        ("2026-09-08", "2026-09-15"), source="canonical_weekly_expiry_dates",
    ) is False


def test_runtime_store_replay_summaries_omit_event_payloads(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run("summary-run", {"session_date": "2026-01-05"})
    store.update_replay_run(
        "summary-run",
        status="completed",
        result={"events": [{"event_type": "REJECTEDDECISION", "payload": {"x": "y"}}]},
    )

    summaries = store.read_replay_run_summaries()

    assert summaries[0]["run_id"] == "summary-run"
    assert summaries[0]["result"] is None


def test_runtime_store_replay_summaries_keep_latest_run_per_day(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run("old-run", {"session_date": "2026-01-05"})
    store.create_replay_run("new-run", {"session_date": "2026-01-05"})
    store.create_replay_run("other-day", {"session_date": "2026-01-06"})

    assert {run["run_id"] for run in store.read_replay_run_summaries()} == {"new-run", "other-day"}
    latest = store.read_latest_replay_for_date("2026-01-05")
    assert latest is not None
    assert latest["run_id"] == "new-run"


def test_runtime_store_preserves_warmup_event_payload(tmp_path):
    store = RuntimeStore(tmp_path)
    store.append_event("WARMUP", {
        "decision_at": "2026-09-10T09:17:00+05:30",
        "sequence": 2,
        "reason": "insufficient_history",
    })

    event = store.read_events()[0]
    assert event["category"] == "runtime"
    assert event["payload"] == {
        "decision_at": "2026-09-10T09:17:00+05:30",
        "sequence": 2,
        "reason": "insufficient_history",
    }


def test_runtime_store_persists_order_before_external_dispatch(tmp_path):
    store = RuntimeStore(tmp_path)
    assert store.create_order_lifecycle(
        "order-1", {"client_order_id": "order-1", "quantity": 2},
    ) is True
    assert store.create_order_lifecycle(
        "order-1", {"client_order_id": "order-1", "quantity": 2},
    ) is False

    assert store.update_order_lifecycle("order-1", state="SUBMITTING") is True
    order = store.read_order_lifecycle("order-1")
    assert order is not None
    assert order["state"] == "SUBMITTING"
    assert store.read_in_flight_orders()[0]["client_order_id"] == "order-1"


def test_runtime_store_rejects_illegal_order_transition(tmp_path):
    store = RuntimeStore(tmp_path)
    assert store.create_order_lifecycle("order-graph", {"client_order_id": "order-graph"})

    assert store.update_order_lifecycle("order-graph", state="AUTHORIZED") is True
    assert store.update_order_lifecycle("order-graph", state="FILLED") is False
    order = store.read_order_lifecycle("order-graph")
    assert order is not None
    assert order["state"] == "AUTHORIZED"


def test_runtime_store_keeps_unknown_and_cancel_requested_in_flight(tmp_path):
    store = RuntimeStore(tmp_path)
    assert store.create_order_lifecycle("unknown", {"client_order_id": "unknown"})
    assert store.update_order_lifecycle("unknown", state="SUBMITTING")
    assert store.update_order_lifecycle("unknown", state="UNKNOWN")
    assert store.create_order_lifecycle("cancel", {"client_order_id": "cancel"})
    assert store.update_order_lifecycle("cancel", state="AUTHORIZED")
    assert store.update_order_lifecycle("cancel", state="SUBMITTING")
    assert store.update_order_lifecycle("cancel", state="ACKNOWLEDGED")
    assert store.update_order_lifecycle("cancel", state="CANCEL_REQUESTED")

    assert {row["client_order_id"] for row in store.read_in_flight_orders()} == {"unknown", "cancel"}


def test_runtime_store_rejects_reused_order_id_with_different_payload(tmp_path):
    store = RuntimeStore(tmp_path)
    payload = {"client_order_id": "order-retry", "quantity": 2}
    assert store.create_order_lifecycle("order-retry", payload) is True
    assert store.create_order_lifecycle("order-retry", dict(payload)) is False

    with pytest.raises(ValueError, match="reused with a different command"):
        store.create_order_lifecycle(
            "order-retry", {"client_order_id": "order-retry", "quantity": 3},
        )


def test_runtime_store_persists_fill_quantities_idempotently(tmp_path):
    store = RuntimeStore(tmp_path)
    assert store.create_order_lifecycle(
        "order-fill", {"client_order_id": "order-fill", "quantity": 3}, state="ACKNOWLEDGED",
    )

    assert store.record_order_fill("order-fill", fill_id="fill-1", quantity=2) is True
    assert store.record_order_fill("order-fill", fill_id="fill-1", quantity=2) is False
    order = store.read_order_lifecycle("order-fill")
    assert order is not None
    assert order["state"] == "PARTIALLY_FILLED"
    assert order["payload"]["filled_quantity"] == 2
    assert order["payload"]["remaining_quantity"] == 1

    with pytest.raises(ValueError, match="exceeds remaining"):
        store.record_order_fill("order-fill", fill_id="fill-2", quantity=2)


def test_runtime_store_persists_contract_changes_once(tmp_path):
    store = RuntimeStore(tmp_path)
    contract = {
        "exchange": "NFO", "underlying": "NIFTY", "role": "futures",
        "tradingsymbol": "NIFTY26OCTFUT", "instrument_token": 42,
        "expiry": "2026-10-27",
    }

    assert store.record_runtime_contract(contract) is True
    assert store.record_runtime_contract(contract) is False
    runtime_contract = store.read_runtime_contract(
        exchange="NFO", underlying="NIFTY", role="futures",
    )
    assert runtime_contract is not None
    assert runtime_contract["tradingsymbol"] == "NIFTY26OCTFUT"
    history = store.read_runtime_contracts(
        exchange="NFO", underlying="NIFTY", role="futures",
    )
    assert [item["tradingsymbol"] for item in history] == ["NIFTY26OCTFUT"]
    assert [event["event_type"] for event in store.read_events()] == ["CONTRACT_CHANGED"]


def test_runtime_store_migrates_legacy_contract_key(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE runtime_contracts ("
            "exchange TEXT NOT NULL, underlying TEXT NOT NULL, role TEXT NOT NULL, "
            "tradingsymbol TEXT NOT NULL, instrument_token INTEGER NOT NULL, "
            "expiry TEXT NOT NULL, selected_at TEXT NOT NULL, "
            "PRIMARY KEY (exchange, underlying, role))",
        )
        connection.execute(
            "INSERT INTO runtime_contracts VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("NFO", "NIFTY", "futures", "NIFTY26OCTFUT", 2, "2026-10-27", "now"),
        )

    store = RuntimeStore(tmp_path)

    contracts = store.read_runtime_contracts(exchange="NFO", underlying="NIFTY", role="futures")
    assert [(item["tradingsymbol"], item["instrument_token"]) for item in contracts] == [
        ("NIFTY26OCTFUT", 2),
    ]
