import os
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

from ftx_paper.contracts import Instrument, MarketBar, OptionType
from ftx_paper.runtime import RuntimeStore
from ftx_paper.runtime.replay_worker import ReplayWorker


def test_runtime_store_bootstraps_event_indexes(tmp_path):
    store = RuntimeStore(tmp_path)
    with sqlite3.connect(store.database) as connection:
        indexes = {
            row[1]
            for row in connection.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE type = 'index' AND tbl_name = 'runtime_events'"
            )
        }

    assert {
        "idx_runtime_events_id_desc",
        "idx_runtime_events_type_id",
        "ix_runtime_events_event_type",
        "ix_runtime_events_decision_id",
        "ux_runtime_events_idempotency",
    } <= indexes


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


def test_runtime_store_normalizes_nifty_index_bars(tmp_path):
    store = RuntimeStore(tmp_path)
    bar = MarketBar(
        Instrument("NIFTY 50", "NSE_INDEX", "UNKNOWN"),
        datetime(2026, 1, 5, 4, 0, tzinfo=ZoneInfo("UTC")),
        100, 101, 99, 100,
    )

    store.append_market_bars((bar,), source="historical_backfill")

    assert store.read_market_bars("2026-01-05")[0]["instrument_type"] == "INDEX"


def test_runtime_store_clears_only_replay_runs(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run("diagnostic-run", {"session_date": "2026-01-05"})

    assert store.clear_replay_runs() == 1
    assert store.read_replay_runs() == []
    assert store.read_market_bars("2026-01-05") == []


def test_runtime_store_clears_replay_runs_for_one_date_only(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run("old-jan-5", {"session_date": "2026-01-05"})
    store.create_replay_run("old-jan-6", {"session_date": "2026-01-06"})

    assert store.clear_replay_runs_for_date("2026-01-05") == 1
    assert [run["run_id"] for run in store.read_replay_runs()] == ["old-jan-6"]


def test_replay_submit_replaces_prior_run_for_same_date(tmp_path):
    store = RuntimeStore(tmp_path)
    from ftx_paper.capital_config import RESEARCH_CAPITAL_PROFILE
    worker = ReplayWorker(store, capital_profile=RESEARCH_CAPITAL_PROFILE)
    worker._ensure_started = lambda: None

    first = worker.submit({"session_date": "2026-09-10"})
    second = worker.submit({"session_date": "2026-09-10"})

    assert [run["run_id"] for run in store.read_replay_runs()] == [second]
    assert store.read_replay_run(first) is None


def test_runtime_store_preserves_warmup_in_ordered_runtime_stream(tmp_path):
    store = RuntimeStore(tmp_path)
    store.append_event("WARMUP", {
        "decision_at": "2026-09-10T09:17:00+05:30",
        "sequence": 2,
        "reason": "insufficient_history",
    })

    event = store.read_events()[0]
    assert event["category"] == "runtime"
    assert event["payload"]["decision_at"] == "2026-09-10T09:17:00+05:30"
