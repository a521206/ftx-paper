# Keep this file focused on small store behavior; do not reintroduce broad
# lease/process-thread tests or artificial ReplayWorker thread stubs here.
from datetime import datetime
from zoneinfo import ZoneInfo

from ftx_paper.contracts import Instrument, MarketBar, OptionType
from ftx_paper.runtime import RuntimeStore


def test_runtime_store_round_trips_market_bars(tmp_path):
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


def test_runtime_store_clears_replay_runs_for_requested_date(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run("old-jan-5", {"session_date": "2026-01-05"})
    store.create_replay_run("old-jan-6", {"session_date": "2026-01-06"})

    assert store.clear_replay_runs_for_date("2026-01-05") == 1
    assert [run["run_id"] for run in store.read_replay_runs()] == ["old-jan-6"]


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
