from __future__ import annotations

from datetime import timezone

import pytest

from ftx_paper.strategies.orderflow import (
    LiveSourceAdapter,
    ReplaySourceAdapter,
    completed_observations,
    observe_market,
    source_metadata,
)


def _rows(count: int = 6) -> list[dict[str, object]]:
    return [{
        "time": f"09:{15 + index:02d}", "open": 100 + index, "high": 101 + index,
        "low": 99 + index, "close": 100.5 + index, "volume": 10,
        "symbol": "NIFTY", "expiry": "", "contract_type": "CONTINUOUS",
        "session_date": "2026-09-29",
    } for index in range(count)]


def test_source_metadata_rejects_no_identity_inference_and_reports_order() -> None:
    rows = _rows(2)
    metadata = source_metadata(rows)
    assert metadata["ordered_unique_minutes"] is True
    assert metadata["source_bar_count"] == 2
    assert metadata["instrument_type"] == "FUT_CONTINUOUS"


def test_observation_filters_close_and_reports_gaps() -> None:
    rows = _rows()
    rows.append({**rows[-1], "time": "09:22"})
    rows.append({**rows[-1], "time": "15:31"})
    observation = observe_market(rows)
    assert observation.source_gaps[0].previous_time == "09:20"
    assert all(item.available_at > item.end_time for item in observation.aggregates["1m"])


def test_paper_adapters_use_same_ordered_completed_inputs() -> None:
    observations = completed_observations(_rows(6))
    assert observations
    assert all(item.available_at >= item.observed_at for item in observations)
    assert all(item.observed_at.tzinfo is timezone.utc for item in observations)
    replay = ReplaySourceAdapter()
    replay.process(observations)
    live = LiveSourceAdapter()
    for item in observations:
        live.publish(item)
    assert replay.boundary.state.snapshot() == live.boundary.state.snapshot()


def test_five_minute_observation_keeps_all_one_minute_lineage() -> None:
    observations = completed_observations(_rows(6))
    five = next(item for item in observations if item.identity.startswith("aggregate:5m:"))

    assert five.lineage == tuple(f"aggregate:1m:2026-09-29T09:{minute:02d}" for minute in range(15, 20))


def test_duplicate_source_rows_are_rejected() -> None:
    rows = _rows(2)
    with pytest.raises(ValueError, match="strictly ordered"):
        observe_market([rows[0], rows[0]])
