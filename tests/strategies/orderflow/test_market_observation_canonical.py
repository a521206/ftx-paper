from __future__ import annotations

import pytest

from ftx_paper.strategies.orderflow.market_observation import ObservationGap, observe_market
from ftx_paper.strategies.orderflow.orderflow_common import build_aggregate_result


def _rows(count: int = 30) -> list[dict[str, object]]:
    return [{
        "time": f"09:{15 + index:02d}", "open": 100.0 + index * 0.1,
        "high": 101.0 + index * 0.1, "low": 99.0 + index * 0.1,
        "close": 100.5 + index * 0.1, "volume": 10,
        "symbol": "NIFTY", "expiry": "2026-10-29", "contract_type": "FUT",
        "session_date": "2026-09-29",
    } for index in range(count)]


def test_completed_aggregates_publish_at_bar_close() -> None:
    rows = _rows(31)
    five = build_aggregate_result(rows, 5).aggregates
    fifteen = build_aggregate_result(rows, 15).aggregates
    assert five[0].available_at == "2026-09-29T09:20"
    assert fifteen[0].available_at == "2026-09-29T09:30"


def test_market_observer_exposes_completed_timeframes_and_gaps() -> None:
    rows = _rows(6)
    rows.append({**rows[-1], "time": "09:22"})
    observation = observe_market(rows)
    assert set(observation.aggregates) == {"1m", "5m"}
    assert observation.source_gaps == (ObservationGap(6, "09:20", "09:22"),)


def test_market_observer_excludes_bars_after_market_close() -> None:
    rows = _rows(1)
    rows.append({**rows[0], "time": "15:31"})
    observation = observe_market(rows)
    assert all(aggregate.time.split("T")[-1] <= "15:30" for aggregates in observation.aggregates.values() for aggregate in aggregates)


def test_market_observer_rejects_unsorted_rows() -> None:
    rows = _rows(2)
    with pytest.raises(ValueError, match="strictly ordered"):
        observe_market([rows[1], rows[0]])
