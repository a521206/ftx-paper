from __future__ import annotations

from ftx_paper.strategies.orderflow.contextual import build_context_scores
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate


def _aggregate(start: str, *, available_at: str, close: float) -> Aggregate:
    date = start[:10]
    return Aggregate(start, f"{date}T09:19", available_at, "5m", 5, 100, 105, 99, close, 50, "NIFTY", "", "FUT", date, start)


def test_context_scores_use_causal_prior_and_hierarchical_children() -> None:
    prior = _aggregate("2026-09-29T09:15", available_at="2026-09-29T09:20", close=101)
    current = _aggregate("2026-09-29T09:20", available_at="2026-09-29T09:25", close=104)
    scores = build_context_scores(current, raw_rows=[], aggregates={"5m": [prior, current], "1m": []}, atr=4.0)

    assert scores is not None
    assert scores[0] > scores[1]


def test_context_scores_align_utc_replay_rows_at_the_boundary() -> None:
    current = Aggregate(
        "2026-10-01T03:45:00+00:00",
        "2026-10-01T03:49:00+00:00",
        "2026-10-01T03:50:00+00:00",
        "5m", 5, 100, 110, 99, 108, 50, "NIFTY", "", "FUT", "2026-10-01", "opening",
    )
    rows = [
        {
            "time": f"2026-10-01T03:{minute:02d}:00+00:00",
            "session_date": "2026-10-01",
            "open": 100, "high": 102, "low": 99, "close": 101, "volume": 10,
        }
        for minute in range(45, 50)
    ]

    scores = build_context_scores(
        current, raw_rows=rows, aggregates={"5m": [current], "1m": []}, atr=4.0,
    )

    assert scores is not None
    assert scores[0] > 0
