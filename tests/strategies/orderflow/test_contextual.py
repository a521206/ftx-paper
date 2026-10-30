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
