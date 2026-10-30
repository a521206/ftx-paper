from __future__ import annotations

from dataclasses import replace

from ftx_paper.strategies.orderflow.contextual import build_context_scores, publish_market_contexts
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import MarketContextStore


def _aggregate(start: str, *, available_at: str, close: float) -> Aggregate:
    date = start[:10]
    return Aggregate(start, f"{date}T09:19", available_at, "5m", 5, 100, 105, 99, close, 50, "NIFTY", "", "FUT", date, start)


def test_contexts_publish_only_completed_causal_versions() -> None:
    store = MarketContextStore()
    first = _aggregate("2026-09-29T09:15", available_at="2026-09-29T09:20", close=101)
    late = replace(first, time="2026-09-29T09:20", end_time="2026-09-29T09:24", available_at="2026-09-29T09:25", close=99)

    refs = publish_market_contexts([late, first], stream_id="ctx", store=store)

    assert [ref.version for ref in refs] == ["1", "2"]
    assert store.latest_as_of("ctx", "2026-09-29T09:22").version == "1"  # type: ignore[union-attr]
    assert store.latest_as_of("ctx", "2026-09-29T09:25").version == "2"  # type: ignore[union-attr]


def test_contexts_do_not_republish_unchanged_state() -> None:
    store = MarketContextStore()
    first = _aggregate("2026-09-29T09:15", available_at="2026-09-29T09:20", close=101)
    second = replace(first, time="2026-09-29T09:20", end_time="2026-09-29T09:24", available_at="2026-09-29T09:25")

    assert len(publish_market_contexts([first, second], stream_id="ctx", store=store, atr=4.0)) == 2


def test_context_scores_use_causal_prior_and_hierarchical_children() -> None:
    prior = _aggregate("2026-09-29T09:15", available_at="2026-09-29T09:20", close=101)
    current = _aggregate("2026-09-29T09:20", available_at="2026-09-29T09:25", close=104)
    scores = build_context_scores(current, raw_rows=[], aggregates={"5m": [prior, current], "1m": []}, atr=4.0)

    assert scores is not None
    assert scores[0] > scores[1]
