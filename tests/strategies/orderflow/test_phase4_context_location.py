from __future__ import annotations

from dataclasses import replace

import pytest

from ftx_paper.strategies.orderflow.hierarchy import MarketContext
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.location_observation import LocationObservation
from ftx_paper.strategies.orderflow.opening_gap_policy import OpeningGapPolicy
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import (
    LocationRegistry,
    MarketContextStore,
)
from ftx_paper.strategies.orderflow.vwap_location import causal_vwap_prefix


def _aggregate(time: str, *, available_at: str | None = None) -> Aggregate:
    return Aggregate(time, time, available_at or time, "1m", 1, 100, 102, 98, 100, 10, "NIFTY", "", "FUT", time[:10], "source")


def test_context_versions_and_location_generations_are_as_of_immutable() -> None:
    store = MarketContextStore()
    first = MarketContext("ctx", "1", "2026-09-29T09:20", "2026-09-29T09:21", "OR_LOW", "BALANCE")
    second = replace(first, version="2", timestamp="2026-09-29T09:25", available_at="2026-09-29T09:26")
    store.publish(first)
    store.publish(second)
    assert store.latest_as_of("ctx", "2026-09-29T09:25").version == "1"  # type: ignore[union-attr]
    registry = LocationRegistry("stream")
    registry.register_opening_range(99, 101, "2026-09-29T09:20", atr=4)
    registry.register_opening_range(100, 102, "2026-09-29T09:30", atr=5)
    assert registry.resolve_as_of("OR_LOW", "2026-09-29T09:29").generation == 1  # type: ignore[union-attr]
    assert registry.resolve_as_of("OR_LOW", "2026-09-29T09:30").generation == 2  # type: ignore[union-attr]


def test_session_extreme_replacements_use_cumulative_extreme() -> None:
    registry = LocationRegistry("stream")
    first = replace(_aggregate("2026-09-29T09:20", available_at="2026-09-29T09:25"), timeframe="5m", source_bar_count=5, end_time="2026-09-29T09:24")
    second = replace(first, time="2026-09-29T09:25", end_time="2026-09-29T09:29", available_at="2026-09-29T09:30", high=101, low=99)
    published = registry.publish_session_extrema([first, second], session_date="2026-09-29", atr=4)
    highs = [record for record, _ in published if record.alias == "SESSION_HIGH"]
    assert len(highs) == 1
    assert highs[0].price == 102


def test_location_observation_requires_causal_lineage() -> None:
    with pytest.raises(ValueError, match="causally available"):
        LocationObservation("OR_LOW", None, 1, 100, 99, 101, 100, 101, 99, 100, "INSIDE_BAND", False, "a", "2026-09-29T09:21", "2026-09-29T09:20", "m")
    assert LocationBand(99, 101).intersects(high=100, low=98)


def test_vwap_is_completed_prefix_and_gap_policy_is_directional() -> None:
    values = causal_vwap_prefix([_aggregate("2026-09-29T09:15"), _aggregate("2026-09-29T09:16")], session_date="2026-09-29", available_at="2026-09-29T09:15")
    assert len(values) == 1
    gap = OpeningGapPolicy().classify(opening={"open": 95}, prior_low=100, prior_high=110, prior_value_low=98, prior_value_high=108, available_at="2026-09-29T09:16")
    assert gap["classification"] == "GAP_DOWN_INITIATIVE"
    assert OpeningGapPolicy().regime_for(state="GAP_DOWN_ACCEPTED", direction="down", version="1", context="gap").permits("short")
