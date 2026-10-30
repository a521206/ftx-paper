from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json

import pytest

from ftx_paper.strategies.orderflow.market_context_history import (
    IntervalSpecification,
    coalesce_intervals,
    project_history,
    project_saved_session,
)


def _artifact() -> dict[str, object]:
    return {
        "artifact_type": "orderflow_state_machine_trace", "schema_version": 7,
        "replay_mode": "full", "date": "2026-09-29",
        "source": {"source_fingerprint": "source-1", "timestamp_convention": "1m label; completed at next minute", "evaluation_timeframe": "1m"},
        "method": {"policy_version": "test"},
        "replay_records": [
            {"sequence": 0, "timestamp": "2026-09-29T09:20", "available_at": "2026-09-29T09:21", "primary_state_before": "BALANCE", "primary_state_after": "BALANCE", "market_state_before": "BALANCE", "market_state_after": "BALANCE", "bar": {"time": "09:20", "open": 100.0}, "latest_5m": {"time": "2026-09-29T09:15", "available_at": "2026-09-29T09:20"}, "ordinary_context_ref": {"context_id": "ctx-1", "version": "v1"}, "permission": {"status": "ALLOWED"}, "events": [{"event_id": "evt-0", "kind": "state_transition"}]},
            {"sequence": 1, "timestamp": "2026-09-29T09:21", "available_at": "2026-09-29T09:22", "primary_state_before": "BALANCE", "primary_state_after": "ARMED", "market_state_before": "BALANCE", "market_state_after": "DISPLACEMENT", "bar": {"time": "09:21", "open": 100.0}, "latest_5m": {"time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"}, "permission": {"status": "BLOCKED"}, "events": [{"event_id": "evt-1", "kind": "state_transition"}]},
            {"sequence": 2, "timestamp": "2026-09-29T09:22", "available_at": "2026-09-29T09:23", "primary_state_before": "ARMED", "primary_state_after": "ARMED", "market_state_before": "DISPLACEMENT", "market_state_after": "DISPLACEMENT", "bar": {"time": "09:22", "open": 100.0}, "latest_5m": {"time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"}, "permission": {"status": "BLOCKED"}, "events": []},
        ],
    }


def test_projection_is_causal_immutable_and_deterministic() -> None:
    artifact = _artifact()
    before = deepcopy(artifact)
    first = project_history(artifact)
    second = project_history(artifact)
    assert artifact == before
    assert first == second
    assert [row.replay_sequence for row in first.timeline] == [0, 1, 2]
    assert first.timeline[0].event_ids == ("evt-0",)
    assert first.timeline[0].ordinary_context_ref is not None
    assert first.timeline[1].ordinary_context_ref is None
    assert first.provenance.source_fingerprint == "source-1"
    assert json.dumps(first.to_dict(), sort_keys=True) == json.dumps(second.to_dict(), sort_keys=True)
    with pytest.raises(TypeError):
        first.timeline[0].source_record["sequence"] = 4  # type: ignore[index]
    assert project_saved_session({"replay": artifact}).timeline == first.timeline


def test_nested_projected_values_are_immutable() -> None:
    artifact = _artifact()
    records = artifact["replay_records"]
    assert isinstance(records, list)
    records[0]["permission"] = {"before": {"nested": [1]}, "after": {"nested": [2]}}
    row = project_history(artifact).timeline[0]
    with pytest.raises(TypeError):
        row.permission_before["nested"][0] = 3  # type: ignore[index]


def test_projection_rejects_non_full_or_duplicate_sequences() -> None:
    summary = _artifact()
    summary["replay_mode"] = "summary"
    with pytest.raises(ValueError, match="full replay"):
        project_history(summary)
    duplicate = _artifact()
    records = duplicate["replay_records"]
    assert isinstance(records, list)
    records[1]["sequence"] = 0
    with pytest.raises(ValueError, match="unique"):
        project_history(duplicate)
    duplicate_candle = _artifact()
    candle_records = duplicate_candle["replay_records"]
    assert isinstance(candle_records, list)
    candle_records[1]["bar"]["time"] = candle_records[0]["bar"]["time"]
    with pytest.raises(ValueError, match="one-to-one"):
        project_history(duplicate_candle)
    missing_mode = _artifact()
    del missing_mode["replay_mode"]
    with pytest.raises(ValueError, match="explicit full capture"):
        project_history(missing_mode)


def test_context_visibility_and_canonical_decision_refs() -> None:
    artifact = _artifact()
    source = artifact["source"]
    assert isinstance(source, dict)
    source["same_record_context_visibility"] = "allowed"
    records = artifact["replay_records"]
    assert isinstance(records, list)
    records[0]["ordinary_context_ref"] = {"context_id": "ctx-1", "version": "v1"}
    records[0]["context_references"] = {"legacy": True}
    records[0]["decision_input_context_refs"] = {"intent-1": {"context_id": "ctx-1"}}
    row = project_history(artifact).timeline[0]
    assert row.ordinary_context_ref is not None
    assert row.decision_input_context_refs == {"intent-1": {"context_id": "ctx-1"}}


def test_intervals_split_on_value_and_causal_gap() -> None:
    projection = project_history(_artifact())
    intervals = coalesce_intervals(projection.timeline, IntervalSpecification("market_state_after"))
    assert [(item.value, item.start_at, item.end_at, item.source_sequences) for item in intervals] == [("BALANCE", "2026-09-29T09:20", "2026-09-29T09:21", (0,)), ("DISPLACEMENT", "2026-09-29T09:21", None, (1, 2))]
    gap = list(projection.timeline)
    gap[1] = replace(gap[1], available_at="2026-09-29T09:30")
    gap_intervals = coalesce_intervals(gap, IntervalSpecification("market_state_after"))
    assert gap_intervals[0].end_at == "2026-09-29T09:21"
    assert gap_intervals[1].gap_after is True


def test_coalescer_rejects_ambiguous_timeline_order() -> None:
    timeline = project_history(_artifact()).timeline
    with pytest.raises(ValueError, match="ordered"):
        coalesce_intervals(tuple(reversed(timeline)), IntervalSpecification("market_state_after"))
    ambiguous = list(timeline)
    ambiguous[1] = replace(ambiguous[1], observed_at=ambiguous[0].observed_at)
    with pytest.raises(ValueError, match="ambiguous"):
        coalesce_intervals(ambiguous, IntervalSpecification("market_state_after"))


def test_location_observations_are_causally_checked() -> None:
    artifact = _artifact()
    records = artifact["replay_records"]
    assert isinstance(records, list)
    observation = {"location": "OR_LOW", "timeframe": "1m", "observation_id": "obs-0", "source_aggregate_id": "2026-09-29T09:15", "reference_generation": 1, "band_low": 99.0, "band_high": 101.0, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "close_relation": "INSIDE_BAND", "observed_at": "2026-09-29T09:19", "available_at": "2026-09-29T09:21"}
    records[0]["location_observations"] = [observation]
    assert project_history(artifact).timeline[0].location_observations[0]["close_relation"] == "INSIDE_BAND"
    future = _artifact()
    future_records = future["replay_records"]
    assert isinstance(future_records, list)
    future_records[0]["location_observations"] = [{**observation, "observation_id": "obs-future", "available_at": "2026-09-29T09:30"}]
    with pytest.raises(ValueError, match="future location"):
        project_history(future)


def test_interaction_state_as_of_keeps_timeframes_separate() -> None:
    artifact = _artifact()
    records = artifact["replay_records"]
    assert isinstance(records, list)
    records[0]["events"] = [{"event_id": "created-5m", "kind": "interaction_created", "interaction_id": "NIFTY:2026-09-29:OR_LOW:5m:2026-09-29T09:15", "location": "OR_LOW", "timeframe": "5m", "reference_generation": 1}, {"event_id": "created-1m", "kind": "interaction_created", "interaction_id": "NIFTY:2026-09-29:OR_LOW:1m:2026-09-29T09:16", "location": "OR_LOW", "timeframe": "1m", "reference_generation": 1}]
    projection = project_history(artifact)
    assert set(projection.timeline[0].interaction_state_as_of) == {"OR_LOW:5m:1", "OR_LOW:1m:1"}
