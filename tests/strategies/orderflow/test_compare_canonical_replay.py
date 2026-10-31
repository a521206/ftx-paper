import json

from scripts.orderflow.compare_canonical_replay import (
    _compare_phase35,
    _bounded_atr_dependent_difference,
    _resolve_canonical_artifact,
    _paper_phase35_projection,
)
from scripts.orderflow.orderflow_replay import _observation_rows


def test_phase35_comparison_projection_matches_canonical_schema() -> None:
    result = {
        "evidence": {
            "atr": {"value": 2.5, "observed_at": "t0", "lineage": ["atr-1"]},
            "locations": [
                {"alias": "VWAP", "location_id": "vwap-1"},
                {"alias": "OR_HIGH", "location_id": "or-high-1"},
            ],
            "vwap_measurements": [{"value": 101.0, "available_at": "t1"}],
            "prior_value": {
                "reference": {"session_date": "2026-09-28", "value_area_low": 99.0},
                "observed_at": "t0",
                "available_at": "t1",
                "lineage": ["profile-1"],
            },
            "contexts": [{"context_id": "ctx-1", "version": 2, "available_at": "t1"}],
            "unavailable": [],
            "context_stream_id": "contexts",
            "location_stream_id": "locations",
        },
        "replay_records": [
            {"ordinary_context_ref": {"context_id": "ctx-1", "version": 2}}
        ],
    }

    assert _paper_phase35_projection(result) == {
        "atr": {"value": 2.5, "lineage": ["atr-1"]},
        "locations": result["evidence"]["locations"],
        "vwap_measurements": [{"alias": "VWAP", "location_id": "vwap-1"}],
        "prior_value": {"session_date": "2026-09-28", "value_area_low": 99.0},
        "contexts": [{"context_id": "ctx-1", "version": 2}],
        "unavailable": [],
        "context_stream_id": "contexts",
        "location_stream_id": "locations",
    }


def test_phase35_atr_variance_is_not_a_match() -> None:
    canonical = {"evidence": {"atr": {"value": 2.5}, **{
        field: [] for field in (
            "locations", "vwap_measurements", "prior_value", "contexts",
            "unavailable", "context_stream_id", "location_stream_id",
        )
    }}}
    paper = {"evidence": {"atr": {"value": 3.5}, **{
        field: [] for field in (
            "locations", "vwap_measurements", "prior_value", "contexts",
            "unavailable", "context_stream_id", "location_stream_id",
        )
    }}, "replay_records": []}

    assert _compare_phase35(canonical, paper)["status"] == "INPUT_VARIANCE"


def test_phase35_large_atr_variance_is_not_accepted() -> None:
    canonical = {"evidence": {"atr": {"value": 100.0}, **{
        field: [] for field in (
            "locations", "vwap_measurements", "prior_value", "contexts",
            "unavailable", "context_stream_id", "location_stream_id",
        )
    }}}
    paper = {"evidence": {"atr": {"value": 110.0}, **{
        field: [] for field in (
            "locations", "vwap_measurements", "prior_value", "contexts",
            "unavailable", "context_stream_id", "location_stream_id",
        )
    }}, "replay_records": []}

    result = _compare_phase35(canonical, paper)
    assert result["status"] == "MISMATCHED"
    assert result["atr"]["absolute_difference"] == 10.0


def test_location_band_variance_uses_same_bound() -> None:
    assert _bounded_atr_dependent_difference(
        {"field": "values.locations[0].band_high", "canonical_value": 100.0, "paper_value": 101.0},
        2.5,
    )
    assert not _bounded_atr_dependent_difference(
        {"field": "values.locations[0].band_high", "canonical_value": 100.0, "paper_value": 110.0},
        2.5,
    )
    assert not _bounded_atr_dependent_difference(
        {"field": "values.locations[0].method", "canonical_value": "a", "paper_value": "b"},
        2.5,
    )


def test_canonical_artifact_must_contain_requested_session(tmp_path) -> None:
    artifact = tmp_path / "replay.json"
    artifact.write_text(json.dumps({"sessions": {"2026-09-30": {}}}), encoding="utf-8")

    assert _resolve_canonical_artifact(artifact, "2026-09-30") == artifact
    assert _resolve_canonical_artifact(artifact, "2026-10-01") is None


def test_observation_boundary_normalizes_replay_bar_timestamps() -> None:
    rows = [{"session_date": "2026-09-25", "time": "09:15", "close": 100.0}]

    assert _observation_rows(rows)[0]["time"] == "2026-09-25T09:15"
