import json

from scripts.orderflow.compare_canonical_replay import (
    _compare_phase45,
    _compare_phase35,
    _bounded_atr_dependent_difference,
    _resolve_canonical_artifact,
    _paper_phase35_projection,
    _normalize_canonical_timestamps,
    _normalize_observation_payload,
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
        "atr": {"value": 2.5, "lineage": []},
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


def test_canonical_local_minute_timestamps_are_normalized_only_at_comparison_ingress() -> None:
    record = {
        "observed_at": "2026-10-01T09:20",
        "values": {"as_of": "2026-10-01T09:25", "evidence_ids": ["09:25"]},
    }
    normalized = _normalize_canonical_timestamps(record, "2026-10-01")
    assert normalized["observed_at"] == "2026-10-01T03:50:00.000000Z"
    assert normalized["values"]["as_of"] == "2026-10-01T03:55:00.000000Z"
    assert normalized["values"]["evidence_ids"] == ["09:25"]


def test_observation_comparison_drops_presentation_timestamp_labels() -> None:
    payload = {"aggregates": {"1m": [{
        "time": "2026-10-01T09:15", "end_time": "2026-10-01T09:15",
        "available_at": "2026-10-01T09:16", "time_label": "2026-10-01T09:15",
        "end_time_label": "2026-10-01T09:15",
    }]}}
    normalized = _normalize_observation_payload(payload, "2026-10-01")
    assert normalized["aggregates"]["1m"][0]["available_at"] == "2026-10-01T03:46:00.000000Z"
    assert "time_label" not in normalized["aggregates"]["1m"][0]


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


def test_observation_boundary_preserves_full_replay_artifact_timestamps() -> None:
    rows = [{"session_date": "2026-09-25", "time": "2026-09-25T09:15", "close": 100.0}]

    assert _observation_rows(rows)[0]["time"] == "2026-09-25T09:15"


def test_phase45_comparison_does_not_invent_missing_stream(tmp_path) -> None:
    artifact = tmp_path / "phase45.json"
    artifact.write_text(
        json.dumps({"sessions": {"2026-09-29": {"parity_phase_facts": {}}}}),
        encoding="utf-8",
    )
    assert _compare_phase45(artifact, {"parity_phase_facts": {}}, "2026-09-29") == {
        "status": "INCOMPLETE",
        "stream": "interaction_response",
        "reason": "canonical facts are unavailable",
    }


def test_phase45_comparison_rejects_naive_causal_timestamps(tmp_path) -> None:
    record = {
        "kind": "interaction",
        "occurred_at": "2026-09-29T09:20:00",
        "observed_at": "2026-09-29T09:20:00",
        "available_at": "2026-09-29T09:21:00",
        "identity": "interaction-1",
        "causal_parent_id": None,
        "values": {},
        "unavailable": [],
    }
    artifact = tmp_path / "phase45.json"
    artifact.write_text(
        json.dumps({"sessions": {"2026-09-29": {"parity_phase_facts": {"interaction_response": [record]}}}}),
        encoding="utf-8",
    )

    assert _compare_phase45(
        artifact,
        {"parity_phase_facts": {"interaction_response": [record]}},
        "2026-09-29",
    ) == {
        "status": "INCOMPLETE",
        "stream": "interaction_response",
        "reason": "parity timestamps must be timezone-aware",
    }


def test_phase45_comparison_accepts_timezone_aware_causal_timestamps(tmp_path) -> None:
    record = {
        "kind": "interaction",
        "occurred_at": "2026-09-29T03:50:00Z",
        "observed_at": "2026-09-29T03:50:00+00:00",
        "available_at": "2026-09-29T09:21:00+05:30",
        "identity": "interaction-1",
        "causal_parent_id": None,
        "values": {},
        "unavailable": [],
    }
    artifact = tmp_path / "phase45.json"
    artifact.write_text(
        json.dumps({"sessions": {"2026-09-29": {"parity_phase_facts": {"interaction_response": [record]}}}}),
        encoding="utf-8",
    )

    assert _compare_phase45(
        artifact,
        {"parity_phase_facts": {"interaction_response": [record]}},
        "2026-09-29",
    ) == {"status": "MATCHED", "stream": "interaction_response", "count": 1}


def test_phase45_comparison_ignores_producer_identity_artifacts(tmp_path) -> None:
    canonical_record = {
        "kind": "location_interaction_observed",
        "occurred_at": "2026-10-01T09:16",
        "observed_at": "2026-10-01T09:16",
        "available_at": "2026-10-01T09:16",
        "identity": "niftyzoning:derived:6a97",
        "causal_parent_id": "niftyzoning:derived:8fab",
        "values": {
            "interaction_id": "NIFTY:2026-10-01:PD_LOW:1m:2026-10-01T09:16",
            "source_aggregate_id": "aggregate:1m:2026-10-01T09:16",
            "causal_available_at": "2026-10-01T09:16",
            "reference_generation": 1,
            "outcome": "observed",
        },
        "unavailable": [],
    }
    paper_record = {
        **canonical_record,
        "occurred_at": "2026-10-01T03:46:00Z",
        "observed_at": "2026-10-01T03:46:00Z",
        "available_at": "2026-10-01T03:46:00Z",
        "identity": "niftyzoning:derived:c118",
        "causal_parent_id": "niftyzoning:derived:6396",
        "interaction_id": canonical_record["values"]["interaction_id"],
        "causal_available_at": "2026-10-01T03:46:00Z",
        "values": {
            "source_aggregate_id": "aggregate:1m:2026-10-01T09:16",
            "reference_generation": 1,
            "outcome": "observed",
        },
    }
    artifact = tmp_path / "phase45.json"
    artifact.write_text(
        json.dumps({"sessions": {"2026-10-01": {"parity_phase_facts": {"interaction_response": [canonical_record]}}}}),
        encoding="utf-8",
    )

    result = _compare_phase45(
        artifact,
        {"parity_phase_facts": {"interaction_response": [paper_record]}},
        "2026-10-01",
    )

    assert result["status"] == "MATCHED"
    assert result["ignored_producer_metadata"][0]["canonical_identity"] != result["ignored_producer_metadata"][0]["paper_identity"]
