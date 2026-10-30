from __future__ import annotations

from ftx_paper.strategies.orderflow.parity_artifacts import (
    PHASE_STREAMS,
    build_phase4_artifact,
    compare_phase4_manifests,
    phase_facts_from_payload,
)


def _fact(identity: str = "ctx-1") -> dict[str, object]:
    return {
        "kind": "context_published",
        "occurred_at": "2026-09-15T09:20:00+05:30",
        "observed_at": "2026-09-15T09:20:00+05:30",
        "available_at": "2026-09-15T09:21:00+05:30",
        "identity": identity,
        "causal_parent_id": "source-1",
        "values": {
            "context_id": "ctx",
            "context_version": "1",
            "location_id": "OR_LOW:2026-09-15:1",
            "reference_generation": 1,
            "source_observation_id": "source-1",
        },
        "unavailable": [],
    }


def test_missing_context_facts_are_explicitly_unavailable() -> None:
    records, coverage = phase_facts_from_payload({})
    assert records == ()
    assert coverage["status"] == "UNAVAILABLE"
    assert coverage["reason"] == "missing_context_location_facts"


def test_persisted_direct_context_stream_is_accepted_without_reconstruction() -> None:
    records, coverage = phase_facts_from_payload({"context_location": [_fact()]})
    assert coverage["status"] == "MATCHED"
    assert records[0]["identity"] == "ctx-1"


def test_phase4_artifact_is_deterministic_and_hash_first() -> None:
    payload = {"parity_phase_facts": {"context_location": [_fact()]}}
    first = build_phase4_artifact(run_id="run-1", producer={"name": "ftx-paper"}, scope={"date": "2026-09-15"}, payload=payload)
    second = build_phase4_artifact(run_id="run-1", producer={"name": "ftx-paper"}, scope={"date": "2026-09-15"}, payload=payload)
    assert first == second
    assert first["manifest"]["streams"]["context_location"]["count"] == 1
    assert first["phase_streams"]["context_location"][0]["causal_parent_id"] == "source-1"


def test_comparison_stops_at_first_unavailable_stream() -> None:
    canonical = build_phase4_artifact(run_id="c", producer={"name": "canonical"}, scope={}, payload={"parity_phase_facts": {"context_location": [_fact()]}})
    paper = build_phase4_artifact(run_id="p", producer={"name": "paper"}, scope={}, payload={})
    result = compare_phase4_manifests(canonical, paper)
    assert result["status"] == "UNAVAILABLE"
    assert result["stream"] == "source_observation"


def test_phase_stream_contract_is_complete() -> None:
    assert PHASE_STREAMS == ("source_observation", "context_location", "interaction_response", "opportunity_lifecycle", "intent_admission", "trigger_exit")


def test_first_record_mismatch_is_reported_after_manifest_mismatch() -> None:
    left_facts = {stream: [] for stream in PHASE_STREAMS}
    right_facts = {stream: [] for stream in PHASE_STREAMS}
    left_facts["context_location"] = [_fact("ctx-left")]
    right_facts["context_location"] = [_fact("ctx-right")]
    left = build_phase4_artifact(run_id="c", producer={"name": "canonical"}, scope={}, payload={"parity_phase_facts": left_facts})
    right = build_phase4_artifact(run_id="p", producer={"name": "paper"}, scope={}, payload={"parity_phase_facts": right_facts})
    result = compare_phase4_manifests(
        left,
        right,
        canonical_records=lambda stream: left["phase_streams"][stream],  # type: ignore[index]
        paper_records=lambda stream: right["phase_streams"][stream],  # type: ignore[index]
    )
    assert result["status"] == "MISMATCHED"
    assert result["stream"] == "context_location"
    assert result["index"] == 0
