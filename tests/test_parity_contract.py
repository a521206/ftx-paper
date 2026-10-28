import pytest

from ftx_paper.runtime.parity_artifacts import (
    PARITY_LAYERS,
    build_parity_artifact,
    build_parity_manifest,
    compare_parity_artifacts,
    compare_parity_manifests,
    read_parity_artifact,
)


def _artifact(*, timezone="America/New_York"):
    return build_parity_artifact(
        {"strategy": {"algorithm": "flow"}, "bars_seen": 1,
         "metadata": {"timezone": timezone, "input_fingerprint": "fixture",
                      "selected_source_rows": 1, "filtered_source_rows": 0,
                      "completed_source_rows": 1, "gap_rows": 0}},
        request={"session_date": "2026-01-05", "timezone": timezone},
         layer_records={"decision": [{"event_type": "POLICY_DECISION", "decision_id": "accepted",
                                         "stage": "admission", "outcome": "accepted", "decision_at": "2026-01-05T14:30:00Z"}]},
    )


def test_contract_is_producer_neutral_and_four_layer() -> None:
    artifact = read_parity_artifact(_artifact())
    assert artifact["contract"]["layers"] == list(PARITY_LAYERS)
    assert set(artifact["layers"]) == set(PARITY_LAYERS)


def test_scope_difference_is_reported_before_layer_comparison() -> None:
    assert compare_parity_manifests(_artifact()["manifest"], _artifact(timezone="UTC")["manifest"])["layer"] == "run_scope"


def test_manifest_hashes_are_layer_specific() -> None:
    artifact = _artifact()
    artifact["layers"]["decision"][0]["values"]["changed"] = True
    with pytest.raises(ValueError, match="manifest"):
        read_parity_artifact(artifact)


def test_invalid_layer_records_are_rejected() -> None:
    artifact = _artifact()
    artifact["layers"]["decision"] = [None]
    with pytest.raises(ValueError, match="invalid decision"):
        build_parity_manifest(artifact)


def test_malformed_observed_layers_are_treated_as_unobserved() -> None:
    artifact = _artifact()
    artifact["coverage"]["observed_layers"] = []
    manifest = build_parity_manifest(artifact)
    assert manifest["layers"]["decision"]["status"] == "UNAVAILABLE"


def test_records_use_canonical_kinds_and_neutral_identity() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={
             "decision": [{"event_type": "POLICY_DECISION", "decision_id": "accepted", "stage": "admission", "outcome": "accepted",
                            "decision_at": "2026-01-05T14:30:00Z",
                            "cell": "A", "client_order_id": "producer-local"}],
            "execution_settlement": [{"event_type": "ORDER_INTENT", "timestamp": "2026-01-05T14:30:00Z",
                            "cell": "A", "client_order_id": "producer-local"},
                           {"event_type": "FILL", "fill_timestamp": "2026-01-05T14:31:00Z",
                            "cell": "A", "client_order_id": "producer-local"}],
        },
    )
    assert artifact["layers"]["decision"][0]["kind"] == "decision"
    assert [record["kind"] for record in artifact["layers"]["execution_settlement"]] == ["order_intent", "fill"]
    assert "producer-local" not in artifact["layers"]["decision"][0]["identity"]


def test_observed_empty_scope_is_complete() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
    )
    assert artifact["coverage"]["complete"] is True
    assert artifact["manifest"]["layers"]["decision"]["status"] == "AVAILABLE"


def test_scope_uses_canonical_orderflow_run_identity() -> None:
    artifact = build_parity_artifact(
        {
            "strategy": {},
            "bars_seen": 375,
            "metadata": {
                "timezone": "Asia/Kolkata",
                "input_fingerprint": "fingerprint",
                "input_provenance": {"current": {
                    "selected_symbol": "NIFTY",
                    "selected_expiry": "",
                    "instrument_type": "CONTINUOUS",
                    "source_bar_count": 375,
                }},
            },
        },
        request={
            "session_date": "2026-09-29",
            "strategy_id": "ftx-paper-orderflow",
            "vehicles": ["futures"],
        },
    )

    scope = artifact["scope"]
    assert scope["requested_date_from"] == "2026-09-29"
    assert scope["effective_date_to"] == "2026-09-29"
    assert "sessions" not in scope
    assert scope["vehicles"] == ["futures"]
    assert scope["counts"]["processed_decision_bars"] == 375
    assert scope["replay_input"] == {
        "identity": ["fingerprint"],
            "provenance": [{
                "symbol": "NIFTY",
                "series": "I",
                "bar_count": 375,
                "contract_type": "CONTINUOUS",
            "expiry": "",
            "fingerprint": "fingerprint",
            "identities": [{
                "contract_type": "CONTINUOUS",
                "expiry": "",
                "series": "I",
                "symbol": "NIFTY",
            }],
            "identity": {
                "contract_type": "CONTINUOUS",
                "expiry": "",
                "series": "I",
                "symbol": "NIFTY",
            },
        }],
    }


def test_missing_record_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="occurred_at"):
        build_parity_artifact(
            {"strategy": {}, "bars_seen": 1, "metadata": {}},
            request={"session_date": "2026-01-05"},
        layer_records={"decision": [{"event_type": "POLICY_DECISION", "decision_id": "missing", "stage": "trigger", "outcome": "observed"}]},
        )


def test_status_comparison_preserves_unavailable_verdict() -> None:
    left = _artifact()
    right = _artifact()
    right["coverage"]["observed_layers"]["execution_settlement"] = False
    right["manifest"] = build_parity_manifest(right)
    assert compare_parity_manifests(left["manifest"], right["manifest"]) == {
        "status": "UNAVAILABLE", "layer": "execution_settlement",
    }


def test_unobserved_layers_are_not_compared() -> None:
    left = _artifact()
    right = _artifact()
    left["coverage"]["observed_layers"]["execution_settlement"] = False
    right["coverage"]["observed_layers"]["execution_settlement"] = False
    right["layers"]["execution_settlement"].append({
        "kind": "fill", "occurred_at": "2026-01-05T14:31:00Z", "sequence": 0,
        "identity": "x", "values": {}, "unavailable": [],
    })
    left["manifest"] = build_parity_manifest(left)
    right["manifest"] = build_parity_manifest(right)
    assert compare_parity_manifests(left["manifest"], right["manifest"]) == {
        "status": "UNAVAILABLE", "layer": "execution_settlement",
    }


def test_malformed_producer_is_rejected() -> None:
    artifact = _artifact()
    artifact["producer"] = "ftx-paper"
    with pytest.raises(ValueError, match="producer"):
        read_parity_artifact(artifact)


def test_single_artifact_never_receives_cross_producer_match() -> None:
    artifact = _artifact()
    assert compare_parity_artifacts(artifact, artifact)["status"] == "UNAVAILABLE"


def test_cross_producer_scope_match_decision_mismatch() -> None:
    left = _artifact()
    right = _artifact()
    right["producer"] = {"name": "NiftyZoning", "version": "fixture"}
    right["layers"]["decision"][0]["values"]["changed"] = True
    right["manifest"] = build_parity_manifest(right)
    result = compare_parity_artifacts(left, right)
    assert result["status"] == "MISMATCHED"
    assert result["layer"] == "decision"
    assert result["context"]


def test_cross_producer_decision_match_execution_mismatch() -> None:
    left = _artifact()
    right = _artifact()
    right["producer"] = {"name": "NiftyZoning", "version": "fixture"}
    right["layers"]["execution_settlement"].append({
        "kind": "fill", "occurred_at": "2026-01-05T14:31:00Z", "sequence": 0,
        "identity": "x", "values": {}, "unavailable": [],
    })
    right["manifest"] = build_parity_manifest(right)
    result = compare_parity_artifacts(left, right)
    assert result["status"] == "UNAVAILABLE"
