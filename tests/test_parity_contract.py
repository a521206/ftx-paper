import pytest

from ftx_paper.runtime.parity_artifacts import (
    PARITY_LAYERS,
    build_parity_artifact,
    build_parity_manifest,
    compare_parity_manifests,
    read_parity_artifact,
)


def _artifact(*, timezone="America/New_York"):
    return build_parity_artifact(
        {"strategy": {"algorithm": "flow"}, "bars_seen": 1,
         "metadata": {"timezone": timezone, "input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05", "timezone": timezone},
        layer_records={"decisions": [{"event_type": "ACCEPTEDDECISION", "timestamp": "2026-01-05T14:30:00Z"}]},
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


def test_records_use_canonical_kinds_and_neutral_identity() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={
            "decisions": [{"event_type": "ACCEPTEDDECISION", "timestamp": "2026-01-05T14:30:00Z",
                           "cell": "A", "client_order_id": "producer-local"}],
            "execution": [{"event_type": "ORDER_INTENT", "timestamp": "2026-01-05T14:30:00Z",
                            "cell": "A", "client_order_id": "producer-local"},
                           {"event_type": "FILL", "fill_timestamp": "2026-01-05T14:31:00Z",
                            "cell": "A", "client_order_id": "producer-local"}],
        },
    )
    assert artifact["layers"]["decision"][0]["kind"] == "accepted"
    assert [record["kind"] for record in artifact["layers"]["execution_settlement"]] == ["order_intent", "fill"]
    assert "producer-local" not in artifact["layers"]["decision"][0]["identity"]


def test_observed_empty_scope_is_complete() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
    )
    assert artifact["coverage"]["complete"] is True
    assert artifact["manifest"]["layers"]["decision"]["status"] == "MATCHED"


def test_missing_record_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="occurred_at"):
        build_parity_artifact(
            {"strategy": {}, "bars_seen": 1, "metadata": {}},
            request={"session_date": "2026-01-05"},
            layer_records={"decisions": [{"event_type": "ACCEPTEDDECISION"}]},
        )


def test_status_comparison_preserves_unavailable_verdict() -> None:
    left = _artifact()
    right = _artifact()
    right["coverage"]["observed_layers"]["execution_settlement"] = False
    right["manifest"] = build_parity_manifest(right)
    assert compare_parity_manifests(left["manifest"], right["manifest"]) == {
        "status": "UNAVAILABLE", "layer": "execution_settlement",
    }
