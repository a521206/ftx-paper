from ftx_paper.runtime.parity_artifacts import (
    PARITY_LAYERS,
    build_parity_artifact,
    compare_parity_manifests,
    parity_layer_records,
    validate_parity_artifact,
)


def _artifact() -> dict:
    result = {
        "strategy": {"name": "paper", "version": "v1"},
        "bars_seen": 1,
        "events": [],
        "ledger": {"2026-01-05": {"net_pnl_rs": 1.0}},
        "metadata": {"timezone": "UTC", "input_fingerprint": "fixture"},
    }
    return build_parity_artifact(
        result,
        request={"session_date": "2026-01-05"},
        layer_records={"decisions": [{"event_type": "POLICY_DECISION", "timestamp": "2026-01-05T14:30:00Z"}], "execution": []},
    )


def test_artifact_uses_only_four_layers() -> None:
    artifact = _artifact()
    validate_parity_artifact(artifact)
    assert set(artifact["layers"]) == set(PARITY_LAYERS)
    assert "p9" not in artifact and "p10" not in artifact
    assert parity_layer_records(artifact, "decision")[0]["sequence"] == 0


def test_manifest_comparison_detects_first_layer_difference() -> None:
    left = _artifact()
    right = _artifact()
    right["layers"]["decision"][0]["values"]["changed"] = True
    right["manifest"] = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {}}, request={"session_date": "2026-01-05"}
    )["manifest"]
    assert compare_parity_manifests(left["manifest"], right["manifest"])["status"] == "MISMATCHED"


def test_observed_empty_layers_are_matchable() -> None:
    artifact = build_parity_artifact({"strategy": {}, "bars_seen": 1, "metadata": {"input_fingerprint": "fixture"}}, request={"session_date": "2026-01-05"})
    assert artifact["manifest"]["layers"]["execution_settlement"]["status"] == "MATCHED"


def test_orderflow_scope_uses_canonical_session_and_vehicle_fields() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05", "strategy_id": "ftx-paper-orderflow", "vehicles": ["futures"]},
    )

    scope = artifact["scope"]
    assert scope["sessions"] == ["orderflow"]
    assert scope["vehicles"] == ["futures"]
    assert scope["partitions"] == {"source": ["orderflow"]}
