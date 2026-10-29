from ftx_paper.runtime.parity_artifacts import (
    PARITY_LAYERS,
    build_parity_artifact,
    build_parity_manifest,
    causal_lifecycle_diff,
    compare_parity_artifacts,
    compare_parity_manifests,
    decision_diagnostics,
    parity_layer_records,
    validate_parity_artifact,
)


def _artifact() -> dict:
    result = {
        "strategy": {"name": "paper", "version": "v1"},
        "bars_seen": 1,
        "events": [],
        "ledger": {"2026-01-05": {"net_pnl_rs": 1.0}},
        "metadata": {"timezone": "UTC", "normalized_fingerprint": "fixture",
                     "selected_source_rows": 1, "filtered_source_rows": 0,
                     "completed_source_rows": 1, "gap_rows": 0},
    }
    return build_parity_artifact(
        result,
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [{"event_type": "POLICY_DECISION", "timestamp": "2026-01-05T14:30:00Z", "stage": "trigger", "outcome": "observed"}], "execution_settlement": []},
    )


def test_artifact_uses_only_four_layers() -> None:
    artifact = _artifact()
    validate_parity_artifact(artifact)
    assert set(artifact["layers"]) == set(PARITY_LAYERS)
    assert "p9" not in artifact and "p10" not in artifact


def test_artifact_resequences_restarted_producer_decision_stream() -> None:
    artifact = build_parity_artifact(
        {"strategy": {"name": "paper", "version": "v1"}, "bars_seen": 3,
         "events": [], "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [
            {"event_type": "POLICY_DECISION", "decision_id": "d1",
             "timestamp": "2026-01-05T09:15:00Z", "stage": "interaction", "outcome": "observed", "sequence": 0},
            {"event_type": "POLICY_DECISION", "decision_id": "d2",
             "timestamp": "2026-01-05T09:20:00Z", "stage": "candidate", "outcome": "observed", "sequence": 0},
            {"event_type": "POLICY_DECISION", "decision_id": "d3",
             "timestamp": "2026-01-05T09:25:00Z", "stage": "admission", "outcome": "rejected", "sequence": 0},
        ], "execution_settlement": []},
    )

    validate_parity_artifact(artifact)
    assert [item["sequence"] for item in artifact["layers"]["decision"]] == [0, 1, 2]
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
    artifact = build_parity_artifact({"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}}, request={"session_date": "2026-01-05"})
    assert artifact["manifest"]["layers"]["execution_settlement"]["status"] == "UNAVAILABLE"


def test_orderflow_scope_uses_canonical_session_and_vehicle_fields() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05", "strategy_id": "ftx-paper-orderflow", "vehicles": ["futures"]},
    )

    scope = artifact["scope"]
    assert scope["vehicles"] == ["futures"]


def test_scope_uses_selected_source_fingerprint_and_neutral_contract() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {
            "normalized_fingerprint": "source-fingerprint",
            "input_provenance": {"current": {
                "selected_symbol": "NIFTY", "selected_expiry": "",
                "instrument_type": "FUT_CONTINUOUS", "source_bar_count": 375,
                "normalized_fingerprint": "normalized-fingerprint",
            }},
            "selected_source_rows": 375, "completed_source_rows": 375,
            "processed_decision_bars": 375,
        }},
        request={"session_date": "2026-09-29", "strategy_id": "ftx-paper-orderflow"},
    )
    scope = artifact["scope"]
    assert scope["source_contract"] == "orderflow-replay-v1"
    assert scope["replay_input"]["identity"] == ["normalized-fingerprint"]
    assert scope["counts"]["processed_decision_bars"] == 375


def test_decision_diagnostics_exposes_paper_stage_and_reason_counts() -> None:
    records = [
        {"kind": "recorded", "values": {"decision_stage": "trigger", "outcome": "recorded", "reason": "trigger_observation"}},
        {"kind": "rejected", "values": {"decision_stage": "trigger", "outcome": "rejected", "reason": "no_orderflow_interaction"}},
        {"kind": "accepted", "values": {"decision_stage": "admission", "outcome": "accepted", "reason": "orderflow_admission"}},
    ]

    diagnostics = decision_diagnostics(records)

    assert diagnostics["count"] == 3
    assert diagnostics["kind_counts"] == {"accepted": 1, "recorded": 1, "rejected": 1}
    assert diagnostics["stage_counts"] == {"admission": 1, "trigger": 2}
    assert diagnostics["reason_counts"]["trigger_observation"] == 1


def test_decision_diagnostics_prefers_projected_stage_and_outcome() -> None:
    diagnostics = decision_diagnostics([{
        "kind": "decision",
        "stage": "admission",
        "outcome": "accepted",
        "values": {"decision_stage": "trigger", "outcome": "rejected", "reason": "kept"},
    }])
    assert diagnostics["stage_counts"] == {"admission": 1}
    assert diagnostics["outcome_counts"] == {"accepted": 1}


def test_projection_preserves_decision_lineage_and_excludes_extensions() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [{
            "event_type": "POLICY_DECISION", "decision_id": "d1",
            "decision_at": "2026-01-05T14:30:00+05:30", "stage": "admission",
            "outcome": "accepted", "causal_parent_id": "p1",
            "decision_view": {"stage": "admission", "outcome": "accepted",
                              "evidence": {"timestamps": ["2026-01-05T14:29:00Z"],
                                           "references": ["bar-1"]}},
            "producer_extensions": {"paper_only": True}, "quantity": 1,
        }]},
    )
    record = artifact["layers"]["decision"][0]
    assert record["native_kind"] == "admission"
    assert record["stage"] == "admission"
    assert record["outcome"] == "accepted"
    assert record["causal_parent_id"] is None
    assert "missing_parent_link:decision:0" in artifact["coverage"]["issues"]
    assert record["evidence"]["references"] == ["bar-1"]
    assert record["producer_extensions"] == {"paper_only": True}
    assert "paper_only" not in record["values"]
    artifact["layers"]["decision"][0]["producer_extensions"] = {"paper_only": False}
    assert artifact["manifest"] == build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [{
            "event_type": "POLICY_DECISION", "decision_id": "d1",
            "decision_at": "2026-01-05T14:30:00+05:30", "stage": "admission",
            "outcome": "accepted", "causal_parent_id": "p1",
            "decision_view": {"stage": "admission", "outcome": "accepted",
                              "evidence": {"timestamps": ["2026-01-05T14:29:00Z"],
                                           "references": ["bar-1"]}},
            "producer_extensions": {"paper_only": True}, "quantity": 1,
        }]},
    )["manifest"]


def test_projection_does_not_wrap_producer_identity_into_strict_identity() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [{
            "event_type": "POLICY_DECISION", "decision_at": "2026-01-05T14:30:00Z", "stage": "admission",
            "outcome": "accepted", "identity": "producer-local-id",
        }]},
    )
    record = artifact["layers"]["decision"][0]
    assert record["identity"] != "producer-local-id"


def test_projection_does_not_self_parent_and_reports_missing_parent() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [{
            "event_type": "POLICY_DECISION", "decision_id": "d1",
            "causal_parent_id": "missing", "decision_at": "2026-01-05T14:30:00Z",
            "stage": "admission", "outcome": "accepted",
        }]},
    )
    record = artifact["layers"]["decision"][0]
    assert record["causal_parent_id"] is None
    assert "missing_parent_link:decision:0" in artifact["coverage"]["issues"]


def test_projection_preserves_source_timing_and_neutral_event_key() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"execution_settlement": [{
            "event_type": "FILL", "observed_at": "2026-01-05T14:30:00+05:30",
            "available_at": "2026-01-05T14:31:00+05:30", "neutral_event_key": "fill-1",
        }]},
    )
    record = artifact["layers"]["execution_settlement"][0]
    assert record["observed_at"] == "2026-01-05T14:30:00+05:30"
    assert record["available_at"] == "2026-01-05T14:31:00+05:30"
    assert record["neutral_event_key"] == "fill-1"


def test_settlement_availability_reflects_projected_settlements() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"},
         "synthetic_settlements": [{"timestamp": "2026-01-05T14:31:00Z"}]},
        request={"session_date": "2026-01-05"},
    )
    assert artifact["coverage"]["field_availability"]["execution_settlement"] is True

def test_ledger_projection_does_not_invent_closeout() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"},
         "ledger": {"2026-01-05": {"net_pnl_rs": 2.0}}},
        request={"session_date": "2026-01-05"},
    )
    assert artifact["layers"]["portfolio_ledger"] == []


def test_projection_excludes_reference_replacement_and_preserves_native_reason() -> None:
    decisions = [
        {
            "event_type": "POLICY_DECISION",
            "decision_at": "2026-01-05T14:30:00Z",
            "outcome": "rejected",
            "decision_stage": "lifecycle",
            "reason": "REFERENCE_REPLACED",
            "interaction_id": "interaction-1",
        },
        {
            "event_type": "POLICY_DECISION",
            "decision_at": "2026-01-05T14:31:00Z",
            "outcome": "rejected",
            "decision_stage": "lifecycle",
            "reason": "setup_expired",
            "opportunity_id": "opportunity-1",
        },
    ]
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": decisions},
    )

    records = artifact["layers"]["decision"]
    assert len(records) == 1
    assert records[0]["native_kind"] == "setup_expired"


def test_run_scope_record_identity_remains_strict() -> None:
    left = _artifact()
    right = _artifact()
    right["producer"] = {"name": "NiftyZoning", "version": "fixture"}
    right["layers"]["run_scope"][0]["occurred_at"] = "2026-01-05T05:30:00+05:30"
    right["layers"]["run_scope"][0]["identity"] = "canonical-local-scope"
    right["layers"]["run_scope"][0]["comparison_identity"] = "canonical-local-scope"
    right["manifest"] = build_parity_manifest(right)
    result = compare_parity_artifacts(left, right)
    assert result["status"] == "UNAVAILABLE"


def test_run_scope_uses_deterministic_local_session_boundary() -> None:
    artifact = _artifact()
    assert artifact["layers"]["run_scope"][0]["occurred_at"] == "2026-01-05T00:00:00Z"


def test_causal_lifecycle_diff_reports_duplicate_terminal_structural_stages() -> None:
    decisions = []
    for reason in ("setup_invalidated", "setup_invalidated", "orderflow_admission", "orderflow_admission"):
        decisions.append({
            "event_type": "POLICY_DECISION", "decision_at": "2026-01-05T14:30:00Z",
            "outcome": "rejected", "reason": reason, "opportunity_id": "opp-1",
        })
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"}, layer_records={"decision": decisions},
    )
    report = causal_lifecycle_diff(artifact, artifact)
    assert report["duplicate_terminal_transitions"]
    assert all(item["causal_id"] for item in report["duplicate_terminal_transitions"])


def test_causal_lifecycle_diff_indexes_ids_and_reports_terminal_duplicates() -> None:
    paper = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [
            {"event_type": "POLICY_DECISION", "decision_at": "2026-01-05T14:30:00Z",
             "outcome": "recorded", "reason": "setup_candidate_formed", "opportunity_id": "opp-1"},
            {"event_type": "POLICY_DECISION", "decision_at": "2026-01-05T14:31:00Z",
             "outcome": "rejected", "reason": "setup_expired", "opportunity_id": "opp-1"},
            {"event_type": "POLICY_DECISION", "decision_at": "2026-01-05T14:32:00Z",
             "outcome": "rejected", "reason": "setup_expired", "opportunity_id": "opp-1"},
        ]},
    )
    canonical = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [
            {"event_type": "POLICY_DECISION", "decision_at": "2026-01-05T14:30:00Z",
             "outcome": "recorded", "reason": "setup_candidate_formed", "opportunity_id": "opp-1"},
        ]},
    )
    report = causal_lifecycle_diff(paper, canonical)
    assert report["counts_by_native_kind"]["paper"]
    assert report["counts_by_native_kind"]["paper"]["setup_expired"] == 2


def test_projection_reads_reason_and_outcome_from_canonical_policy_view() -> None:
    decisions = [
        {
            "event_type": "POLICY_DECISION",
            "decision_id": "interaction-1",
            "decision_at": "2026-01-05T14:30:00Z",
            "decision_view": {
                "stage": "lifecycle",
                "policy": {"outcome": "rejected", "reason_code": "REFERENCE_REPLACED"},
            },
        },
        {
            "event_type": "POLICY_DECISION",
            "decision_id": "opportunity-1",
            "decision_at": "2026-01-05T14:31:00Z",
            "decision_view": {
                "stage": "lifecycle",
                "policy": {"outcome": "rejected", "reason_code": "setup_expired"},
            },
        },
    ]
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": decisions},
    )

    records = artifact["layers"]["decision"]
    assert len(records) == 1
    assert records[0]["native_kind"] == "setup_expired"


def test_neutral_projection_uses_stage_outcome_and_utc_identity() -> None:
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request={"session_date": "2026-01-05"},
        layer_records={"decision": [{
            "event_type": "POLICY_DECISION", "decision_at": "2026-01-05T14:30:00+05:30",
            "stage": "interaction_response", "outcome": "invalidated",
            "interaction_id": "producer-local", "reason": "source_gap",
        }], "execution_settlement": [], "portfolio_ledger": []},
    )
    record = artifact["layers"]["decision"][0]
    assert record["kind"] == "decision"
    assert record["stage"] == "interaction"
    assert record["outcome"] == "invalidated"
    assert record["occurred_at"] == "2026-01-05T09:00:00Z"
    assert artifact["manifest"]["layers"]["execution_settlement"]["status"] == "AVAILABLE"
