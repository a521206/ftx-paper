import json
from pathlib import Path

from ftx_paper.runtime.parity_artifacts import build_parity_artifact, strategy_payload_hash


def test_parity_artifact_is_deterministic_and_separates_p9_p10():
    result = {
        "strategy": {"name": "paper", "version": "v1", "config_hash": "cfg"},
        "vehicles": ["futures", "synthetic"],
        "bars_seen": 2,
        "unavailable_fields": [],
        "metadata": {
            "generated_at": "2026-01-05T06:00:00Z",
            "input_fingerprint": "input-1",
            "input_provenance": {"source": "fixture"},
        },
        "events": [
            {"event_type": "ACCEPTEDDECISION", "decision_id": "d1", "session_date": "2026-01-05", "decision_at": "2026-01-05T04:30:00+00:00", "sequence": 0, "outcome": "accepted"},
            {"event_type": "FILL", "decision_id": "d1", "fill_timestamp": "2026-01-05T04:30:00+00:00", "price": 100.0},
            {"event_type": "EXECUTEDDECISION", "decision_id": "d1", "fill_timestamp": "2026-01-05T04:30:00+00:00", "price": 100.0},
            {"event_type": "EXITDECISION", "decision_id": "x1", "decision_at": "2026-01-05T05:00:00+00:00", "outcome": "closed"},
        ],
        "ledger": {"2026-01-05": {"net_pnl_rs": 1.0}},
    }
    artifact = build_parity_artifact(result, request={"session_date": "2026-01-05"})
    assert artifact["schema_version"] == 1
    assert artifact["p9"]["decision_records"][0]["stream_sequence"] == 0
    assert artifact["p9"]["decision_records"][0]["comparison_identity"]
    assert artifact["p10"]["execution"][0]["ordinal"] == 0
    assert artifact["p10"]["exits"][0]["ordinal"] == 0
    assert len(artifact["p9"]["decision_records"]) == 1
    assert all(record["event_type"] != "EXECUTEDDECISION" for record in artifact["p9"]["decision_records"])
    assert any(record["event_type"] == "EXECUTEDDECISION" for record in artifact["p10"]["execution"])
    assert artifact["scope"]["generated_at"] == "2026-01-05T06:00:00Z"
    assert artifact["scope"]["replay_input"]["provenance"] == {"source": "fixture"}
    assert artifact == build_parity_artifact(result, request={"session_date": "2026-01-05"})
    assert strategy_payload_hash(result["strategy"])[1] == artifact["strategy"]["payload_hash"]


def test_missing_required_diagnostics_mark_coverage_incomplete():
    result = {"strategy": {}, "vehicles": ["futures"], "bars_seen": 1, "events": [{"event_type": "CANDIDATEDECISION", "decision_id": "d1", "session_date": "2026-01-05"}], "ledger": {}}
    artifact = build_parity_artifact(result, request={"session_date": "2026-01-05"})
    assert artifact["coverage"]["complete"] is False
    assert artifact["coverage"]["issues"]


def test_p9_preserves_missing_pcr_diagnostics() -> None:
    result = {
        "strategy": {"name": "paper", "version": "v1", "config_hash": "cfg"},
        "vehicles": ["futures"],
        "bars_seen": 3,
        "events": [{
            "event_type": "CANDIDATEDECISION",
            "decision_at": "2026-01-05T10:01:00+05:30",
            "session_date": "2026-01-05",
            "cell": "vwap_zone",
            "decision_stage": "policy",
            "outcome": "rejected",
            "reason": "admission_rejected",
            "score_factors": {"pcr_extreme": None, "volume_drying": True},
            "feature_values": {"pcr_at_event": None, "vix_at_event": 15.0},
            "pcr_at_event": None,
            "vix_at_event": 15.0,
        }],
    }

    record = build_parity_artifact(result, request={"session_date": "2026-01-05"})["p9"]["decision_records"][0]

    assert record["vehicle"] == "futures"
    assert record["values"]["pcr"] is None
    assert record["values"]["vix"] == 15.0
    assert record["values"]["confirmation_count"] == 1
    assert "pcr_extreme" in record["values"]["missing_inputs"]
    assert "pcr_at_event" in record["values"]["missing_inputs"]


def test_shared_fixture_contract_and_strategy_fingerprint():
    fixture = json.loads(Path("tests/fixtures/ftx_producer_parity_v1.json").read_text())
    assert fixture["artifact_type"] == "ftx_producer_parity"
    assert fixture["p9"]["decision_records"] == []
    assert strategy_payload_hash(fixture["strategy"]["payload"])[1] == fixture["strategy"]["payload_hash"]
