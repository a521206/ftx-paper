from __future__ import annotations

import pytest

from ftx_paper.contracts.replay import ReplayRequest, ReplayResult, detach_replay_value
from ftx_paper.runtime.replay_worker import ReplayWorker


def _request() -> dict[str, object]:
    return {
        "session_date": "2026-09-29",
        "strategy_id": "ftx-paper-orderflow",
        "vehicles": ["futures", "synthetic"],
        "mode": "replay",
        "include_parity_layers": False,
    }


def test_replay_request_rejects_legacy_execution_fields() -> None:
    with pytest.raises(ValueError, match="unsupported replay request fields"):
        ReplayRequest.from_mapping({**_request(), "execution_mode": "diagnostic"})


def test_replay_request_rejects_shadow_mode() -> None:
    with pytest.raises(ValueError, match="mode must be 'replay'"):
        ReplayRequest.from_mapping({**_request(), "mode": "shadow"})


def test_replay_request_defaults_to_lightweight_capture() -> None:
    request = ReplayRequest.from_mapping(_request())
    assert request.capture_mode == "summary"
    assert request.to_dict()["capture_mode"] == "summary"
    assert ReplayRequest.from_mapping({**_request(), "capture_mode": "full"}).capture_mode == "full"
    with pytest.raises(ValueError, match="capture_mode"):
        ReplayRequest.from_mapping({**_request(), "capture_mode": "invalid"})


def test_replay_result_requires_only_canonical_fields() -> None:
    result = {
        "run_id": "run-1",
        "schema_version": 1,
        "execution_scope": "replay",
        "ledger_scope": "replay",
        "summary": {"setup_candidates": 0, "location_interactions": 0, "entries_filled": 0,
                     "gross_reward_points": 0.0, "achieved_risk_reward": None},
        "events": [],
        "shadow_trades": [],
        "shadow_ledger": [],
        "authoritative_trades": [],
        "authoritative_ledger": [],
        "parity_artifact": None,
    }
    assert ReplayResult.from_mapping(result).to_dict() == result


def test_replay_result_requires_shadow_trades() -> None:
    result = {
        "run_id": "run-1",
        "schema_version": 1,
        "execution_scope": "replay",
        "ledger_scope": "replay",
        "summary": {"setup_candidates": 0, "location_interactions": 0, "entries_filled": 0,
                     "gross_reward_points": 0.0, "achieved_risk_reward": None},
        "events": [],
        "shadow_ledger": [],
        "authoritative_trades": [],
        "authoritative_ledger": [],
        "parity_artifact": None,
    }
    with pytest.raises(ValueError, match="missing replay result fields: shadow_trades"):
        ReplayResult.from_mapping(result)


def test_replay_result_rejects_ambiguous_aliases() -> None:
    result = {
        "run_id": "run-1", "schema_version": 1, "execution_scope": "replay", "ledger_scope": "replay",
        "summary": {"setup_candidates": 0, "location_interactions": 0, "entries_filled": 0,
                     "gross_reward_points": 0.0, "achieved_risk_reward": None},
        "events": [], "shadow_trades": [], "shadow_ledger": [],
        "authoritative_trades": [], "authoritative_ledger": [], "parity_artifact": None,
        "trades": [],
    }
    with pytest.raises(ValueError, match="unsupported replay result fields"):
        ReplayResult.from_mapping(result)


def test_replay_result_round_trips_versioned_paper_report() -> None:
    result = {
        "run_id": "run-1", "schema_version": 1, "execution_scope": "replay", "ledger_scope": "replay",
        "summary": {"setup_candidates": 0, "location_interactions": 0, "entries_filled": 0,
                     "gross_reward_points": 0.0, "achieved_risk_reward": None},
        "events": [], "shadow_trades": [], "shadow_ledger": [],
        "authoritative_trades": [], "authoritative_ledger": [], "parity_artifact": None,
        "report": {"schema_version": 1, "report_type": "paper_orderflow", "actions": []},
    }
    assert ReplayResult.from_mapping(result).to_dict() == result


@pytest.mark.parametrize("field, value, message", [
    ("report_type", "canonical_orderflow", "unsupported replay report type"),
    ("schema_version", 2, "unsupported replay report schema version"),
])
def test_replay_result_rejects_incompatible_report_envelopes(
    field: str, value: object, message: str,
) -> None:
    report = {"schema_version": 1, "report_type": "paper_orderflow", "actions": []}
    report[field] = value
    result = {
        "run_id": "run-1", "schema_version": 1, "execution_scope": "replay", "ledger_scope": "replay",
        "summary": {"setup_candidates": 0, "location_interactions": 0, "entries_filled": 0,
                     "gross_reward_points": 0.0, "achieved_risk_reward": None},
        "events": [], "shadow_trades": [], "shadow_ledger": [],
        "authoritative_trades": [], "authoritative_ledger": [], "parity_artifact": None,
        "report": report,
    }
    with pytest.raises(ValueError, match=message):
        ReplayResult.from_mapping(result)


def test_detached_replay_value_rejects_removed_fields_recursively() -> None:
    with pytest.raises(ValueError, match="legacy replay fields"):
        detach_replay_value({"nested": [{"canonical_summary": {}}]})


def test_replay_summary_exposes_metric_lineage_without_changing_canonical_counts() -> None:
    events = [
        {"reason": "setup_candidate_formed", "opportunity_id": "setup-1"},
        {"reason": "setup_candidate_formed", "opportunity_id": "setup-1"},
        {"reason": "location_interaction_observed", "interaction_id": "i-1", "location": "OR_HIGH", "timeframe": "5m"},
        {"reason": "location_interaction_observed", "interaction_id": "i-2", "location": "VWAP", "timeframe": "1m"},
        {"reason": "location_interaction_observed", "interaction_id": "i-3", "location": "vwap", "timeframe": "5m"},
    ]
    trades = [
        {"vehicle": "futures", "gross_pnl_rs": 100.0, "initial_risk_rs": 50.0},
        {"vehicle": "futures", "gross_pnl_rs": -25.0},
        {"vehicle": "synthetic", "gross_pnl_rs": 999.0, "initial_risk_rs": 1.0},
    ]

    summary = ReplayWorker._build_summary(events, trades)

    assert summary["setup_candidates"] == 1
    assert summary["location_interactions"] == 1
    assert summary["entries_filled"] == 2
    assert summary["achieved_risk_reward"] == 2.0
    assert summary["metric_lineage"] == {
        "setup_candidate_event_count": 2,
        "setup_candidate_unique_count": 1,
        "location_interaction_event_count": 3,
        "location_interaction_unique_count": 3,
        "canonical_location_interaction_unique_count": 1,
        "location_interaction_by_timeframe": {"1m": 1, "5m": 2},
        "location_interaction_by_location": {"OR_HIGH": 1, "VWAP": 2},
        "futures_trade_count": 2,
        "futures_trades_missing_initial_risk": 1,
        "risk_reward_component_count": 1,
    }
