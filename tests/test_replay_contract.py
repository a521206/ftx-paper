from __future__ import annotations

import pytest

from ftx_paper.contracts.replay import ReplayRequest, ReplayResult


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
