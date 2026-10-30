from ftx_paper.runtime.replay_projection import normalize_trade
from ftx_paper.strategies.research.parity_artifacts import build_parity_artifact, read_parity_artifact
from scripts.orderflow.api_replay_regression import _actual_metrics


def _trade(entry_timestamp: str) -> dict[str, object]:
    return {
        "entry_timestamp": entry_timestamp,
        "entry_price": 100.0,
        "stop_price": 99.0,
        "quantity": 1,
        "side": "BUY",
    }


def test_normalize_trade_preserves_session_window() -> None:
    assert normalize_trade(_trade("2026-01-01T09:15:00+05:30"))["session"] == "morning"
    assert normalize_trade(_trade("2026-01-01T13:15:00+05:30"))["session"] == "afternoon"
    assert normalize_trade(_trade("2026-01-01T12:00:00+05:30"))["session"] == "unknown"


def test_exit_execution_and_ledger_project_to_valid_artifact() -> None:
    decision = {
        "event_type": "CANDIDATEDECISION",
        "decision_id": "entry-1",
        "stage": "candidate",
        "outcome": "accepted",
        "decision_at": "2026-01-01T03:45:00Z",
        "neutral_event_key": "candidate:entry-1",
    }
    artifact = build_parity_artifact(
        {
            "run_id": "run-1",
            "execution_scope": "replay",
            "ledger_scope": "replay",
            "bars_seen": 1,
            "strategy": {"version": "test"},
            "metadata": {},
        },
        request={"session_date": "2026-01-01", "vehicles": ["futures"]},
        layer_records={
            "decision": [decision],
            "execution_settlement": [{
                "event_type": "EXITDECISION",
                "decision_id": "entry-1",
                "entry_decision_id": "entry-1",
                "decision_at": "2026-01-01T03:46:00Z",
                "neutral_event_key": "exit:entry-1",
            }],
            "portfolio_ledger": [{
                "event_type": "EXIT_SETTLEMENT",
                "kind": "exit_settlement",
                "decision_id": "entry-1",
                "timestamp": "2026-01-01T03:46:00Z",
                "neutral_event_key": "settlement:entry-1",
            }],
        },
    )

    validated = read_parity_artifact(artifact)
    assert validated["layers"]["execution_settlement"][0]["kind"] == "exit"
    assert validated["layers"]["portfolio_ledger"][0]["kind"] == "exit_settlement"


def test_regression_script_accepts_current_replay_result_schema() -> None:
    result = {
        "run_id": "run-1",
        "schema_version": 1,
        "execution_scope": "replay",
        "ledger_scope": "replay",
        "summary": {},
        "events": [],
        "shadow_trades": [],
        "shadow_ledger": [],
        "parity_artifact": None,
    }

    assert _actual_metrics(result) == {
        "setup_candidates": None,
        "entries_filled": None,
        "location_interactions": None,
        "total_location_interactions": None,
        "vwap_interactions": None,
        "gross_reward_points": None,
        "achieved_risk_reward": None,
    }
