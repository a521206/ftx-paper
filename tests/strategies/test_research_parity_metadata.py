from __future__ import annotations

from ftx_paper.strategies.research.parity_artifacts import (
    _comparison_value,
    compare_parity_manifests,
)


def test_research_parity_scope_ignores_contract_metadata() -> None:
    left = {
        "contract": {"id": "producer_replay_parity", "version": 1},
        "scope": {"timezone": "Asia/Kolkata", "replay_input": {
            "provenance": [{"symbol": "NIFTY-I", "expiry": "2026-01", "series": "I"}],
        }},
    }
    right = {
        "contract": {"id": "producer_replay_parity", "version": 1},
        "scope": {"timezone": "Asia/Kolkata", "replay_input": {
            "provenance": [{"symbol": "NIFTY", "expiry": "2026-02", "series": "II"}],
        }},
    }

    assert _comparison_value(left["scope"]) == _comparison_value(right["scope"])
    assert compare_parity_manifests(left, right)["status"] != "MISMATCHED"


def test_research_comparison_value_ignores_producer_extensions() -> None:
    assert _comparison_value({
        "direction": "long", "producer_extensions": {"producer": "paper"},
    }) == {"direction": "long"}
