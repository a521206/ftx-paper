from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ftx_paper.core.live_decision import IndependentLiveDecisionEngine
from ftx_paper.strategy.config import (
    AFTERNOON_CELL_POLICIES,
    AFTERNOON_ENTRY_MINUTES,
    MORNING_CELL_POLICIES,
    MORNING_ENTRY_MINUTES,
    DEFAULT_CONFIG,
    Session,
)


IST = ZoneInfo("Asia/Kolkata")


def _engine() -> IndependentLiveDecisionEngine:
    return IndependentLiveDecisionEngine(version="test", config_hash="test", capital=2_500_000.0)


def test_session_entry_gates_are_half_open_at_both_boundaries() -> None:
    engine = _engine()
    market_open = datetime(2026, 1, 1, 9, 15, tzinfo=IST)
    for start, end in (MORNING_ENTRY_MINUTES, AFTERNOON_ENTRY_MINUTES):
        assert engine._session_for_time(market_open + timedelta(minutes=start)) is not None
        boundary = market_open + timedelta(minutes=end)
        assert engine._session_for_time(boundary) is Session.OUTSIDE


def test_configured_cell_matrix_has_fixed_session_directions() -> None:
    morning = {(item.cell): item.direction.value.lower() for item in MORNING_CELL_POLICIES}
    afternoon = {(item.cell): item.direction.value.lower() for item in AFTERNOON_CELL_POLICIES}

    assert morning == {
        "session_low+or_low": "buy",
        "vwap_zone+session_low+or_low": "buy",
        "session_high+or_high": "sell",
        "vwap_zone+or_low": "sell",
    }
    assert afternoon == {
        "session_high+or_high": "buy",
        "or_low": "buy",
        "vwap_zone+prior_day_high": "sell",
    }


def test_policy_lookup_is_session_specific_for_shared_cell() -> None:
    engine = _engine()
    assert engine._cell_policies[(Session.MORNING, "session_high+or_high")].direction.value == "SELL"
    assert engine._cell_policies[(Session.AFTERNOON, "session_high+or_high")].direction.value == "BUY"


def test_policy_manifest_is_separate_from_runtime_snapshot() -> None:
    assert "cell_policies" not in DEFAULT_CONFIG.as_dict()
    assert DEFAULT_CONFIG.policy_manifest()[0] == {
        "session": "morning",
        "cell": "session_low+or_low",
        "direction": "buy",
    }
