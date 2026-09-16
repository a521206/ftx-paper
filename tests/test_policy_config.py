from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ftx_paper.core.live_decision import IndependentLiveDecisionEngine
from ftx_paper.core.risk_state import RiskGateState
from ftx_paper.core.risk import RiskConfig, RiskEngine
from ftx_paper.capital_config import ResearchCapitalProfile, VehicleLimits
from ftx_paper.capital_context import CapitalRuntimeContext
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


def test_cooldown_segments_reset_every_30_minutes_in_both_sessions() -> None:
    engine = _engine()
    for before, after in (("10:44", "10:45"), ("13:59", "14:00")):
        before_segment = engine._session_segment(
            datetime.fromisoformat(f"2026-01-01T{before}:00+05:30")
        )
        after_segment = engine._session_segment(
            datetime.fromisoformat(f"2026-01-01T{after}:00+05:30")
        )
        assert before_segment != after_segment


def test_configured_cell_matrix_has_fixed_session_directions() -> None:
    morning = {(item.cell): item.direction.value.lower() for item in MORNING_CELL_POLICIES}
    afternoon = {(item.cell): item.direction.value.lower() for item in AFTERNOON_CELL_POLICIES}

    assert morning == {
        "session_low+or_low": "buy",
        "vwap_zone+or_low": "sell",
        "session_high+or_high": "sell",
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


def test_configured_cell_policies_expose_canonical_stability_factors() -> None:
    morning = {item.cell: item.stability for item in MORNING_CELL_POLICIES}
    afternoon = {item.cell: item.stability for item in AFTERNOON_CELL_POLICIES}

    assert morning["session_high+or_high"] == 0.5
    assert morning["vwap_zone+or_low"] == 0.5
    assert afternoon["session_high+or_high"] == 0.5
    assert afternoon["or_low"] == 0.5
    assert afternoon["vwap_zone+prior_day_high"] == 1.0


def test_code_defined_research_profile_is_the_paper_configuration() -> None:
    profile = ResearchCapitalProfile(
        initial_capital=100_000.0,
        max_daily_loss=0.02,
        max_net_directional_lots=3.0,
        risk_per_trade=0.01,
        max_lots=2,
    )
    assert profile.initial_capital == 100_000.0
    assert profile.max_net_directional_lots == 3.0
    assert profile.enabled_vehicles == ("futures", "synthetic")
    assert profile.strategy_version == ""


def test_capital_runtime_context_is_deterministic_and_profile_owned() -> None:
    profile = ResearchCapitalProfile(candidate_id="candidate-a", stability_policy=(("cell", 0.5),))
    first = CapitalRuntimeContext(profile, environment="replay")
    second = CapitalRuntimeContext(profile, environment="replay")
    assert first == second
    assert first.max_net_directional_lots == 8.0
    assert first.profile.stability_for("cell") == 0.5
    assert first.profile == second.profile


def test_risk_gate_is_initialized_from_runtime_context() -> None:
    profile = ResearchCapitalProfile(max_net_directional_lots=3.0)
    context = CapitalRuntimeContext(profile, environment="replay")
    gate = RiskGateState.from_context(context)
    assert gate.max_net_directional_lots == 3.0


def test_selected_sessions_are_enforced_by_the_decision_engine() -> None:
    profile = ResearchCapitalProfile(selected_sessions=("afternoon",))
    engine = IndependentLiveDecisionEngine(
        version="test", config_hash="test", capital=2_500_000.0,
        capital_context=CapitalRuntimeContext(profile),
    )
    assert not engine._session_selected(Session.MORNING)
    assert engine._session_selected(Session.AFTERNOON)


def test_profile_vehicle_limits_drive_contextual_risk_engine() -> None:
    profile = ResearchCapitalProfile(
        vehicle_limits=(("futures", VehicleLimits(1, 10_000.0)), ("synthetic", VehicleLimits(1, 10_000.0))),
    )
    context = CapitalRuntimeContext(profile)
    decision = RiskEngine(
        config=RiskConfig(contract_lot_size=1), context=context,
    ).size(capital=100_000, equity=100_000, peak_equity=100_000, entry=100, stop=99)
    assert decision.risk_ceiling == 1
    assert decision.margin_lots == 8


def test_policy_manifest_is_separate_from_runtime_snapshot() -> None:
    assert "cell_policies" not in DEFAULT_CONFIG.as_dict()
    assert DEFAULT_CONFIG.policy_manifest()[0] == {
        "session": "morning",
        "cell": "session_low+or_low",
        "direction": "buy",
        "exit_mode": "signal",
        "stability": 1.0,
    }
