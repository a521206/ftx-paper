from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from ftx_paper.contracts.decision_artifacts import DecisionTrace, FuturesExecutionPlan, SyntheticSettlement, TradePlan


AT = datetime(2026, 1, 5, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata"))


def make_plan(**changes):
    values = dict(decision_id="d-1", sequence=4, decision_at=AT, cell="VWAP", direction="LONG",
                  entry_price=100.0, setup_type="A", score=7.0, score_factors={"trend": 1},
                  vix=14.0, pcr=0.9, stop=98.0, requested_quantity=2, final_quantity=2,
                  outcome="accepted", rejection_reason=None, exit_mode="signal", exit_result=None)
    values.update(changes)
    return TradePlan(**values)


def test_trade_plan_round_trips_and_rejects_missing_or_obsolete_fields():
    plan = make_plan()
    assert TradePlan.from_dict(plan.to_dict()) == plan
    with pytest.raises(ValueError, match="missing required field"):
        TradePlan.from_dict({"decision_id": "d-1"})
    with pytest.raises(ValueError, match="missing required field"):
        TradePlan.from_dict(plan.to_dict() | {"cell": None})
    with pytest.raises(ValueError, match="unexpected decision artifact fields"):
        TradePlan.from_dict(plan.to_dict() | {"time": "10:00"})


def test_trade_plan_rejects_naive_timestamp_and_negative_quantity():
    with pytest.raises(ValueError, match="timezone-aware"):
        make_plan(decision_at=datetime(2026, 1, 5, 10, 0))
    with pytest.raises(ValueError, match="cannot be negative"):
        make_plan(requested_quantity=-1)
    with pytest.raises(ValueError, match="cannot be negative"):
        make_plan(final_quantity=-1)


def test_accepted_plan_and_policy_rejection_have_typed_outcomes():
    accepted = make_plan()
    rejected = make_plan(outcome="policy rejection", final_quantity=0, rejection_reason="outside_session")
    assert FuturesExecutionPlan(accepted, "NIFTY-FUT", "o-1", "signal").to_dict()["entry_order_id"] == "o-1"
    assert rejected.rejection_reason == "outside_session"
    with pytest.raises(ValueError):
        FuturesExecutionPlan(rejected, "NIFTY-FUT", "o-2", "signal")


def test_sizing_rejection_requires_reason_and_zero_quantity():
    rejected = make_plan(outcome="sizing rejection", requested_quantity=1, final_quantity=0, rejection_reason="risk_budget")
    assert rejected.final_quantity == 0
    with pytest.raises(ValueError):
        make_plan(outcome="sizing rejection", final_quantity=0)


def test_synthetic_unavailable_is_explicit_and_never_zero_pnl():
    settlement = SyntheticSettlement("d-1", "missing_entry_premium", None, AT, AT, None, None, None, None)
    assert settlement.to_dict()["status"] == "missing_entry_premium"
    assert SyntheticSettlement.from_dict(settlement.to_dict()) == settlement
    with pytest.raises(ValueError):
        SyntheticSettlement("d-1", "missing_exit_premium", None, AT, AT, None, None, 0.0, 0.0)


def test_decision_trace_restores_typed_plan():
    trace = DecisionTrace(make_plan(), "accepted", {"policy": "eligible"})
    assert DecisionTrace.from_dict(trace.to_dict()) == trace
