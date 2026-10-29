from datetime import datetime
from zoneinfo import ZoneInfo

from ftx_paper.contracts.decision_artifacts import FuturesExecutionPlan, TradePlan
from ftx_paper.execution.settlement import settle_synthetic_plan


AT = datetime(2026, 1, 5, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
EXIT_AT = datetime(2026, 1, 5, 11, 0, tzinfo=ZoneInfo("Asia/Kolkata"))


def make_plan(**changes) -> FuturesExecutionPlan:
    values = dict(
        decision_id="d-1", sequence=4, decision_at=AT, cell="VWAP", direction="LONG",
        entry_price=100.0, setup_type="A", score=7.0, score_factors={"trend": 1},
        vix=14.0, pcr=0.9, stop=98.0, requested_quantity=2, final_quantity=2,
        outcome="accepted", rejection_reason=None, exit_mode="signal", exit_result=None,
    )
    values.update(changes)
    contract = values.pop("contract", {"expiry": "2026-01-08", "strike": 25000, "ce_symbol": "CE", "pe_symbol": "PE"})
    return FuturesExecutionPlan(
        TradePlan(**values), "NIFTY-FUT", "o-1", "signal",
        contract=contract,
    )


def test_settlement_is_derived_once_from_a_futures_plan() -> None:
    plan = make_plan()
    settlement = settle_synthetic_plan(
        plan, entry_premiums={"ce": 10.0, "pe": 8.0},
        exit_premiums={"ce": 14.0, "pe": 6.0}, exit_at=EXIT_AT,
    )

    assert settlement.status == "settled"
    assert settlement.plan_id == plan.trade_plan.decision_id
    assert settlement.pnl is not None
    assert settlement.pnl < 780.0
    assert settlement.premium_provenance == {"entry": None, "exit": None}


def test_missing_premiums_are_unavailable_and_never_zero_pnl() -> None:
    plan = make_plan()
    missing_entry = settle_synthetic_plan(
        plan, entry_premiums=None, exit_premiums={"ce": 14.0, "pe": 6.0}, exit_at=EXIT_AT,
    )
    missing_exit = settle_synthetic_plan(
        plan, entry_premiums={"ce": 10.0, "pe": 8.0}, exit_premiums=None, exit_at=EXIT_AT,
    )

    assert missing_entry.status == "missing_entry_premium"
    assert missing_exit.status == "missing_exit_premium"
    assert missing_entry.pnl is None and missing_exit.pnl is None


def test_non_finite_premiums_are_missing_and_provenance_is_preserved() -> None:
    settlement = settle_synthetic_plan(
        make_plan(),
        entry_premiums={"ce": float("nan"), "pe": 8.0, "provenance": "entry-feed"},
        exit_premiums={"ce": 14.0, "pe": 6.0, "provenance": "exit-feed"},
        exit_at=EXIT_AT,
    )

    assert settlement.status == "missing_entry_premium"
    assert settlement.premium_provenance == {"entry": "entry-feed", "exit": "exit-feed"}


def test_invalid_contract_is_explicit() -> None:
    plan = make_plan(contract=None)
    settlement = settle_synthetic_plan(
        plan, entry_premiums={"ce": 10.0, "pe": 8.0},
        exit_premiums={"ce": 14.0, "pe": 6.0}, exit_at=EXIT_AT,
    )

    assert settlement.status == "invalid_contract"
    assert settlement.pnl is None


def test_settlement_does_not_mutate_the_futures_plan() -> None:
    plan = make_plan()
    before = plan.to_dict()
    settle_synthetic_plan(
        plan, entry_premiums={"ce": 10.0, "pe": 8.0},
        exit_premiums={"ce": 14.0, "pe": 6.0}, exit_at=EXIT_AT,
    )

    assert plan.to_dict() == before
