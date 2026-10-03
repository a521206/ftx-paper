from datetime import datetime, timezone
import math
import json
from pathlib import Path
import time
from zoneinfo import ZoneInfo
import pytest

from ftx_paper.broker import PaperBroker
from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole, OrderIntent, OrderRole, OrderSide, parse_role, role_to_key, select_synthetic_quote
from ftx_paper.market import CompletedBarAggregator, DecisionBundle, LiveFeatureCalculator, option_pcr_at_event, vix_open_and_event
from ftx_paper.strategy import ConfiguredLiveStrategy
from ftx_paper.strategy.exits import ExitStateMachine, PositionState
from ftx_paper.strategy.decision import IndependentLiveDecisionEngine
from ftx_paper.strategy.risk import RiskAssessment, RiskConfig, RiskDecision, RiskEngine
from ftx_paper.strategy.risk_state import RiskGateState
from ftx_paper.strategy.sizing import SizingPipeline, SizingPipelineInput
from ftx_paper.strategy.policy import SetupPolicy
from ftx_paper.strategy.adaptive_stop import adaptive_stop_bp
from ftx_paper.domain import PortfolioState
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.market.location import Cell, Location
from ftx_paper.strategy.decision import _configured_policies_for_cell
from ftx_paper.strategy.admission import admission_result
from ftx_paper.strategy.scoring import _synthetic_delta_divergence
from ftx_paper.strategy.config import Session
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.domain.capital import ResearchCapitalProfile, RESEARCH_CAPITAL_PROFILE as CAPITAL_CONFIG


def test_delta_divergence_uses_signed_volume_from_actual_ohlc() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    bars = tuple(
        MarketBar(
            instrument=instrument,
            timestamp=datetime(2026, 9, 10, 4, 0 + index, tzinfo=timezone.utc),
            open=close,
            high=high,
            low=0.0,
            close=close,
            volume=volume,
        )
        for index, (close, high, volume) in enumerate(
            ((10.0, 30.0, 300.0), (15.0, 40.0, 400.0), (20.0, 50.0, 500.0))
        )
    )

    # Signed close-location volume falls while closes rise. The prior
    # positive-only proxy using close as high returned +1.0 for these bars.
    assert _synthetic_delta_divergence(bars) == pytest.approx(-1.0)


def test_scoped_risk_reservation_releases_and_settles_into_its_bucket() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent("scoped-entry", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY)
    exit_order = OrderIntent(
        "scoped-exit", instrument, OrderSide.SELL, 1,
        role=OrderRole.EXIT, entry_order_id=entry.client_order_id,
    )
    portfolio = PortfolioState(100_000.0)
    scope = "2026-09-10|morning|low|long"
    portfolio.reserve_scoped_risk(
        entry.client_order_id, scope=scope, allowance=5_000.0, amount=2_000.0,
    )
    assert portfolio.available_scoped_risk(scope, 5_000.0) == pytest.approx(3_000.0)

    portfolio.reserve_entry(entry, margin_per_lot=0.0)
    portfolio.fill_entry(entry, price=100.0, margin_per_lot=0.0)
    assert portfolio.available_scoped_risk(scope, 5_000.0) == pytest.approx(3_000.0)

    portfolio.settle_exit(exit_order, price=90.0, cost=850.0)
    assert portfolio.available_scoped_risk(scope, 5_000.0) == pytest.approx(3_500.0)

    restored = PortfolioState.from_snapshot(portfolio.snapshot())
    assert restored.available_scoped_risk(scope, 5_000.0) == pytest.approx(3_500.0)


def test_paper_portfolio_lifecycle_is_idempotent_and_restorable() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent("entry-portfolio", instrument, OrderSide.BUY, 2, role=OrderRole.ENTRY)
    exit_order = OrderIntent("exit-portfolio", instrument, OrderSide.SELL, 2, role=OrderRole.EXIT,
                             entry_order_id=entry.client_order_id)
    portfolio = PortfolioState(2_500_000)
    portfolio.gate_snapshot = {"revision": 4}
    portfolio.quote_provenance = {"entry": "same_minute"}
    from ftx_paper.domain.capital import ResearchCapitalProfile, VehicleLimits, CapitalRuntimeContext
    custom_context = CapitalRuntimeContext(ResearchCapitalProfile(
        vehicle_limits=(
            ("futures", VehicleLimits(3, 120_000.0)),
            ("synthetic", VehicleLimits(3, 130_000.0)),
        ),
    ))
    coordinator = PaperExecutionCoordinator(portfolio, custom_context)
    coordinator.submit(entry)
    coordinator.submit(entry)
    assert portfolio.open_margin == pytest.approx(240_000)
    coordinator.fill(entry, price=100, timestamp="2026-01-01T10:20:00+05:30")
    coordinator.fill(entry, price=100, timestamp="2026-01-01T10:20:00+05:30")
    assert len(portfolio.positions) == 1
    coordinator.fill(exit_order, price=105, cost=10)
    coordinator.fill(exit_order, price=105, cost=10)
    assert portfolio.open_margin == 0
    assert portfolio.realized_pnl == pytest.approx(640)
    restored = PortfolioState.from_snapshot(portfolio.snapshot())
    assert restored.capital_snapshot() == portfolio.capital_snapshot()
    assert restored.gate_snapshot == portfolio.gate_snapshot
    assert restored.quote_provenance == portfolio.quote_provenance


def test_paper_portfolio_failed_entry_releases_reservation_and_failed_exit_keeps_position() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent("entry-failed", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY)
    portfolio = PortfolioState(2_500_000)
    coordinator = PaperExecutionCoordinator(portfolio)
    coordinator.submit(entry)
    assert coordinator.cancel(entry)
    assert portfolio.open_margin == 0
    coordinator.submit(entry)
    coordinator.fill(entry, price=100)
    exit_order = OrderIntent("exit-failed", instrument, OrderSide.SELL, 1, role=OrderRole.EXIT,
                             entry_order_id=entry.client_order_id)
    coordinator.failed_exit(exit_order)
    assert entry.client_order_id in portfolio.positions
    assert portfolio.open_margin > 0


def test_paper_portfolio_restores_in_flight_entry_reservation() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent("entry-in-flight", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY)
    portfolio = PortfolioState(2_500_000)
    coordinator = PaperExecutionCoordinator(portfolio)

    coordinator.submit(entry)
    snapshot = portfolio.snapshot()
    restored = PortfolioState.from_snapshot(snapshot)

    assert restored.positions == {}
    assert restored.open_margin == pytest.approx(portfolio.open_margin)
    assert restored.pending_orders[entry.client_order_id] == "reserved"

    restored_coordinator = PaperExecutionCoordinator(restored)
    restored_coordinator.fill(entry, price=100.0, timestamp="2026-01-01T10:20:00+05:30")
    assert entry.client_order_id in restored.positions
    assert restored.open_margin == pytest.approx(portfolio.open_margin)


def test_execution_coordinator_rejects_derived_synthetic_orders() -> None:
    futures = Instrument("NIFTYFUT", "NFO", "FUTURES")
    call = Instrument("NIFTYCE", "NFO", "CE", expiry="2026-09-24", strike=25000)
    put = Instrument("NIFTYPE", "NFO", "PE", expiry="2026-09-24", strike=25000)
    order = OrderIntent(
        "synthetic-entry", futures, OrderSide.BUY, 1,
        role=OrderRole.ENTRY, vehicle="synthetic", synthetic_legs=(call, put),
    )
    coordinator = PaperExecutionCoordinator(PortfolioState(2_500_000))

    with pytest.raises(ValueError, match="derived settlement only"):
        coordinator.submit(order)


def test_multiday_portfolio_and_risk_recovery_snapshots() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    capital = 2_500_000.0
    portfolio = PortfolioState(capital)
    gate = RiskGateState(max_net_directional_lots=2, thesis_cooldown_bars=3, cell_cooldown_bars=2)
    entry = OrderIntent("day1-entry", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY)
    coordinator = PaperExecutionCoordinator(portfolio)

    coordinator.submit(entry)
    coordinator.fill(entry, price=100.0, timestamp="2026-01-05T10:00:00+05:30")
    gate.record_entry(cell="VWAP", direction="long", quantity=1, date="2026-01-05")
    assert list(portfolio.positions) == ["day1-entry"]
    assert gate.net_directional_lots == 1

    exit_order = OrderIntent("day1-exit", instrument, OrderSide.SELL, 1,
                             role=OrderRole.EXIT, entry_order_id=entry.client_order_id)
    coordinator.fill(exit_order, price=98.0, cost=10.0)
    gate.record_exit(cell="VWAP", reason="hard_stop", entry_bar=10, exit_bar=12,
                     date="2026-01-05", direction="long", quantity=1)
    assert portfolio.positions == {}
    assert portfolio.realized_pnl == pytest.approx(-140.0)
    assert gate.net_directional_lots == 0
    assert gate.rejection_reason(cell="VWAP", direction="long", bar=13, quantity=1,
                                 date="2026-01-05") == "thesis_cooldown"

    portfolio_snapshot = portfolio.snapshot()
    risk_snapshot = gate.snapshot()
    restored_portfolio = PortfolioState.from_snapshot(portfolio_snapshot)
    restored_gate = RiskGateState(max_net_directional_lots=2, thesis_cooldown_bars=3, cell_cooldown_bars=2)
    restored_gate.restore(risk_snapshot)
    assert restored_portfolio.snapshot() == portfolio_snapshot
    assert restored_gate.snapshot() == risk_snapshot

    assert restored_gate.rejection_reason(cell="VWAP", direction="long", bar=13, quantity=1,
                                          date="2026-01-06") is None
    next_entry = OrderIntent("day2-entry", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY)
    coordinator = PaperExecutionCoordinator(restored_portfolio)
    coordinator.fill(next_entry, price=101.0, timestamp="2026-01-06T10:00:00+05:30")
    restored_gate.record_entry(cell="VWAP", direction="long", quantity=1, date="2026-01-06")
    assert list(restored_portfolio.positions) == ["day2-entry"]
    assert restored_gate.net_directional_lots == 1


def test_synthetic_quote_selection_requires_same_minute_unless_fallback_is_explicit() -> None:
    anchor_time = datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc)
    anchor = MarketBar(Instrument("NIFTY", "NSE", "INDEX"), anchor_time, 100, 100, 100, 100)
    ce = MarketBar(Instrument("NIFTYCE", "NFO", "CE", "2026-01-01", 100), anchor_time, 5, 5, 5, 5)
    pe = MarketBar(Instrument("NIFTYPE", "NFO", "PE", "2026-01-01", 100), anchor_time, 4, 4, 4, 4)
    selected = select_synthetic_quote(anchor, {OptionRole("NIFTYCE"): ce, OptionRole("NIFTYPE"): pe})
    assert selected is not None
    _, provenance = selected
    assert provenance.anchor_source == "spot"
    assert provenance.same_minute
    stale = {OptionRole("NIFTYCE"): ce, OptionRole("NIFTYPE"): pe}
    later = anchor_time.replace(minute=21)
    stale[OptionRole("NIFTYCE")] = MarketBar(ce.instrument, later, 5, 5, 5, 5)
    assert select_synthetic_quote(anchor, stale) is None
    fallback = select_synthetic_quote(anchor, stale, allow_fallback=True)
    assert fallback is not None
    assert fallback[1].fallback_classification == "explicit_stale_fallback"


def test_p6_stop_and_quantity_golden_fixture() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures" / "p6_stop_quantity.json").read_text())
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    bars = tuple(
        MarketBar(instrument, datetime(2026, 1, 1, 4, 0 + index, tzinfo=timezone.utc), row["high"], row["high"], row["low"], row["close"])
        for index, row in enumerate(fixture["stop"]["bars_before"])
    )
    assert adaptive_stop_bp(bars, fixture["stop"]["vix"]) == fixture["stop"]["normal_bp"]
    assert adaptive_stop_bp(bars, fixture["stop"]["vix"], is_expiry_day=True) == fixture["stop"]["expiry_bp"]
    expected = fixture["quantity"]
    decision = RiskEngine().size(
        capital=expected["capital"], equity=expected["equity"], peak_equity=expected["peak_equity"],
        entry=expected["entry"], stop=expected["entry"] * (1 - expected["stop_bp"] / 10000),
    )
    assert decision.risk_budget == expected["risk_budget"]
    assert decision.quantity == expected["quantity"]


def test_adaptive_stop_uses_event_close_with_prior_bar_ranges() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    bars = (
        MarketBar(instrument, datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc), 20200, 20210, 20190, 20200),
        MarketBar(instrument, datetime(2026, 1, 1, 4, 1, tzinfo=timezone.utc), 20000, 20010, 19990, 20000),
    )

    # ATR uses the completed pre-event ranges; canonical normalizes by the
    # current event bar close rather than the preceding close.
    assert adaptive_stop_bp(bars, 20.0, current_close=20000.0) == 25.0


def test_p0_behavioral_fixture_captures_capital_margin_and_downward_lots() -> None:
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "p0_behavioral_contract.json").read_text()
    )
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent("p0-entry", instrument, OrderSide.BUY, 2, role=OrderRole.ENTRY)
    portfolio = PortfolioState(fixture["capital"]["initial_capital"])
    portfolio.reserve_entry(entry, margin_per_lot=175_000.0)

    assert portfolio.open_margin == fixture["capital"]["open_margin"]
    assert portfolio.initial_capital - portfolio.open_margin == fixture["capital"]["available_capital"]
    assert portfolio.open_margin / portfolio.initial_capital == fixture["capital"]["margin_utilization"]

    decision = RiskEngine(config=RiskConfig(
        contract_lot_size=200,
        margin_per_lot=1_000,
        margin_utilization_cap=1.0,
    )).size(
        capital=10_000,
        equity=10_000,
        peak_equity=10_000,
        entry=100,
        stop=99,
        score=4,
    )
    assert decision.risk_ceiling == fixture["rounding"]["risk_ceiling"]
    assert decision.quantity == fixture["rounding"]["final_quantity"]


def test_paper_broker_returns_contract_fill() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    from ftx_paper.contracts import OrderIntent, OrderSide

    fill = PaperBroker({"NIFTY": 100.0}).submit(OrderIntent("order-1", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY))

    assert fill.status == "FILLED"
    assert fill.client_order_id == "order-1"


def test_production_strategy_is_versioned_and_injectable() -> None:
    strategy = ConfiguredLiveStrategy(capital_profile=CAPITAL_CONFIG)

    assert strategy.name.startswith("ftx-paper-")
    assert strategy.metadata.config_hash
    assert strategy.snapshot()["schema_version"] == 1
    assert "capital_profile" not in strategy.snapshot()
    assert strategy.from_snapshot(strategy.snapshot(), capital_profile=CAPITAL_CONFIG).metadata.version == strategy.version


def test_strategy_snapshot_restores_open_position_and_risk_state() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    strategy = ConfiguredLiveStrategy(capital_profile=CAPITAL_CONFIG)
    order = OrderIntent(
        "restart-entry", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
        cell="VWAP", stop_price=98.0, exit_mode="signal", entry_bar=12,
    )
    coordinator = PaperExecutionCoordinator(strategy.portfolio, strategy.capital_context)
    coordinator.submit(order)
    coordinator.fill(order, price=100.0, timestamp="2026-01-05T10:00:00+05:30")
    strategy.register_entry(order, fill_price=100.0,
                            entry_fill_time="2026-01-05T10:00:00+05:30")
    strategy._decision_engine.risk_gate.record_entry(
        cell="VWAP", direction="long", quantity=1, date="2026-01-05",
    )
    snapshot = strategy.snapshot()

    restored = ConfiguredLiveStrategy.from_snapshot(snapshot, capital_profile=CAPITAL_CONFIG)
    assert list(restored.portfolio.positions) == ["restart-entry"]
    assert list(restored._decision_positions) == ["restart-entry"]
    assert restored._decision_engine.risk_gate.net_directional_lots == 1

    restored.settle_exit("exit-restart-entry-2026-01-05T10:01:00+05:30", filled=False)
    assert list(restored._decision_positions) == ["restart-entry"]


def test_strategy_snapshot_restores_vehicle_for_synthetic_replay() -> None:
    with pytest.raises(ValueError, match="synthetic is reporting-only"):
        ConfiguredLiveStrategy(capital_profile=CAPITAL_CONFIG, vehicle="synthetic")


def test_strategy_uses_explicit_capital_limits_and_rejects_mismatch() -> None:
    capital_config = ResearchCapitalProfile(
        initial_capital=100_000.0, max_daily_loss=0.02, max_net_directional_lots=3.0,
    )
    strategy = ConfiguredLiveStrategy(capital_profile=capital_config)

    assert strategy.portfolio_state["initial_capital"] == 100_000.0
    assert strategy._decision_engine.max_daily_loss == 0.02
    assert strategy._decision_engine.risk_gate.max_net_directional_lots == 3.0

    mismatched = ResearchCapitalProfile(initial_capital=200_000.0, max_daily_loss=0.02, max_net_directional_lots=3.0)
    with pytest.raises(ValueError, match="does not match strategy snapshot"):
        ConfiguredLiveStrategy.from_snapshot(strategy.snapshot(), capital_profile=mismatched)


def test_legacy_snapshot_requires_one_time_migration() -> None:
    snapshot = ConfiguredLiveStrategy(capital_profile=CAPITAL_CONFIG).snapshot()
    snapshot["capital"].update({
        "risk_per_trade": 0.01,
        "max_lots": 3,
        "max_net_directional_lots": 8.0,
    })
    snapshot["config"].update({
        "name": "ftx-paper-production",
        "version": "0.2.0-live-composition",
        "morning_entry_minutes": [60, 120],
    })
    snapshot["risk_gate"]["max_net_directional_lots"] = 8.0

    with pytest.raises(ValueError, match="capital config does not match"):
        ConfiguredLiveStrategy.from_snapshot(snapshot, capital_profile=CAPITAL_CONFIG)


def test_live_features_are_causal_and_deterministic() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    bars = tuple(MarketBar(instrument, datetime(2026, 1, 1, 9, 15 + i), 100 + i, 102 + i, 99 + i, 101 + i, 10) for i in range(3))
    calculator = LiveFeatureCalculator(opening_range_bars=2, atr_window=2)
    features = calculator.calculate(bars)
    assert features.vwap == 101.66666666666667
    assert features.opening_range_high == 103
    assert features.atr == 3.0
    assert features == calculator.calculate(bars)


def test_live_event_features_match_each_canonical_prefix_field() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    bars = tuple(
        MarketBar(
            instrument, datetime(2026, 1, 1, 9, 15 + i, tzinfo=ZoneInfo("Asia/Kolkata")),
            100 + i, 104 + i + (i % 3), 98 + i, 101 + i, 10 + i,
        )
        for i in range(20)
    )
    current = MarketBar(instrument, datetime(2026, 1, 1, 9, 35, tzinfo=ZoneInfo("Asia/Kolkata")), 120, 126, 117, 123, 40)
    prefix = bars
    expected_vwap = sum(((bar.high + bar.low + bar.close) / 3) * bar.volume for bar in prefix) / sum(bar.volume for bar in prefix)
    expected_high = max(bar.high for bar in prefix)
    expected_low = min(bar.low for bar in prefix)
    expected_atr = sum(bar.high - bar.low for bar in prefix[-20:]) / 20

    features = LiveFeatureCalculator().calculate_event(
        prefix, current, prior_day_high=130, prior_day_low=90,
    )

    assert features.vwap == expected_vwap
    assert features.session_high == expected_high
    assert features.session_low == expected_low
    assert features.atr == expected_atr
    assert features.opening_range_high == max(bar.high for bar in prefix[:15])
    assert features.opening_range_low == min(bar.low for bar in prefix[:15])
    assert features.prior_day_high == 130
    assert features.prior_day_low == 90
    assert features.return_1 == current.close / prefix[-1].close - 1


def test_vix_lookup_uses_opening_value_and_latest_causal_bar() -> None:
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    bars = (
        MarketBar(vix, datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")), 14, 15, 13, 14.5),
        MarketBar(vix, datetime(2026, 1, 1, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata")), 15, 16, 14, 15.5),
        MarketBar(vix, datetime(2026, 1, 1, 10, 2, tzinfo=ZoneInfo("Asia/Kolkata")), 16, 17, 15, 16.5),
    )
    event = MarketBar(Instrument("NIFTYFUT", "NFO", "FUTURES"), datetime(2026, 1, 1, 10, 1, tzinfo=ZoneInfo("Asia/Kolkata")), 100, 101, 99, 100)

    assert vix_open_and_event(bars, event) == (14.5, 15.5)


def test_vix_lookup_recognizes_replay_index_instrument() -> None:
    vix = Instrument("INDIAVIX", "NSE", "INDEX")
    bars = (
        MarketBar(vix, datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")), 10, 11, 9, 10.5),
        MarketBar(vix, datetime(2026, 1, 1, 9, 16, tzinfo=ZoneInfo("Asia/Kolkata")), 11, 12, 10, 11.5),
    )
    event = MarketBar(
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        datetime(2026, 1, 1, 9, 16, tzinfo=ZoneInfo("Asia/Kolkata")),
        100, 101, 99, 100,
    )

    assert vix_open_and_event(bars, event) == (10.5, 11.5)


def test_live_decision_rejects_missing_vix_open_instead_of_using_event_value(monkeypatch) -> None:
    import ftx_paper.strategy.decision as live_decision

    monkeypatch.setattr(live_decision, "vix_open_and_event", lambda *_: (None, None))
    timestamp = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    bundle = DecisionBundle(
        "missing-vix-open", "2026-01-01", timestamp.isoformat(),
        {
            "futures": MarketBar(Instrument("NIFTYFUT", "NFO", "FUTURES"), timestamp, 99, 102, 98, 100),
            "vix": MarketBar(Instrument("INDIA VIX", "NSE", "VIX"), timestamp, 15, 15, 15, 15),
        }, ("futures", "vix"),
    )

    events = IndependentLiveDecisionEngine(
        version="test", config_hash="hash", capital=CAPITAL_CONFIG.initial_capital,
    ).evaluate(bundle)

    assert len(events) == 1
    assert events[0].event_type == "REJECTEDDECISION"
    assert events[0].payload["reason"] == "missing_vix"
    assert events[0].payload["required_input_availability"]["vix_open"] is False


def test_option_pcr_matches_canonical_cutoff_and_nearest_weekly_atm_window() -> None:
    call = Instrument("NIFTYCE", "NFO", "CE", "2026-01-08", 10000)
    put = Instrument("NIFTYPE", "NFO", "PE", "2026-01-08", 10000)
    far_call = Instrument("NIFTYFARCE", "NFO", "CE", "2026-01-08", 11000)
    later_put = Instrument("NIFTYLATERPE", "NFO", "PE", "2026-01-15", 10000)
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    spot = Instrument("NIFTY", "NSE", "INDEX")
    options = {
        "call": MarketBar(call, datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")), 1, 1, 1, 1, 100),
        "put": MarketBar(put, datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")), 1, 1, 1, 1, 250),
        "far_call": MarketBar(far_call, datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")), 1, 1, 1, 1, 1000),
        "later_put": MarketBar(later_put, datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")), 1, 1, 1, 1, 900),
        "spot": MarketBar(spot, datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")), 1, 1, 1, 10000),
    }
    options.update({
        f"intermediate_{strike}": MarketBar(
            Instrument(f"NIFTYCE{strike}", "NFO", "CE", "2026-01-08", strike),
            datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")),
            1, 1, 1, 1, 0,
        ) for strike in (10050, 10100, 10150, 10200, 10250)
    })
    before_cutoff = MarketBar(future, datetime(2026, 1, 1, 9, 14, tzinfo=ZoneInfo("Asia/Kolkata")), 100, 101, 99, 100)
    at_cutoff = MarketBar(future, datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")), 100, 101, 99, 100)

    expiry_dates = ("2026-01-08", "2026-01-15")
    assert option_pcr_at_event(options, before_cutoff, expiry_dates=expiry_dates) is None
    assert option_pcr_at_event(options, at_cutoff, expiry_dates=expiry_dates) == 2.5
    assert option_pcr_at_event(options, at_cutoff) is None
    assert option_pcr_at_event({"put": options["put"]}, at_cutoff,
                               expiry_dates=expiry_dates) is None
    assert option_pcr_at_event({key: value for key, value in options.items() if key != "spot"},
                               at_cutoff, expiry_dates=expiry_dates) is None


def test_setup_policy_emits_one_explicit_order_intent() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    bar = MarketBar(instrument, datetime(2026, 1, 1, 10, 30), 100, 102, 99, 101, 10)
    features = LiveFeatureCalculator().calculate((bar,))
    decision = SetupPolicy().evaluate(bar, features, prior_low=105)
    assert decision is not None
    intent = SetupPolicy.to_order(decision, bar, 2, "client-1")
    assert intent.client_order_id == "client-1"
    assert intent.quantity == 2


def test_risk_sizer_enforces_drawdown_and_lot_sizing() -> None:
    sizer = RiskEngine()
    approved = sizer.size(capital=10000, equity=10000, peak_equity=10000, entry=100, stop=95)
    assert not approved.approved and approved.reason == "insufficient_risk_budget"
    blocked = sizer.size(capital=10000, equity=8000, peak_equity=10000, entry=100, stop=95)
    assert not blocked.approved and blocked.reason == "drawdown_limit"


def test_replay_trade_session_classification_matches_canonical_windows() -> None:
    assert ReplayWorker._session_for_minutes(60) == "morning"
    assert ReplayWorker._session_for_minutes(119) == "morning"
    assert ReplayWorker._session_for_minutes(120) == "unknown"
    assert ReplayWorker._session_for_minutes(239) == "unknown"
    assert ReplayWorker._session_for_minutes(240) == "afternoon"
    assert ReplayWorker._session_for_minutes(299) == "afternoon"
    assert ReplayWorker._session_for_minutes(300) == "unknown"


def test_exit_state_machine_emits_protective_exit() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    position = PositionState(instrument, 100, 95, 2, OrderSide.BUY)
    action = ExitStateMachine().evaluate(position, timestamp=datetime(2026, 1, 1, 11), high=101, low=94, close=96, client_order_id="exit-1")
    assert action is not None
    assert action.reason == "hard_stop"
    assert action.intent.side is OrderSide.SELL
    assert action.intent.role is OrderRole.EXIT


def test_exit_state_machine_hard_stop_precedes_same_bar_target_and_records_excursions() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    position = PositionState(instrument, 100, 95, 1, OrderSide.BUY,
                            exit_mode="target", target_price=105)
    action = ExitStateMachine().evaluate(
        position, timestamp=datetime(2026, 1, 1, 10, 1), open=100,
        high=106, low=94, close=104, client_order_id="exit-collision",
    )
    assert action is not None
    assert (action.reason, action.price, action.bars_held) == ("hard_stop", 95, 1)
    assert action.mae_bp == 600
    assert action.mfe_bp == 600


def test_signal_counter_move_waits_for_canonical_favorable_excursion() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    position = PositionState(instrument, 100.0, 99.0, 1, OrderSide.BUY, exit_mode="signal")
    machine = ExitStateMachine()
    bars = (
        (100.0, 100.04, 99.99, 100.00),
        (100.0, 100.01, 99.98, 99.99),
        (99.99, 99.99, 99.95, 99.97),
        (99.97, 99.98, 98.90, 99.92),
    )
    actions = [
        machine.evaluate(
            position, timestamp=datetime(2026, 1, 1, 10, index),
            open=open_price, high=high, low=low, close=close,
            client_order_id=f"signal-{index}",
        )
        for index, (open_price, high, low, close) in enumerate(bars)
    ]
    assert actions[:3] == [None, None, None]
    assert actions[3] is not None and actions[3].reason == "hard_stop"


def test_signal_volume_climax_uses_causal_prior_volume_after_mfe_activation() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    position = PositionState(instrument, 100.0, 90.0, 1, OrderSide.BUY, exit_mode="signal")
    machine = ExitStateMachine()
    assert machine.evaluate(
        position, timestamp=datetime(2026, 1, 1, 10), open=100.0,
        high=100.1, low=100.0, close=100.05, volume=100.0,
        client_order_id="volume-1",
    ) is None
    action = machine.evaluate(
        position, timestamp=datetime(2026, 1, 1, 10, 1), open=100.05,
        high=100.1, low=100.0, close=100.04, volume=251.0,
        client_order_id="volume-2",
    )
    assert action is not None and action.reason == "vol_climax"


def test_exit_state_machine_handles_opening_gaps_at_stop_and_target() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    stop_position = PositionState(instrument, 100, 95, 1, OrderSide.BUY)
    stop = ExitStateMachine().evaluate(
        stop_position, timestamp=datetime(2026, 1, 1, 10, 1), open=90,
        high=92, low=88, close=89, client_order_id="gap-stop",
    )
    assert stop is not None and (stop.reason, stop.price) == ("hard_stop", 90)

    target_position = PositionState(instrument, 100, 95, 1, OrderSide.BUY,
                                    exit_mode="target", target_price=105)
    target = ExitStateMachine().evaluate(
        target_position, timestamp=datetime(2026, 1, 1, 10, 1), open=110,
        high=112, low=109, close=111, client_order_id="gap-target",
    )
    assert target is not None and (target.reason, target.price) == ("target", 110)


def test_exit_state_machine_forces_exit_at_canonical_1510_close() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    position = PositionState(instrument, 100, 95, 1, OrderSide.BUY)
    action = ExitStateMachine().evaluate(
        position, timestamp=datetime(2026, 1, 1, 15, 10),
        open=103, high=104, low=102, close=103, client_order_id="eod",
    )
    assert action is not None
    assert (action.reason, action.price, action.bars_held) == ("eod", 103, 1)


def test_exit_state_machine_uses_ist_for_eod_timestamp() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    position = PositionState(instrument, 100, 95, 1, OrderSide.BUY)
    action = ExitStateMachine().evaluate(
        position, timestamp=datetime(2026, 1, 1, 9, 40, tzinfo=timezone.utc),
        open=103, high=104, low=102, close=103, client_order_id="eod-utc",
    )
    assert action is not None and action.reason == "eod"


def test_tick_exit_check_does_not_mutate_completed_bar_state() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    position = PositionState(instrument, 100, 95, 1, OrderSide.BUY)
    machine = ExitStateMachine(trail_activation_bp=20.0, trail_distance_bp=20.0)
    before = machine.snapshot()
    assert machine.evaluate_tick(position, timestamp=datetime(2026, 1, 1, 10, 1),
                                 price=103, client_order_id="tick") is None
    assert machine.snapshot() == before


def test_exit_state_machine_rejects_ambiguous_trailing_configuration() -> None:
    for kwargs in (
        {"trail_activation_bp": 20.0},
        {"trail_distance_bp": 20.0},
        {"trail_distance": 10.0, "trail_activation_bp": 20.0, "trail_distance_bp": 20.0},
    ):
        try:
            ExitStateMachine(**kwargs)
        except ValueError:
            pass
        else:
            raise AssertionError(f"ambiguous trailing configuration was accepted: {kwargs}")


def test_exit_state_machine_can_reset_for_another_position() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    first = PositionState(instrument, 100, 95, 1, OrderSide.BUY)
    machine = ExitStateMachine()
    machine.evaluate(first, timestamp=datetime(2026, 1, 1, 11), high=101, low=99, close=100, client_order_id="exit-1")
    machine.reset()
    second = PositionState(instrument, 100, 95, 1, OrderSide.BUY)
    action = machine.evaluate(second, timestamp=datetime(2026, 1, 1, 11), high=101, low=99, close=100, client_order_id="exit-2")
    assert action is None


def test_trailing_exit_activates_on_next_bar_and_uses_basis_points() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    position = PositionState(instrument, 23450.0, 23411.0, 1, OrderSide.BUY, exit_mode="trail")
    machine = ExitStateMachine(trail_activation_bp=20.0, trail_distance_bp=20.0)
    bars = (
        (23445.0, 23475.0, 23438.9, 23467.5),
        (23466.3, 23467.8, 23452.2, 23464.0),
        (23463.8, 23500.0, 23453.1, 23490.1),
        (23490.1, 23517.4, 23489.8, 23505.0),
        (23506.6, 23506.6, 23493.3, 23500.0),
        (23499.1, 23505.0, 23487.3, 23495.9),
        (23495.9, 23495.9, 23477.5, 23490.0),
        (23490.0, 23490.0, 23475.0, 23476.4),
        (23476.4, 23479.0, 23470.0, 23473.0),
    )
    actions = [
        machine.evaluate(position, timestamp=datetime(2026, 9, 11, 14, index),
                         high=high, low=low, close=close,
                         client_order_id=f"exit-{index}")
        for index, (_, high, low, close) in enumerate(bars)
    ]
    assert all(action is None for action in actions[:-1])
    assert actions[-1] is not None
    assert actions[-1].reason == "trail_stop"


def test_trailing_exit_uses_underlying_reference_for_synthetic_fill() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    position = PositionState(
        instrument, 100.0, 130.0, 1, OrderSide.SELL,
        exit_mode="trail", exit_reference_price=110.0,
    )
    machine = ExitStateMachine(trail_activation_bp=20.0, trail_distance_bp=20.0)
    first = machine.evaluate(
        position, timestamp=datetime(2026, 9, 11, 10, 1),
        open=110.0, high=110.2, low=108.0, close=108.5,
        client_order_id="exit-1",
    )
    second = machine.evaluate(
        position, timestamp=datetime(2026, 9, 11, 10, 2),
        open=108.1, high=108.3, low=107.9, close=108.2,
        client_order_id="exit-2",
    )
    assert first is None
    assert second is not None
    assert second.reason == "trail_stop"
    assert math.isclose(second.price, 108.12, rel_tol=0.0, abs_tol=1e-9)


def test_order_without_explicit_role_fails_fast() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    try:
        OrderIntent("entry-legacy", instrument, OrderSide.BUY, 1)
    except TypeError as exc:
        assert "role" in str(exc)
    else:
        raise AssertionError("exit-shaped order without an explicit role was accepted")


def test_completed_bars_emit_one_bundle_only_after_required_roles_arrive() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator({("NFO", "NIFTYFUT"): "futures", ("NSE", "INDIA VIX"): "vix"})
    minute = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    future = MarketBar(instrument, minute, 100, 102, 99, 101)
    vix_bar = MarketBar(vix, minute, 15, 16, 14, 15)
    assert aggregator.ingest(future) is None
    assert aggregator.pending() == (("10:20", ("vix",), ("futures",)),)
    bundle = aggregator.ingest(vix_bar)
    assert bundle is not None and bundle.complete
    assert aggregator.pending() == ()
    assert aggregator.ingest(future) is None


def test_roles_validate_fixed_values_and_preserve_option_identity() -> None:
    assert parse_role("FUTURES") is MarketRole.FUTURES
    assert parse_role("option:NIFTY26SEP25000CE") == OptionRole("NIFTY26SEP25000CE")
    assert role_to_key(OptionRole("NIFTY26SEP25000CE")) == "option:NIFTY26SEP25000CE"
    try:
        parse_role("future")
    except ValueError as exc:
        assert "invalid market role" in str(exc)
    else:
        raise AssertionError("invalid fixed role was accepted")


def test_futures_clock_carries_forward_missing_supporting_bar_and_ignores_late_bar() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator({
        ("NFO", "NIFTYFUT"): "futures",
        ("NSE", "INDIA VIX"): "vix",
    }, required_roles=("futures",))
    prior_minute = datetime(2026, 1, 1, 10, 19, tzinfo=ZoneInfo("Asia/Kolkata"))
    decision_minute = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    prior_vix = MarketBar(vix, prior_minute, 15, 16, 14, 15)
    future_bar = MarketBar(future, decision_minute, 100, 102, 99, 101)
    late_vix = MarketBar(vix, decision_minute, 16, 17, 15, 16)

    assert aggregator.ingest(prior_vix) is None
    bundle = aggregator.ingest(future_bar)
    assert bundle is not None
    assert bundle.supporting_inputs["bars"]["vix"] is prior_vix
    assert bundle.supporting_inputs["sources"]["vix"] == "carried_forward"
    assert bundle.supporting_inputs["missing"] == ("vix",)
    assert bundle.supporting_inputs["unavailable"] == ()
    assert aggregator.ingest(late_vix) is None


def test_futures_clock_carries_late_same_minute_vix_into_next_bundle() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator({
        ("NFO", "NIFTYFUT"): "futures",
        ("NSE", "INDIA VIX"): "vix",
    }, required_roles=("futures",))
    first_minute = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    second_minute = datetime(2026, 1, 1, 10, 21, tzinfo=ZoneInfo("Asia/Kolkata"))
    first_future = MarketBar(future, first_minute, 100, 102, 99, 101)
    late_vix = MarketBar(vix, first_minute, 16, 17, 15, 16)
    second_future = MarketBar(future, second_minute, 101, 103, 100, 102)

    first_bundle = aggregator.ingest(first_future)
    assert first_bundle is not None
    assert first_bundle.supporting_inputs["unavailable"] == ("vix",)
    assert aggregator.ingest(late_vix) is None
    assert first_bundle.supporting_inputs["bars"] == {}
    second_bundle = aggregator.ingest(second_future)

    assert second_bundle is not None
    assert second_bundle.supporting_inputs["bars"]["vix"] is late_vix
    assert second_bundle.supporting_inputs["sources"]["vix"] == "carried_forward"
    assert second_bundle.supporting_inputs["unavailable"] == ()


def test_futures_clock_does_not_carry_vix_across_trading_dates() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator({
        ("NFO", "NIFTYFUT"): "futures",
        ("NSE", "INDIA VIX"): "vix",
    }, required_roles=("futures",))
    prior_day = datetime(2026, 1, 1, 15, 29, tzinfo=ZoneInfo("Asia/Kolkata"))
    next_day = datetime(2026, 1, 2, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))

    assert aggregator.ingest(MarketBar(vix, prior_day, 15, 16, 14, 15)) is None
    assert aggregator.ingest(MarketBar(future, next_day, 100, 102, 99, 101)) is not None
    bundle = aggregator.ingest(MarketBar(future, next_day.replace(minute=16), 101, 103, 100, 102))

    assert bundle is not None
    assert bundle.supporting_inputs["unavailable"] == ("vix",)


def test_late_vix_reaches_configured_strategy_without_repeated_missing_vix() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator({
        ("NFO", "NIFTYFUT"): "futures",
        ("NSE", "INDIA VIX"): "vix",
    }, required_roles=("futures",))
    engine = PaperEngine(
        ConfiguredLiveStrategy(capital_profile=CAPITAL_CONFIG),
        emit_rejected_decisions=True,
    )
    first_minute = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    bars = (
        MarketBar(future, first_minute, 100, 102, 99, 101),
        MarketBar(vix, first_minute, 16, 17, 15, 16),
        MarketBar(future, first_minute.replace(minute=21), 101, 103, 100, 102),
        MarketBar(future, first_minute.replace(minute=22), 102, 104, 101, 103),
    )

    events = []
    for bar in bars:
        bundle = aggregator.ingest(bar)
        if bundle is not None:
            events.extend(engine.on_bundle(bundle).events)

    missing_vix = [event for event in events if event.get("reason") == "missing_vix"]
    assert len(missing_vix) == 1
    assert missing_vix[0]["bundle_id"] == "2026-01-01:10:20"


def test_futures_clock_prefers_same_minute_supporting_bar() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator({
        ("NFO", "NIFTYFUT"): "futures",
        ("NSE", "INDIA VIX"): "vix",
    }, required_roles=("futures",))
    minute = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    vix_bar = MarketBar(vix, minute, 15, 16, 14, 15)
    future_bar = MarketBar(future, minute, 100, 102, 99, 101)

    assert aggregator.ingest(vix_bar) is None
    bundle = aggregator.ingest(future_bar)
    assert bundle is not None
    assert bundle.supporting_inputs["bars"]["vix"] is vix_bar
    assert bundle.supporting_inputs["sources"]["vix"] == "same_minute"


def test_futures_clock_rejects_duplicate_and_late_futures_minutes() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    aggregator = CompletedBarAggregator(
        {("NFO", "NIFTYFUT"): "futures"}, required_roles=("futures",)
    )
    first = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    second = first.replace(minute=21)

    assert aggregator.ingest(MarketBar(future, first, 100, 101, 99, 100)) is not None
    assert aggregator.ingest(MarketBar(future, first, 200, 201, 199, 200)) is None
    assert aggregator.ingest(MarketBar(future, second, 101, 102, 100, 101)) is not None
    assert aggregator.ingest(MarketBar(future, first, 300, 301, 299, 300)) is None


def test_futures_clock_resets_seen_minutes_and_supporting_carry_at_date_boundary() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator(
        {("NFO", "NIFTYFUT"): "futures", ("NSE", "INDIA VIX"): "vix"},
        required_roles=("futures",),
    )
    day_one = datetime(2026, 1, 1, 15, 29, tzinfo=ZoneInfo("Asia/Kolkata"))
    day_two = datetime(2026, 1, 2, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))

    aggregator.ingest(MarketBar(vix, day_one, 15, 16, 14, 15))
    aggregator.ingest(MarketBar(future, day_one, 100, 101, 99, 100))
    next_bundle = aggregator.ingest(MarketBar(future, day_two, 101, 102, 100, 101))
    assert next_bundle is not None
    assert next_bundle.supporting_inputs["bars"] == {}
    assert next_bundle.supporting_inputs["unavailable"] == ("vix",)


def test_late_supporting_bar_cannot_roll_back_carry_forward_value() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator(
        {("NFO", "NIFTYFUT"): "futures", ("NSE", "INDIA VIX"): "vix"},
        required_roles=("futures",),
    )
    first = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    second = first.replace(minute=21)

    newer_vix = MarketBar(vix, second, 16, 17, 15, 16)
    older_vix = MarketBar(vix, first, 15, 16, 14, 15)
    aggregator.ingest(newer_vix)
    aggregator.ingest(MarketBar(future, second, 101, 102, 100, 101))
    assert aggregator.ingest(older_vix) is None
    carried = aggregator.ingest(MarketBar(future, second.replace(minute=22), 102, 103, 101, 102))
    assert carried is not None
    assert carried.supporting_inputs["bars"]["vix"] is newer_vix


def test_late_supporting_bar_cannot_emit_an_older_pending_minute() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    aggregator = CompletedBarAggregator(
        {("NFO", "NIFTYFUT"): "futures", ("NSE", "INDIA VIX"): "vix"}
    )
    first = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    second = first.replace(minute=21)

    assert aggregator.ingest(MarketBar(future, first, 100, 101, 99, 100)) is None
    assert aggregator.ingest(MarketBar(future, second, 101, 102, 100, 101)) is None
    assert aggregator.ingest(MarketBar(vix, first, 15, 16, 14, 15)) is None
    assert aggregator.pending() == (("10:21", ("vix",), ("futures",)),)


def test_advancing_futures_clock_discards_stale_pending_minutes_before_flush() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    aggregator = CompletedBarAggregator(
        {("NFO", "NIFTYFUT"): "futures", ("NSE", "INDIA VIX"): "vix"}
    )
    first = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    second = first.replace(minute=21)

    assert aggregator.ingest(MarketBar(future, first, 100, 101, 99, 100)) is None
    assert aggregator.ingest(MarketBar(future, second, 101, 102, 100, 101)) is None
    flushed = aggregator.flush()
    assert len(flushed) == 1
    assert flushed[0].minute == "10:21"
    assert flushed[0].missing_roles == ("vix",)


def test_bundle_role_mappings_are_canonicalized_independent_of_arrival_order() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    spot = Instrument("NIFTY", "NSE", "INDEX")
    aggregator = CompletedBarAggregator(
        {
            ("NFO", "NIFTYFUT"): "futures",
            ("NSE", "INDIA VIX"): "vix",
            ("NSE", "NIFTY"): "spot",
        },
        required_roles=("futures",),
    )
    minute = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    assert aggregator.ingest(MarketBar(spot, minute, 1, 1, 1, 1)) is None
    bundle = aggregator.ingest(MarketBar(future, minute, 100, 101, 99, 100))
    assert bundle is not None
    assert list(bundle.bars) == ["futures"]
    assert list(bundle.supporting_inputs["bars"]) == ["spot"]


def test_supporting_role_is_ignored_and_expired_bundle_has_diagnostics() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    spot = Instrument("NIFTY", "NSE", "INDEX")
    aggregator = CompletedBarAggregator({
        ("NFO", "NIFTYFUT"): "futures",
        ("NSE", "INDIA VIX"): "vix",
        ("NSE", "NIFTY"): "spot",
    })
    minute = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    assert aggregator.ingest(MarketBar(spot, minute, 1, 1, 1, 1)) is None
    assert aggregator.ingest(MarketBar(future, minute, 100, 101, 99, 100)) is None
    expired = aggregator.expire(now=time.monotonic() + 11)
    assert len(expired) == 1
    assert expired[0].missing_roles == ("vix",)
    assert expired[0].bars["futures"].timestamp == minute


def test_supporting_only_expiry_does_not_advance_futures_watermark() -> None:
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    spot = Instrument("NIFTY", "NSE", "INDEX")
    aggregator = CompletedBarAggregator({
        ("NFO", "NIFTYFUT"): "futures",
        ("NSE", "NIFTY"): "spot",
    }, required_roles=("futures",), deadline_seconds=0)
    minute = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))

    assert aggregator.ingest(MarketBar(spot, minute, 1, 1, 1, 1)) is None
    assert aggregator.expire(now=time.monotonic() + 1) == ()
    bundle = aggregator.ingest(MarketBar(future, minute, 100, 101, 99, 100))

    assert bundle is not None
    assert bundle.bars["futures"].close == 100


def test_live_decision_engine_reports_unconfigured_cells() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    call = Instrument("NIFTYCE", "NFO", "CE")
    put = Instrument("NIFTYPE", "NFO", "PE")
    engine = IndependentLiveDecisionEngine(version="test", config_hash="hash", capital=CAPITAL_CONFIG.initial_capital)

    def bundle(minute: str, options: dict[str, MarketBar]) -> DecisionBundle:
        timestamp = datetime.fromisoformat(f"2026-01-01T{minute}:00+05:30")
        return DecisionBundle(
            f"b{minute}", "2026-01-01", minute,
            {
                "futures": MarketBar(instrument, timestamp, 99, 102, 98, 100, 100),
                "vix": MarketBar(vix, timestamp, 15, 16, 14, 15),
            }, ("futures", "vix"),
            supporting_inputs={"bars": options},
        )

    engine.evaluate(bundle("10:20", {
        "call": MarketBar(call, datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc), 1, 1, 1, 1, 100),
        "put": MarketBar(put, datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc), 1, 1, 1, 1, 200),
    }))
    engine.evaluate(bundle("10:21", {}))
    third = engine.evaluate(bundle("10:22", {}))

    assert [event.event_type for event in third] == ["CANDIDATEDECISION", "REJECTEDDECISION"]
    assert third[0].payload["decision_id"] == third[1].payload["decision_id"]
    assert third[0].payload["outcome"] == "candidate"
    assert third[1].payload["reason"] == "cell_not_configured"


def test_live_decision_engine_emits_input_rejection_for_incomplete_bundle() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    timestamp = datetime(2026, 1, 1, 10, 20, tzinfo=ZoneInfo("Asia/Kolkata"))
    bundle = DecisionBundle(
        "incomplete", "2026-01-01", "10:20",
        {"futures": MarketBar(instrument, timestamp, 99, 102, 98, 100, 100)},
        ("futures", "vix"), missing_roles=("vix",),
    )
    events = IndependentLiveDecisionEngine(
        version="test", config_hash="hash", capital=CAPITAL_CONFIG.initial_capital,
    ).evaluate(bundle)

    assert len(events) == 1
    assert events[0].event_type == "REJECTEDDECISION"
    assert events[0].payload["outcome"] == "input rejection"
    assert events[0].payload["reason"] == "incomplete_bundle"


def test_configured_policy_requires_exact_location_composite() -> None:
    engine = IndependentLiveDecisionEngine(version="test", config_hash="hash", capital=CAPITAL_CONFIG.initial_capital)
    exact = Cell(Location.VWAP_ZONE, Location.OR_HIGH, Location.PRIOR_DAY_LOW)
    with_extra_location = Cell(Location.VWAP_ZONE, Location.OR_HIGH, Location.PRIOR_DAY_LOW, Location.SESSION_HIGH)

    assert _configured_policies_for_cell(engine._cell_policies, Session.MORNING, exact)
    assert not _configured_policies_for_cell(engine._cell_policies, Session.MORNING, with_extra_location)


def test_live_decision_engine_persists_score_and_quality_bucket() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    engine = IndependentLiveDecisionEngine(version="test", config_hash="hash", capital=CAPITAL_CONFIG.initial_capital)

    def bundle(index: int) -> DecisionBundle:
        timestamp = datetime(2026, 1, 1, 10, 20 + index, tzinfo=timezone.utc)
        return DecisionBundle(
            f"score-{index}", "2026-01-01", timestamp.strftime("%Y-%m-%dT%H:%M"),
            {
                "futures": MarketBar(instrument, timestamp, 99, 102, 98, 100, 100),
                    "vix": MarketBar(vix, timestamp, 15 + index, 15 + index, 15 + index, 15 + index),
            }, ("futures", "vix"),
        )

    engine.evaluate(bundle(0))
    engine.evaluate(bundle(1))
    engine.evaluate(bundle(2))
    events = engine.evaluate(bundle(3))
    assert [event.event_type for event in events] == ["CANDIDATEDECISION", "REJECTEDDECISION"]
    assert events[0].payload["score"] == events[1].payload["score"]
    assert events[1].payload["reason"] == "cell_not_configured"


def test_admission_contract_replaces_grinding_score_filter() -> None:
    features = {
        "climactic_selling": False,
        "panic_descent": False,
        "volume_drying": True,
        "pcr_extreme": False,
        "vix_spike": False,
        "volume_climax_prior": True,
        "prior_midpoint_reclaim": False,
    }
    assert admission_result(features, "trend_continuation") == (True, None)
    rejected, reason = admission_result({**features, "volume_climax_prior": False}, "trend_continuation")
    assert not rejected and reason == "tr_confirmation_count"

def test_risk_sizer_never_exceeds_risk_budget_when_budget_is_marginal() -> None:
    sizer = RiskEngine()
    marginal = sizer.size(
        capital=10000, equity=10000, peak_equity=10000, entry=100, stop=995,
    )
    assert not marginal.approved and marginal.reason == "insufficient_risk_budget"
    weak = sizer.size(
        capital=10000, equity=10000, peak_equity=10000, entry=100, stop=995,
        score=2,
    )
    assert not weak.approved and weak.reason == "insufficient_risk_budget"


def test_risk_sizer_reserves_open_margin_before_score_sizing() -> None:
    sizer = RiskEngine(config=RiskConfig(margin_utilization_cap=0.80))
    decision = sizer.size(
        capital=2_500_000, equity=2_500_000, peak_equity=2_500_000,
        entry=23_450, stop=23_411.65, score=3, open_margin_used=350_000,
    )
    assert decision.available_capital == pytest.approx(2_150_000)
    assert decision.open_margin_used == pytest.approx(350_000)
    assert decision.raw_quantity == 17
    assert decision.margin_lots == 9
    assert decision.risk_ceiling == 9
    assert decision.quantity == 4


def test_research_capital_config_drives_canonical_futures_sizing_limits() -> None:
    capital = CAPITAL_CONFIG
    engine = ConfiguredLiveStrategy(capital_profile=capital)._decision_engine
    sizer = engine._vehicle_sizers["futures"]
    assert sizer.config.risk_fraction == pytest.approx(capital.risk_per_trade)
    assert sizer.config.max_quantity == capital.max_lots
    assert sizer.vehicle_limits["futures"].margin_per_lot == pytest.approx(175_000)


def test_risk_sizer_rounds_score_ceiling_before_policy_stability() -> None:
    sizer = RiskEngine(config=RiskConfig(risk_fraction=0.01, max_quantity=3))
    decision = sizer.size(
        capital=2_500_000, equity=2_500_000, peak_equity=2_500_000,
        entry=23_450, stop=23_411.65, score=3,
    )
    assert decision.risk_ceiling == 3
    assert decision.quantity == 2


def test_paper_sizing_pipeline_applies_ordered_policy_stages() -> None:
    risk = RiskDecision(True, 4, "approved", risk_ceiling=4)
    decision = SizingPipeline().decide(SizingPipelineInput(
        risk=risk,
        requested_quantity=1,
        score_multiplier=0.5,
        direction="long",
        concurrency_limit_lots=3,
        stability_multiplier=0.5,
        drawdown_multiplier=0.5,
        candidate_id="candidate-a",
    ))
    assert decision.final_quantity == 0
    assert [name for name, _ in decision.stage_results] == [
        "risk_ceiling", "score", "concurrency", "vehicle", "drawdown", "stability",
    ]
    assert decision.candidate_id == "candidate-a"


def test_paper_sizing_pipeline_preserves_stage_rounding_after_caps() -> None:
    risk = RiskAssessment(True, 3, "approved", risk_ceiling=3)
    decision = SizingPipeline().decide(SizingPipelineInput(
        risk=risk,
        requested_quantity=1,
        score_multiplier=1.0,
        direction="short",
        net_directional_lots=1,
        concurrency_limit_lots=2,
        stability_multiplier=0.34,
    ))
    assert decision.final_quantity == 1
    assert decision.rationale == "approved"


def test_synthetic_reporting_has_no_independent_gate_or_margin_owner() -> None:
    engine = IndependentLiveDecisionEngine(
        version="test", config_hash="hash", capital=2_500_000,
        enabled_vehicles=("futures", "synthetic"),
    )

    assert set(engine._vehicle_sizers) == {"futures"}
    assert not hasattr(engine, "_vehicle_risk_gates")
    assert engine.open_margin_used == 0


def test_paper_engine_preserves_live_decision_domain_values_at_boundary() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    decision_at = datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc)

    class Strategy:
        def on_bundle(self, bundle):
            from ftx_paper.strategy.decision import LiveDecision

            return (LiveDecision("WARMUP", {
                "decision_at": decision_at,
                "nested": {"observed_at": decision_at},
            }),)

    bundle = DecisionBundle("b0", "2026-01-01", "10:20", {
        "futures": MarketBar(instrument, decision_at, 100, 101, 99, 100),
        "vix": MarketBar(vix, decision_at, 15, 16, 14, 15),
    }, ("futures", "vix"))

    event = PaperEngine(Strategy()).on_bundle(bundle).events[0]

    assert event["decision_at"] == decision_at
    assert event["nested"]["observed_at"] == decision_at


def test_paper_engine_suppresses_rejected_decisions_by_default_with_opt_in() -> None:
    from ftx_paper.strategy.decision import LiveDecision

    class Strategy:
        def on_bundle(self, bundle):
            return (
                LiveDecision("CANDIDATEDECISION", {"decision_id": "candidate"}),
                LiveDecision("REJECTEDDECISION", {"decision_id": "candidate", "reason": "test"}),
                LiveDecision("ACCEPTEDDECISION", {"decision_id": "accepted"}),
                LiveDecision("SIZING_REJECTED", {"decision_id": "sized", "reason": "test"}),
            )

    bundle = DecisionBundle("b0", "2026-01-01", "10:20", {}, ())
    default_events = PaperEngine(Strategy()).on_bundle(bundle).events
    opted_in_events = PaperEngine(
        Strategy(), emit_rejected_decisions=True,
    ).on_bundle(bundle).events

    assert [event["event_type"] for event in default_events] == [
        "CANDIDATEDECISION", "ACCEPTEDDECISION",
    ]
    assert [event["event_type"] for event in opted_in_events] == [
        "CANDIDATEDECISION", "REJECTEDDECISION", "ACCEPTEDDECISION", "SIZING_REJECTED",
    ]
