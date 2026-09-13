from datetime import datetime, timezone
import math
import json
from pathlib import Path
import time
from zoneinfo import ZoneInfo
import pytest

from ftx_paper.broker import PaperBroker
from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole, OrderIntent, OrderRole, OrderSide, parse_role, role_to_key
from ftx_paper.core import CompletedBarAggregator, DecisionBundle, ExitAction, ExitStateMachine, IndependentLiveDecisionEngine, LiveFeatureCalculator, PaperEngine, PositionState, RiskSizer, SetupPolicy, adaptive_stop_bp, option_pcr_at_event, replay, vix_open_and_event
from ftx_paper.core import LiveSession
from ftx_paper.core.location_engine import Cell, Location
from ftx_paper.core.live_decision import _configured_policies_for_cell
from ftx_paper.strategy.config import Session
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.strategy import ConfiguredLiveStrategy
from ftx_paper.capital_config import FtxCapitalConfig, RESEARCH_CAPITAL_CONFIG as CAPITAL_CONFIG


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
    decision = RiskSizer().size(
        capital=expected["capital"], equity=expected["equity"], peak_equity=expected["peak_equity"],
        entry=expected["entry"], stop=expected["entry"] * (1 - expected["stop_bp"] / 10000),
    )
    assert decision.risk_budget == expected["risk_budget"]
    assert decision.quantity == expected["quantity"]


def test_paper_broker_returns_contract_fill() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    from ftx_paper.contracts import OrderIntent, OrderSide

    fill = PaperBroker({"NIFTY": 100.0}).submit(OrderIntent("order-1", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY))

    assert fill.status == "FILLED"
    assert fill.client_order_id == "order-1"


def test_live_session_owns_session_state() -> None:
    bar = MarketBar(Instrument("NIFTY", "NSE", "INDEX"), datetime.now(timezone.utc), 1, 2, 0, 1)
    session = LiveSession(PaperEngine())

    session.on_bar(bar)

    assert session.state.bars_seen == 1


def test_production_strategy_is_versioned_and_injectable() -> None:
    strategy = ConfiguredLiveStrategy(capital_config=CAPITAL_CONFIG, decide=lambda bar: ())

    assert strategy.name.startswith("ftx-paper-")
    assert strategy.on_bar(None) == ()
    assert strategy.metadata.config_hash
    assert strategy.snapshot()["schema_version"] == 1
    assert strategy.from_snapshot(strategy.snapshot(), capital_config=CAPITAL_CONFIG).metadata.version == strategy.version


def test_strategy_snapshot_restores_vehicle_for_synthetic_replay() -> None:
    strategy = ConfiguredLiveStrategy(capital_config=CAPITAL_CONFIG, vehicle="synthetic")
    restored = ConfiguredLiveStrategy.from_snapshot(strategy.snapshot(), capital_config=CAPITAL_CONFIG)
    assert restored.vehicle == "synthetic"


def test_strategy_uses_explicit_capital_limits_and_rejects_mismatch() -> None:
    capital_config = FtxCapitalConfig(
        initial_capital=100_000.0, max_daily_loss=0.02, max_net_directional_lots=3.0,
    )
    strategy = ConfiguredLiveStrategy(capital_config=capital_config)

    assert strategy.portfolio_state["initial_capital"] == 100_000.0
    assert strategy._decision_engine.max_daily_loss == 0.02
    assert strategy._decision_engine.risk_gate.max_net_directional_lots == 3.0

    mismatched = FtxCapitalConfig(initial_capital=200_000.0, max_daily_loss=0.02, max_net_directional_lots=3.0)
    with pytest.raises(ValueError, match="does not match strategy snapshot"):
        ConfiguredLiveStrategy.from_snapshot(strategy.snapshot(), capital_config=mismatched)


def test_capital_config_rejects_non_object_json(tmp_path) -> None:
    path = tmp_path / "ftx.json"
    path.write_text('{"capital": []}', encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid FTX paper capital config"):
        FtxCapitalConfig.from_file(path)


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


def test_option_pcr_matches_cutoff_and_unavailable_contract() -> None:
    call = Instrument("NIFTYCE", "NFO", "CE")
    put = Instrument("NIFTYPE", "NFO", "PE")
    future = Instrument("NIFTYFUT", "NFO", "FUTURES")
    options = {
        "call": MarketBar(call, datetime(2026, 1, 1, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata")), 1, 1, 1, 1, 100),
        "put": MarketBar(put, datetime(2026, 1, 1, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata")), 1, 1, 1, 1, 250),
    }
    before_cutoff = MarketBar(future, datetime(2026, 1, 1, 9, 59, tzinfo=ZoneInfo("Asia/Kolkata")), 100, 101, 99, 100)
    at_cutoff = MarketBar(future, datetime(2026, 1, 1, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata")), 100, 101, 99, 100)

    assert option_pcr_at_event(options, before_cutoff) is None
    assert option_pcr_at_event(options, at_cutoff) == 2.5
    assert option_pcr_at_event({"put": options["put"]}, at_cutoff) is None


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
    sizer = RiskSizer()
    approved = sizer.size(capital=10000, equity=10000, peak_equity=10000, entry=100, stop=95)
    assert not approved.approved and approved.reason == "insufficient_risk_budget"
    blocked = sizer.size(capital=10000, equity=8000, peak_equity=10000, entry=100, stop=95)
    assert not blocked.approved and blocked.reason == "drawdown_limit"


def test_replay_trade_session_classification_matches_canonical_windows() -> None:
    assert ReplayWorker._session_for_minutes(60) == "morning"
    assert ReplayWorker._session_for_minutes(119) == "morning"
    assert ReplayWorker._session_for_minutes(120) == "unknown"
    assert ReplayWorker._session_for_minutes(255) == "afternoon"
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
    assert math.isclose(second.price, 108.1158, rel_tol=0.0, abs_tol=1e-9)


def test_order_without_explicit_role_fails_fast() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    try:
        OrderIntent("entry-legacy", instrument, OrderSide.BUY, 1)
    except TypeError as exc:
        assert "role" in str(exc)
    else:
        raise AssertionError("exit-shaped order without an explicit role was accepted")


def test_replay_is_deterministic_and_rejects_reordering() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    bars = tuple(MarketBar(instrument, datetime(2026, 1, 1, 9, 15 + i), 100, 101, 99, 100 + i, 10) for i in range(2))
    first = replay(PaperEngine(), bars)
    second = replay(PaperEngine(), bars)
    assert first == second
    try:
        replay(PaperEngine(), (bars[1], bars[0]))
    except ValueError as exc:
        assert "strictly increasing" in str(exc)
    else:
        raise AssertionError("out-of-order replay was accepted")


def test_production_strategy_composes_features_policy_risk_and_exit() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    strategy = ConfiguredLiveStrategy(capital_config=CAPITAL_CONFIG)
    bars = tuple(MarketBar(instrument, datetime(2026, 1, 1, 10, 15 + i), 100 + i, 102 + i, 99 + i, 101 + i, 10) for i in range(2))
    orders = [order for bar in bars for order in strategy.on_bar(bar)]
    assert orders and orders[0].quantity > 0
    exit_orders = strategy.on_bar(MarketBar(instrument, datetime(2026, 1, 1, 15, 20), 90, 91, 89, 90, 10))
    assert exit_orders and exit_orders[0].reason in {"hard_stop", "trail_stop", "eod"}


def test_on_bar_sizing_scales_with_setup_score() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    weak_strategy = ConfiguredLiveStrategy(capital_config=CAPITAL_CONFIG)
    weak_orders = [order for order in weak_strategy.on_bar(MarketBar(instrument, datetime(2026, 1, 1, 10, 15), 100, 102, 99, 101, 10))]

    closes = (110, 108, 106, 104, 102, 100, 98, 96)
    volumes = (3, 3, 3, 20, 3, 3, 2, 2)
    strong_strategy = ConfiguredLiveStrategy(capital_config=CAPITAL_CONFIG)
    strong_orders = []
    for i, close in enumerate(closes):
        strong_orders.extend(strong_strategy.on_bar(
            MarketBar(instrument, datetime(2026, 1, 1, 10, 15 + i), close + 1, close + 1, close - 1, close, volumes[i])
        ))
    strong_orders.extend(strong_strategy.on_bar(
        MarketBar(instrument, datetime(2026, 1, 1, 10, 23), 95, 96, 88, 95.5, 30)
    ))

    assert weak_orders and strong_orders
    assert strong_orders[-1].quantity > weak_orders[-1].quantity


def test_replay_fixture_has_stable_production_transcript() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    fixture = tuple(
        MarketBar(instrument, datetime(2026, 1, 1, 10, 15 + i), 100 + i, 102 + i, 99 + i, 101 + i, 10)
        for i in range(3)
    )
    result = replay(PaperEngine(ConfiguredLiveStrategy(capital_config=CAPITAL_CONFIG)), fixture)
    assert result.bars_seen == 3
    assert [(order.client_order_id, order.quantity, order.side.value) for order in result.orders] == [
        ("entry-2026-01-01T10:15:00", 5, "BUY"),
        ("entry-2026-01-01T10:16:00", 5, "BUY"),
        ("entry-2026-01-01T10:17:00", 5, "BUY"),
    ]


def test_replay_completes_entry_fill_exit_and_realized_trade() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    first = MarketBar(instrument, datetime(2026, 1, 1, 10, 15), 100, 101, 99, 100, 10)
    second = MarketBar(instrument, datetime(2026, 1, 1, 10, 16), 110, 111, 109, 110, 10)

    class LifecycleStrategy:
        metadata = None

        def on_bar(self, bar):
            if bar.timestamp == first.timestamp:
                return (OrderIntent("entry-1", instrument, OrderSide.BUY, 2, role=OrderRole.ENTRY),)
            return ()

        def on_closed_bar(self, bar):
            if bar.timestamp == second.timestamp:
                intent = OrderIntent("exit-entry-1", instrument, OrderSide.SELL, 2, reason="target", role=OrderRole.EXIT)
                return (ExitAction("target", 108.0, intent),)
            return ()

        def settle_exit(self, order_id, *, filled):
            return None

        def register_entry(self, order, *, fill_price=None):
            return None

    result = replay(PaperEngine(LifecycleStrategy()), (first, second))

    assert [order.client_order_id for order in result.orders] == ["entry-1", "exit-entry-1"]
    assert len(result.trades) == 1
    trade = result.trades[0]
    assert (trade.entry_price, trade.exit_price, trade.exit_reason, trade.realized_pnl, trade.status) == (100, 108.0, "target", 1040.0, "closed")
    assert [event["event_type"] for event in result.events if "event_type" in event] == ["FILL", "EXITDECISION", "FILL"]


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
    engine = PaperEngine(ConfiguredLiveStrategy(capital_config=CAPITAL_CONFIG))
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


def test_live_decision_engine_suppresses_unconfigured_cells() -> None:
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

    assert third == ()


def test_configured_policy_requires_exact_location_composite() -> None:
    engine = IndependentLiveDecisionEngine(version="test", config_hash="hash", capital=CAPITAL_CONFIG.initial_capital)
    exact = Cell(Location.SESSION_HIGH, Location.OR_HIGH)
    with_vwap = Cell(Location.VWAP_ZONE, Location.SESSION_HIGH, Location.OR_HIGH)

    assert _configured_policies_for_cell(engine._cell_policies, Session.MORNING, exact)
    assert not _configured_policies_for_cell(engine._cell_policies, Session.MORNING, with_vwap)


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
    assert events == ()


def test_risk_sizer_never_exceeds_risk_budget_when_budget_is_marginal() -> None:
    sizer = RiskSizer()
    marginal = sizer.size(
        capital=10000, equity=10000, peak_equity=10000, entry=100, stop=995,
    )
    assert not marginal.approved and marginal.reason == "insufficient_risk_budget"
    weak = sizer.size(
        capital=10000, equity=10000, peak_equity=10000, entry=100, stop=995,
        score=2,
    )
    assert not weak.approved and weak.reason == "insufficient_risk_budget"


def test_paper_engine_preserves_live_decision_domain_values_at_boundary() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    vix = Instrument("INDIA VIX", "NSE", "VIX")
    decision_at = datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc)

    class Strategy:
        def on_bundle(self, bundle):
            from ftx_paper.core import LiveDecision

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
