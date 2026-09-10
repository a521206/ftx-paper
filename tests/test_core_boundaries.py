from datetime import datetime, timezone

from ftx_paper.broker import PaperBroker
from ftx_paper.contracts import Instrument, MarketBar, OrderSide
from ftx_paper.core import ExitStateMachine, LiveFeatureCalculator, PaperEngine, PositionState, RiskSizer, SetupPolicy, replay
from ftx_paper.core import LiveSession
from ftx_paper.strategy import ConfiguredLiveStrategy


def test_engine_does_not_require_broker() -> None:
    bar = MarketBar(Instrument("NIFTY", "NSE", "INDEX"), datetime.now(timezone.utc), 1, 2, 0, 1)

    result = PaperEngine().on_bar(bar)

    assert result.events[0]["symbol"] == "NIFTY"


def test_paper_broker_returns_contract_fill() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    from ftx_paper.contracts import OrderIntent, OrderSide

    fill = PaperBroker({"NIFTY": 100.0}).submit(OrderIntent("order-1", instrument, OrderSide.BUY, 1))

    assert fill.status == "FILLED"
    assert fill.client_order_id == "order-1"


def test_live_session_owns_session_state() -> None:
    bar = MarketBar(Instrument("NIFTY", "NSE", "INDEX"), datetime.now(timezone.utc), 1, 2, 0, 1)
    session = LiveSession(PaperEngine())

    session.on_bar(bar)

    assert session.state.bars_seen == 1


def test_production_strategy_is_versioned_and_injectable() -> None:
    strategy = ConfiguredLiveStrategy(lambda bar: ())

    assert strategy.name.startswith("ftx-paper-")
    assert strategy.on_bar(None) == ()
    assert strategy.metadata.config_hash
    assert strategy.snapshot()["schema_version"] == 1
    assert strategy.from_snapshot(strategy.snapshot()).metadata.version == strategy.version


def test_live_features_are_causal_and_deterministic() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    bars = tuple(MarketBar(instrument, datetime(2026, 1, 1, 9, 15 + i), 100 + i, 102 + i, 99 + i, 101 + i, 10) for i in range(3))
    calculator = LiveFeatureCalculator(opening_range_bars=2, atr_window=2)
    features = calculator.calculate(bars)
    assert features.vwap == 101.66666666666667
    assert features.opening_range_high == 103
    assert features.atr == 3.0
    assert features == calculator.calculate(bars)


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
    assert approved.approved and approved.quantity == 20
    blocked = sizer.size(capital=10000, equity=8900, peak_equity=10000, entry=100, stop=95)
    assert not blocked.approved and blocked.reason == "drawdown_limit"


def test_exit_state_machine_emits_protective_exit() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    position = PositionState(instrument, 100, 95, 2, OrderSide.BUY)
    action = ExitStateMachine().evaluate(position, timestamp=datetime(2026, 1, 1, 11), high=101, low=94, close=96, client_order_id="exit-1")
    assert action is not None
    assert action.reason == "stop"
    assert action.intent.side is OrderSide.SELL


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
    strategy = ConfiguredLiveStrategy()
    bars = tuple(MarketBar(instrument, datetime(2026, 1, 1, 10, 15 + i), 100 + i, 102 + i, 99 + i, 101 + i, 10) for i in range(2))
    orders = [order for bar in bars for order in strategy.on_bar(bar)]
    assert orders and orders[0].quantity > 0
    exit_orders = strategy.on_bar(MarketBar(instrument, datetime(2026, 1, 1, 15, 20), 90, 91, 89, 90, 10))
    assert exit_orders and exit_orders[0].reason in {"stop", "trailing_stop", "session_close"}


def test_replay_fixture_has_stable_production_transcript() -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    fixture = tuple(
        MarketBar(instrument, datetime(2026, 1, 1, 10, 15 + i), 100 + i, 102 + i, 99 + i, 101 + i, 10)
        for i in range(3)
    )
    result = replay(PaperEngine(ConfiguredLiveStrategy()), fixture)
    assert result.bars_seen == 3
    assert [(order.client_order_id, order.quantity, order.side.value) for order in result.orders] == [
        ("entry-2026-01-01T10:15:00", 100, "BUY"),
        ("entry-2026-01-01T10:16:00", 100, "BUY"),
        ("entry-2026-01-01T10:17:00", 100, "BUY"),
    ]
