from datetime import datetime, timezone
from threading import Event
from zoneinfo import ZoneInfo

from ftx_paper.broker import Fill, PaperBroker
from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole, OrderIntent, OrderRole, OrderSide
from ftx_paper.runtime.store import _json_safe
from ftx_paper.core import DecisionBundle, EngineResult, PaperEngine
from ftx_paper.core import CompletedBarAggregator
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.runtime import RuntimeSession, RuntimeStore
from ftx_paper.capital_config import ResearchCapitalProfile
from ftx_paper.strategy import ConfiguredLiveStrategy
import ftx_paper.broker.zerodha as zerodha


class Feed:
    def __init__(self, callback):
        self.callback = callback
        self.started = False
        self.stopped = False
        self.flushed = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def flush(self):
        self.flushed = True


def test_replay_and_runtime_share_paper_engine_bundle_path(monkeypatch, tmp_path):
    calls = []

    def record_bundle(self, bundle):
        assert isinstance(bundle, DecisionBundle)
        calls.append(bundle.bundle_id)
        self.bars_seen += len(bundle.bars)
        return EngineResult(events=({"event_type": "BUNDLE_PATH_CHECK", "bundle_id": bundle.bundle_id},))

    monkeypatch.setattr(PaperEngine, "on_bundle", record_bundle)
    bar = MarketBar(
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        datetime(2026, 1, 1, 4, 50, tzinfo=timezone.utc),
        100, 102, 99, 101,
    )
    aggregator = CompletedBarAggregator(
        {("NFO", "NIFTYFUT"): MarketRole.FUTURES},
        required_roles=(MarketRole.FUTURES,), deadline_seconds=0,
    )
    bundle = aggregator.ingest(bar)
    assert bundle is not None

    runtime_store = RuntimeStore(tmp_path / "runtime")
    RuntimeSession(runtime_store, None, [], engine=PaperEngine())._process_bundle(bundle, source="live")

    replay = ReplayWorker(RuntimeStore(tmp_path / "replay"))
    replay._bars = lambda _date: (bar,)
    result = replay._execute({"session_date": "2026-01-01"}, Event())

    assert calls == [bundle.bundle_id, bundle.bundle_id]
    assert any(event["event_type"] == "BUNDLE_PATH_CHECK" for event in result["events"])


def test_closed_bar_is_processed_under_session_lifecycle(tmp_path):
    store = RuntimeStore(tmp_path)
    session = RuntimeSession(store, None, [], engine=PaperEngine())
    session._aggregator = CompletedBarAggregator(
        {("NSE", "NIFTY"): MarketRole.FUTURES},
        required_roles=(MarketRole.FUTURES,), deadline_seconds=0,
    )
    session.feed = Feed(session.on_closed_bar)
    session.broker = PaperBroker()
    store.write_status({"state": "RUNNING"})
    bar = MarketBar(Instrument("NIFTY", "NSE", "FUTURES"), datetime.now(timezone.utc), 1, 2, 0, 1)
    session.on_closed_bar(bar)
    assert session.engine.bars_seen == 1
    assert any(event["event_type"] == "BUNDLE_COMPLETE" for event in store.read_events())
    session_date = bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat()
    assert len(store.read_market_bars(session_date)) == 1


def test_role_objects_are_json_safe_at_runtime_boundary():
    payload = _json_safe({MarketRole.FUTURES: 1, OptionRole("NIFTYCE"): 2}, path="payload")
    assert payload == {"futures": 1, "option:NIFTYCE": 2}


def test_exit_order_carries_explicit_entry_identity():
    entry_id = "entry-2026-01-01T10:20:00+05:30"
    order = OrderIntent(
        "exit-1", Instrument("NIFTY", "NSE", "INDEX"), OrderSide.SELL, 1,
        role=OrderRole.EXIT, entry_order_id=entry_id,
    )

    assert order.entry_order_id == entry_id


def test_stop_orders_feed_cleanup_before_broker(tmp_path):
    store = RuntimeStore(tmp_path)
    session = RuntimeSession(store, None, [])
    session.feed = Feed(session.on_closed_bar)
    session.broker = PaperBroker()
    session.stop()
    assert session.feed.stopped and not session.feed.flushed
    assert store.read_status()["state"] == "STOPPED"


def test_incomplete_bundle_is_diagnosed_without_advancing_completion(tmp_path):
    store = RuntimeStore(tmp_path)
    session = RuntimeSession(store, None, [], engine=PaperEngine())
    session._aggregator = CompletedBarAggregator({
        ("NFO", "NIFTYFUT"): "futures",
        ("NSE", "INDIA VIX"): "vix",
    }, deadline_seconds=0)
    store.write_status({"state": "RUNNING"})

    bar = MarketBar(
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc),
        100, 102, 99, 101,
    )
    session.on_closed_bar(bar)

    status = store.read_status()
    assert status["pending_bundle_minutes"] == ["15:50"]
    assert status["pending_bundle_details"] == [{
        "trading_date": "2026-01-01", "bundle_id": "2026-01-01:15:50",
        "minute": "15:50", "required_roles": ["futures", "vix"],
        "missing_roles": ["vix"], "roles_present": ["futures"],
        "last_bar_by_role": {"futures": "2026-01-01T10:20:00+00:00"},
    }]
    assert "last_completed_bundle_minute" not in status
    assert all(event["event_type"] not in {"BUNDLE_INCOMPLETE", "BUNDLE_COMPLETE"} for event in store.read_events())

    next_bar = MarketBar(
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        datetime(2026, 1, 1, 10, 21, tzinfo=timezone.utc),
        100, 102, 99, 101,
    )
    session.on_closed_bar(next_bar)

    events = store.read_events()
    assert events[0]["event_type"] == "BUNDLE_INCOMPLETE"
    assert events[0]["payload"]["minute"] == "15:50"


def test_runtime_session_does_not_process_startup_replay():
    assert not hasattr(RuntimeSession, "_replay_dates")


def test_live_order_reaches_paper_broker_and_persists_fill(tmp_path):
    store = RuntimeStore(tmp_path)
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")

    class Engine:
        bars_seen = 0
        strategy_metadata = None

        def on_bundle(self, bundle):
            return EngineResult(
                orders=(OrderIntent("live-order", instrument, OrderSide.BUY, 2, role=OrderRole.ENTRY),),
                events=({"event_type": "ACCEPTEDDECISION", "decision_id": "live-order"},),
            )

    session = RuntimeSession(store, None, [], engine=Engine())
    session.broker = PaperBroker({"NIFTYFUT": 101.5})
    session._process_bundle(
        type("Bundle", (), {
            "bundle_id": "2026-01-01:10:20",
            "trading_date": "2026-01-01",
            "minute": "10:20",
            "required_roles": (MarketRole.FUTURES, MarketRole.VIX),
            "bars": {MarketRole.FUTURES: object(), MarketRole.VIX: object()},
        })(),
        source="live",
    )

    events = store.read_events(20)
    event_types = {event["event_type"] for event in events}
    assert {"ORDER_ACK", "FILL"}.issubset(event_types)
    assert "EXECUTEDDECISION" not in event_types
    assert "STRATEGY_EVALUATION" in event_types
    assert "ORDER_SUPPRESSED" not in event_types
    fill = next(event for event in events if event["event_type"] == "FILL")
    assert fill["payload"]["decision_id"] == "live-order"
    assert fill["payload"]["price"] == 101.5
    assert fill["payload"]["outcome"] == "filled"
    assert fill["created_at"] != "2026-01-01T10:20:00+05:30"


def test_exit_without_entry_identity_is_rejected_before_broker_submission(tmp_path):
    store = RuntimeStore(tmp_path)
    session = RuntimeSession(store, None, [])
    session.broker = PaperBroker({"NIFTYFUT": 101.5})
    order = OrderIntent(
        "exit-1", Instrument("NIFTYFUT", "NFO", "FUTURES"), OrderSide.SELL, 2,
        role=OrderRole.EXIT,
    )

    assert not session._execute_paper_order(order, session_date="2026-01-01", timestamp="2026-01-01T10:20:00+05:30")
    error = next(event for event in store.read_events() if event["event_type"] == "EXECUTION_ERROR")
    assert error["payload"]["reason"] == "exit_without_entry_order_id"
    assert not any(event["event_type"] == "ORDER_ACK" for event in store.read_events())


def _session_with_futures_position(tmp_path, *, entry_id="entry-1", instrument=None, quantity=2):
    store = RuntimeStore(tmp_path)
    profile = ResearchCapitalProfile(initial_capital=2_500_000)
    session = RuntimeSession(store, None, [], engine=PaperEngine(
        ConfiguredLiveStrategy(capital_profile=profile),
    ), capital_profile=profile)
    session.broker = PaperBroker({"NIFTYFUT": 101.5})
    instrument = instrument or Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent(entry_id, instrument, OrderSide.BUY, quantity, role=OrderRole.ENTRY)
    session.coordinator.submit(entry)
    session.coordinator.fill(entry, price=100.0)
    return session, store


def test_matched_exit_removes_portfolio_position_after_fill(tmp_path):
    session, _ = _session_with_futures_position(tmp_path)
    entry_id = "entry-1"
    order = OrderIntent(
        "exit-1", Instrument("NIFTYFUT", "NFO", "FUTURES"), OrderSide.SELL, 2,
        role=OrderRole.EXIT, entry_order_id=entry_id,
    )

    assert session._execute_paper_order(order, session_date="2026-01-01", timestamp="2026-01-01T10:20:00+05:30")
    assert entry_id not in session.portfolio.positions


def test_exit_quantity_mismatch_is_rejected_without_removing_entry(tmp_path):
    session, store = _session_with_futures_position(tmp_path)
    entry_id = "entry-1"
    order = OrderIntent(
        "exit-1", Instrument("NIFTYFUT", "NFO", "FUTURES"), OrderSide.SELL, 1,
        role=OrderRole.EXIT, entry_order_id=entry_id,
    )

    assert not session._execute_paper_order(order, session_date="2026-01-01", timestamp="2026-01-01T10:20:00+05:30")
    assert entry_id in session.portfolio.positions
    error = next(event for event in store.read_events() if event["event_type"] == "EXECUTION_ERROR")
    assert error["payload"]["reason"] == "exit_quantity_mismatch"


def test_exit_rejects_broker_fill_for_a_different_instrument(tmp_path):
    entry_instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    wrong_instrument = Instrument("OTHERFUT", "NFO", "FUTURES")
    session, store = _session_with_futures_position(tmp_path, instrument=entry_instrument)

    class WrongFillBroker:
        def submit(self, order):
            return type("Ack", (), {"client_order_id": order.client_order_id,
                                     "broker_order_id": "broker-1", "status": "FILLED"})()

        def poll_fills(self, order, _broker_order_id):
            return (Fill(order.client_order_id, wrong_instrument, order.quantity, 101.0,
                         "2026-01-01T10:20:00+00:00"),)

    session.broker = WrongFillBroker()
    order = OrderIntent("exit-1", entry_instrument, OrderSide.SELL, 2,
                        role=OrderRole.EXIT, entry_order_id="entry-1")

    assert not session._execute_paper_order(order)
    assert store.read_status()["state"] == "ERROR"
    assert session.portfolio.positions["entry-1"].instrument == entry_instrument.symbol
    assert any(event["payload"]["reason"] == "uncompensated_exit_fill:fill_instrument_mismatch"
               for event in store.read_events() if event["event_type"] == "EXECUTION_ERROR")


def test_session_uses_strategy_portfolio_as_execution_owner(tmp_path):
    store = RuntimeStore(tmp_path)
    store.write_status({
        "state": "STOPPED",
        "capital": 800.0,
        "initial_capital": 1000.0,
        "open_positions": [],
    })
    capital_profile = ResearchCapitalProfile(initial_capital=1000.0)
    strategy = ConfiguredLiveStrategy(capital_profile=capital_profile)
    session = RuntimeSession(store, None, [], engine=PaperEngine(strategy), capital_profile=capital_profile)

    assert session.coordinator is not None
    assert session.portfolio is strategy.portfolio
    assert session.portfolio.equity == 1000.0
    assert not hasattr(session, "ledger")


def test_startup_discovers_options_before_backfill_and_feed_subscription(monkeypatch, tmp_path):
    configured = [
        {"exchange": "NFO", "tradingsymbol": "NIFTY26SEPFUT", "role": "futures"},
        {"exchange": "NSE_INDEX", "tradingsymbol": "INDIA VIX", "role": "vix"},
    ]
    resolved = [
        {"instrument_token": 1, "exchange": "NFO", "symbol": "NIFTY26SEPFUT", "tradingsymbol": "NIFTY26SEPFUT", "instrument_type": "FUT", "role": MarketRole.FUTURES},
        {"instrument_token": 2, "exchange": "NSE_INDEX", "symbol": "INDIA VIX", "tradingsymbol": "INDIA VIX", "instrument_type": "INDEX", "role": MarketRole.VIX},
    ]
    option = {"instrument_token": 3, "exchange": "NFO", "symbol": "NIFTY26SEP25000CE", "tradingsymbol": "NIFTY26SEP25000CE", "instrument_type": "CE", "expiry": "2026-09-24", "strike": 25000, "role": None}
    captured = {}

    class Auth:
        api_key = "key"

        @staticmethod
        def access_token():
            return "token"

    class Client:
        pass

    class Feed:
        def __init__(self, _socket, tokens, _normalize, _on_bar, **_kwargs):
            captured["tokens"] = tokens

        def start(self):
            return None

        def stop(self):
            return None

    monkeypatch.setattr(zerodha, "resolve_instruments", lambda _client, _specs: list(resolved))
    monkeypatch.setattr(zerodha, "discover_option_surface_contracts", lambda _client, **_kwargs: [option])
    def fake_backfill(_client, instruments):
        captured["backfill"] = list(instruments)
        return ()

    monkeypatch.setattr(zerodha, "load_startup_backfill", fake_backfill)
    monkeypatch.setattr(zerodha, "create_kite_socket", lambda *_args: object())

    session = RuntimeSession(
        RuntimeStore(tmp_path), Auth(), configured, client_factory=lambda: Client(), feed_factory=Feed,
        engine=PaperEngine(), market_clock=lambda: datetime(2026, 9, 15, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata")),
    )
    session._start_impl()

    assert any(item["symbol"] == option["symbol"] for item in captured["backfill"])
    assert 3 in captured["tokens"]
