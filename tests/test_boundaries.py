import json
from pathlib import Path
import sqlite3

from ftx_paper.api import create_app
from ftx_paper.runtime import RuntimeStore
from ftx_paper.ui import create_ui_app
from ftx_paper.broker.zerodha import ReconnectPolicy, ZerodhaFeed
from ftx_paper.broker.zerodha import ZerodhaBroker, classify_runtime_roles, load_startup_backfill, resolve_instruments
from ftx_paper.broker.zerodha import ZerodhaAuth
import ftx_paper.broker.zerodha.adapter as zerodha_adapter
from ftx_paper.contracts import OrderIntent, OrderSide
from ftx_paper.contracts import Instrument, MarketBar
from datetime import datetime


def test_api_reads_runtime_store(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    store.write_status({"state": "STOPPED"})

    response = create_app(store).test_client().get("/api/v1/runtime")

    assert response.status_code == 200
    assert response.get_json() == {"state": "STOPPED"}


def test_health_is_available_without_broker_or_niftyzoning(tmp_path: Path) -> None:
    response = create_app(RuntimeStore(tmp_path)).test_client().get("/api/v1/health")

    assert response.status_code == 200
    assert response.get_json()["status"] == "pass"


def test_api_exposes_persisted_events(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    store.append_event("TEST", {"value": 1})

    response = create_app(store).test_client().get("/api/v1/events?limit=10")

    assert response.status_code == 200
    event = response.get_json()["events"][0]
    assert event["payload"] == {"value": 1}
    assert event["category"] == "runtime"
    assert event["created_at"]
    assert "timestamp" not in event


def test_interrupted_paper_session_returns_to_stopped(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    store.write_status({"state": "RUNNING", "feed_connected": True})

    assert store.recover_interrupted() is True
    assert store.read_status()["state"] == "STOPPED"
    assert store.read_status()["feed_connected"] is False
    recovery = store.read_events()[0]
    assert recovery["event_type"] == "RUNTIME_RECOVERY"
    assert recovery["payload"]["resulting_state"] == "STOPPED"


def test_api_exposes_decisions_and_logs_as_projections(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    store.append_event("ENGINE_EVENT", {"bar": 1})
    store.append_event("REJECTEDDECISION", {
        "decision_id": "d1", "minute": "2026-09-10T10:20:00+05:30",
        "cell": "vwap", "direction": "short", "reason": "setup_score_skip",
    })
    store.append_event("RUNTIME_LOG", {"message": "started"})
    client = create_app(store).test_client()
    decisions = client.get("/api/v1/decisions").get_json()["decisions"]
    assert len(decisions) == 1
    assert decisions[0]["payload"]["decision_id"] == "d1"
    assert client.get("/api/v1/bundles").status_code == 404
    assert len(client.get("/api/v1/logs").get_json()["logs"]) == 3


def test_decision_projection_includes_execution_outcome(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    store.append_event("ACCEPTEDDECISION", {
        "decision_id": "d1", "decision_source": "live", "minute": "2026-09-10T10:20:00+05:30",
    })
    store.append_event("ORDER_ACK", {"decision_id": "d1", "status": "FILLED"})
    store.append_event("FILL", {"decision_id": "d1", "price": 101.5})

    decisions = create_app(store).test_client().get("/api/v1/decisions").get_json()["decisions"]

    assert len(decisions) == 1
    assert decisions[0]["payload"]["execution_status"] == "filled"
    assert decisions[0]["payload"]["execution_event_type"] == "FILL"


def test_decision_projection_attaches_risk_without_counting_it_as_a_decision(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    store.append_event("ACCEPTEDDECISION", {"decision_id": "d1", "minute": "2026-09-10T10:20:00+05:30"})
    store.append_event("SIZING_REJECTED", {"decision_id": "d1", "reason": "insufficient_capital"})
    store.append_event("STRATEGY_EVALUATION", {"bundle_id": "b1"})

    decisions = create_app(store).test_client().get("/api/v1/decisions").get_json()["decisions"]

    assert len(decisions) == 1
    assert decisions[0]["category"] == "decision"
    assert decisions[0]["payload"]["risk_status"] == "sizing_rejected"
    assert decisions[0]["payload"]["risk_event"]["event_type"] == "SIZING_REJECTED"


def test_new_decisions_store_only_minute_for_decision_time(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    store.append_event("ACCEPTEDDECISION", {
        "decision_id": "d1", "minute": "2026-09-10T10:20:00+05:30",
        "bar_datetime": "2026-09-10T10:21:00+05:30", "event_time": "2026-09-10T10:22:00+05:30",
    })

    event = store.read_events()[0]
    assert event["payload"]["minute"] == "2026-09-10T10:20:00+05:30"
    assert not {"bar_datetime", "bar_timestamp", "event_time", "decision_minute", "timestamp"} & event["payload"].keys()
    assert event["created_at"] != event["payload"]["minute"]


def test_legacy_decision_timestamps_migrate_without_overwriting_minute(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            "CREATE TABLE runtime_events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, "
            "payload TEXT NOT NULL, timestamp TEXT NOT NULL, idempotency_key TEXT UNIQUE);"
        )
        connection.execute(
            "INSERT INTO runtime_events(event_type, payload, timestamp) VALUES (?, ?, ?)",
            ("ACCEPTEDDECISION", json.dumps({"bar_datetime": "2026-09-10T10:20:00+05:30"}), "2026-09-10T15:00:00Z"),
        )
        connection.execute(
            "INSERT INTO runtime_events(event_type, payload, timestamp) VALUES (?, ?, ?)",
            ("REJECTEDDECISION", json.dumps({"minute": "2026-09-10T10:21:00+05:30", "event_time": "bad"}), "2026-09-10T15:00:00Z"),
        )

    store = RuntimeStore(tmp_path)
    events = {event["event_type"]: event for event in store.read_events()}
    assert events["ACCEPTEDDECISION"]["payload"]["minute"] == "2026-09-10T10:20:00+05:30"
    assert events["REJECTEDDECISION"]["payload"]["minute"] == "2026-09-10T10:21:00+05:30"
    assert "event_time" not in events["REJECTEDDECISION"]["payload"]
    assert store.decision_timestamp_migration == {"migrated": 1, "unmigratable": 0}


def test_runtime_actions_are_api_boundaries(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    class FakeController:
        def request_start(self): store.patch_status({"state": "STARTING"})
        def request_stop(self): pass
        def request_restart(self): pass
    response = create_app(store, controller=FakeController()).test_client().post("/api/v1/runtime/start")

    assert response.status_code == 202
    assert store.read_status()["state"] == "STARTING"


def test_ui_is_a_separate_http_client() -> None:
    response = create_ui_app("http://api.test").test_client().get("/")

    assert response.status_code == 200
    assert b"http://api.test" in response.data
    assert b"/api/v1/positions" in response.data
    assert b"/api/v1/capital" in response.data
    assert b"Connect Zerodha" in response.data


def test_events_and_logs_redirect_to_combined_activity_page() -> None:
    client = create_ui_app("http://api.test").test_client()

    for legacy_page in ("events", "logs"):
        response = client.get(f"/{legacy_page}")
        assert response.status_code == 302
        assert response.headers["Location"] == "/activity"

    activity = client.get("/activity")
    assert activity.status_code == 200
    html = activity.get_data(as_text=True)
    assert "Runtime activity" in html
    assert "activity-session" in html
    assert "activity-category" in html
    assert "activity-type" in html


def test_browser_smoke_dashboard_contains_live_api_sections() -> None:
    response = create_ui_app("http://api.test").test_client().get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    for endpoint in ("health", "runtime", "capital", "positions", "trades", "events"):
        assert f"/api/v1/{endpoint}" in html


def test_zerodha_auth_routes_use_injected_adapter() -> None:
    class FakeAuth:
        def login_url(self):
            return "https://kite.test/login"

        def exchange(self, request_token):
            assert request_token == "token"

        def access_token(self):
            return "access"

    class FakeController:
        def request_start(self): pass
        def request_stop(self): pass
        def request_restart(self): pass
    client = create_app(RuntimeStore(Path(".")), FakeAuth(), controller=FakeController()).test_client()
    assert client.get("/api/v1/broker/zerodha/login").get_json()["login_url"] == "https://kite.test/login"
    response = client.post("/api/v1/broker/zerodha/callback", json={"request_token": "token"})
    assert response.status_code == 200
    assert client.get("/api/v1/broker/zerodha/status").get_json()["authenticated"] is True


def test_standalone_zerodha_auth_reads_its_own_config(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "zerodha.json"
    config.write_text('{"api_key":"key","api_secret":"secret","auth_db":"tokens.sqlite3"}', encoding="utf-8")
    monkeypatch.delenv("KITE_API_KEY", raising=False)
    monkeypatch.delenv("KITE_API_SECRET", raising=False)
    auth = ZerodhaAuth.from_config_path(config)
    assert auth.api_key == "key"
    assert auth.token_db == tmp_path / "tokens.sqlite3"


def test_zerodha_instrument_resolution_and_role_classification() -> None:
    class Client:
        def instruments(self, exchange):
            return [{"tradingsymbol": "NIFTYFUT", "instrument_token": 1}, {"tradingsymbol": "INDIA VIX", "instrument_token": 2}]
    rows = resolve_instruments(Client(), [{"exchange": "NFO", "tradingsymbol": "NIFTYFUT", "role": "futures"}, {"exchange": "NSE", "tradingsymbol": "INDIA VIX", "role": "vix"}])
    roles = classify_runtime_roles(rows)
    assert roles.futures["instrument_token"] == 1
    assert roles.vix["instrument_token"] == 2


def test_zerodha_instrument_resolution_caches_exchange_master() -> None:
    class Client:
        def __init__(self):
            self.calls = []

        def instruments(self, exchange):
            self.calls.append(exchange)
            return [
                {"tradingsymbol": "NIFTYFUT", "instrument_token": 1},
                {"tradingsymbol": "NIFTYVIX", "instrument_token": 2},
            ]

    client = Client()
    resolve_instruments(client, [
        {"exchange": "NFO", "tradingsymbol": "NIFTYFUT", "role": "futures"},
        {"exchange": "NFO", "tradingsymbol": "NIFTYVIX", "role": "vix"},
    ])

    assert client.calls == ["NFO"]


def test_zerodha_historical_rows_become_normalized_market_bars() -> None:
    class Client:
        def historical_data(self, token, start, end, interval):
            return [{"date": "2026-01-01T10:00:00+05:30", "open": 1, "high": 2, "low": 0, "close": 1.5, "volume": 10}]
    bars = load_startup_backfill(Client(), [{"instrument_token": 1, "symbol": "NIFTYFUT", "exchange": "NFO"}])
    assert bars[0].close == 1.5 and bars[0].instrument.symbol == "NIFTYFUT"


def test_zerodha_historical_backfill_retries_rate_limits(monkeypatch) -> None:
    class Client:
        def __init__(self):
            self.calls = 0

        def historical_data(self, token, start, end, interval):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("Too many requests")
            return [{"date": "2026-01-01T10:00:00+05:30", "open": 1, "high": 2, "low": 0, "close": 1.5}]

    monkeypatch.setattr(zerodha_adapter, "_reserve_historical_request_slot", lambda: None)
    monkeypatch.setattr(zerodha_adapter.time, "sleep", lambda _seconds: None)
    client = Client()

    bars = load_startup_backfill(client, [{"instrument_token": 1, "symbol": "NIFTYFUT", "exchange": "NFO"}])

    assert client.calls == 3
    assert bars[0].close == 1.5


def test_zerodha_feed_emits_only_closed_minute_bars() -> None:
    class Socket:
        def connect(self, on_message, on_close): self.on_message = on_message
        def subscribe(self, tokens): pass
        def close(self): pass
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    first = MarketBar(instrument, datetime(2026, 1, 1, 10, 0), 100, 101, 99, 100)
    second = MarketBar(instrument, datetime(2026, 1, 1, 10, 1), 101, 102, 100, 101)
    emitted = []
    feed = ZerodhaFeed(Socket(), [1], lambda payload: payload, emitted.append, ReconnectPolicy())
    feed.start()
    feed.socket.on_message(first)
    assert not emitted
    feed.socket.on_message(second)
    assert emitted == [first]
    feed.flush()
    assert emitted == [first, second]


def test_zerodha_feed_tracks_per_instrument_health() -> None:
    class Socket:
        def connect(self, on_message, on_close): self.on_message = on_message
        def subscribe(self, tokens): pass
        def close(self): pass
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    first = MarketBar(instrument, datetime(2026, 1, 1, 10, 0), 100, 101, 99, 100)
    second = MarketBar(instrument, datetime(2026, 1, 1, 10, 1), 101, 102, 100, 101)
    feed = ZerodhaFeed(Socket(), [1], lambda payload: payload, lambda bar: None, watchdog_interval_seconds=60)
    feed.start()
    feed.socket.on_message(first)
    feed.socket.on_message(second)
    health = feed.health_snapshot()
    feed.stop()
    assert health["instruments"]["NSE:NIFTY"]["tick_count"] == 2
    assert health["instruments"]["NSE:NIFTY"]["last_closed_bar_at"].startswith("2026-01-01T10:00")


def test_api_requires_bearer_token_except_health(tmp_path: Path) -> None:
    app = create_app(RuntimeStore(tmp_path), auth_token="secret")
    client = app.test_client()
    assert client.get("/api/v1/health").status_code == 200
    assert client.get("/api/v1/runtime").status_code == 401
    response = client.get("/api/v1/runtime", headers={"Authorization": "Bearer secret"})
    assert response.status_code == 200
    assert response.headers["Access-Control-Allow-Origin"] == "http://127.0.0.1:8502"


def test_start_requires_zerodha_auth_and_returns_login_url(tmp_path: Path) -> None:
    class Auth:
        def access_token(self): return None
        def login_url(self): return "https://kite.test/login"
    response = create_app(RuntimeStore(tmp_path), Auth()).test_client().post("/api/v1/runtime/start")
    assert response.status_code == 409
    assert response.get_json()["error"]["code"] == "auth_required"
    assert response.get_json()["error"]["login_url"] == "https://kite.test/login"


def test_zerodha_browser_callback_exchanges_redirect_token(tmp_path: Path) -> None:
    class Auth:
        def exchange(self, token): assert token == "callback-token"
        def access_token(self): return "access"
    response = create_app(RuntimeStore(tmp_path), Auth()).test_client().get(
        "/zerodha/callback?status=success&request_token=callback-token"
    )
    assert response.status_code == 200
    assert b"connected successfully" in response.data


def test_api_serves_versioned_openapi_document(tmp_path: Path) -> None:
    response = create_app(RuntimeStore(tmp_path)).test_client().get("/api/v1/openapi.json")
    document = response.get_json()
    assert response.status_code == 200
    assert document["openapi"] == "3.0.3"
    assert "/api/v1/runtime" in document["paths"]


def test_zerodha_feed_reconnects_with_bounded_backoff() -> None:
    class Socket:
        def __init__(self):
            self.connections = 0

        def connect(self, on_message, on_close):
            self.connections += 1
            self.close_callback = on_close

        def subscribe(self, tokens):
            self.tokens = tokens

        def close(self):
            pass
    socket = Socket()
    bars = []
    bar = MarketBar(Instrument("NIFTY", "NSE", "INDEX"), datetime(2026, 1, 1, 10), 1, 1, 1, 1)
    feed = ZerodhaFeed(socket, [1], lambda _: bar, bars.append, ReconnectPolicy(2, 1, 2), lambda delay: None)
    feed.start()
    socket.close_callback()
    assert socket.connections == 2
    socket.close_callback()
    assert socket.connections == 3


def test_zerodha_feed_uses_long_cooldown_for_rate_limited_close() -> None:
    class Socket:
        def __init__(self):
            self.connections = 0

        def connect(self, on_message, on_close):
            self.connections += 1
            self.close_callback = on_close

        def subscribe(self, tokens):
            pass

        def close(self):
            pass

    socket = Socket()
    pauses = []
    feed = ZerodhaFeed(
        socket,
        [1],
        lambda payload: payload,
        lambda bar: None,
        ReconnectPolicy(2, 1, 2, 10),
        pauses.append,
    )
    feed.start()
    socket.close_callback(1006, "WebSocket connection upgrade failed (429 - TooManyRequests)")

    assert pauses == [10]
    assert socket.connections == 2
    socket.close_callback()
    assert socket.connections == 3


def test_zerodha_order_submission_is_disabled_for_paper_runtime() -> None:
    class Client:
        def place_order(self, **kwargs):
            return "kite-1"

        def order_history(self, order_id):
            return [{"status": "OPEN"}, {"status": "COMPLETE", "filled_quantity": 2, "average_price": 101, "exchange_timestamp": "2026-01-01T10:00:00+05:30"}]
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    order = OrderIntent("client-1", instrument, OrderSide.BUY, 2)
    broker = ZerodhaBroker(Client())
    try:
        broker.submit(order)
    except RuntimeError as exc:
        assert "disabled" in str(exc)
    else:
        raise AssertionError("paper runtime submitted a real Zerodha order")


def test_zerodha_socket_defers_subscription_until_connected(monkeypatch) -> None:
    import kiteconnect
    from ftx_paper.broker.zerodha import create_kite_socket

    created = {}

    class FakeTicker:
        MODE_QUOTE = 2

        def __init__(self, *args, **kwargs):
            self.ws = None
            self.subscribed = []
            created["ticker"] = self

        def connect(self, threaded=False, **kwargs):
            pass

        def subscribe(self, tokens):
            assert self.ws is not None
            self.subscribed.append(list(tokens))

        def set_mode(self, mode, tokens):
            assert self.ws is not None

        def close(self):
            pass

    monkeypatch.setattr(kiteconnect, "KiteTicker", FakeTicker)
    socket = create_kite_socket("key", "token")
    socket.connect(lambda _message: None, lambda *_args: None)
    socket.subscribe([1, 2])

    ticker = created["ticker"]
    assert ticker.subscribed == []
    ticker.ws = object()
    ticker.on_connect(ticker.ws, {})
    assert ticker.subscribed == [[1, 2]]
