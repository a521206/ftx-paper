from pathlib import Path

from ftx_paper.api import create_app
from ftx_paper.runtime import RuntimeStore
from ftx_paper.ui import create_ui_app
from ftx_paper.broker.zerodha import ReconnectPolicy, ZerodhaFeed
from ftx_paper.broker.zerodha import ZerodhaBroker, classify_runtime_roles, load_startup_backfill, resolve_instruments
from ftx_paper.broker.zerodha import ZerodhaAuth
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
    assert response.get_json()["events"][0]["payload"] == {"value": 1}


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
    assert roles["futures"]["instrument_token"] == 1
    assert roles["vix"]["instrument_token"] == 2


def test_zerodha_historical_rows_become_normalized_market_bars() -> None:
    class Client:
        def historical_data(self, token, start, end, interval):
            return [{"date": "2026-01-01T10:00:00+05:30", "open": 1, "high": 2, "low": 0, "close": 1.5, "volume": 10}]
    bars = load_startup_backfill(Client(), [{"instrument_token": 1, "symbol": "NIFTYFUT", "exchange": "NFO"}])
    assert bars[0].close == 1.5 and bars[0].instrument.symbol == "NIFTYFUT"


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
    socket.close_callback()
    assert socket.connections == 3


def test_zerodha_ack_is_distinct_from_polled_fill() -> None:
    class Client:
        def place_order(self, **kwargs):
            return "kite-1"

        def order_history(self, order_id):
            return [{"status": "OPEN"}, {"status": "COMPLETE", "filled_quantity": 2, "average_price": 101, "exchange_timestamp": "2026-01-01T10:00:00+05:30"}]
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    order = OrderIntent("client-1", instrument, OrderSide.BUY, 2)
    broker = ZerodhaBroker(Client())
    ack = broker.submit(order)
    fill = broker.poll_fill(order, ack.broker_order_id)
    assert ack.status == "ACCEPTED"
    assert fill is not None and fill.price == 101 and fill.quantity == 2


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
