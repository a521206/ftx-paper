from datetime import datetime, timezone

from ftx_paper.broker import PaperBroker
from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.core import PaperEngine
from ftx_paper.core import CompletedBarAggregator
from ftx_paper.runtime import RuntimeSession, RuntimeStore


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


def test_closed_bar_is_processed_under_session_lifecycle(tmp_path):
    store = RuntimeStore(tmp_path)
    session = RuntimeSession(store, None, [], engine=PaperEngine())
    session.feed = Feed(session.on_closed_bar)
    session.broker = PaperBroker()
    store.write_status({"state": "RUNNING"})
    bar = MarketBar(Instrument("NIFTY", "NSE", "INDEX"), datetime.now(timezone.utc), 1, 2, 0, 1)
    session.on_closed_bar(bar)
    assert session.engine.bars_seen == 1
    assert store.read_events()[0]["event_type"] == "ENGINE_EVENT"


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
    })
    store.write_status({"state": "RUNNING"})

    bar = MarketBar(
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc),
        100, 102, 99, 101,
    )
    session.on_closed_bar(bar)

    status = store.read_status()
    assert status["pending_bundle_minutes"] == ["10:20"]
    assert status["pending_bundle_details"] == [{
        "minute": "10:20", "missing_roles": ["vix"], "roles_present": ["futures"]
    }]
    assert "last_completed_bundle_minute" not in status
    assert store.read_events() == []

    next_bar = MarketBar(
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        datetime(2026, 1, 1, 10, 21, tzinfo=timezone.utc),
        100, 102, 99, 101,
    )
    session.on_closed_bar(next_bar)

    events = store.read_events()
    assert events[0]["event_type"] == "BUNDLE_INCOMPLETE"
    assert events[0]["payload"]["minute"] == "10:20"
