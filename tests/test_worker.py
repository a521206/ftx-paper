from datetime import datetime, timezone
from pathlib import Path

from ftx_paper.broker import PaperBroker
from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.core import PaperEngine
from ftx_paper.strategy import ConfiguredLiveStrategy
from ftx_paper.runtime import RuntimeStore, RuntimeWorker
from ftx_paper.execution import PositionLedger


class FakeFeed:
    def __init__(self, bars):
        self._bars = bars

    def bars(self):
        yield from self._bars


def test_worker_persists_engine_events_and_preserves_status(tmp_path: Path) -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    bar = MarketBar(instrument, datetime.now(timezone.utc), 1, 2, 0, 1)
    store = RuntimeStore(tmp_path)
    store.write_status({"capital": 100000, "open_positions": []})

    RuntimeWorker(store, PaperEngine(), FakeFeed([bar]), PaperBroker({"NIFTY": 1})).run()

    status = store.read_status()
    assert status["capital"] == 100000
    assert status["state"] == "STOPPED"
    assert store.read_events()[0]["event_type"] == "ENGINE_EVENT"


def test_worker_can_persist_ledger_state(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    ledger = PositionLedger(1000)
    RuntimeWorker(store, PaperEngine(), FakeFeed([]), PaperBroker({}), ledger).run()

    assert store.read_status()["capital"] == 1000


def test_runtime_commands_are_queued_and_claimed_once(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    store.enqueue_command("START")
    assert store.claim_commands() == [{"id": 1, "command": "START"}]
    assert store.claim_commands() == []


def test_runtime_events_are_idempotent_and_running_state_recovers(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path)
    assert store.append_event("ORDER_ACK", {"id": "1"}, "ack:1")
    assert not store.append_event("ORDER_ACK", {"id": "1"}, "ack:1")
    store.write_status({"state": "RUNNING"})
    assert store.recover_interrupted()
    assert store.read_status()["state"] == "RECOVERY_REQUIRED"


def test_worker_persists_strategy_provenance_on_status_and_decision(tmp_path: Path) -> None:
    instrument = Instrument("NIFTY", "NSE", "INDEX")
    bar = MarketBar(instrument, datetime.now(timezone.utc), 1, 2, 0, 1)
    store = RuntimeStore(tmp_path)
    engine = PaperEngine(ConfiguredLiveStrategy())
    RuntimeWorker(store, engine, FakeFeed([bar]), PaperBroker({})).run()
    assert store.read_status()["strategy"]["config_hash"]
    event = store.read_events()[0]
    assert event["payload"]["strategy_version"] == "0.2.0-live-composition"
