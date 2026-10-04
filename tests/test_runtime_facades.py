from datetime import datetime, timezone
from typing import cast

import pytest

from ftx_paper.contracts import Instrument, MarketBar, OrderIntent, OrderRole, OrderSide
from ftx_paper.market import DecisionBundle
from ftx_paper.contracts import RuntimeEvent
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.execution.service import ExecutionService
from ftx_paper.runtime import EventBus, FeedManager, OrderManager, PnlManager, StrategyRunner
from ftx_paper.ports import ExecutionPort, MarketDataPort, OrderStatusPort, PnlQueryPort
from ftx_paper.broker import PaperBroker
from ftx_paper.domain import PortfolioState
from ftx_paper.market import LiveMarketNormalizer
from ftx_paper.runtime.engine import EngineResult, PaperEngine


class Feed:
    def __init__(self):
        self.started = False
        self.stopped = False
        self.flushed = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True
        return None

    def flush(self):
        self.flushed = True


def test_feed_manager_delegates_lifecycle_without_owning_feed_state():
    feed = Feed()
    manager = FeedManager(feed)

    manager.start()
    manager.flush()
    assert manager.stop() is True
    assert feed.started and feed.flushed and feed.stopped


def test_feed_manager_fails_explicitly_for_unimplemented_capabilities():
    feed = Feed()
    manager = FeedManager(feed)
    with pytest.raises(NotImplementedError):
        manager.health()


def test_strategy_runner_delegates_to_one_engine():
    class Engine(PaperEngine):
        def on_bundle(self, bundle, context=None):
            return EngineResult(events=({"event_type": "DELEGATED"},))

    engine = Engine()
    runner = StrategyRunner(engine)
    bar = MarketBar(
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc),
        100, 100, 100, 100,
    )
    bundle = type("Bundle", (), {"bars": {}, "bundle_id": "bundle", "minute": "04:00"})()

    assert runner.evaluate(cast(DecisionBundle, bundle)).events == ({"event_type": "DELEGATED"},)
    assert runner.on_tick(bar) == ()


def test_execution_service_isolates_optional_observers_but_propagates_critical_failure():
    observed = []

    def failing_observer(_event):
        raise RuntimeError("observer failure")

    service = ExecutionService(
        None, None, object(), notify=lambda event: observed.append(event),
        observers=(failing_observer, lambda event: observed.append(event)),
    )
    notification = ExecutionNotification("FILLED", "order-1", "FILLED")

    service.publish(notification)
    assert observed == [notification, notification]

    critical = ExecutionService(None, None, object(), notify=lambda _event: (_ for _ in ()).throw(
        RuntimeError("critical failure"),
    ))
    with pytest.raises(RuntimeError, match="critical failure"):
        critical.publish(notification)


def test_contract_metadata_is_contextual_and_preserved():
    event = RuntimeEvent(
        "QUOTE", datetime.now(timezone.utc), {}, event_id="event-1", source="zerodha",
    )
    notification = ExecutionNotification(
        "FILLED", "order-1", "FILLED", event_id="event-2", source="paper",
    )

    assert event.event_id == "event-1" and event.strategy_id is None
    assert notification.event_id == "event-2" and notification.account_id is None


def test_event_bus_propagates_critical_and_isolates_optional_handlers():
    delivered = []
    event = RuntimeEvent("HEALTH", datetime.now(timezone.utc), {}, event_id="health-1")
    bus = EventBus(
        critical=(lambda item: delivered.append(("critical", item.event_id)),),
        optional=(lambda _item: (_ for _ in ()).throw(RuntimeError("projection")),
                  lambda item: delivered.append(("optional", item.event_id))),
    )

    bus.publish(event)

    assert delivered == [("critical", "health-1"), ("optional", "health-1")]


def test_capability_ports_are_split_and_paper_execution_is_compatible():
    broker = PaperBroker({"NIFTYFUT": 100.0})
    assert isinstance(broker, ExecutionPort)
    assert not isinstance(broker, MarketDataPort)
    assert isinstance(PnlManager(PortfolioState(1000.0)), PnlQueryPort)
    assert not isinstance(object(), OrderStatusPort)


def test_order_manager_unsupported_operations_are_explicit():
    service = ExecutionService(None, None, object())
    manager = OrderManager(service)
    with pytest.raises(NotImplementedError):
        manager.modify(cast(OrderIntent, None))
    with pytest.raises(NotImplementedError):
        manager.cancel("order-1")


def test_order_manager_submits_through_paper_coordinator():
    class Coordinator:
        def submit(self, _order):
            self.submitted = True

        submitted = False

    coordinator = Coordinator()
    service = ExecutionService(None, cast(PaperExecutionCoordinator, coordinator), object())
    manager = OrderManager(service)
    order = OrderIntent(
        "paper-1", Instrument("NIFTYFUT", "NFO", "FUTURES"), OrderSide.BUY, 1,
        role=OrderRole.ENTRY,
    )
    manager.submit(order)
    assert coordinator.submitted


def test_market_normalizer_owns_raw_payload_conversion():
    normalizer = LiveMarketNormalizer(
        [{"instrument_token": 1, "exchange": "NFO", "symbol": "NIFTYFUT", "instrument_type": "FUT"}],
        expiry_normalizer=lambda value: str(value) if value is not None else None,
    )

    bar = normalizer({"instrument_token": 1, "last_price": 101.5, "timestamp": 1_790_000_000_000})

    assert bar.instrument.symbol == "NIFTYFUT"
    assert bar.timestamp.tzinfo is not None
    assert bar.close == 101.5
