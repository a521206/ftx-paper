from datetime import datetime, timezone
from typing import cast

import pytest

from ftx_paper.contracts import Instrument, MarketBar, OrderIntent, OrderRole, OrderSide
from ftx_paper.market import DecisionBundle
from ftx_paper.contracts import RuntimeEvent
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.execution.service import ExecutionService
from ftx_paper.runtime import EventBus, FeedManager, MultiStrategyEngine, OrderManager, PnlManager, StrategyInstance, StrategyRunner
from ftx_paper.ports import ExecutionPort, MarketDataPort, OrderStatusPort, PnlQueryPort
from ftx_paper.broker import PaperBroker
from ftx_paper.domain import PortfolioState
from ftx_paper.market import LiveMarketNormalizer
from ftx_paper.runtime.engine import EngineResult, PaperEngine
from ftx_paper.strategy.decision import LiveDecision
from ftx_paper.strategy.protocol import StrategyMetadata
from ftx_paper.strategy.exits import ExitAction


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


def test_multi_strategy_engine_dispatches_and_attributes_orders():
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    order = OrderIntent("decision-1", instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY)

    class Strategy:
        def __init__(self, name, should_fail=False):
            self.name = name
            self.should_fail = should_fail
            self.calls = 0

        @property
        def metadata(self):
            return StrategyMetadata(self.name, "1", self.name)

        def on_bundle(self, _bundle):
            self.calls += 1
            if self.should_fail:
                raise RuntimeError("isolated")
            return (LiveDecision("ENTRY", {"decision_id": self.name}, order),)

        def evaluate(self, bundle, _context):
            return self.on_bundle(bundle)

        def on_execution_event(self, _event):
            pass

        def snapshot(self):
            return {}

    first = Strategy("first")
    second = Strategy("second", should_fail=True)
    engine = MultiStrategyEngine({
        "first": StrategyInstance("first", first, account_id="paper"),
        "second": second,
    })
    bundle = type("Bundle", (), {"bars": {}, "bundle_id": "bundle", "minute": "04:00", "complete": True})()

    with pytest.raises(RuntimeError, match="strategy 'second' failed"):
        engine.on_bundle(cast(DecisionBundle, bundle))

    assert first.calls == second.calls == 1


def test_multi_strategy_engine_collects_attributed_orders_in_configured_order():
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")

    class Strategy:
        def __init__(self, name):
            self.name = name

        @property
        def metadata(self):
            return StrategyMetadata(self.name, "1", self.name)

        def on_bundle(self, _bundle):
            return (LiveDecision("ENTRY", {}, OrderIntent(
                self.name, instrument, OrderSide.BUY, 1, role=OrderRole.ENTRY,
            )),)

        def on_execution_event(self, _event):
            pass

        def snapshot(self):
            return {}

    engine = MultiStrategyEngine({"first": Strategy("first"), "second": Strategy("second")})
    bundle = type("Bundle", (), {"bars": {}, "bundle_id": "bundle", "minute": "04:00", "complete": True})()

    result = engine.on_bundle(cast(DecisionBundle, bundle))

    assert [order.strategy_id for order in result.orders] == ["first", "second"]
    assert [order.client_order_id for order in result.orders] == ["first:first", "second:second"]


def test_multi_strategy_engine_attributes_protective_exits_to_owner():
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")

    class Strategy:
        @property
        def metadata(self):
            return StrategyMetadata("owner", "1", "owner")

        def on_closed_bar(self, _bar):
            return (ExitAction("stop", 99.0, OrderIntent(
                "exit", instrument, OrderSide.SELL, 1, role=OrderRole.EXIT,
            )),)

    engine = MultiStrategyEngine({"owner": Strategy()})
    bar = MarketBar(instrument, datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc), 100, 100, 100, 100)

    actions = engine.on_closed_bar(bar)

    assert actions[0].intent.strategy_id == "owner"


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


def test_order_manager_authorizes_and_submits_through_paper_coordinator():
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
    manager.authorize_and_submit(order)
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
