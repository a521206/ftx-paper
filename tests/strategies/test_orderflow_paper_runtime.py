from datetime import datetime, timezone
from types import SimpleNamespace

from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.runtime.session import RuntimeSession
from ftx_paper.runtime import RuntimeStore
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.strategies.orderflow import PaperOrderflowRuntime
from ftx_paper.strategies.orderflow.boundary.factory import OrderflowStrategyFactory
from ftx_paper.strategies.shared.factory import StrategyContext


def test_paper_runtime_constructs_one_shared_transition_owner() -> None:
    runtime = PaperOrderflowRuntime()

    assert runtime.strategy._transition_owner is runtime.transition_owner
    assert runtime.transition_owner.execution_adapter is runtime.execution_adapter
    assert runtime.snapshot()["runtime_schema_version"] == 1


def test_orderflow_factory_uses_paper_runtime_owner() -> None:
    strategy = OrderflowStrategyFactory().create(StrategyContext())

    assert strategy.transition_owner.location_registry.stream_id == "NIFTY::1m"


def test_bounded_replay_uses_fresh_owner_and_is_idempotent() -> None:
    bar = MarketBar(
        instrument=Instrument("NIFTY", "NFO", "FUTURES"),
        timestamp=datetime(2026, 10, 1, 4, 0, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )
    bundle = SimpleNamespace(bars={MarketRole.FUTURES: bar})

    first = PaperOrderflowRuntime()
    first.replay((bundle, bundle))
    retry = PaperOrderflowRuntime()
    retry.replay((bundle,))

    assert first.transition_owner.parity_streams == retry.transition_owner.parity_streams
    assert first.transition_owner.clock == retry.transition_owner.clock
    assert PaperOrderflowRuntime().replay((bundle,), cancelled=lambda: True) == ()


def test_live_session_binds_existing_paper_execution_service(tmp_path) -> None:
    runtime = PaperOrderflowRuntime()
    session = RuntimeSession(RuntimeStore(tmp_path), None, [], engine=PaperEngine(runtime.strategy))

    assert runtime.execution_adapter.service is session.execution
    assert runtime.execution_adapter.diagnostic is False


def test_execution_notifications_are_recorded_by_canonical_owner_once() -> None:
    runtime = PaperOrderflowRuntime()
    notification = ExecutionNotification(
        event_type="FILLED",
        client_order_id="order-1",
        status="FILLED",
        quantity=1,
        filled_quantity=1,
        price=100.0,
        occurred_at="2026-10-01T04:00:00+00:00",
        available_at="2026-10-01T04:00:00+00:00",
    )

    runtime.on_execution_event(notification)
    runtime.on_execution_event(notification)

    streams = runtime.transition_owner.parity_streams
    assert len(streams["execution_transitions"]) == 1
    assert streams["execution_transitions"][0]["kind"] == "filled"


def test_parity_payload_publishes_phase35_evidence_shape() -> None:
    rows = [
        {
            "time": f"2026-10-01T09:{15 + index:02d}",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1,
            "oi": 0,
            "symbol": "NIFTY",
            "expiry": "",
            "contract_type": "FUT",
            "session_date": "2026-10-01",
        }
        for index in range(5)
    ]

    payload = PaperOrderflowRuntime.from_rows(rows).parity_payload(rows)

    assert isinstance(payload["evidence"], dict)
    assert isinstance(payload["evidence"]["atr"], dict)
