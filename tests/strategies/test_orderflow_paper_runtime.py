from datetime import datetime, timezone
from types import SimpleNamespace

from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.runtime.session import RuntimeSession
from ftx_paper.runtime import RuntimeStore
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.strategies.orderflow import PaperOrderflowRuntime
from ftx_paper.strategies.orderflow.providers import CanonicalInputProvider


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


def test_input_provider_reports_selected_bundle_route_and_observation_counts() -> None:
    bar = MarketBar(
        instrument=Instrument("NIFTY", "NFO", "FUTURES"),
        timestamp=datetime(2026, 10, 1, 4, 0, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )

    provider = CanonicalInputProvider()
    bundle_input = provider(
        SimpleNamespace(bars={MarketRole.FUTURES: bar}),
    )

    assert bundle_input.input_diagnostic["input_source"] == "provider_appended_bar"
    assert bundle_input.input_diagnostic["observations_1m"] == 0
    assert bundle_input.input_diagnostic["observations_5m"] == 0
    assert "eligible_location_records" in bundle_input.input_diagnostic

    explicit_input = provider(SimpleNamespace(
        supporting_inputs={"orderflow_clock_input": provider.envelopes[0].input},
    ))

    assert explicit_input.input_diagnostic["input_source"] == "bundle_orderflow_clock_input"
    assert explicit_input.input_diagnostic["observations_1m"] == 0


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
            "session_date": "2026-10-01",
        }
        for index in range(5)
    ]

    payload = PaperOrderflowRuntime.from_rows(rows).parity_payload(rows)

    assert isinstance(payload["evidence"], dict)
    assert isinstance(payload["evidence"]["atr"], dict)
