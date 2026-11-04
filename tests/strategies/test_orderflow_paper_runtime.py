from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.runtime.session import RuntimeSession
from ftx_paper.runtime import RuntimeStore
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.strategies.orderflow import PaperOrderflowRuntime
from ftx_paper.strategies.orderflow.evidence_publication import AtrPublication
from ftx_paper.strategies.orderflow.market_observation import observe_market
from ftx_paper.strategies.orderflow.vwap_location import causal_vwap_prefix
from ftx_paper.strategies.orderflow.providers import OrderflowInputStream
from ftx_paper.strategies.orderflow.timestamps import nifty_label, parse_nifty_label


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


def test_input_stream_reports_selected_bundle_route_and_observation_counts() -> None:
    bar = MarketBar(
        instrument=Instrument("NIFTY", "NFO", "FUTURES"),
        timestamp=datetime(2026, 10, 1, 4, 0, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )

    provider = OrderflowInputStream()
    provider.append_continuous_bars((bar,))
    bundle_input = provider(
        SimpleNamespace(bars={MarketRole.FUTURES: bar}),
    )

    assert bundle_input.input_diagnostic["input_source"] == "stream_envelope"
    assert bundle_input.input_diagnostic["observations_1m"] == 0
    assert bundle_input.input_diagnostic["observations_5m"] == 0
    assert "eligible_location_records" in bundle_input.input_diagnostic

    duplicate_input = provider(SimpleNamespace(bars={MarketRole.FUTURES: bar}))

    assert duplicate_input.input_diagnostic["input_source"] == "stream_envelope"
    assert duplicate_input.input_diagnostic["observations_1m"] == 0


def test_input_stream_orders_rows_before_observation() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bars = tuple(
        MarketBar(
            instrument=instrument,
            timestamp=datetime(2026, 10, 1, 4, minute, tzinfo=timezone.utc),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10,
        )
        for minute in (1, 0)
    )

    provider = OrderflowInputStream.from_continuous_bars(bars)

    assert [item.input.one_minute.time.minute for item in provider.envelopes] == [0, 1]


def test_live_inputs_publish_opening_range_only_when_first_five_minute_completes() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bars = tuple(
        MarketBar(
            instrument=instrument,
            timestamp=datetime(2026, 10, 1, 3, 45 + minute, tzinfo=timezone.utc),
            open=100.0 + minute,
            high=101.0 + minute,
            low=99.0 + minute,
            close=100.5 + minute,
            volume=10,
        )
        for minute in range(5)
    )
    atr = AtrPublication(
        2.0,
        parse_nifty_label("2026-10-01T09:00"),
        parse_nifty_label("2026-10-01T09:00"),
        "atr:2026-10-01",
        ("atr-source",),
    )

    provider = OrderflowInputStream.from_continuous_bars(
        bars,
        atr_values={"2026-10-01": atr},
        prior_session_levels={"2026-10-01": {"low": 95.0, "high": 105.0}},
    )

    inputs = tuple(envelope.input for envelope in provider.envelopes)
    assert all(item.session_levels is None for item in inputs[:-1])
    assert inputs[-1].session_levels is not None
    assert inputs[-1].session_levels.opening_range_available_at == inputs[-1].one_minute.available_at
    assert inputs[-1].five_minute is not None
    assert len(inputs[-1].vwap_measurements) == 6
    assert all(
        {item.location for item in input.observations_1m} == {"PD_LOW", "PD_HIGH"}
        for input in inputs[:-1]
    )
    assert {item.location for item in inputs[-1].observations_1m} > {"PD_LOW", "PD_HIGH"}
    assert inputs[-1].observations_5m
    rows = [
        {
            "time": nifty_label(bar.timestamp),
            "session_date": "2026-10-01",
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "symbol": bar.instrument.symbol,
            "expiry": bar.instrument.expiry or "",
        }
        for bar in bars
    ]
    aggregates = observe_market(rows).aggregates
    expected_vwap = (
        *causal_vwap_prefix(aggregates["1m"], session_date="2026-10-01"),
        *causal_vwap_prefix(aggregates["5m"], session_date="2026-10-01"),
    )
    assert inputs[-1].vwap_measurements == expected_vwap


def test_live_restart_rederives_prior_levels_when_setup_follows_seeding() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")

    def bar(minute: int) -> MarketBar:
        return MarketBar(
            instrument=instrument,
            timestamp=datetime(2026, 10, 1, 3, 45 + minute, tzinfo=timezone.utc),
            open=100.0 + minute,
            high=101.0 + minute,
            low=99.0 + minute,
            close=100.5 + minute,
            volume=10,
        )

    atr = AtrPublication(
        2.0,
        parse_nifty_label("2026-10-01T09:00"),
        parse_nifty_label("2026-10-01T09:00"),
        "atr:2026-10-01",
        ("atr-source",),
    )

    provider = OrderflowInputStream.from_continuous_bars(tuple(bar(m) for m in range(5)))
    provider.publish_setup(
        "2026-10-01",
        atr=atr,
        prior_session_levels={"low": 95.0, "high": 105.0},
    )
    clock = provider(SimpleNamespace(bars={MarketRole.FUTURES: bar(5)}))

    assert clock.session_levels is not None
    assert clock.session_levels.prior_day_low == 95.0
    assert clock.session_levels.prior_day_high == 105.0
    assert {item.location for item in clock.observations_1m} > {"PD_LOW", "PD_HIGH"}


def test_live_input_prefix_does_not_depend_on_future_bars() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bars = tuple(
        MarketBar(
            instrument=instrument,
            timestamp=datetime(2026, 10, 1, 3, 45 + minute, tzinfo=timezone.utc),
            open=100.0 + minute,
            high=101.0 + minute,
            low=99.0 + minute,
            close=100.5 + minute,
            volume=10,
        )
        for minute in range(5)
    )

    prefix = OrderflowInputStream.from_continuous_bars(bars[:1])
    full = OrderflowInputStream.from_continuous_bars(bars)

    assert prefix.envelopes[0].input == full.envelopes[0].input


def test_orderflow_stream_does_not_publish_future_atr() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bar = MarketBar(
        instrument=instrument,
        timestamp=datetime(2026, 10, 1, 3, 45, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )
    future_atr = AtrPublication(
        2.0,
        parse_nifty_label("2026-10-01T09:15"),
        parse_nifty_label("2026-10-01T10:00"),
        "atr:2026-10-01",
        ("atr-source",),
    )

    stream = OrderflowInputStream.from_continuous_bars(
        (bar,), atr_values={"2026-10-01": future_atr},
    )

    assert stream.envelopes[0].input.atr is not None
    assert stream.envelopes[0].input.atr.value is None
    assert "atr_not_yet_available" in stream.envelopes[0].unavailable


def test_orderflow_stream_publishes_accumulated_vwap_after_atr_arrives() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bars = tuple(
        MarketBar(
            instrument=instrument,
            timestamp=datetime(2026, 10, 1, 3, 45 + minute, tzinfo=timezone.utc),
            open=100.0 + minute,
            high=101.0 + minute,
            low=99.0 + minute,
            close=100.5 + minute,
            volume=10,
        )
        for minute in range(6)
    )
    stream = OrderflowInputStream.from_continuous_bars(bars[:5])
    atr = AtrPublication(
        2.0,
        parse_nifty_label("2026-10-01T09:20"),
        parse_nifty_label("2026-10-01T09:20"),
        "atr:2026-10-01",
        ("atr-source",),
    )

    stream.publish_setup("2026-10-01", atr=atr)
    stream.append_continuous_bars(bars[5:])

    latest = stream.envelopes[-1].input
    assert latest.atr is not None and latest.atr.value == 2.0
    assert len(latest.vwap_measurements) == 7
    assert any(item.location == "VWAP" for item in latest.observations_1m)
    assert latest.session_levels is not None
    assert latest.session_levels.opening_range_available_at == parse_nifty_label("2026-10-01T09:20")


def test_orderflow_stream_reports_incomplete_five_minute_bucket_and_recovers() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    minute_offsets = (0, 1, 3, 4, 5, 6, 7, 8, 9)
    bars = tuple(
        MarketBar(
            instrument=instrument,
            timestamp=datetime(2026, 10, 1, 3, 45, tzinfo=timezone.utc) + timedelta(minutes=offset),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10,
        )
        for offset in minute_offsets
    )

    stream = OrderflowInputStream.from_continuous_bars(bars)
    gap_input = stream.envelopes[4].input
    recovered_input = stream.envelopes[-1].input

    assert gap_input.input_diagnostic["five_minute_gap"]["missing_minutes"] == ("09:17",)
    assert recovered_input.five_minute is not None
    assert nifty_label(recovered_input.five_minute.time).endswith("T09:20")


def test_orderflow_stream_ignores_duplicate_live_bar_without_advancing() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bar = MarketBar(
        instrument=instrument,
        timestamp=datetime(2026, 10, 1, 3, 45, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )
    stream = OrderflowInputStream.from_continuous_bars((bar,))

    stream.append_continuous_bars((bar,))

    assert len(stream.envelopes) == 1

    earlier = MarketBar(
        instrument=instrument,
        timestamp=datetime(2026, 10, 1, 3, 44, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )
    with pytest.raises(ValueError, match="strictly ordered"):
        stream.append_continuous_bars((earlier,))

    assert len(stream.envelopes) == 1


def test_orderflow_stream_deduplicates_history_and_target_overlap() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bar = MarketBar(
        instrument=instrument,
        timestamp=datetime(2026, 10, 1, 3, 45, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )

    stream = OrderflowInputStream.from_continuous_bars(
        (bar,), feature_history_bars=(bar,),
    )

    assert len(stream.envelopes) == 1


def test_orderflow_stream_logs_conflicting_incremental_row(caplog) -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bar = MarketBar(
        instrument=instrument,
        timestamp=datetime(2026, 10, 1, 3, 45, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )
    conflicting = MarketBar(
        instrument=instrument,
        timestamp=bar.timestamp,
        open=100.0,
        high=101.0,
        low=99.0,
        close=101.5,
        volume=10,
    )

    stream = OrderflowInputStream.from_continuous_bars((bar,))

    with caplog.at_level("WARNING"):
        with pytest.raises(ValueError, match="conflicting canonical input row"):
            stream.append_continuous_bars((conflicting,))

    message = " ".join(caplog.messages)
    assert "identity=('2026-10-01', '2026-10-01T09:15')" in message
    assert "previous_origin=startup_seed" in message
    assert "incoming_origin=incremental_append" in message
    assert "'close': (100.5, 101.5)" in message


def test_live_ingress_consumes_already_appended_bar_without_reappending() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")
    bar = MarketBar(
        instrument=instrument,
        timestamp=datetime(2026, 10, 1, 3, 45, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10,
    )
    stream = OrderflowInputStream.from_continuous_bars((bar,))

    observed = stream(SimpleNamespace(bars={MarketRole.FUTURES: bar}))

    assert observed.one_minute.time == stream.envelopes[-1].input.one_minute.time
    assert len(stream.envelopes) == 1


def test_live_input_stream_rolls_over_session_without_resetting_order() -> None:
    instrument = Instrument("NIFTY", "NFO", "FUTURES")

    def bar(session_day: int, minute: int) -> MarketBar:
        return MarketBar(
            instrument=instrument,
            timestamp=datetime(2026, 10, session_day, 3, 45 + minute, tzinfo=timezone.utc),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10,
        )

    provider = OrderflowInputStream.from_continuous_bars((bar(1, 0),))
    provider.append_continuous_bars((bar(2, 0),))

    assert len(provider.envelopes) == 2
    assert provider.envelopes[-1].input.one_minute.session_date == "2026-10-02"
    assert provider.envelopes[-1].input.prior_session == "2026-10-01"


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
