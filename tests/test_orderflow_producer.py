from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy import CausalOrderflowProducer, source_fingerprint


def test_causal_producer_builds_complete_five_minute_context() -> None:
    producer = CausalOrderflowProducer()
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    start = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    for index in range(5):
        timestamp = start + timedelta(minutes=index)
        producer.ingest(MarketBar(instrument, timestamp, 99.0, 102.0, 98.0, 100.0 + index, 10.0))

    context = producer.context("2026-01-01", "09:34")

    assert context is not None
    parent = context.observations_for("5m")[0]
    assert context.validate_complete_parent(parent)
    assert parent.features["open"] == 99.0
    assert parent.features["high"] == 102.0
    assert parent.features["low"] == 98.0
    assert parent.features["close"] == 104.0
    assert parent.features["volume"] == 50.0
    assert parent.observed_at == start
    assert parent.available_at == start + timedelta(minutes=5)
    assert parent.context is not None
    assert parent.context.replay_provenance["input_availability"] == "complete"
    assert parent.context.replay_provenance["gap_status"] == "none"
    assert parent.context.replay_provenance["source_fingerprint"]
    assert parent.location_observations
    assert all(item.available_at == parent.available_at for item in parent.location_observations)
    assert all(item.source_aggregate_id == parent.observation_id for item in parent.location_observations)
    by_alias = {item.location_alias: item for item in parent.location_observations}
    assert "VWAP" not in by_alias
    assert by_alias["SESSION_HIGH"].location_type == "session_reference"
    assert all("vwap" not in parent.features for parent in context.observations)


def test_causal_producer_uses_canonical_source_fingerprint_for_children() -> None:
    producer = CausalOrderflowProducer()
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    start = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    bar = MarketBar(instrument, start, 99.0, 102.0, 98.0, 100.0, 10.0)
    producer.ingest(bar)

    context = producer.context("2026-01-01", "09:15")
    assert context is not None
    child = context.observations_for("1m")[0]
    assert child.context is not None
    assert child.context.replay_provenance["source_fingerprint"] == source_fingerprint((bar,))


def test_causal_producer_accepts_detached_prior_close_for_opening_gap() -> None:
    producer = CausalOrderflowProducer()
    producer.set_prior_close(100.0)
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    start = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    producer.ingest(MarketBar(instrument, start, 102.0, 103.0, 101.0, 102.0, 10.0))

    context = producer.context("2026-01-01", "09:15")

    assert context is not None
    assert context.observations_for("1m")[0].features["previous_close"] == 100.0


def test_causal_producer_keeps_later_parent_windows_complete() -> None:
    producer = CausalOrderflowProducer()
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    start = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    for index in range(10):
        timestamp = start + timedelta(minutes=index)
        producer.ingest(MarketBar(instrument, timestamp, 99.0, 102.0, 98.0, 100.0 + index, 10.0))

    context = producer.context("2026-01-01", "09:40")

    assert context is not None
    parents = context.observations_for("5m")
    assert len(parents) == 2
    assert all(context.validate_complete_parent(parent) for parent in parents)
    assert [parent.context.child_observation_ids for parent in parents if parent.context] == [
        tuple(item.observation_id for item in context.observations_for("1m")[:5]),
        tuple(item.observation_id for item in context.observations_for("1m")[5:10]),
    ]


def test_causal_producer_rejects_mixed_source_instruments() -> None:
    producer = CausalOrderflowProducer()
    timestamp = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    producer.ingest(MarketBar(
        Instrument("NIFTYFUT", "NSE", "FUTURES"), timestamp,
        99.0, 102.0, 98.0, 100.0, 10.0,
    ))
    with pytest.raises(ValueError, match="source identity changed"):
        producer.ingest(MarketBar(
            Instrument("BANKFUT", "NSE", "FUTURES"), timestamp,
            99.0, 102.0, 98.0, 100.0, 10.0,
        ))


def test_causal_producer_ignores_duplicate_source_bar() -> None:
    producer = CausalOrderflowProducer()
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    timestamp = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    bar = MarketBar(instrument, timestamp, 99.0, 102.0, 98.0, 100.0, 10.0)

    producer.ingest(bar)
    producer.ingest(bar)

    context = producer.context("2026-01-01", "09:15")
    assert context is not None
    assert len(context.observations_for("1m")) == 1


def test_causal_producer_uses_configured_session_alignment() -> None:
    producer = CausalOrderflowProducer(session_start="10:00", session_end="10:30")
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    start = datetime(2026, 1, 1, 4, 30, tzinfo=timezone.utc)
    for index in range(5):
        timestamp = start + timedelta(minutes=index)
        producer.ingest(MarketBar(instrument, timestamp, 99.0, 102.0, 98.0, 100.0, 10.0))
    context = producer.context("2026-01-01", "10:04")
    assert context is not None
    assert context.observations_for("5m")[0].interval_start == start


def test_incomplete_source_row_is_not_published_as_one_or_five_minute_evidence() -> None:
    producer = CausalOrderflowProducer()
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    start = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    for index in range(5):
        timestamp = start + timedelta(minutes=index)
        producer.ingest(MarketBar(
            instrument, timestamp, 99.0, 102.0, 98.0, 100.0 + index,
            None if index == 2 else 10.0,
        ))

    context = producer.context("2026-01-01", "09:19")

    assert context is not None
    assert len(context.observations_for("1m")) == 4
    assert context.observations_for("5m") == ()
