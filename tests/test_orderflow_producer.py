from datetime import datetime, timedelta, timezone

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy import CausalOrderflowProducer


def test_causal_producer_builds_complete_five_minute_context() -> None:
    producer = CausalOrderflowProducer()
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    start = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    for index in range(5):
        timestamp = start + timedelta(minutes=index)
        producer.ingest(MarketBar(instrument, timestamp, 99.0, 102.0, 98.0, 100.0 + index, 10.0))

    context = producer.context("2026-01-01", "15:34")

    assert context is not None
    parent = context.observations_for("5m")[0]
    assert context.validate_complete_parent(parent)
    assert parent.features["open"] == 99.0
    assert parent.features["high"] == 102.0
    assert parent.features["low"] == 98.0
    assert parent.features["close"] == 104.0
    assert parent.features["volume"] == 50.0
    assert parent.features["vwap"] is not None
    assert parent.observed_at == start
    assert parent.available_at == start + timedelta(minutes=5)
    assert parent.context is not None
    assert parent.context.replay_provenance["input_availability"] == "complete"
    assert parent.context.replay_provenance["gap_status"] == "none"
    assert parent.context.replay_provenance["source_fingerprint"]
