from datetime import datetime, timedelta, timezone

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy import CausalOrderflowProducer, VwapLocationProvider


def test_vwap_provider_publishes_immutable_measurements_without_structural_features() -> None:
    producer = CausalOrderflowProducer()
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    start = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    for index in range(5):
        producer.ingest(MarketBar(
            instrument, start + timedelta(minutes=index),
            99.0, 102.0 + index, 98.0, 100.0 + index, 10.0,
        ))
    context = producer.context("2026-01-01", "09:34")
    assert context is not None

    measurements = VwapLocationProvider().publish(
        context.observations, cutoff=context.cutoff,
    )

    assert measurements
    assert all(item.calculation_version for item in measurements)
    assert all(item.source_aggregate_id for item in measurements)
    assert all("vwap" not in aggregate.features for aggregate in context.observations)
