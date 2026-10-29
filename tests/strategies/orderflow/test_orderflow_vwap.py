from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy import (
    CausalOrderflowProducer, LocationRegistry, VwapLocationProvider, VwapMeasurement,
)
from ftx_paper.strategy.orderflow_vwap import publish_vwap_locations
from ftx_paper.strategy.orderflow_vwap import calculate_session_vwap, calculate_vwap_stop


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
    assert all(item.source_observation_id != item.source_aggregate_id for item in measurements)
    assert all("vwap" not in aggregate.features for aggregate in context.observations)


def test_vwap_locations_require_frozen_atr_and_preserve_measurement_provenance() -> None:
    now = datetime.now(timezone.utc)
    measurement = VwapMeasurement(
        "5m", "2026-01-01", "aggregate-1", "measurement-1", now, now, 100.0, "v1",
    )
    assert publish_vwap_locations(LocationRegistry(), (measurement,), atr=None) == ()
    registry = LocationRegistry()
    records = publish_vwap_locations(registry, (measurement,), atr=2.0)
    assert records[0].source_observation_id == "measurement-1"
    assert records[0].band.lower < records[0].price < records[0].band.upper
    assert records[0].frozen_atr == 2.0
    assert records[0].session_id == "2026-01-01"
    assert records[0].calculation_version == "v1"


def test_session_vwap_is_causal_typical_price_volume_weighted_and_resets_by_timezone() -> None:
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    first = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    bars = (
        MarketBar(instrument, first, 99, 103, 97, 101, 10),
        MarketBar(instrument, first + timedelta(minutes=5), 101, 105, 99, 103, 20),
        MarketBar(instrument, datetime(2026, 1, 2, 3, 45, tzinfo=timezone.utc), 110, 112, 108, 111, 10),
    )
    value, source = calculate_session_vwap(bars, cutoff=first + timedelta(minutes=6))
    assert value == (((103 + 97 + 101) / 3) * 10 + ((105 + 99 + 103) / 3) * 20) / 30
    assert source == bars[1]
    next_value, next_source = calculate_session_vwap(bars, cutoff=bars[-1].timestamp)
    assert next_value == pytest.approx((112 + 108 + 111) / 3)
    assert next_source == bars[-1]


def test_vwap_stop_reports_unavailable_invalid_and_directional_candidates() -> None:
    assert calculate_vwap_stop(direction="long", entry_price=100, vwap=None, atr=2).reason == "vwap_unavailable"
    assert calculate_vwap_stop(direction="long", entry_price=100, vwap=101, atr=2).reason == "vwap_stop_invalid"
    long_stop = calculate_vwap_stop(direction="long", entry_price=100, vwap=98, atr=2)
    short_stop = calculate_vwap_stop(direction="short", entry_price=100, vwap=102, atr=2)
    assert long_stop.candidate == 97.9
    assert short_stop.candidate == 102.1
