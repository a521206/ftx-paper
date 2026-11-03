from datetime import datetime, timezone

from ftx_paper.strategies.orderflow.context_location_replay import (
    _replay_market,
)
from ftx_paper.strategies.orderflow.evidence_publication import (
    AtrPublication,
    EvidencePublication,
)
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.market_observation import MarketObservation
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import LocationRecord
from ftx_paper.strategies.orderflow.timestamps import serialize_utc
from ftx_paper.strategies.orderflow.value_publication import ValuePublication
from ftx_paper.strategies.orderflow.vwap_location import VwapMeasurement


SESSION = "2026-10-05"
UTC = timezone.utc


def _aggregate(timeframe: str, minute: int, *, count: int) -> Aggregate:
    start = datetime(2026, 10, 5, 3, minute, tzinfo=UTC)
    end = start.replace(minute=minute + count - 1)
    available = end.replace(minute=end.minute + 1)
    return Aggregate(
        start, end, available, timeframe, count, 100.0, 101.0, 99.0, 100.5,
        100, "NIFTY", "", SESSION, "source",
    )


def _evidence(*locations: LocationRecord) -> EvidencePublication:
    measurements = tuple(
        VwapMeasurement(
            item.timeframe, SESSION, item.source_aggregate_id or "", item.created_at,
            item.available_at, item.price, item.source_observation_id or "", "test",
        )
        for item in locations if item.alias == "VWAP"
    )
    return EvidencePublication(
        AtrPublication.unavailable(), tuple(locations), measurements, (),
        ValuePublication(None, None, None, (), "missing_prior_session_value_evidence"),
        location_stream_id="stream",
    )


def _vwap(available_at: datetime) -> LocationRecord:
    return LocationRecord(
        "VWAP:2026-10-05:5m:1", "VWAP", 100.0, SESSION,
        available_at, available_at, "stream", LocationBand(99.0, 101.0),
        frozen_atr=4.0, source_aggregate_id="2026-10-05T08:45", source_observation_id="vwap-1",
        timeframe="5m", source_method="vwap_typical_price_volume_v1",
    )


def test_vwap_is_unavailable_before_first_completed_5m() -> None:
    one = _aggregate("1m", 14, count=1)
    five = _aggregate("5m", 15, count=5)
    replay = _replay_market(
        MarketObservation({"1m": (one,), "5m": (five,)}, (), {}, "source"),
        session_date=SESSION,
        evidence=_evidence(_vwap(five.available_at)),
        replay_clock=None,
    )

    first = replay.context_location[0]
    assert first["values"]["locations"] == []
    assert "missing_location_evidence" in first["unavailable"]


def test_vwap_same_timestamp_uses_completed_5m_availability() -> None:
    children = tuple(_aggregate("1m", minute, count=1) for minute in range(15, 20))
    five = _aggregate("5m", 15, count=5)
    five = Aggregate(
        five.time, five.end_time, children[-1].available_at, five.timeframe,
        five.source_bar_count, five.open, five.high, five.low, five.close,
        five.volume, five.symbol, five.expiry,
        five.session_date, five.source_fingerprint,
    )
    replay = _replay_market(
        MarketObservation({"1m": children, "5m": (five,)}, (), {}, "source"),
        session_date=SESSION,
        evidence=_evidence(_vwap(five.available_at)),
        replay_clock=None,
    )

    assert replay.context_location[-1]["values"]["locations"]


def test_5m_child_observations_publish_at_their_causal_availability() -> None:
    children = tuple(_aggregate("1m", minute, count=1) for minute in range(15, 20))
    five = _aggregate("5m", 15, count=5)
    replay = _replay_market(
        MarketObservation({"1m": children, "5m": (five,)}, (), {}, "source"),
        session_date=SESSION,
        evidence=_evidence(
            LocationRecord(
                "OR_LOW:2026-10-05:1", "OR_LOW", 100.0, SESSION,
                five.available_at, five.available_at, "stream",
                LocationBand(99.0, 101.0), frozen_atr=4.0, generation=1,
            ),
        ),
        replay_clock=None,
    )

    observations = replay.context_location[-1]["values"]["locations"]
    child_observations = observations[:5]
    assert [item["available_at"] for item in child_observations] == [
        serialize_utc(five.available_at)
    ] * 5


def test_misaligned_5m_records_unavailable_context_location_fact() -> None:
    one = _aggregate("1m", 19, count=1)
    five = _aggregate("5m", 15, count=5)
    replay = _replay_market(
        MarketObservation({"1m": (one,), "5m": (five,)}, (), {}, "source"),
        session_date=SESSION,
        evidence=_evidence(),
        replay_clock=None,
    )

    # The 5m aggregate has no causal 1m children in the supplied market.
    fact = replay.context_location[-1]
    assert fact["values"]["timeframe"] == "5m"
    assert fact["values"]["locations"] == []
    assert "missing_context" in fact["unavailable"]
