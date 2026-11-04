from datetime import datetime, timedelta, timezone

from ftx_paper.strategies.orderflow.location_observation import (
    causal_children,
    location_observation,
)
from ftx_paper.strategies.orderflow.coordinator import (
    OrderflowInput,
    OrderflowCoordinator,
)
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import LocationRecord, LocationRegistry
from ftx_paper.strategies.orderflow.timestamps import nifty_label


def _aggregate(minute: int, *, timeframe: str = "1m") -> Aggregate:
    start = datetime(2026, 10, 1, 3, 15, tzinfo=timezone.utc) + timedelta(minutes=minute)
    end = start + timedelta(minutes=4 if timeframe == "5m" else 0)
    available = end + timedelta(minutes=1)
    return Aggregate(
        time=start,
        end_time=end,
        available_at=available,
        timeframe=timeframe,
        source_bar_count=5 if timeframe == "5m" else 1,
        open=100.0,
        high=102.0,
        low=98.0,
        close=101.0,
        volume=1,
        symbol="NIFTY",
        expiry="",
        session_date="2026-10-01",
        source_fingerprint=f"source-{minute}-{timeframe}",
    )


def test_causal_children_requires_complete_aligned_available_minutes() -> None:
    parent = _aggregate(0, timeframe="5m")
    children = tuple(_aggregate(minute) for minute in range(5))

    assert causal_children(parent, children) == children
    assert causal_children(parent, children[:-1]) == ()
    assert causal_children(parent, (children[0], *children[2:])) == ()


def test_location_observation_uses_canonical_band_intersection() -> None:
    aggregate = _aggregate(0)
    record = LocationRecord(
        location_id="OR_HIGH:2026-10-01:1",
        alias="OR_HIGH",
        price=100.0,
        source_session="2026-10-01",
        created_at=aggregate.available_at,
        available_at=aggregate.available_at,
        stream_id="test",
        band=LocationBand(99.0, 101.0),
        generation=1,
    )

    observation = location_observation(aggregate, record)

    assert observation.band_contact is True
    assert observation.close_relation == "INSIDE_BAND"
    assert observation.source_aggregate_id == nifty_label(aggregate.time)
    assert observation.observation_id == (
        f"{nifty_label(aggregate.time)}:1m:OR_HIGH:2026-10-01:1"
    )


def test_coordinator_observes_five_minute_clock_without_observations() -> None:
    one_minute = _aggregate(0)
    five_minute = _aggregate(0, timeframe="5m")
    coordinator = OrderflowCoordinator(LocationRegistry("test"))

    class Detector:
        def __init__(self) -> None:
            self.called = False

        def observe_1m(self, **_: object) -> list[object]:
            return []

        def observe(self, **_: object) -> list[object]:
            self.called = True
            return []

    detector = Detector()
    coordinator.detector = detector  # type: ignore[assignment]
    item = OrderflowInput(one_minute, five_minute=five_minute)
    coordinator.ingest(item)
    coordinator.observe_interactions(item, new_five_minute=True)

    assert detector.called is True
