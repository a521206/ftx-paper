from datetime import datetime, timezone
from types import SimpleNamespace

from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.strategies.orderflow.boundary.orderflow import OrderflowStrategy
from ftx_paper.strategies.orderflow.coordinator import OrderflowClockInput, OrderflowCoordinator
from ftx_paper.strategies.orderflow.evidence_publication import AtrPublication
from ftx_paper.strategies.orderflow.interaction_detector import InteractionTransition
from ftx_paper.strategies.orderflow.interactions import LocationInteraction
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry, SessionLevels


def _bar(time: str) -> Aggregate:
    return Aggregate(
        time, time, time, "1m", 1, 100.0, 101.0, 99.0, 100.0, 10,
        "NIFTY", "", "FUT", time[:10], f"fp:{time}",
    )


def test_coordinator_publishes_source_once_and_preserves_missing_evidence() -> None:
    coordinator = OrderflowCoordinator(LocationRegistry("stream"))
    item = OrderflowClockInput(_bar("2026-09-29T09:15"))

    first = coordinator.advance(item)
    duplicate = coordinator.advance(item)

    assert first.source_event is not None
    assert first.source_event["identity"] == "aggregate:1m:2026-09-29T09:15"
    assert duplicate.events == ()
    assert duplicate.source_event == first.source_event
    assert coordinator.detector.interactions == {}


def test_strategy_delegates_source_publication_to_coordinator() -> None:
    coordinator = OrderflowCoordinator(LocationRegistry("stream"))
    strategy = OrderflowStrategy(coordinator=coordinator)
    bundle = SimpleNamespace(
        bars={MarketRole.FUTURES: MarketBar(
            Instrument("NIFTY", "NFO", "FUTURES", "2026-09-29"),
            datetime(2026, 9, 29, 3, 45, tzinfo=timezone.utc),
            100.0, 101.0, 99.0, 100.0, 10,
        )},
    )

    decisions = strategy.on_bundle(bundle)

    assert [item.event_type for item in decisions] == ["SOURCE_OBSERVATION"]
    assert decisions[0].payload["identity"] == "aggregate:1m:2026-09-29T09:15"


def test_coordinator_publishes_supplied_session_levels_only_with_atr() -> None:
    coordinator = OrderflowCoordinator(LocationRegistry("stream"))
    levels = SessionLevels(99.0, 101.0, 98.0, 102.0, "2026-09-29T09:15", "2026-09-29T09:15")
    atr = AtrPublication(10.0, "2026-09-29T09:14", "2026-09-29T09:15", "atr", ("atr",))

    coordinator.advance(OrderflowClockInput(_bar("2026-09-29T09:15"), atr=atr, session_levels=levels))

    assert {record.alias for record in coordinator.location_registry.records.values()} == {
        "OR_LOW", "OR_HIGH", "PD_LOW", "PD_HIGH",
    }


def test_transition_events_use_canonical_lifecycle_and_observation_kinds() -> None:
    bar = _bar("2026-09-29T09:20")
    interaction = LocationInteraction.create(
        instrument="NIFTY", session_date=bar.session_date, location="PD_LOW",
        template_version="1", candle=bar, reference_level=100.0, atr=10.0,
        band=LocationBand(99.0, 101.0), created_at=bar.available_at,
        available_at=bar.available_at,
    )
    assert interaction is not None
    events = OrderflowCoordinator._transition_events(
        InteractionTransition(interaction.evaluation_snapshot(), created=True), bar,
    )
    assert [event["kind"] for event in events] == ["interaction_created"]
    assert events[0]["values"]["stage"] == "OBSERVING_LOCATION"
    assert events[0]["values"]["reference_level"] == 100.0
