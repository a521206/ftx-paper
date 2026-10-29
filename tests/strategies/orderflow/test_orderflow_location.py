from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.strategy import LocationBand, LocationRecord, LocationRegistry


UTC = timezone.utc


def _record(location_id: str, generation: int, available_at: datetime) -> LocationRecord:
    return LocationRecord(location_id, "opening_range", 100.0, LocationBand(99.0, 101.0), generation, available_at, f"aggregate-{generation}")


def test_location_registry_resolves_latest_generation_as_of_cutoff() -> None:
    start = datetime(2026, 1, 1, 9, 15, tzinfo=UTC)
    registry = LocationRegistry()
    registry.publish(_record("or-1", 1, start))
    registry.publish(_record("or-2", 2, start + timedelta(minutes=5)))

    assert registry.resolve_as_of("opening_range", start - timedelta(seconds=1)) is None
    assert registry.resolve_as_of("opening_range", start + timedelta(minutes=1)).generation == 1
    assert registry.resolve_as_of("opening_range", start + timedelta(minutes=6)).generation == 2


def test_location_registry_keeps_identity_immutable() -> None:
    start = datetime(2026, 1, 1, 9, 15, tzinfo=UTC)
    registry = LocationRegistry()
    registry.publish(_record("or-1", 1, start))
    with pytest.raises(ValueError, match="already"):
        registry.publish(_record("or-1", 2, start + timedelta(minutes=1)))


def test_location_registry_resolves_overlap_by_priority_and_generation() -> None:
    start = datetime(2026, 1, 1, 9, 15, tzinfo=UTC)
    registry = LocationRegistry()
    registry.publish(LocationRecord(
        "low", "low", 100, LocationBand(99, 101), 1, start, "a", priority=1,
    ))
    registry.publish(LocationRecord(
        "high", "high", 100, LocationBand(99.5, 100.5), 1, start, "b", priority=2,
    ))

    assert registry.resolve_owner(100, start).location_id == "high"
