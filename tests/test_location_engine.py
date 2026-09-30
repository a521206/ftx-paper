import json
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.core.location_engine import Cell, Location, LocationDetector, LocationFeatures, detect_all_locations


_FIXTURES = json.loads((Path(__file__).parent / "fixtures" / "location_cells.json").read_text())


def test_location_and_cell_golden_fixtures() -> None:
    for expected in _FIXTURES.values():
        features = LocationFeatures(**expected["features"])
        locations = detect_all_locations(features, expected["close"])
        assert [location.value for location in locations] == expected["locations"]
        assert Cell(*locations).name == expected["cell"]


def test_cell_normalization_is_independent_of_input_order() -> None:
    assert Cell("or_low", "session_low", "vwap_zone").name == "vwap_zone+session_low+or_low"


def test_cell_normalization_prunes_implied_new_extremes() -> None:
    assert Cell("new_low", "or_low", "prior_day_low").name == "or_low+prior_day_low"
    assert Cell("new_high", "or_high", "prior_day_high").name == "or_high+prior_day_high"


def test_empty_cell_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least one location"):
        Cell()


def test_cell_parse_accepts_cell_or_wire_name() -> None:
    assert Cell.parse("new_high") == Cell(Location.NEW_HIGH)
    cell = Cell(Location.VWAP_ZONE)
    assert Cell.parse(cell) is cell


def test_detector_vwap_uses_canonical_ordered_accumulation() -> None:
    start = datetime(2026, 1, 5, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))
    instrument = Instrument("NIFTY26JANFUT", "NFO", "FUTURES", "2026-01-29")
    bars = tuple(
        MarketBar(instrument, start + timedelta(minutes=index), value, value, value, value, 1)
        for index, value in enumerate((0.1, 0.2, 0.3, 0.4))
    )
    detector = LocationDetector()
    snapshot = None
    for bar in bars:
        snapshot = detector.observe(bar)

    assert snapshot is not None
    assert snapshot.features.vwap == (0.1 + 0.2 + 0.3) / 3
