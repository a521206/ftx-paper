import json
from pathlib import Path

import pytest

from ftx_paper.core.location_engine import Cell, Location, LocationFeatures, detect_all_locations


_FIXTURES = json.loads((Path(__file__).parent / "fixtures" / "location_cells.json").read_text())


def test_location_and_cell_golden_fixtures() -> None:
    for expected in _FIXTURES.values():
        features = LocationFeatures(**expected["features"])
        locations = detect_all_locations(features, expected["close"])
        assert [location.value for location in locations] == expected["locations"]
        assert Cell(*locations).name == expected["cell"]


def test_cell_normalization_is_independent_of_input_order() -> None:
    assert Cell("or_low", "session_low", "vwap_zone").name == "vwap_zone+new_low+or_low"


def test_empty_cell_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least one location"):
        Cell()


def test_cell_parse_accepts_cell_or_wire_name() -> None:
    assert Cell.parse("new_high") == Cell(Location.NEW_HIGH)
    cell = Cell(Location.VWAP_ZONE)
    assert Cell.parse(cell) is cell
