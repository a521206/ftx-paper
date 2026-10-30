import pytest

from ftx_paper.strategies.orderflow.location_geometry import LocationBand


def test_location_band_validates_and_intersects_inclusively() -> None:
    band = LocationBand(99.0, 101.0)
    assert band.center == 100.0
    assert band.intersects(low=101.0, high=102.0)
    assert not band.intersects(low=101.1, high=102.0)


def test_location_band_rejects_reversed_bounds() -> None:
    with pytest.raises(ValueError, match="low must not exceed high"):
        LocationBand(101.0, 99.0)
