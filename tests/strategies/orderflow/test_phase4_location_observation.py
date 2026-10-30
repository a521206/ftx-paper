import pytest

from ftx_paper.strategies.orderflow.location_observation import LocationObservation


def _observation(**changes: object) -> LocationObservation:
    values: dict[str, object] = {
        "location": "OR_LOW", "location_id": "OR_LOW:session:1", "reference_generation": 1,
        "reference": 100.0, "band_low": 99.0, "band_high": 101.0, "open": 100.0,
        "high": 101.0, "low": 99.0, "close": 100.0, "close_relation": "INSIDE_BAND",
        "band_contact": True, "source_aggregate_id": "2026-09-29T09:15",
        "observed_at": "2026-09-29T09:19", "available_at": "2026-09-29T09:20",
        "method": "test", "timeframe": "1m", "observation_id": "obs-1",
    }
    values.update(changes)
    return LocationObservation(**values)  # type: ignore[arg-type]


def test_observation_requires_causal_timestamps_and_valid_timeframe() -> None:
    with pytest.raises(ValueError, match="causally available"):
        _observation(available_at="2026-09-29T09:18")
    with pytest.raises(ValueError, match="timeframe must be"):
        _observation(timeframe="15m")


def test_vwap_and_five_minute_observations_require_lineage() -> None:
    with pytest.raises(ValueError, match="source observation ID"):
        _observation(location="VWAP")
    with pytest.raises(ValueError, match="exactly five"):
        _observation(timeframe="5m")
    observation = _observation(timeframe="5m", child_observation_ids=("a", "b", "c", "d", "e"))
    assert observation.to_dict()["child_observation_ids"] == ["a", "b", "c", "d", "e"]
