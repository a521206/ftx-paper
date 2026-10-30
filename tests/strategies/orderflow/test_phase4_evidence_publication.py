from __future__ import annotations

import pytest

from ftx_paper.strategies.orderflow import (
    AtrPublication,
    publish_phase4a_evidence,
)
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry
from ftx_paper.strategies.orderflow.vwap_location import causal_vwap_prefix


def _rows(count: int = 10) -> list[dict[str, object]]:
    return [{
        "time": f"09:{15 + index:02d}", "open": 100 + index,
        "high": 101 + index, "low": 99 + index, "close": 100.5 + index,
        "volume": 10, "symbol": "NIFTY", "expiry": "",
        "contract_type": "CONTINUOUS", "session_date": "2026-09-29",
    } for index in range(count)]


def _atr() -> AtrPublication:
    return AtrPublication(4.0, "2026-09-29T09:15", "2026-09-29T09:16", "atr:1", ("atr:1",))


def _published(rows: list[dict[str, object]], atr: AtrPublication):
    from ftx_paper.strategies.orderflow import observe_market

    market = observe_market(rows)
    registry = LocationRegistry("NIFTY::FUT:1m")
    registry.register_opening_range(99.0, 105.0, "2026-09-29T09:20", source_session="2026-09-29", atr=atr.value)
    registry.register_prior_day_extremes(98.0, 102.0, "2026-09-29T09:15", source_session="2026-09-28", atr=atr.value)
    registry.publish_session_extrema(market.aggregates["5m"], session_date="2026-09-29", atr=atr.value)
    measurements = tuple(
        measurement
        for timeframe in ("1m", "5m")
        for measurement in causal_vwap_prefix(market.aggregates[timeframe], session_date="2026-09-29")
    )
    for measurement in measurements:
        registry.publish_vwap(
            value=measurement.value, available_at=measurement.available_at,
            source_session="2026-09-29", source_aggregate_id=measurement.source_aggregate_id,
            source_observation_id=measurement.source_observation_id,
            timeframe=measurement.timeframe, atr=atr.value,
        )
    return publish_phase4a_evidence(
        atr=atr, locations=registry.records.values(), vwap_measurements=measurements,
    )


def test_phase4a_accepts_canonical_locations_and_vwap_publications() -> None:
    evidence = _published(_rows(), _atr())

    aliases = {item.alias for item in evidence.locations}
    assert {"OR_LOW", "OR_HIGH", "PD_LOW", "PD_HIGH", "SESSION_HIGH", "SESSION_LOW", "VWAP"} <= aliases
    session_highs = [item for item in evidence.locations if item.alias == "SESSION_HIGH"]
    assert session_highs[-1].parent_extreme_generation == session_highs[-2].generation
    assert all(item.available_at >= item.created_at for item in evidence.locations)
    assert evidence.vwap_measurements
    assert all(item.source_observation_id for item in evidence.vwap_measurements)


def test_phase4a_keeps_missing_atr_and_prior_levels_explicitly_unavailable() -> None:
    evidence = publish_phase4a_evidence(
        atr=AtrPublication.unavailable(),
    )

    assert not any(item.alias in {"OR_LOW", "OR_HIGH"} for item in evidence.locations)
    assert evidence.vwap_measurements == ()
    assert evidence.unavailable == ("missing_atr",)
    assert evidence.prior_value.unavailable_reason == "missing_prior_session_value_evidence"


def test_phase4a_rejects_non_causal_atr_publication() -> None:
    with pytest.raises(ValueError, match="availability cannot precede"):
        AtrPublication(4.0, "2026-09-29T09:20", "2026-09-29T09:19", "atr:1", ("atr:1",))


def test_phase4a_does_not_build_bands_before_atr_availability() -> None:
    evidence = publish_phase4a_evidence(
        atr=AtrPublication(4.0, "2026-09-29T09:24", "2026-09-29T09:25", "atr:1", ("atr:1",)),
        unavailable=("atr_not_yet_available",),
    )

    assert not any(item.alias in {"OR_LOW", "OR_HIGH"} for item in evidence.locations)
    assert "atr_not_yet_available" in evidence.unavailable


def test_phase4a_ignores_incomplete_opening_aggregate() -> None:
    incomplete = Aggregate(
        "2026-09-29T09:15", "2026-09-29T09:19", "2026-09-29T09:20", "5m", 4,
        100.0, 104.0, 99.0, 103.0, 40, "NIFTY", "", "CONTINUOUS",
        "2026-09-29", "source",
    )
    evidence = publish_phase4a_evidence(
        atr=_atr(), unavailable=("missing_opening_range",),
    )

    assert incomplete.source_bar_count == 4
    assert evidence.locations == ()
    assert "missing_opening_range" in evidence.unavailable
