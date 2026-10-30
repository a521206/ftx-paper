from __future__ import annotations

from datetime import datetime, timezone

from ftx_paper.strategies.orderflow import replay_context_location, replay_context_location_artifact
from ftx_paper.strategies.orderflow.hierarchy import MarketContext
from ftx_paper.strategies.orderflow.value_publication import PriorSessionValueReference, ValuePublication


def _rows(count: int = 10) -> list[dict[str, object]]:
    return [{
        "time": f"09:{15 + index:02d}", "open": 100 + index, "high": 101 + index,
        "low": 99 + index, "close": 100.5 + index, "volume": 10,
        "symbol": "NIFTY", "expiry": "", "contract_type": "CONTINUOUS",
        "session_date": "2026-09-29",
    } for index in range(count)]


def _prior_value() -> ValuePublication:
    observed = datetime(2026, 9, 29, 3, 0, tzinfo=timezone.utc)
    reference = PriorSessionValueReference(
        "2026-09-28", 98.0, 100.0, 102.0, observed, observed, ("value:2026-09-28",)
    )
    return ValuePublication(reference, observed, observed, reference.lineage)


def test_context_location_replay_emits_causal_unavailable_then_available_facts() -> None:
    result = replay_context_location(
        _rows(),
        session_date="2026-09-29",
        atr=4.0,
        prior_value=_prior_value(),
        contexts=[MarketContext("ctx", "1", "2026-09-29T09:20", "2026-09-29T09:20", "OR_LOW", "BALANCE")],
    )

    assert result.source_observations
    early = next(item for item in result.context_location if item["identity"] == "context-location:1m:2026-09-29T09:15")
    assert "opening_range_not_yet_available" in early["unavailable"]
    assert "missing_context" in early["unavailable"]

    five = next(item for item in result.context_location if item["identity"] == "context-location:5m:2026-09-29T09:15")
    assert five["causal_parent_id"] == "aggregate:5m:2026-09-29T09:15"
    assert five["unavailable"] == []
    locations = five["values"]["locations"]
    assert isinstance(locations, list)
    vwap = next(item for item in locations if item["location"] == "VWAP")
    assert vwap["source_observation_id"]
    assert len(vwap["child_observation_ids"]) == 5


def test_context_location_replay_does_not_infer_missing_atr() -> None:
    result = replay_context_location(
        _rows(5),
        session_date="2026-09-29",
        prior_value=_prior_value(),
        contexts=[MarketContext("ctx", "1", "2026-09-29T09:20", "2026-09-29T09:20", "OR_LOW", "BALANCE")],
    )

    assert result.context_location
    assert all("missing_atr" in item["unavailable"] for item in result.context_location)
    assert all(item["values"]["locations"] == [] for item in result.context_location)


def test_context_location_replay_consumes_phase3_aggregate_artifact() -> None:
    source = replay_context_location(_rows(5), session_date="2026-09-29")
    aggregate = source.source_observations[0]["values"]["aggregate"]
    phase3 = {
        "source_fingerprint": "source-1",
        "aggregates": {"1m": [aggregate], "5m": []},
    }

    result = replay_context_location_artifact(phase3, session_date="2026-09-29")

    assert len(result.source_observations) == 1
    assert result.context_location[0]["identity"] == "context-location:1m:2026-09-29T09:15"


def test_context_location_replay_reports_missing_prior_value_evidence() -> None:
    result = replay_context_location(_rows(5), session_date="2026-09-29", atr=4.0)

    assert result.context_location
    assert all("missing_prior_session_value_evidence" in item["unavailable"] for item in result.context_location)
