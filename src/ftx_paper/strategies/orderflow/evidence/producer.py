"""Artifact-driven port of the canonical evidence publication sequence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from ..evidence_publication import AtrPublication, publish_phase4a_evidence
from ..orderflow_common import Aggregate
from ..state_machine_types import LocationRegistry, SessionLevels
from ..value_publication import ValuePublication
from ..vwap_location import VwapMeasurement, causal_vwap_prefix
from ..timestamps import coerce_timestamp


class EvidenceAdapter(Protocol):
    """Host-supplied canonical evidence unavailable in a market artifact."""

    def atr(self, session_date: str, symbol: str) -> AtrPublication: ...

    def session_levels(self, session_date: str) -> SessionLevels | None: ...

    def prior_session(self, session_date: str) -> str | None: ...

    def prior_value(self, session_date: str) -> ValuePublication: ...



def _text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Phase 3 aggregate {field} must be a string")
    return value


def _number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Phase 3 aggregate {field} must be numeric")
    return float(value)


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Phase 3 aggregate {field} must be an integer")
    return value


def _aggregate(value: object, *, timeframe: str) -> Aggregate:
    if not isinstance(value, dict):
        raise ValueError(f"Phase 3 {timeframe} aggregate must be an object")
    required = {
        "time", "end_time", "available_at", "timeframe", "source_bar_count",
        "open", "high", "low", "close", "volume", "symbol", "expiry",
        "session_date", "source_fingerprint",
    }
    if not required <= set(value):
        raise ValueError(f"Phase 3 {timeframe} aggregate is incomplete")
    if value["timeframe"] != timeframe:
        raise ValueError(f"Phase 3 aggregate timeframe mismatch: {timeframe}")
    aggregate_fingerprint = value["source_fingerprint"]
    if not isinstance(aggregate_fingerprint, str) or not aggregate_fingerprint:
        raise ValueError("Phase 3 aggregate source fingerprint is required")
    return Aggregate(
        coerce_timestamp(_text(value["time"], "time")), coerce_timestamp(_text(value["end_time"], "end_time")),
        coerce_timestamp(_text(value["available_at"], "available_at")), timeframe,
        _integer(value["source_bar_count"], "source_bar_count"), _number(value["open"], "open"),
        _number(value["high"], "high"), _number(value["low"], "low"),
        _number(value["close"], "close"), _integer(value["volume"], "volume"),
        _text(value["symbol"], "symbol"), _text(value["expiry"], "expiry"),
        _text(value["session_date"], "session_date"), aggregate_fingerprint,
    )


def _validate_artifact(artifact: dict[str, object]) -> str:
    if artifact.get("artifact_type") != "canonical_p2_market_observation":
        raise ValueError("invalid Phase 3 artifact type")
    if artifact.get("schema_version") != 1:
        raise ValueError("invalid Phase 3 schema version")
    source_fingerprint = artifact.get("source_fingerprint")
    if not isinstance(source_fingerprint, str) or not source_fingerprint:
        raise ValueError("Phase 3 source_fingerprint is required")
    return source_fingerprint


def _artifact_aggregates(artifact: dict[str, object], session_date: str) -> dict[str, tuple[Aggregate, ...]]:
    _validate_artifact(artifact)
    raw = artifact.get("aggregates")
    if not isinstance(raw, dict) or set(raw) != {"1m", "5m"}:
        raise ValueError("Phase 3 aggregates must contain exactly 1m and 5m")
    result: dict[str, tuple[Aggregate, ...]] = {}
    for timeframe in ("1m", "5m"):
        values = raw[timeframe]
        if not isinstance(values, list):
            raise ValueError(f"Phase 3 {timeframe} aggregates must be a list")
        aggregates = tuple(
            _aggregate(item, timeframe=timeframe)
            for item in values
        )
        if any(item.session_date != session_date for item in aggregates):
            raise ValueError(f"Phase 3 {timeframe} aggregate session mismatch")
        result[timeframe] = aggregates
    return result


def produce_evidence(
    artifact: dict[str, object], *, session_date: str, adapter: EvidenceAdapter,
    parsed_aggregates: Mapping[str, Sequence[Aggregate]] | None = None,
    precomputed_vwap_measurements: Mapping[str, Sequence[VwapMeasurement]] | None = None,
) -> dict[str, object]:
    source_fingerprint = _validate_artifact(artifact)
    aggregates = (
        {timeframe: tuple(values) for timeframe, values in parsed_aggregates.items()}
        if parsed_aggregates is not None
        else _artifact_aggregates(artifact, session_date)
    )
    if set(aggregates) != {"1m", "5m"}:
        raise ValueError("Phase 3 aggregates must contain exactly 1m and 5m")
    if any(
        aggregate.session_date != session_date
        for values in aggregates.values()
        for aggregate in values
    ):
        raise ValueError("Phase 3 aggregate session mismatch")
    first = next((item for item in aggregates["1m"] + aggregates["5m"]), None)
    if first is None:
        raise ValueError("aggregate artifact has no aggregates")
    atr = adapter.atr(session_date, first.symbol)
    unavailable: list[str] = []
    if atr.value is None:
        unavailable.append(atr.unavailable_reason or "missing_atr")

    registry = LocationRegistry(f"{first.symbol}:{first.expiry}:1m")
    levels = adapter.session_levels(session_date)
    prior_session = adapter.prior_session(session_date)
    if levels is None:
        unavailable.append("missing_session_levels")
    elif atr.value is not None:
        registry.publish_session_levels(
            levels, source_session=session_date, prior_session=prior_session, atr=atr.value,
        )
    else:
        unavailable.append("session_levels_waiting_for_atr")

    measurements: list[VwapMeasurement] = []
    if atr.value is not None:
        registry.publish_session_extrema(
            aggregates["5m"], session_date=session_date, atr=atr.value,
        )
        for timeframe in ("1m", "5m"):
            if precomputed_vwap_measurements is None:
                measurements.extend(causal_vwap_prefix(
                    aggregates[timeframe], session_date=session_date,
                ))
                continue
            available_at = max(
                (aggregate.available_at for aggregate in aggregates[timeframe]),
                default=None,
            )
            measurements.extend(
                item for item in precomputed_vwap_measurements[timeframe]
                if available_at is not None and item.available_at <= available_at
            )
        for measurement in measurements:
            registry.publish_vwap(
                value=measurement.value, available_at=measurement.available_at,
                source_session=session_date, source_aggregate_id=measurement.source_aggregate_id,
                source_observation_id=measurement.source_observation_id,
                timeframe=measurement.timeframe, atr=atr.value,
            )
    else:
        unavailable.append("vwap_locations_waiting_for_atr")

    return {
        "session_date": session_date,
        "source_fingerprint": source_fingerprint,
        "publication": publish_phase4a_evidence(
            atr=atr,
            locations=registry.records.values(),
            vwap_measurements=measurements,
            prior_value=adapter.prior_value(session_date),
            contexts=(),
            unavailable=unavailable,
            location_stream_id=getattr(adapter, "location_stream_id", None),
        ),
    }
