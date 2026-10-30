"""Artifact-driven port of the canonical evidence publication sequence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from ..evidence_publication import AtrPublication, EvidencePublication, publish_phase4a_evidence
from ..hierarchy import MarketContext
from ..orderflow_common import Aggregate
from ..state_machine_types import LocationRegistry, SessionLevels
from ..value_publication import ValuePublication
from ..vwap_location import VwapMeasurement, causal_vwap_prefix


class EvidenceAdapter(Protocol):
    """Host-supplied canonical evidence unavailable in a market artifact."""

    def atr(self, session_date: str, symbol: str) -> AtrPublication: ...

    def session_levels(self, session_date: str) -> SessionLevels | None: ...

    def prior_session(self, session_date: str) -> str | None: ...

    def prior_value(self, session_date: str) -> ValuePublication: ...

    def contexts(self, session_date: str) -> Sequence[MarketContext]: ...


class UnavailableEvidenceAdapter:
    """Explicitly unavailable host evidence for artifact-only harness runs."""

    def atr(self, session_date: str, symbol: str) -> AtrPublication:
        del session_date, symbol
        return AtrPublication.unavailable("missing_atr_adapter")

    def session_levels(self, session_date: str) -> SessionLevels | None:
        del session_date
        return None

    def prior_session(self, session_date: str) -> str | None:
        del session_date
        return None

    def prior_value(self, session_date: str) -> ValuePublication:
        del session_date
        return ValuePublication(None, None, None, (), "missing_prior_session_value_evidence")

    def contexts(self, session_date: str) -> Sequence[MarketContext]:
        del session_date
        return ()


@dataclass(frozen=True, slots=True)
class Phase35Evidence:
    """Evidence publication plus the source scope used to create it."""

    session_date: str
    source_fingerprint: str
    publication: EvidencePublication


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
        "contract_type", "session_date", "source_fingerprint",
    }
    if not required <= set(value):
        raise ValueError(f"Phase 3 {timeframe} aggregate is incomplete")
    if value["timeframe"] != timeframe:
        raise ValueError(f"Phase 3 aggregate timeframe mismatch: {timeframe}")
    aggregate_fingerprint = value["source_fingerprint"]
    if not isinstance(aggregate_fingerprint, str) or not aggregate_fingerprint:
        raise ValueError("Phase 3 aggregate source fingerprint is required")
    return Aggregate(
        _text(value["time"], "time"), _text(value["end_time"], "end_time"),
        _text(value["available_at"], "available_at"), timeframe,
        _integer(value["source_bar_count"], "source_bar_count"), _number(value["open"], "open"),
        _number(value["high"], "high"), _number(value["low"], "low"),
        _number(value["close"], "close"), _integer(value["volume"], "volume"),
        _text(value["symbol"], "symbol"), _text(value["expiry"], "expiry"),
        _text(value["contract_type"], "contract_type"),
        _text(value["session_date"], "session_date"), aggregate_fingerprint,
    )


def _artifact_aggregates(artifact: dict[str, object], session_date: str) -> dict[str, tuple[Aggregate, ...]]:
    if artifact.get("artifact_type") != "canonical_p2_market_observation":
        raise ValueError("invalid Phase 3 artifact type")
    if artifact.get("schema_version") != 1:
        raise ValueError("invalid Phase 3 schema version")
    source_fingerprint = artifact.get("source_fingerprint")
    if not isinstance(source_fingerprint, str) or not source_fingerprint:
        raise ValueError("Phase 3 source_fingerprint is required")
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


class Phase35Producer:
    """Reproduce canonical publication ordering from a Phase 3 artifact."""

    def produce(self, artifact: dict[str, object], *, session_date: str, adapter: EvidenceAdapter) -> Phase35Evidence:
        aggregates = _artifact_aggregates(artifact, session_date)
        first = next((item for item in aggregates["1m"] + aggregates["5m"]), None)
        if first is None:
            raise ValueError("Phase 3 artifact has no aggregates")
        atr = adapter.atr(session_date, first.symbol)
        unavailable: list[str] = []
        if atr.value is None:
            unavailable.append(atr.unavailable_reason or "missing_atr")

        registry = LocationRegistry(f"{first.symbol}:{first.expiry}:{first.contract_type}:1m")
        levels = adapter.session_levels(session_date)
        prior_session = adapter.prior_session(session_date)
        if levels is None:
            unavailable.append("missing_session_levels")
        elif atr.value is not None:
            registry.publish_session_levels(
                levels, source_session=session_date, prior_session=prior_session, atr=atr.value,
            )
        elif levels is not None:
            unavailable.append("session_levels_waiting_for_atr")

        measurements: list[VwapMeasurement] = []
        if atr.value is not None:
            completed_5m = tuple(
                item for item in aggregates["5m"]
                if item.source_bar_count == 5 and item.available_at >= item.end_time
            )
            if completed_5m:
                high = max(completed_5m, key=lambda item: item.high)
                low = min(completed_5m, key=lambda item: item.low)
                registry.publish_session_extreme(
                    "SESSION_HIGH", high.high, high, atr=atr.value,
                )
                registry.publish_session_extreme(
                    "SESSION_LOW", low.low, low, atr=atr.value,
                )
            for timeframe in ("1m", "5m"):
                measurements.extend(causal_vwap_prefix(
                    aggregates[timeframe], session_date=session_date,
                ))
            for measurement in measurements:
                registry.publish_vwap(
                    value=measurement.value, available_at=measurement.available_at,
                    source_session=session_date, source_aggregate_id=measurement.source_aggregate_id,
                    source_observation_id=measurement.source_observation_id,
                    timeframe=measurement.timeframe, atr=atr.value,
                )
        else:
            unavailable.append("vwap_locations_waiting_for_atr")

        return Phase35Evidence(
            session_date=session_date,
            source_fingerprint=str(artifact["source_fingerprint"]),
            publication=publish_phase4a_evidence(
                atr=atr,
                locations=registry.records.values(),
                vwap_measurements=measurements,
                prior_value=adapter.prior_value(session_date),
                contexts=adapter.contexts(session_date),
                unavailable=unavailable,
                context_stream_id=getattr(adapter, "context_stream_id", None),
                location_stream_id=getattr(adapter, "location_stream_id", None),
            ),
        )


def produce_evidence(
    artifact: dict[str, object], *, session_date: str, adapter: EvidenceAdapter,
) -> Phase35Evidence:
    return Phase35Producer().produce(artifact, session_date=session_date, adapter=adapter)
