"""Optional causal VWAP provider for native order-flow locations and stops."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from .orderflow_contract import stable_orderflow_id
from .orderflow_geometry import BAND_ATR_FRACTION
from .orderflow_location import LocationBand, LocationRecord


class MarketAggregate(Protocol):
    """Minimum completed aggregate contract consumed by the VWAP owner."""

    @property
    def aggregate_id(self) -> str: ...
    @property
    def observation_id(self) -> str: ...
    @property
    def timeframe(self) -> str: ...
    @property
    def observed_at(self) -> datetime: ...
    @property
    def available_at(self) -> datetime: ...
    @property
    def features(self) -> Any: ...
    @property
    def context(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class VwapCalculationConfig:
    calculation_version: str = "vwap_typical_price_volume_v1"

    def __post_init__(self) -> None:
        if not self.calculation_version.strip():
            raise ValueError("invalid VWAP calculation configuration")
@dataclass(frozen=True, slots=True)
class VwapMeasurement:
    timeframe: str
    session_id: str
    source_aggregate_id: str
    source_observation_id: str
    observed_at: datetime
    available_at: datetime
    value: float
    calculation_version: str


@dataclass(slots=True)
class _VwapState:
    aggregate_ids: tuple[str, ...] = ()
    measurements: list[VwapMeasurement] = field(default_factory=list)
    cumulative_volume: float = 0.0
    cumulative_value: float = 0.0


@dataclass(frozen=True, slots=True)
class VwapStopResult:
    candidate: float | None
    reason: str
    vwap: float | None = None
    atr: float | None = None
    source_aggregate_id: str | None = None
    source_observation_id: str | None = None


def _number(aggregate: MarketAggregate, name: str) -> float | None:
    features = getattr(aggregate, "features", None)
    value = features.get(name) if features is not None else getattr(aggregate, name, None)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _session_key(timestamp: datetime, timezone: str, session_start: str) -> str:
    local = timestamp.astimezone(ZoneInfo(timezone))
    return f"{local.date().isoformat()}:{session_start}"


def _observed_at(aggregate: Any) -> datetime:
    observed = getattr(aggregate, "observed_at", None)
    return observed if observed is not None else aggregate.timestamp


def _available_at(aggregate: Any) -> datetime:
    available = getattr(aggregate, "available_at", None)
    return available if available is not None else aggregate.timestamp


def _interval_end(aggregate: Any) -> datetime:
    return getattr(aggregate, "interval_end", _observed_at(aggregate))


def _source_fingerprint(aggregate: Any) -> str:
    context = getattr(aggregate, "context", None)
    provenance = getattr(context, "replay_provenance", {}) if context is not None else {}
    value = provenance.get("source_fingerprint")
    return str(value) if value else str(getattr(aggregate, "source_observation_id", aggregate.aggregate_id))


def calculate_session_vwap(
    aggregates: Sequence[Any], *, cutoff: datetime,
    timezone: str = "Asia/Kolkata", session_start: str = "09:15",
) -> tuple[float | None, MarketAggregate | None]:
    """Calculate a completed, session-scoped typical-price VWAP causally."""
    eligible = [
        item for item in aggregates
        if getattr(item, "timeframe", "5m") == "5m"
        and _available_at(item) <= cutoff
    ]
    if not eligible:
        return None, None
    session = _session_key(_observed_at(eligible[-1]), timezone, session_start)
    bars = [item for item in eligible if _session_key(_observed_at(item), timezone, session_start) == session]
    valid: list[tuple[MarketAggregate, float, float, float, float]] = []
    for item in bars:
        high = _number(item, "high")
        low = _number(item, "low")
        close = _number(item, "close")
        volume = _number(item, "volume")
        if high is not None and low is not None and close is not None and volume is not None and volume > 0:
            valid.append((item, high, low, close, volume))
    if not valid:
        return None, None
    total_volume = sum(item[4] for item in valid)
    total_value = sum(((item[1] + item[2] + item[3]) / 3.0) * item[4] for item in valid)
    return total_value / total_volume, valid[-1][0]


def calculate_vwap_stop(
    *, direction: str, entry_price: float, vwap: float | None, atr: float | None,
    source_aggregate_id: str | None = None, source_observation_id: str | None = None,
) -> VwapStopResult:
    if vwap is None or atr is None or atr <= 0:
        return VwapStopResult(None, "vwap_unavailable", vwap, atr, source_aggregate_id, source_observation_id)
    candidate = vwap - 0.05 * atr if direction == "long" else vwap + 0.05 * atr
    protective = candidate < entry_price if direction == "long" else candidate > entry_price
    if not protective:
        return VwapStopResult(None, "vwap_stop_invalid", vwap, atr, source_aggregate_id, source_observation_id)
    return VwapStopResult(candidate, "vwap_stop", vwap, atr, source_aggregate_id, source_observation_id)


class VwapLocationProvider:
    """Calculate VWAP independently from completed structural aggregates."""

    def __init__(self, config: VwapCalculationConfig = VwapCalculationConfig()) -> None:
        self.config = config
        self._states: dict[str, _VwapState] = {}

    def publish(
        self,
        aggregates: tuple[MarketAggregate, ...],
        *,
        cutoff: datetime,
    ) -> tuple[VwapMeasurement, ...]:
        by_timeframe: dict[tuple[str, str], list[MarketAggregate]] = {}
        for aggregate in aggregates:
            if (
                aggregate.available_at <= cutoff
                and aggregate.available_at >= _interval_end(aggregate)
                and aggregate.context is not None
            ):
                key = (aggregate.timeframe, aggregate.context.session_id)
                by_timeframe.setdefault(key, []).append(aggregate)

        measurements: list[VwapMeasurement] = []
        for (timeframe, session_id), values in by_timeframe.items():
            ordered = sorted(values, key=lambda item: (item.observed_at, item.observation_id))
            aggregate_ids = tuple(item.aggregate_id for item in ordered)
            state_key = f"{timeframe}:{session_id}"
            state = self._states.setdefault(state_key, _VwapState())
            cached_ids = state.aggregate_ids
            append_only = (
                len(cached_ids) <= len(aggregate_ids)
                and aggregate_ids[:len(cached_ids)] == cached_ids
            )
            if not append_only:
                state = _VwapState()
                self._states[state_key] = state
            start_index = len(state.aggregate_ids) if append_only else 0
            state.aggregate_ids = aggregate_ids
            for aggregate in ordered[start_index:]:
                high = _number(aggregate, "high")
                low = _number(aggregate, "low")
                close = _number(aggregate, "close")
                volume = _number(aggregate, "volume")
                if None in (high, low, close, volume):
                    continue
                assert high is not None and low is not None and close is not None and volume is not None
                volume = max(0.0, volume)
                state.cumulative_volume += volume
                state.cumulative_value += ((high + low + close) / 3.0) * volume
                value = (
                    state.cumulative_value / state.cumulative_volume
                    if state.cumulative_volume else (high + low + close) / 3.0
                )
                source_identity = (
                    f"{_source_fingerprint(aggregate)}:{session_id}:{timeframe}:"
                    f"{aggregate.observed_at.isoformat()}:{self.config.calculation_version}:"
                    f"{value:.12g}:{volume:.12g}"
                )
                state.measurements.append(VwapMeasurement(
                    timeframe, session_id,
                    aggregate.aggregate_id,
                    hashlib.sha256(source_identity.encode("utf-8")).hexdigest()[:24],
                    _interval_end(aggregate), aggregate.available_at, value,
                    self.config.calculation_version,
                ))
            measurements.extend(state.measurements)
        return tuple(sorted(measurements, key=lambda item: (item.available_at, item.timeframe, item.source_aggregate_id)))


def derive_vwap_band(value: float, atr: float) -> LocationBand:
    if atr <= 0:
        raise ValueError("VWAP location bands require a positive frozen ATR")
    width = float(atr) * BAND_ATR_FRACTION
    return LocationBand(value - width, value + width)


def publish_vwap_locations(
    registry: Any,
    measurements: Sequence[VwapMeasurement],
    *,
    atr: float | None = None,
) -> tuple[LocationRecord, ...]:
    """Convert immutable measurements into ordinary diagnostic records."""
    if atr is None or atr <= 0:
        return ()
    records: list[LocationRecord] = []
    generations: dict[str, int] = {}
    existing = tuple(getattr(registry, "records", {}).values())
    existing_keys = {
        (item.source_aggregate_id, item.timeframe)
        for item in existing
        if item.alias == "VWAP"
    }
    for item in existing:
        if item.alias == "VWAP":
            key = item.timeframe
            generations[key] = max(generations.get(key, 0), item.generation)
    for measurement in measurements:
        if (measurement.source_aggregate_id, measurement.timeframe) in existing_keys:
            continue
        key = measurement.timeframe
        generation = generations.get(key, 0) + 1
        generations[key] = generation
        record = LocationRecord(
            location_id=stable_orderflow_id("location", measurement.session_id, measurement.timeframe, "VWAP", generation),
            alias="VWAP", price=measurement.value,
            source_session=measurement.session_id,
            created_at=measurement.available_at,
            available_at=measurement.available_at,
            stream_id=getattr(registry, "stream_id", measurement.session_id),
            band=derive_vwap_band(measurement.value, atr),
            generation=generation, frozen_atr=float(atr),
            source_aggregate_id=measurement.source_aggregate_id,
            source_observation_id=measurement.source_observation_id,
            timeframe=measurement.timeframe,
            source_method=measurement.calculation_version,
        )
        registry.register(record)
        existing_keys.add((measurement.source_aggregate_id, measurement.timeframe))
        records.append(record)
    return tuple(records)


def evaluate_vwap_diagnostic_response(interaction: object, candles: Sequence[object]) -> dict[str, object]:
    """Return reporting evidence without producing a response or opportunity."""
    return {
        "event": "vwap_diagnostic_response",
        "location_alias": getattr(interaction, "location_alias", "VWAP"),
        "location_type": "diagnostic_reference",
        "interaction_id": getattr(interaction, "interaction_id", None),
        "evidence_ids": tuple(getattr(candle, "observation_id", "") for candle in candles),
    }


__all__ = [
    "MarketAggregate", "VwapCalculationConfig", "VwapMeasurement", "VwapStopResult", "VwapLocationProvider",
    "calculate_session_vwap", "calculate_vwap_stop",
    "derive_vwap_band", "publish_vwap_locations",
    "evaluate_vwap_diagnostic_response",
]
