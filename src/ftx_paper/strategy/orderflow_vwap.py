"""Optional causal VWAP provider for native order-flow locations and stops."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from .orderflow_contract import stable_orderflow_id
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
    calculation_version: str = "typical_price_volume_v1"
    band_atr_fraction: float = 0.05
    recovery_bars: int = 5

    def __post_init__(self) -> None:
        if not self.calculation_version.strip() or self.recovery_bars < 1:
            raise ValueError("invalid VWAP calculation configuration")
        if self.band_atr_fraction < 0:
            raise ValueError("VWAP band fraction cannot be negative")


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

    def publish(
        self,
        aggregates: tuple[MarketAggregate, ...],
        *,
        cutoff: datetime,
    ) -> tuple[VwapMeasurement, ...]:
        by_timeframe: dict[str, list[MarketAggregate]] = {}
        for aggregate in aggregates:
            if aggregate.available_at <= cutoff and aggregate.context is not None:
                by_timeframe.setdefault(aggregate.timeframe, []).append(aggregate)

        measurements: list[VwapMeasurement] = []
        for timeframe, values in by_timeframe.items():
            ordered = sorted(values, key=lambda item: (item.observed_at, item.observation_id))
            cumulative_volume = 0.0
            cumulative_value = 0.0
            unavailable = False
            last: datetime | None = None
            interval = timedelta(minutes=5 if timeframe == "5m" else 1)
            recovery: list[MarketAggregate] = []
            for aggregate in ordered:
                provenance_gap = aggregate.context is not None and aggregate.context.replay_provenance.get(
                    "gap_status", "none",
                ) != "none"
                gap = provenance_gap or (last is not None and aggregate.observed_at - last != interval)
                if gap:
                    unavailable = True
                    cumulative_volume = cumulative_value = 0.0
                    recovery.clear()
                recovery.append(aggregate)
                if unavailable:
                    if len(recovery) < self.config.recovery_bars:
                        last = aggregate.observed_at
                        continue
                    if any(
                        right.observed_at - left.observed_at != interval
                        for left, right in zip(
                            recovery[-self.config.recovery_bars:-1],
                            recovery[-self.config.recovery_bars + 1:],
                        )
                    ):
                        last = aggregate.observed_at
                        continue
                    unavailable = False
                    cumulative_volume = cumulative_value = 0.0
                high = _number(aggregate, "high")
                low = _number(aggregate, "low")
                close = _number(aggregate, "close")
                volume = _number(aggregate, "volume")
                if None in (high, low, close, volume) or volume is None or volume <= 0:
                    unavailable = True
                    cumulative_volume = cumulative_value = 0.0
                    last = aggregate.observed_at
                    continue
                assert high is not None and low is not None and close is not None
                cumulative_volume += volume
                cumulative_value += ((high + low + close) / 3.0) * volume
                measurements.append(VwapMeasurement(
                    timeframe, aggregate.context.session_id if aggregate.context is not None else "",
                    aggregate.aggregate_id,
                    stable_orderflow_id(
                        "vwap_measurement", timeframe,
                        aggregate.context.session_id if aggregate.context is not None else "",
                        aggregate.aggregate_id, self.config.calculation_version,
                    ),
                    aggregate.observed_at, aggregate.available_at,
                    cumulative_value / cumulative_volume, self.config.calculation_version,
                ))
                last = aggregate.observed_at
        return tuple(sorted(measurements, key=lambda item: (item.available_at, item.timeframe, item.source_aggregate_id)))


def derive_vwap_band(value: float, atr: float, *, fraction: float = 0.05) -> LocationBand:
    if atr <= 0:
        raise ValueError("VWAP location bands require a positive frozen ATR")
    width = float(atr) * fraction
    return LocationBand(value - width, value + width)


def publish_vwap_locations(
    registry: Any,
    measurements: Sequence[VwapMeasurement],
    *,
    atr: float | None = None,
    band_fraction: float = 0.05,
) -> tuple[LocationRecord, ...]:
    """Convert immutable measurements into ordinary diagnostic records."""
    if atr is None or atr <= 0:
        return ()
    records: list[LocationRecord] = []
    generations: dict[str, int] = {}
    existing = getattr(registry, "records", ())
    for item in existing:
        if item.alias == "VWAP":
            key = item.timeframe
            generations[key] = max(generations.get(key, 0), item.generation)
    for measurement in measurements:
        if any(
            item.alias == "VWAP"
            and item.source_aggregate_id == measurement.source_aggregate_id
            and item.timeframe == measurement.timeframe
            for item in existing
        ):
            continue
        key = measurement.timeframe
        generation = generations.get(key, 0) + 1
        generations[key] = generation
        record = LocationRecord(
            stable_orderflow_id("location", measurement.session_id, measurement.timeframe, "VWAP", generation),
            "VWAP", measurement.value,
            derive_vwap_band(measurement.value, atr, fraction=band_fraction), generation,
            measurement.available_at, measurement.source_aggregate_id,
            timeframe=measurement.timeframe,
            location_type="diagnostic_reference",
            source_observation_id=measurement.source_observation_id,
            frozen_atr=float(atr),
            session_id=measurement.session_id,
            calculation_version=measurement.calculation_version,
        )
        registry.publish(record)
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
