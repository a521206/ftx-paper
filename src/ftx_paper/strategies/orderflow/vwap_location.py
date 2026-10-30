"""Causal, session-anchored VWAP measurements for registered locations."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Iterable

from .orderflow_common import Aggregate


@dataclass(frozen=True, slots=True)
class VwapCalculationConfig:
    version: str = "vwap_typical_price_volume_v1"
    volume_field: str = "volume"
    price_formula: str = "(high+low+close)/3"


@dataclass(frozen=True, slots=True)
class VwapMeasurement:
    timeframe: str
    session_date: str
    source_aggregate_id: str
    observed_at: str
    available_at: str
    value: float
    source_observation_id: str
    calculation_version: str

    def to_dict(self) -> dict[str, object]:
        return {
            "timeframe": self.timeframe, "session_date": self.session_date,
            "source_aggregate_id": self.source_aggregate_id, "observed_at": self.observed_at,
            "available_at": self.available_at, "value": self.value,
            "source_observation_id": self.source_observation_id,
            "method": self.calculation_version,
        }


def causal_vwap_prefix(
    aggregates: Iterable[Aggregate], *, session_date: str,
    available_at: str | None = None,
    config: VwapCalculationConfig = VwapCalculationConfig(),
) -> tuple[VwapMeasurement, ...]:
    ordered = sorted(
        (item for item in aggregates if item.session_date == session_date
         and item.available_at >= item.end_time
         and (available_at is None or item.available_at <= available_at)),
        key=lambda item: (item.time, item.available_at, item.timeframe),
    )
    timeframes = {item.timeframe for item in ordered}
    if len(timeframes) > 1:
        raise ValueError("VWAP prefix requires one source timeframe")
    numerator = 0.0
    denominator = 0.0
    measurements: list[VwapMeasurement] = []
    for aggregate in ordered:
        if config.price_formula != "(high+low+close)/3":
            raise ValueError(f"unsupported VWAP price formula: {config.price_formula}")
        typical_price = (float(aggregate.high) + float(aggregate.low) + float(aggregate.close)) / 3.0
        volume = max(0.0, float(getattr(aggregate, config.volume_field)))
        numerator += typical_price * volume
        denominator += volume
        value = numerator / denominator if denominator else typical_price
        source_identity = f"{aggregate.source_fingerprint}:{session_date}:{aggregate.timeframe}:{aggregate.time}:{config.version}:{value:.12g}:{volume:.12g}"
        source_observation_id = hashlib.sha256(source_identity.encode("utf-8")).hexdigest()[:24]
        measurements.append(VwapMeasurement(
            aggregate.timeframe, session_date, aggregate.time, aggregate.end_time,
            aggregate.available_at, value, source_observation_id, config.version,
        ))
    return tuple(measurements)
