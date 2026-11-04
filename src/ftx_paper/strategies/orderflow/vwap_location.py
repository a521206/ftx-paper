"""Causal, session-anchored VWAP measurements for registered locations."""

from __future__ import annotations
# pyright: reportArgumentType=false

from dataclasses import dataclass
import hashlib
from typing import Iterable
from datetime import datetime

from .orderflow_common import Aggregate
from .timestamps import coerce_timestamp, nifty_label, serialize_utc


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
    observed_at: datetime
    available_at: datetime
    value: float
    source_observation_id: str
    calculation_version: str

    def __post_init__(self) -> None:
        observed = coerce_timestamp(self.observed_at, field="VWAP observed_at")
        available = coerce_timestamp(self.available_at, field="VWAP available_at")
        if available < observed:
            raise ValueError("VWAP availability cannot precede observation")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "available_at", available)

    def to_dict(self) -> dict[str, object]:
        return {
            "timeframe": self.timeframe, "session_date": self.session_date,
            "source_aggregate_id": self.source_aggregate_id, "observed_at": serialize_utc(self.observed_at),
            "available_at": serialize_utc(self.available_at), "value": self.value,
            "source_observation_id": self.source_observation_id,
            "method": self.calculation_version,
        }


def causal_vwap_prefix(
    aggregates: Iterable[Aggregate], *, session_date: str,
    available_at: datetime | None = None,
    config: VwapCalculationConfig = VwapCalculationConfig(),
) -> tuple[VwapMeasurement, ...]:
    ordered = sorted(
        (item for item in aggregates if item.session_date == session_date
         and item.available_at >= item.end_time
         and (available_at is None or item.available_at <= coerce_timestamp(available_at))),
        key=lambda item: (item.time, item.available_at, item.timeframe),
    )
    timeframes = {item.timeframe for item in ordered}
    if len(timeframes) > 1:
        raise ValueError("VWAP prefix requires one source timeframe")
    accumulator = CausalVwapAccumulator(session_date=session_date, config=config)
    for aggregate in ordered:
        accumulator.append(aggregate)
    return tuple(accumulator.measurements)


class CausalVwapAccumulator:
    """Incremental equivalent of ``causal_vwap_prefix`` for one timeframe."""

    def __init__(self, *, session_date: str, config: VwapCalculationConfig = VwapCalculationConfig()) -> None:
        self.session_date = session_date
        self.config = config
        self.numerator = 0.0
        self.denominator = 0.0
        self.measurements: list[VwapMeasurement] = []

    def append(self, aggregate: Aggregate) -> VwapMeasurement:
        if aggregate.session_date != self.session_date:
            raise ValueError("VWAP accumulator received another session")
        if aggregate.available_at < aggregate.end_time:
            raise ValueError("VWAP aggregate is not causally available")
        if self.config.price_formula != "(high+low+close)/3":
            raise ValueError(f"unsupported VWAP price formula: {self.config.price_formula}")
        typical_price = (float(aggregate.high) + float(aggregate.low) + float(aggregate.close)) / 3.0
        volume = max(0.0, float(getattr(aggregate, self.config.volume_field)))
        self.numerator += typical_price * volume
        self.denominator += volume
        value = self.numerator / self.denominator if self.denominator else typical_price
        timestamp_identity = nifty_label(aggregate.time)
        source_identity = f"{aggregate.source_fingerprint}:{self.session_date}:{aggregate.timeframe}:{timestamp_identity}:{self.config.version}:{value:.12g}:{volume:.12g}"
        measurement = VwapMeasurement(
            aggregate.timeframe, self.session_date, timestamp_identity, aggregate.end_time,
            aggregate.available_at, value,
            hashlib.sha256(source_identity.encode("utf-8")).hexdigest()[:24], self.config.version,
        )
        self.measurements.append(measurement)
        return measurement
