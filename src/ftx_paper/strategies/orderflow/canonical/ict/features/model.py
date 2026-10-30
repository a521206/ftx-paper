"""Causal price measurements over completed ICT bars.

Measurements are prefix-safe: the value at a bar uses that bar and only bars
before it.  A missing value is explicit unavailability, never evidence of
balance or compression.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Callable, Literal, Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.scale.model import AtrReference


@dataclass(frozen=True, slots=True)
class MeasurementConfig:
    """Lookbacks measured in completed bars.

    Directional efficiency needs at least two bars.  Overlap needs one pair
    per configured lookback.  Volatility contraction needs the recent window
    followed by a distinct, non-overlapping baseline window.
    """

    directional_efficiency_lookback: int = 5
    overlap_lookback: int = 1
    rolling_range_lookback: int = 5
    volatility_recent_lookback: int = 3
    volatility_baseline_lookback: int = 3
    atr_lookback: int = 14
    atr_smoothing: Literal["wilder", "sma"] = "wilder"
    atr_min_value: float = 1e-6

    def __post_init__(self) -> None:
        for name, value in (
            ("directional_efficiency_lookback", self.directional_efficiency_lookback),
            ("overlap_lookback", self.overlap_lookback),
            ("rolling_range_lookback", self.rolling_range_lookback),
            ("volatility_recent_lookback", self.volatility_recent_lookback),
            ("volatility_baseline_lookback", self.volatility_baseline_lookback),
            ("atr_lookback", self.atr_lookback),
        ):
            if value < 1:
                raise ValueError(f"{name} must be positive")
        if self.directional_efficiency_lookback < 2:
            raise ValueError("directional_efficiency_lookback must be at least two")
        if self.atr_min_value <= 0 or not math.isfinite(self.atr_min_value):
            raise ValueError("atr_min_value must be a positive finite number")
        if self.atr_smoothing not in ("wilder", "sma"):
            raise ValueError("atr_smoothing must be wilder or sma")


@dataclass(frozen=True, slots=True)
class PriceMeasurement:
    """All Phase 3 measurements for one completed bar."""

    directional_efficiency: float | None
    consecutive_bar_overlap: float | None
    rolling_range_width: float | None
    volatility_contraction: float | None
    true_range: float | None = None
    atr: float | None = None
    unavailable_reasons: tuple[str, ...] = ()


def intraday_atr_reference(
    bar: CompletedBar,
    measurement: PriceMeasurement,
    *,
    config: MeasurementConfig = MeasurementConfig(),
    frozen_at: datetime,
    freeze_basis: str,
    source_available_at: datetime | None = None,
) -> AtrReference:
    """Create an immutable same-timeframe ATR reference for a decision."""
    source_at = source_available_at or bar.available_at
    if frozen_at < source_at:
        return AtrReference(
            value=None,
            status="unavailable",
            reason="atr: freeze time precedes source availability",
            source="intraday",
            lookback=config.atr_lookback,
            smoothing=config.atr_smoothing,
            instrument_or_series=bar.instrument,
            timeframe=bar.timeframe,
            source_session=bar.session_date,
            source_available_at=source_at,
            frozen_at=frozen_at,
            freeze_basis=freeze_basis,
        )
    if measurement.atr is None or measurement.atr <= 0:
        reason = next(
            (item for item in measurement.unavailable_reasons if item.startswith("atr:")),
            "atr: unavailable or non-positive",
        )
        return AtrReference(
            value=None,
            status="unavailable",
            reason=reason,
            source="intraday",
            lookback=config.atr_lookback,
            smoothing=config.atr_smoothing,
            instrument_or_series=bar.instrument,
            timeframe=bar.timeframe,
            source_session=bar.session_date,
            source_available_at=source_at,
            frozen_at=frozen_at,
            freeze_basis=freeze_basis,
        )
    return AtrReference(
        value=measurement.atr,
        status="available",
        reason="completed intraday ATR available at freeze",
        source="intraday",
        lookback=config.atr_lookback,
        smoothing=config.atr_smoothing,
        instrument_or_series=bar.instrument,
        timeframe=bar.timeframe,
        source_session=bar.session_date,
        source_available_at=source_available_at or bar.available_at,
        frozen_at=frozen_at,
        freeze_basis=freeze_basis,
    )


def _valid_bar(bar: CompletedBar) -> None:
    values = (bar.high, bar.low, bar.close)
    if any(not math.isfinite(value) for value in values):
        raise ValueError("measurement bars require finite prices")
    if bar.high < bar.low:
        raise ValueError("measurement bars require high >= low")


def _directional_efficiency(bars: Sequence[CompletedBar], lookback: int) -> float | None:
    if len(bars) < lookback:
        return None
    window = bars[-lookback:]
    movement = sum(abs(current.close - prior.close) for prior, current in zip(window, window[1:]))
    if movement == 0:
        return None
    return abs(window[-1].close - window[0].close) / movement


def _consecutive_overlap(bars: Sequence[CompletedBar], lookback: int) -> float | None:
    if len(bars) < lookback + 1:
        return None
    pairs = zip(bars[-lookback - 1 : -1], bars[-lookback:])
    ratios: list[float] = []
    for prior, current in pairs:
        smaller_range = min(prior.high - prior.low, current.high - current.low)
        if smaller_range == 0:
            return None
        ratios.append(max(0.0, min(prior.high, current.high) - max(prior.low, current.low)) / smaller_range)
    return sum(ratios) / len(ratios)


def _rolling_range_width(bars: Sequence[CompletedBar], lookback: int) -> float | None:
    if len(bars) < lookback:
        return None
    window = bars[-lookback:]
    return max(bar.high for bar in window) - min(bar.low for bar in window)


def _volatility_contraction(
    bars: Sequence[CompletedBar], recent_lookback: int, baseline_lookback: int
) -> float | None:
    required = recent_lookback + baseline_lookback
    if len(bars) < required:
        return None
    baseline = bars[-required : -recent_lookback]
    recent = bars[-recent_lookback:]
    baseline_mean = sum(bar.high - bar.low for bar in baseline) / baseline_lookback
    if baseline_mean == 0:
        return None
    recent_mean = sum(bar.high - bar.low for bar in recent) / recent_lookback
    return recent_mean / baseline_mean


def _true_range(bar: CompletedBar, prev_close: float | None) -> float:
    if prev_close is None:
        return bar.high - bar.low
    return max(bar.high - bar.low, abs(bar.high - prev_close), abs(bar.low - prev_close))


def _atr_from_session_trs(trs: Sequence[float], config: MeasurementConfig) -> float | None:
    lookback = config.atr_lookback
    if len(trs) < lookback:
        return None
    if config.atr_smoothing == "sma":
        value = sum(trs[-lookback:]) / lookback
    else:
        value = sum(trs[:lookback]) / lookback
        for tr in trs[lookback:]:
            value = (value * (lookback - 1) + tr) / lookback
    return max(value, config.atr_min_value)


def _session_true_range_atr(
    bars: Sequence[CompletedBar], index: int, config: MeasurementConfig
) -> tuple[float | None, float | None]:
    session_start = index
    while session_start > 0 and bars[session_start - 1].session_date == bars[index].session_date:
        session_start -= 1
    trs: list[float] = []
    for offset in range(session_start, index + 1):
        bar = bars[offset]
        prior_close = bars[offset - 1].close if offset > session_start else None
        trs.append(_true_range(bar, prior_close))
    return trs[-1], _atr_from_session_trs(trs, config)


def measure_prefix(
    bars: Sequence[CompletedBar], config: MeasurementConfig = MeasurementConfig()
) -> PriceMeasurement:
    """Measure the supplied completed prefix, returning explicit unavailable reasons."""
    for bar in bars:
        _valid_bar(bar)
    return measure_latest(bars, config)


def measure_latest(
    bars: Sequence[CompletedBar], config: MeasurementConfig = MeasurementConfig()
) -> PriceMeasurement:
    """Measure only the latest bar in a completed prefix.

    Callers that append bars incrementally can use this without revalidating or
    rebuilding measurements for earlier prefixes.
    """
    if bars:
        _valid_bar(bars[-1])
    reasons: list[str] = []
    values: list[float | None] = [
        _directional_efficiency(bars, config.directional_efficiency_lookback),
        _consecutive_overlap(bars, config.overlap_lookback),
        _rolling_range_width(bars, config.rolling_range_lookback),
        _volatility_contraction(
            bars, config.volatility_recent_lookback, config.volatility_baseline_lookback
        ),
    ]
    minimums = (
        config.directional_efficiency_lookback,
        config.overlap_lookback + 1,
        config.rolling_range_lookback,
        config.volatility_recent_lookback + config.volatility_baseline_lookback,
    )
    names = ("directional_efficiency", "consecutive_bar_overlap", "rolling_range_width", "volatility_contraction")
    for name, value, minimum in zip(names, values, minimums):
        if value is None:
            reasons.append(f"{name}: insufficient history or zero denominator (minimum {minimum} bars)")
    true_range: float | None = None
    atr: float | None = None
    if bars:
        true_range, atr = _session_true_range_atr(bars, len(bars) - 1, config)
        if atr is None:
            reasons.append(
                f"atr: insufficient history (minimum {config.atr_lookback} completed true ranges in session)"
            )
    return PriceMeasurement(
        directional_efficiency=values[0],
        consecutive_bar_overlap=values[1],
        rolling_range_width=values[2],
        volatility_contraction=values[3],
        true_range=true_range,
        atr=atr,
        unavailable_reasons=tuple(reasons),
    )


def measure_bars(
    bars: Sequence[CompletedBar], config: MeasurementConfig = MeasurementConfig()
) -> tuple[PriceMeasurement, ...]:
    """Return causal measurements aligned one-for-one with completed bars."""
    for bar in bars:
        _valid_bar(bar)
    return tuple(measure_latest(bars[: index + 1], config) for index in range(len(bars)))


MEASUREMENT_BUILDERS: dict[str, Callable[[Sequence[CompletedBar], MeasurementConfig], tuple[float | None, ...]]] = {
    "ict.directional_efficiency": lambda bars, config: tuple(item.directional_efficiency for item in measure_bars(bars, config)),
    "ict.consecutive_bar_overlap": lambda bars, config: tuple(item.consecutive_bar_overlap for item in measure_bars(bars, config)),
    "ict.rolling_range_width": lambda bars, config: tuple(item.rolling_range_width for item in measure_bars(bars, config)),
    "ict.volatility_contraction": lambda bars, config: tuple(item.volatility_contraction for item in measure_bars(bars, config)),
    "ict.true_range": lambda bars, config: tuple(item.true_range for item in measure_bars(bars, config)),
    "ict.atr": lambda bars, config: tuple(item.atr for item in measure_bars(bars, config)),
}
