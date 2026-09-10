from __future__ import annotations

from dataclasses import dataclass
from ftx_paper.contracts import MarketBar


@dataclass(frozen=True, slots=True)
class LiveFeatures:
    """Causal features computed from completed bars only."""

    vwap: float
    atr: float | None
    session_high: float
    session_low: float
    opening_range_high: float | None
    opening_range_low: float | None
    return_1: float | None


class LiveFeatureCalculator:
    """Deterministic, dependency-free feature calculator for the core engine."""

    def __init__(self, opening_range_bars: int = 15, atr_window: int = 20) -> None:
        if opening_range_bars < 1 or atr_window < 1:
            raise ValueError("feature windows must be positive")
        self.opening_range_bars = opening_range_bars
        self.atr_window = atr_window

    def calculate(self, bars: tuple[MarketBar, ...]) -> LiveFeatures:
        if not bars:
            raise ValueError("at least one completed bar is required")
        typical_volume = [(bar.high + bar.low + bar.close) / 3 * (bar.volume or 0.0) for bar in bars]
        volume = sum(bar.volume or 0.0 for bar in bars)
        vwap = sum(typical_volume) / volume if volume else bars[-1].close
        ranges = [bar.high - bar.low for bar in bars]
        window = ranges[-self.atr_window:]
        opening = bars[:self.opening_range_bars]
        return LiveFeatures(
            vwap=vwap,
            atr=sum(window) / len(window) if len(window) > 1 else None,
            session_high=max(bar.high for bar in bars),
            session_low=min(bar.low for bar in bars),
            opening_range_high=max(bar.high for bar in opening) if len(bars) >= self.opening_range_bars else None,
            opening_range_low=min(bar.low for bar in opening) if len(bars) >= self.opening_range_bars else None,
            return_1=(bars[-1].close / bars[-2].close - 1) if len(bars) > 1 else None,
        )
