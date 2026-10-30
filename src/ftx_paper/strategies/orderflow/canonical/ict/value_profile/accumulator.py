"""Single-pass volume-at-price accumulation (VP2+)."""

from math import isfinite

from ftx_paper.strategies.orderflow.canonical.ict.value_profile.bins import (
    HistogramLevels,
    bin_price,
    finalize_histogram,
    typical_price,
)


class ProfileAccumulator:
    """Mutable histogram for one profile scope."""

    def __init__(self, *, value_area_fraction: float, tick_size: float = 0.0) -> None:
        self._value_area_fraction = value_area_fraction
        self._tick_size = tick_size
        self._price_volume: dict[float, float] = {}
        self._vwap_numerator = 0.0
        self._vwap_denominator = 0.0

    def add_bar(self, *, high: float, low: float, close: float, volume: float) -> None:
        try:
            finite = all(isfinite(value) for value in (high, low, close, volume))
        except (TypeError, ValueError) as error:
            raise ValueError("profile bars require finite OHLCV values") from error
        if not finite:
            raise ValueError("profile bars require finite OHLCV values")
        if volume < 0:
            raise ValueError("profile volume cannot be negative")
        if volume == 0:
            return
        raw_price = typical_price(high, low, close)
        price = bin_price(raw_price, self._tick_size)
        self._price_volume[price] = self._price_volume.get(price, 0.0) + volume
        self._vwap_numerator += raw_price * volume
        self._vwap_denominator += volume

    def levels(self) -> HistogramLevels | None:
        return finalize_histogram(
            self._price_volume,
            value_area_fraction=self._value_area_fraction,
            vwap_numerator=self._vwap_numerator,
            vwap_denominator=self._vwap_denominator,
        )
