"""Volume-at-price level extraction."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HistogramLevels:
    """POC and value-area bounds for one histogram."""

    poc: float
    vah: float
    val: float
    total_volume: float
    vwap: float | None = None


def typical_price(high: float, low: float, close: float) -> float:
    return (high + low + close) / 3.0


def bin_price(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    steps = round(price / tick_size)
    return steps * tick_size


def finalize_histogram(
    price_volume: dict[float, float],
    *,
    value_area_fraction: float = 0.70,
    vwap_numerator: float | None = None,
    vwap_denominator: float | None = None,
) -> HistogramLevels | None:
    """Expand value area from POC by adding adjacent price rows until the volume target is met."""
    if not price_volume:
        return None

    poc = max(price_volume, key=lambda price: price_volume[price])
    sorted_prices = sorted(price_volume.keys())
    total = sum(price_volume.values())
    if total <= 0:
        return None

    target = total * value_area_fraction
    poc_index = sorted_prices.index(poc)
    low_index = high_index = poc_index
    cumulative = price_volume[poc]

    while cumulative < target and (low_index > 0 or high_index < len(sorted_prices) - 1):
        below_volume = price_volume[sorted_prices[low_index - 1]] if low_index > 0 else -1.0
        above_volume = (
            price_volume[sorted_prices[high_index + 1]]
            if high_index < len(sorted_prices) - 1
            else -1.0
        )
        if below_volume >= above_volume and low_index > 0:
            low_index -= 1
            cumulative += price_volume[sorted_prices[low_index]]
        elif high_index < len(sorted_prices) - 1:
            high_index += 1
            cumulative += price_volume[sorted_prices[high_index]]
        elif low_index > 0:
            low_index -= 1
            cumulative += price_volume[sorted_prices[low_index]]
        else:
            break

    vwap: float | None = None
    if vwap_numerator is not None and vwap_denominator is not None and vwap_denominator > 0:
        vwap = vwap_numerator / vwap_denominator

    return HistogramLevels(
        poc=poc,
        vah=sorted_prices[high_index],
        val=sorted_prices[low_index],
        total_volume=total,
        vwap=vwap,
    )
