"""Canonical MR/TR admission predicates for the independent paper runtime."""

from __future__ import annotations

from collections.abc import Sequence

from ftx_paper.contracts import MarketBar


ADMISSION_FEATURE_NAMES = (
    "climactic_selling", "panic_descent", "volume_drying", "pcr_extreme",
    "vix_spike", "volume_climax_prior", "prior_midpoint_reclaim",
)


def build_admission_features(
    prior_bars: Sequence[MarketBar], current: MarketBar, *, pcr: float | None,
    vix_open: float, vix_at_event: float,
) -> dict[str, bool | None]:
    """Build the seven causal admission predicates from completed bars."""
    bars = list(prior_bars)
    closes = [float(bar.close) for bar in bars]
    volumes = [float(bar.volume or 0.0) for bar in bars]
    recent = bars[-10:]
    consecutive_down = 0
    for index in range(len(closes) - 1, 0, -1):
        if closes[index] < closes[index - 1]:
            consecutive_down += 1
        else:
            break
    descent_speed = None
    if len(recent) >= 2 and current.close > 0:
        recent_high = max(float(bar.close) for bar in recent)
        descent_speed = (recent_high - float(current.close)) / float(current.close) * 10000 / len(recent)
    volume_drying = None
    if len(volumes) >= 6:
        volume_drying = sum(volumes[-3:]) < sum(volumes[-6:-3])
    ratios: list[float] = []
    for index in range(max(1, len(volumes) - 10), len(volumes)):
        baseline_values = volumes[max(0, index - 5):index]
        baseline = sum(baseline_values) / len(baseline_values) if baseline_values else 0.0
        if baseline > 0:
            ratios.append(volumes[index] / baseline)
    prior_midpoint_reclaim = None
    if bars:
        prior = bars[-1]
        prior_midpoint_reclaim = float(current.close) > (float(prior.high) + float(prior.low)) / 2
    vix_spike = (vix_at_event - vix_open) / vix_open * 100 > 5.0 if vix_open > 0 else None
    return {
        "climactic_selling": None if len(closes) < 2 else consecutive_down >= 3,
        "panic_descent": None if descent_speed is None else descent_speed > 8.0,
        "volume_drying": volume_drying,
        "pcr_extreme": None if pcr is None else pcr > 1.2,
        "vix_spike": vix_spike,
        "volume_climax_prior": max(ratios, default=0.0) >= 2.0 if ratios else None,
        "prior_midpoint_reclaim": prior_midpoint_reclaim,
    }


def admission_result(features: dict[str, bool | None], hypothesis: str) -> tuple[bool, str | None]:
    """Apply the canonical mean-reversion or trend-continuation contract."""
    if hypothesis == "mean_reversion":
        requirements = {"panic_descent": False, "pcr_extreme": False, "climactic_selling": False}
    elif hypothesis == "trend_continuation":
        requirements = {"panic_descent": False, "pcr_extreme": False, "climactic_selling": False, "vix_spike": False}
        confirmations = ("volume_climax_prior", "prior_midpoint_reclaim", "volume_drying")
        if sum(features.get(name) is True for name in confirmations) < 2:
            return False, "tr_confirmation_count"
    else:
        return True, None
    for name, expected in requirements.items():
        if features.get(name) is not expected:
            return False, f"{hypothesis}_{name}_required_{expected}"
    return True, None


__all__ = ["ADMISSION_FEATURE_NAMES", "admission_result", "build_admission_features"]
