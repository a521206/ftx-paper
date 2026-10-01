"""Independent causal setup scoring for the paper runtime.

The implementation mirrors the canonical FTX score contract locally. It is
deliberately duplicated here so ``ftx-paper`` remains independently runnable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from zoneinfo import ZoneInfo

from ftx_paper.contracts import MarketBar

CONSEC_DOWN_THRESH = 3
DESCENT_SPEED_THRESH = 1.5
CLIMAX_VOL_THRESH = 2.0
WICK_RATIO_THRESH = 0.70
DELTA_DIVERGENCE_THRESH = -0.3
PCR_EXTREME = 1.2
VIX_INTRADAY_SPIKE = 5.0
SCORE_A_PLUS = 8
SCORE_STANDARD = 5
SCORE_WEAK = 2


@dataclass(frozen=True, slots=True)
class SellingStructure:
    """Causal selling diagnostics used by the score factors."""

    consec_down: int
    descent_speed_bp: float
    vol_climax: float
    vol_drying: bool
    wick_rejection: float
    delta_divergence: float
    vix_trend_pct: float
    climax_score: int
    selling_type: str


IST = ZoneInfo("Asia/Kolkata")


def _synthetic_delta_divergence(
    bars: Sequence[MarketBar],
    *,
    event_time: str | None = None,
) -> float:
    """Correlate closes with cumulative signed close-location volume.

    This is a bar-based OHLCV order-flow proxy, not exchange-reported signed
    trade flow. Only completed bars before ``event_time`` contribute.
    """
    if len(bars) < 2:
        return 0.0
    cumulative: list[float] = []
    closes: list[float] = []
    running = 0.0
    for bar in bars:
        if event_time is not None:
            minute = bar.timestamp.astimezone(IST).strftime("%H:%M")
            if not ("09:15" <= minute < event_time):
                continue
        high = float(bar.high)
        low = float(bar.low)
        close = float(bar.close)
        volume = float(bar.volume or 0.0)
        if min(high, low, close, volume) < 0 or not all(
            isfinite(value) for value in (high, low, close, volume)
        ):
            continue
        price_range = high - low
        if price_range < 0 or close < low or close > high:
            continue
        signed_close_location = (
            ((close - low) - (high - close)) / price_range
            if price_range > 0 else 0.0
        )
        running += signed_close_location * volume
        cumulative.append(running)
        closes.append(close)
    if len(cumulative) < 2:
        return 0.0
    price_mean = sum(closes) / len(closes)
    delta_mean = sum(cumulative) / len(cumulative)
    price_var = sum((price - price_mean) ** 2 for price in closes)
    delta_var = sum((delta - delta_mean) ** 2 for delta in cumulative)
    if price_var <= 0 or delta_var <= 0:
        return 0.0
    covariance = sum(
        (price - price_mean) * (delta - delta_mean)
        for price, delta in zip(closes, cumulative)
    )
    return covariance / (price_var * delta_var) ** 0.5


def compute_selling_structure(
    prior_bars: Sequence[MarketBar],
    current: MarketBar,
    *,
    vix_open: float,
    vix_at_event: float,
) -> SellingStructure:
    """Build the same nine-factor selling inputs used by canonical FTX."""
    lookback = 10
    all_prior = list(prior_bars)
    bars = all_prior[-lookback:]
    if len(bars) < 3:
        return SellingStructure(0, 0.0, 0.0, False, 0.0, 0.0, 0.0, 0, "grinding")

    consec_down = 0
    previous_close = current.close
    for bar in reversed(bars):
        if previous_close < bar.close:
            consec_down += 1
            previous_close = bar.close
        else:
            break
    recent_high = max(bar.close for bar in bars)
    total_drop = recent_high - current.close
    descent_speed_bp = (
        total_drop / current.close * 10000 / len(bars)
        if current.close > 0
        else 0.0
    )

    vol_climax = 0.0
    first_index = len(all_prior) - len(bars)
    for index, bar in enumerate(bars, start=first_index):
        reference = all_prior[max(0, index - 5):index]
        average = (
            sum(float(item.volume or 0.0) for item in reference) / len(reference)
            if reference
            else float(bar.volume or 0.0)
        )
        if average > 0:
            vol_climax = max(vol_climax, float(bar.volume or 0.0) / average)

    vol_drying = (
        len(bars) >= 6
        and sum(float(bar.volume or 0.0) for bar in bars[-3:])
        < sum(float(bar.volume or 0.0) for bar in bars[-6:-3])
    )
    current_range = current.high - current.low
    wick_rejection = (
        (current.close - current.low) / current_range
        if current_range > 0
        else 0.5
    )
    event_time = current.timestamp.astimezone(IST).strftime("%H:%M")
    delta_divergence = _synthetic_delta_divergence(bars, event_time=event_time)
    vix_trend_pct = (
        (vix_at_event - vix_open) / vix_open * 100
        if vix_open > 0
        else 0.0
    )
    climax_score = sum((vol_climax >= CLIMAX_VOL_THRESH, vol_drying, delta_divergence < DELTA_DIVERGENCE_THRESH))
    selling_type = "climactic" if climax_score >= 2 else "grinding"
    return SellingStructure(
        consec_down=consec_down,
        descent_speed_bp=round(descent_speed_bp, 4),
        vol_climax=round(vol_climax, 4),
        vol_drying=vol_drying,
        wick_rejection=round(wick_rejection, 4),
        delta_divergence=round(delta_divergence, 4),
        vix_trend_pct=round(vix_trend_pct, 4),
        climax_score=climax_score,
        selling_type=selling_type,
    )


def calculate_setup_score(
    selling: SellingStructure,
    event: Mapping[str, object] | None,
    vix_at_event: float,
    vix_open: float,
    pcr: float | None,
    structural_proximity: bool,
    score_time_window: tuple[int, int] | None = None,
) -> tuple[int, dict[str, bool | None]]:
    """Calculate the canonical 0-9 setup score from local paper inputs."""
    factors = {
        "climactic_selling": selling.consec_down >= CONSEC_DOWN_THRESH,
        "panic_descent": selling.descent_speed_bp > DESCENT_SPEED_THRESH,
        "volume_climax": selling.vol_climax >= CLIMAX_VOL_THRESH,
        "volume_drying": selling.vol_drying,
        "rejection_wick": selling.wick_rejection >= WICK_RATIO_THRESH,
        "delta_divergence": selling.delta_divergence < DELTA_DIVERGENCE_THRESH,
        "structural_level": structural_proximity,
        "pcr_extreme": None if pcr is None else pcr > PCR_EXTREME,
    }
    vix_intraday_change = (vix_at_event - vix_open) / vix_open * 100 if vix_open > 0 else 0.0
    factors["vix_spike"] = vix_intraday_change > VIX_INTRADAY_SPIKE
    if score_time_window is not None:
        lo, hi = score_time_window
        raw_minute = (event or {}).get("minutes_from_open", -1)
        if isinstance(raw_minute, (int, float, str)) and not isinstance(raw_minute, bool):
            try:
                minute = float(raw_minute)
            except ValueError:
                minute = -1.0
        else:
            minute = -1.0
        factors["time_window"] = lo <= minute <= hi
    if selling.selling_type == "grinding":
        for key in ("climactic_selling", "panic_descent", "volume_climax", "volume_drying"):
            factors[key] = False
    return sum(value is True for value in factors.values()), factors


def score_to_setup_type(score: int) -> tuple[str, float]:
    if score >= SCORE_A_PLUS:
        return "A+", 1.5
    if score >= SCORE_STANDARD:
        return "Standard", 1.0
    if score >= SCORE_WEAK:
        return "Weak", 0.5
    return "Skip", 0.0


__all__ = [
    "SellingStructure", "calculate_setup_score", "compute_selling_structure",
    "score_to_setup_type",
]
