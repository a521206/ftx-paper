from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable, Mapping
from zoneinfo import ZoneInfo

from ftx_paper.contracts import MarketBar, Role


IST = ZoneInfo("Asia/Kolkata")


def _ist_minute(bar: MarketBar) -> tuple[str, int]:
    timestamp = bar.timestamp
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=IST)
    timestamp = timestamp.astimezone(IST)
    return timestamp.date().isoformat(), timestamp.hour * 60 + timestamp.minute


def _session_bars(bars: tuple[MarketBar, ...]) -> list[MarketBar]:
    """Return bars in the canonical 09:15-onwards session, in input order."""
    return [bar for bar in bars if _ist_minute(bar)[1] >= 9 * 60 + 15]


@dataclass(frozen=True, slots=True)
class LiveFeatures:
    """Causal features computed from completed bars only."""

    vwap: float | None
    atr: float | None
    session_high: float
    session_low: float
    opening_range_high: float | None
    opening_range_low: float | None
    return_1: float | None
    prior_day_high: float | None = None
    prior_day_low: float | None = None


class LiveFeatureCalculator:
    """Deterministic, dependency-free feature calculator for the core engine."""

    def __init__(self, opening_range_bars: int = 15, atr_window: int = 20) -> None:
        if opening_range_bars < 1 or atr_window < 1:
            raise ValueError("feature windows must be positive")
        self.opening_range_bars = opening_range_bars
        self.atr_window = atr_window

    def calculate(self, bars: tuple[MarketBar, ...]) -> LiveFeatures:
        """Calculate features for the supplied completed-bar prefix.

        This remains a small, standalone calculator for callers that already
        pass the causal prefix.  ``calculate_event`` is the explicit helper
        for a current entry bar and never includes that bar in the levels.
        """
        if not bars:
            raise ValueError("at least one completed bar is required")
        session = _session_bars(bars)
        if not session:
            raise ValueError("at least one session bar is required")
        typical_volume = [(bar.high + bar.low + bar.close) / 3 * (bar.volume or 0.0) for bar in session]
        volume = sum(bar.volume or 0.0 for bar in session)
        vwap = sum(typical_volume) / volume if volume else None
        ranges = [bar.high - bar.low for bar in session]
        window = ranges[-self.atr_window:]
        opening = session[:self.opening_range_bars]
        return LiveFeatures(
            vwap=vwap,
            atr=sum(window) / len(window) if len(window) > 1 else None,
            session_high=max(bar.high for bar in session),
            session_low=min(bar.low for bar in session),
            opening_range_high=max(bar.high for bar in opening) if len(session) >= self.opening_range_bars else None,
            opening_range_low=min(bar.low for bar in opening) if len(session) >= self.opening_range_bars else None,
            return_1=(session[-1].close / session[-2].close - 1) if len(session) > 1 else None,
        )

    def calculate_event(
        self,
        prefix: tuple[MarketBar, ...],
        current: MarketBar,
        *,
        prior_day_high: float | None = None,
        prior_day_low: float | None = None,
    ) -> LiveFeatures:
        """Calculate canonical event-time market fields from a causal prefix."""
        if len(prefix) < 2:
            raise ValueError("an event requires at least two completed prefix bars")
        features = self.calculate(prefix)
        session = _session_bars(prefix)
        if not session:
            raise ValueError("at least one session bar is required")
        ranges = [bar.high - bar.low for bar in session]
        atr = sum(ranges[-self.atr_window:]) / min(len(ranges), self.atr_window)
        return LiveFeatures(
            vwap=features.vwap,
            atr=atr,
            session_high=features.session_high,
            session_low=features.session_low,
            opening_range_high=features.opening_range_high,
            opening_range_low=features.opening_range_low,
            return_1=(current.close / prefix[-1].close - 1) if prefix[-1].close else None,
            prior_day_high=prior_day_high,
            prior_day_low=prior_day_low,
        )


def vix_open_and_event(
    bars: tuple[MarketBar, ...], event: MarketBar,
) -> tuple[float | None, float | None]:
    """Return first-session VIX close and latest VIX close at event time."""
    date, event_minute = _ist_minute(event)
    same_day = sorted(
        (bar for bar in bars if _ist_minute(bar)[0] == date),
        key=lambda bar: _ist_minute(bar)[1],
    )
    vix_bars = [bar for bar in same_day if str(bar.instrument.instrument_type).upper() == "VIX"]
    if not vix_bars:
        return None, None
    opening = next((bar for bar in vix_bars if _ist_minute(bar)[1] >= 555), vix_bars[0])
    eligible = [bar for bar in vix_bars if _ist_minute(bar)[1] <= event_minute]
    return float(opening.close), float((eligible or [opening])[-1].close)


def option_pcr_at_event(
    bars: Mapping[Role | str, MarketBar] | None,
    event: MarketBar,
    *,
    expiry_dates: Iterable[str] = (),
) -> float | None:
    """Return nearest-expiry ATM +/- five-strike PE/CE volume PCR after 09:15.

    The input bundle must include the same-minute spot bar and option contracts.
    Missing spot or eligible option legs remain unavailable rather than
    widening the strike/expiry scope.
    """
    if _ist_minute(event)[1] < 9 * 60 + 15 or not bars:
        return None
    session_date = _ist_minute(event)[0]
    spot = next((bar.close for bar in bars.values()
                 if str(bar.instrument.instrument_type).upper() == "INDEX"
                 and bar.instrument.symbol.upper() in {"NIFTY", "NIFTY 50"}
                 and _ist_minute(bar) == _ist_minute(event)), None)
    options = [bar for bar in bars.values()
               if str(bar.instrument.instrument_type).upper() in {"CE", "PE"}
               and bar.instrument.strike is not None
               and bar.instrument.expiry is not None
               and _ist_minute(bar) == _ist_minute(event)]
    expected_expiries = sorted({str(value)[:10] for value in expiry_dates
                                if str(value)[:10] >= session_date})
    if spot is None or not expected_expiries:
        return None
    nearest_expiry = expected_expiries[0]
    selected = [bar for bar in options if str(bar.instrument.expiry)[:10] == nearest_expiry]
    strikes = sorted({float(bar.instrument.strike) for bar in selected})
    if not strikes:
        return None
    atm = min(strikes, key=lambda strike: (abs(strike - spot), strike))
    eligible_strikes = {atm}
    below = sorted((strike for strike in strikes if strike < atm), reverse=True)[:5]
    above = sorted(strike for strike in strikes if strike > atm)[:5]
    eligible_strikes.update(below)
    eligible_strikes.update(above)
    calls = puts = 0.0
    for bar in selected:
        if float(bar.instrument.strike) not in eligible_strikes:
            continue
        kind = str(bar.instrument.instrument_type).upper()
        if kind == "CE":
            calls += float(bar.volume or 0.0)
        elif kind == "PE":
            puts += float(bar.volume or 0.0)
    return puts / calls if calls > 0 else None


__all__ = ["LiveFeatureCalculator", "LiveFeatures", "option_pcr_at_event", "vix_open_and_event"]
