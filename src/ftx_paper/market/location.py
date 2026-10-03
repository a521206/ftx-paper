"""Independent causal structural-location and cell detector.

The detector deliberately has no imports from the historical FTX pipeline.  A
location is evaluated against the completed-bar prefix; the current bar is
only used as the price being classified.  This makes the module suitable for
both replay and live paper decisions.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from zoneinfo import ZoneInfo

from ftx_paper.contracts import MarketBar


IST = ZoneInfo("Asia/Kolkata")


def _ist_minute(bar: MarketBar) -> int:
    timestamp = bar.timestamp if bar.timestamp.tzinfo is not None else bar.timestamp.replace(tzinfo=IST)
    timestamp = timestamp.astimezone(IST)
    return timestamp.hour * 60 + timestamp.minute


class Location(StrEnum):
    VWAP_ZONE = "vwap_zone"
    SESSION_HIGH = "session_high"
    SESSION_LOW = "session_low"
    NEW_HIGH = "new_high"
    NEW_LOW = "new_low"
    OR_HIGH = "or_high"
    OR_LOW = "or_low"
    PRIOR_DAY_HIGH = "prior_day_high"
    PRIOR_DAY_LOW = "prior_day_low"


class ReferenceSide(StrEnum):
    BELOW = "below"
    ABOVE = "above"
    IN_ZONE = "in_zone"


class TransitionKind(StrEnum):
    APPROACH = "approach"
    CROSS = "cross"
    REJECT = "reject"
    RECLAIM = "reclaim"


@dataclass(frozen=True, slots=True)
class LocationTransition:
    reference: Location
    kind: TransitionKind
    from_side: ReferenceSide
    to_side: ReferenceSide


@dataclass(frozen=True, slots=True)
class TransitionPattern:
    reference: Location
    kind: TransitionKind
    from_side: ReferenceSide
    to_side: ReferenceSide

    def matches(self, transition: LocationTransition | None) -> bool:
        return transition == LocationTransition(self.reference, self.kind, self.from_side, self.to_side)


_ORDER = tuple(Location)
# Match the canonical cell normalization: session extremes imply new extremes,
# and new extremes imply the corresponding opening-range level.
_IMPLIED = {
    Location.SESSION_HIGH: Location.NEW_HIGH,
    Location.SESSION_LOW: Location.NEW_LOW,
    Location.NEW_HIGH: Location.OR_HIGH,
    Location.NEW_LOW: Location.OR_LOW,
}


@dataclass(frozen=True, slots=True)
class Cell:
    """Canonical simultaneous-location combination."""

    locations: frozenset[Location]

    def __init__(self, *locations: Location | str) -> None:
        values = frozenset(Location(value) if not isinstance(value, Location) else value for value in locations)
        normalized = frozenset(x for x in values if _IMPLIED.get(x) not in values)
        if not normalized:
            raise ValueError("cell requires at least one location")
        object.__setattr__(self, "locations", normalized)

    @property
    def ordered_locations(self) -> tuple[Location, ...]:
        return tuple(location for location in _ORDER if location in self.locations)

    @property
    def name(self) -> str:
        return "+".join(location.value for location in self.ordered_locations)

    @classmethod
    def parse(cls, value: str | Cell) -> Cell:
        return value if isinstance(value, Cell) else cls(*value.split("+"))

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class LocationFeatures:
    vwap: float | None
    session_high: float
    session_low: float
    atr: float
    opening_range_high: float | None
    opening_range_low: float | None
    opening_range_complete: bool
    prior_day_high: float | None
    prior_day_low: float | None


@dataclass(frozen=True, slots=True)
class LocationSnapshot:
    features: LocationFeatures
    locations: tuple[Location, ...]
    cell: Cell | None
    transitions: tuple[LocationTransition, ...] = ()


def _features(prefix: tuple[MarketBar, ...], *, opening_range_bars: int, atr_window: int,
              prior_day_high: float | None, prior_day_low: float | None) -> LocationFeatures:
    session = tuple(bar for bar in prefix if _ist_minute(bar) >= 555)
    if len(session) < 2:
        raise ValueError("at least two completed prefix bars are required")
    volume = 0.0
    weighted_typical = 0.0
    for bar in session:
        bar_volume = float(bar.volume or 0.0)
        typical_price = (bar.high + bar.low + bar.close) / 3.0
        volume += bar_volume
        weighted_typical += typical_price * bar_volume
    vwap = weighted_typical / volume if volume > 0 else None
    ranges = [bar.high - bar.low for bar in session]
    atr = sum(ranges[-atr_window:]) / len(ranges[-atr_window:])
    opening = session[:opening_range_bars]
    return LocationFeatures(
        vwap=vwap, session_high=max(bar.high for bar in session), session_low=min(bar.low for bar in session),
        atr=max(atr, 1e-6),
        opening_range_high=max(bar.high for bar in opening) if len(session) > opening_range_bars else None,
        opening_range_low=min(bar.low for bar in opening) if len(session) > opening_range_bars else None,
        opening_range_complete=len(session) > opening_range_bars,
        prior_day_high=prior_day_high, prior_day_low=prior_day_low,
    )


def detect_all_locations(
    features: LocationFeatures,
    current_close: float,
    *,
    vwap_zone_pct: float = 0.25,
    vwap_zone_active: bool | None = None,
) -> tuple[Location, ...]:
    """Return every location active on the current close in canonical order."""
    close, atr = current_close, features.atr
    found: list[Location] = []
    if features.vwap is not None and (
        vwap_zone_active is True
        or vwap_zone_active is None
        and close > 0
        and abs(close - features.vwap) / close * 100 <= vwap_zone_pct
    ):
        found.append(Location.VWAP_ZONE)
    if close > features.session_high:
        found.append(Location.NEW_HIGH)
    if close < features.session_low:
        found.append(Location.NEW_LOW)
    session_range = features.session_high - features.session_low
    percentile = (close - features.session_low) / session_range if session_range > 0 else 0.5
    if percentile <= 0.10:
        found.append(Location.SESSION_LOW)
    if percentile >= 0.90:
        found.append(Location.SESSION_HIGH)
    if features.opening_range_complete and features.opening_range_high is not None and close >= features.opening_range_high - 0.25 * atr:
        found.append(Location.OR_HIGH)
    if features.opening_range_complete and features.opening_range_low is not None and close <= features.opening_range_low + 0.25 * atr:
        found.append(Location.OR_LOW)
    if features.prior_day_high is not None and abs(close - features.prior_day_high) <= 0.25 * atr:
        found.append(Location.PRIOR_DAY_HIGH)
    if features.prior_day_low is not None and abs(close - features.prior_day_low) <= 0.25 * atr:
        found.append(Location.PRIOR_DAY_LOW)
    return tuple(found)


def detect_location(features: LocationFeatures, current_close: float, *, vwap_zone_pct: float = 0.25) -> Location | None:
    """Return the canonical priority location, if any."""
    locations = detect_all_locations(features, current_close, vwap_zone_pct=vwap_zone_pct)
    return locations[0] if locations else None


def transition_patterns_allow(snapshot: LocationSnapshot, patterns: Iterable[TransitionPattern] | None) -> bool:
    """Apply canonical any-matching transition-pattern filtering."""
    if not patterns:
        return True
    by_reference = {item.reference: item for item in snapshot.transitions}
    return any(pattern.matches(by_reference.get(pattern.reference)) for pattern in patterns)


def _side(price: float, level: float, zone_pct: float = 0.0) -> ReferenceSide:
    if zone_pct and abs(price - level) / abs(price) * 100 <= zone_pct:
        return ReferenceSide.IN_ZONE
    return ReferenceSide.ABOVE if price > level else ReferenceSide.BELOW


class LocationDetector:
    """Stateful causal detector with transition debounce and OR completion."""

    def __init__(self, *, opening_range_bars: int = 15, atr_window: int = 20,
                 vwap_zone_pct: float = 0.25, transition_residence_bars: int = 2) -> None:
        if opening_range_bars < 1 or atr_window < 1 or transition_residence_bars < 1:
            raise ValueError("detector windows and residence bars must be positive")
        self.opening_range_bars, self.atr_window = opening_range_bars, atr_window
        self.vwap_zone_pct = vwap_zone_pct
        self.transition_residence_bars = transition_residence_bars
        self._bars: list[MarketBar] = []
        self._prior_day_high: float | None = None
        self._prior_day_low: float | None = None
        self._vwap_side: ReferenceSide | None = None
        self._prior_stable: dict[Location, ReferenceSide] = {}
        self._prior_pending: dict[Location, tuple[ReferenceSide, int]] = {}

    def reset(self, *, prior_day_high: float | None = None, prior_day_low: float | None = None) -> None:
        self._bars.clear()
        self._prior_day_high, self._prior_day_low = prior_day_high, prior_day_low
        self._vwap_side = None
        self._prior_stable.clear()
        self._prior_pending.clear()

    def observe(self, current: MarketBar) -> LocationSnapshot | None:
        prefix = tuple(self._bars)
        if len(prefix) < 2:
            self._bars.append(current)
            return None
        features = _features(prefix, opening_range_bars=self.opening_range_bars, atr_window=self.atr_window,
                             prior_day_high=self._prior_day_high, prior_day_low=self._prior_day_low)
        transitions: list[LocationTransition] = []
        if len(prefix) >= 2 and features.vwap is not None:
            current_vwap_side = _side(current.close, features.vwap, 0.25)
            if self._vwap_side is None:
                self._vwap_side = current_vwap_side
            else:
                if self._vwap_side is ReferenceSide.IN_ZONE:
                    if current.close > features.vwap * 1.0025:
                        current_vwap_side = ReferenceSide.ABOVE
                    elif current.close < features.vwap * 0.9975:
                        current_vwap_side = ReferenceSide.BELOW
                    else:
                        current_vwap_side = ReferenceSide.IN_ZONE
                elif self._vwap_side is ReferenceSide.ABOVE and current.close <= features.vwap * 1.0025:
                    current_vwap_side = ReferenceSide.IN_ZONE
                elif self._vwap_side is ReferenceSide.BELOW and current.close >= features.vwap * 0.9975:
                    current_vwap_side = ReferenceSide.IN_ZONE
                if current_vwap_side is not self._vwap_side:
                    old = self._vwap_side
                    kind = (TransitionKind.APPROACH if current_vwap_side is ReferenceSide.IN_ZONE else
                            TransitionKind.RECLAIM if current_vwap_side is ReferenceSide.ABOVE else TransitionKind.REJECT)
                    transitions.append(LocationTransition(Location.VWAP_ZONE, kind, old, current_vwap_side))
                    self._vwap_side = current_vwap_side
        detected_locations = detect_all_locations(
            features,
            current.close,
            vwap_zone_pct=self.vwap_zone_pct,
            vwap_zone_active=self._vwap_side is ReferenceSide.IN_ZONE,
        )
        if len(prefix) >= 2:
            for reference, level in ((Location.PRIOR_DAY_HIGH, features.prior_day_high), (Location.PRIOR_DAY_LOW, features.prior_day_low)):
                if level is None:
                    continue
                raw = _side(current.close, level)
                stable = self._prior_stable.get(reference)
                pending, count = self._prior_pending.get(reference, (raw, 0))
                if raw is not pending:
                    pending, count = raw, 1
                else:
                    count += 1
                self._prior_pending[reference] = (pending, count)
                if stable is None:
                    self._prior_stable[reference] = raw
                elif count >= self.transition_residence_bars and pending is not stable:
                    kind = TransitionKind.CROSS if {stable, pending} == {ReferenceSide.ABOVE, ReferenceSide.BELOW} else (TransitionKind.APPROACH if pending is ReferenceSide.IN_ZONE else TransitionKind.RECLAIM if pending is ReferenceSide.ABOVE else TransitionKind.REJECT)
                    transitions.append(LocationTransition(reference, kind, stable, pending))
                    self._prior_stable[reference] = pending
        cell = Cell(*detected_locations) if detected_locations else None
        # Keep every raw match in canonical order for the Layer 3 trace. Cell
        # normalization remains policy-facing and may intentionally prune
        # implied extremes.
        canonical_locations = tuple(detected_locations)
        snapshot = LocationSnapshot(features, canonical_locations, cell, tuple(transitions))
        self._bars.append(current)
        return snapshot

    def close_day(self) -> None:
        session = [bar for bar in self._bars if _ist_minute(bar) >= 555]
        if session:
            self._prior_day_high = max(bar.high for bar in session)
            self._prior_day_low = min(bar.low for bar in session)
        self._bars.clear()

    @property
    def prior_day_levels(self) -> tuple[float | None, float | None]:
        return self._prior_day_high, self._prior_day_low


__all__ = ["Cell", "Location", "LocationDetector", "LocationFeatures", "LocationSnapshot", "LocationTransition", "ReferenceSide", "TransitionKind", "TransitionPattern", "detect_all_locations", "detect_location", "transition_patterns_allow"]
