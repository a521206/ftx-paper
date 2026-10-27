"""Causal feature snapshots and runtime-scoped caching."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from threading import RLock
from typing import TYPE_CHECKING, cast

from ftx_paper.contracts import MarketBar, MarketRole
from ftx_paper.config import IST
from ftx_paper.ports.repositories import FeatureRepository

from .features import LiveFeatureCalculator, LiveFeatures, option_pcr_at_event, vix_open_and_event

if TYPE_CHECKING:
    from .bundles import DecisionBundle


FEATURE_CALCULATOR_VERSION = "live-features-v1"


@dataclass(frozen=True, slots=True)
class FeatureCacheKey:
    instrument: str
    session_date: str
    timeframe: str
    cutoff_minute: str
    feature_set: str
    calculator_version: str
    parameters: tuple[tuple[str, str], ...]
    input_fingerprint: str

    @property
    def value(self) -> str:
        return json.dumps({
            "instrument": self.instrument,
            "session_date": self.session_date,
            "timeframe": self.timeframe,
            "cutoff_minute": self.cutoff_minute,
            "feature_set": self.feature_set,
            "calculator_version": self.calculator_version,
            "parameters": self.parameters,
            "input_fingerprint": self.input_fingerprint,
        }, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class FeatureSnapshot:
    key: FeatureCacheKey
    values: Mapping[str, float | int | str | None]
    source_bar_ids: tuple[str, ...]
    computed_at: datetime
    availability: str = "available"

    @property
    def live_features(self) -> LiveFeatures | None:
        required = ("vwap", "atr", "session_high", "session_low", "return_1")
        if any(name not in self.values for name in required):
            return None
        return LiveFeatures(
            vwap=_float(self.values["vwap"]),
            atr=_float(self.values["atr"]),
            session_high=_required_float(self.values["session_high"]),
            session_low=_required_float(self.values["session_low"]),
            opening_range_high=_float(self.values.get("opening_range_high")),
            opening_range_low=_float(self.values.get("opening_range_low")),
            return_1=_float(self.values["return_1"]),
            prior_day_high=_float(self.values.get("prior_day_high")),
            prior_day_low=_float(self.values.get("prior_day_low")),
        )

    @property
    def location_features(self):
        features = self.live_features
        if features is None or features.atr is None:
            return None
        from .location import LocationFeatures

        return LocationFeatures(
            vwap=features.vwap,
            session_high=features.session_high,
            session_low=features.session_low,
            atr=features.atr,
            opening_range_high=features.opening_range_high,
            opening_range_low=features.opening_range_low,
            opening_range_complete=(
                features.opening_range_high is not None and features.opening_range_low is not None
            ),
            prior_day_high=features.prior_day_high,
            prior_day_low=features.prior_day_low,
        )


class FeatureMemoryCache:
    """Bounded, runtime-local L1 cache for immutable feature snapshots."""

    def __init__(self, max_entries: int = 4096) -> None:
        if max_entries < 1:
            raise ValueError("feature cache size must be positive")
        self.max_entries = max_entries
        self._items: OrderedDict[str, FeatureSnapshot] = OrderedDict()
        self._lock = RLock()

    def get(self, key: FeatureCacheKey) -> FeatureSnapshot | None:
        with self._lock:
            snapshot = self._items.get(key.value)
            if snapshot is not None:
                self._items.move_to_end(key.value)
            return snapshot

    def put(self, snapshot: FeatureSnapshot) -> None:
        with self._lock:
            self._items[snapshot.key.value] = snapshot
            self._items.move_to_end(snapshot.key.value)
            while len(self._items) > self.max_entries:
                self._items.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._items.clear()


class FeatureService:
    """Build causal event snapshots and resolve them through L1/L2 caching."""

    def __init__(
        self,
        repository: FeatureRepository | None = None,
        *,
        memory_cache: FeatureMemoryCache | None = None,
        calculator: LiveFeatureCalculator | None = None,
    ) -> None:
        self.repository = repository
        self.memory_cache = memory_cache or FeatureMemoryCache()
        self.calculator = calculator or LiveFeatureCalculator()
        self._trading_date: str | None = None
        self._futures: list[MarketBar] = []
        self._vix: list[MarketBar] = []
        self._last_futures_timestamp: datetime | None = None

    def reset(self) -> None:
        self._trading_date = None
        self._futures.clear()
        self._vix.clear()
        self._last_futures_timestamp = None
        self.memory_cache.clear()

    def ingest(self, bar: MarketBar) -> None:
        date = _session_date(bar)
        if self._trading_date is not None and date < self._trading_date:
            return
        if self._trading_date != date:
            self._trading_date = date
            self._futures.clear()
            self._vix.clear()
            self._last_futures_timestamp = None
            self.memory_cache.clear()
        kind = str(bar.instrument.instrument_type).upper()
        if kind in {"FUT", "FUTURES"}:
            if (
                self._last_futures_timestamp is not None
                and bar.timestamp <= self._last_futures_timestamp
            ):
                return
            self._futures.append(bar)
            self._last_futures_timestamp = bar.timestamp
        elif kind == "VIX" or bar.instrument.symbol.upper() == "INDIA VIX":
            self._vix.append(bar)

    def enrich(
        self,
        bundle: "DecisionBundle",
        *,
        prior_day_high: float | None = None,
        prior_day_low: float | None = None,
        expiry_dates: tuple[str, ...] = (),
    ) -> "DecisionBundle":
        futures = bundle.bars.get(MarketRole.FUTURES)
        if futures is None:
            return bundle
        prefix = tuple(bar for bar in self._futures if bar.timestamp < futures.timestamp)
        supporting = bundle.supporting_inputs or {}
        supporting_bars = supporting.get("bars", {})
        if not isinstance(supporting_bars, Mapping):
            supporting_bars = {}
        vix_bars = tuple(bar for bar in self._vix if bar.timestamp <= futures.timestamp)
        sources = supporting.get("sources", {})
        if not isinstance(sources, Mapping):
            sources = {}
        key = FeatureCacheKey(
            instrument=futures.instrument.exchange + ":" + futures.instrument.symbol,
            session_date=bundle.trading_date,
            timeframe="1m",
            cutoff_minute=bundle.minute,
            feature_set="live-market",
            calculator_version=FEATURE_CALCULATOR_VERSION,
            parameters=(
                ("atr_window", str(self.calculator.atr_window)),
                ("opening_range_bars", str(self.calculator.opening_range_bars)),
                ("expiry_dates", ",".join(expiry_dates)),
                ("prior_day_high", str(prior_day_high)),
                ("prior_day_low", str(prior_day_low)),
            ),
            input_fingerprint=_fingerprint(
                (*prefix, futures, *vix_bars),
                cast(Mapping[object, object], supporting_bars),
                cast(Mapping[object, object], sources),
            ),
        )
        snapshot = self.memory_cache.get(key)
        if snapshot is None and self.repository is not None:
            snapshot = self.repository.read_feature_snapshot(key)
        if snapshot is None:
            values: dict[str, float | int | str | None] = {
                "prior_day_high": prior_day_high,
                "prior_day_low": prior_day_low,
                "pcr": option_pcr_at_event(supporting_bars, futures, expiry_dates=expiry_dates),
            }
            if len(prefix) >= 2:
                features = self.calculator.calculate_event(
                    prefix, futures, prior_day_high=prior_day_high, prior_day_low=prior_day_low,
                )
                values.update(_feature_values(features))
            if vix_bars:
                values["vix_open"], values["vix"] = vix_open_and_event(vix_bars, futures)
            snapshot = FeatureSnapshot(
                key=key,
                values=values,
                source_bar_ids=tuple(
                    _bar_id(bar) for bar in (
                        *prefix, futures, *vix_bars,
                        *(bar for bar in supporting_bars.values() if isinstance(bar, MarketBar)),
                    )
                ),
                computed_at=datetime.now(timezone.utc),
                availability="warmup" if len(prefix) < 2 else "available",
            )
            if self.repository is not None and getattr(self.repository, "writable", True):
                self.repository.write_feature_snapshot(snapshot)
        self.memory_cache.put(snapshot)
        return bundle.with_feature_snapshot(snapshot)


def _float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"feature value is not numeric: {type(value).__name__}")


def _required_float(value: object) -> float:
    result = _float(value)
    if result is None:
        raise ValueError("required feature value is missing")
    return result


def _session_date(bar: MarketBar) -> str:
    return bar.timestamp.astimezone(IST).date().isoformat()


def _bar_id(bar: MarketBar) -> str:
    instrument = bar.instrument
    return ":".join((
        instrument.exchange,
        instrument.symbol,
        str(instrument.instrument_type),
        str(instrument.expiry or ""),
        str(instrument.strike if instrument.strike is not None else ""),
        str(instrument.option_type or ""),
        bar.timestamp.isoformat(),
    ))


def _feature_values(features: LiveFeatures) -> dict[str, float | int | str | None]:
    return {
        "vwap": features.vwap,
        "atr": features.atr,
        "session_high": features.session_high,
        "session_low": features.session_low,
        "opening_range_high": features.opening_range_high,
        "opening_range_low": features.opening_range_low,
        "return_1": features.return_1,
        "prior_day_high": features.prior_day_high,
        "prior_day_low": features.prior_day_low,
    }


def _fingerprint(
    bars: tuple[MarketBar, ...],
    supporting: Mapping[object, object],
    sources: Mapping[object, object],
) -> str:
    rows = [{
        "id": _bar_id(bar), "open": bar.open, "high": bar.high, "low": bar.low,
        "close": bar.close, "volume": bar.volume, "open_interest": bar.open_interest,
    } for bar in bars]
    rows.extend({"id": _bar_id(bar), "open": bar.open, "high": bar.high, "low": bar.low,
                 "close": bar.close, "volume": bar.volume, "open_interest": bar.open_interest}
                for bar in supporting.values() if isinstance(bar, MarketBar))
    payload = json.dumps(
        {"bars": rows, "sources": dict(sources)},
        sort_keys=True, separators=(",", ":"), default=str,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "FEATURE_CALCULATOR_VERSION", "FeatureCacheKey", "FeatureMemoryCache",
    "FeatureRepository", "FeatureService", "FeatureSnapshot",
]
