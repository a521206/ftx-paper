"""Causal feature snapshots and runtime-scoped caching."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping, Sequence
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
ORDERFLOW_DAILY_ATR_FEATURE_SET = "orderflow_daily_atr_v1"
ORDERFLOW_DAILY_ATR_CALCULATOR_VERSION = "ftx-paper-orderflow-daily-atr-v2"
DAILY_ATR_FEATURE_SET = "daily_atr_v1"
DAILY_ATR_CALCULATOR_VERSION = "ftx-paper-daily-atr-v1"


def _build_daily_atr_snapshot(
    session_date: str,
    sessions: Sequence[tuple[str, Sequence[Mapping[str, object]]]],
    *,
    window: int = 14,
    session_end_minute: str = "15:30",
    feature_set: str,
    calculator_version: str,
    feature_parameters: tuple[tuple[str, str], ...],
    zone_boundaries: tuple[tuple[str, str], ...] | None = None,
    prior_close: float | None = None,
    computed_at: datetime | None = None,
) -> "FeatureSnapshot":
    """Build one Paper-owned prior-session ATR artifact.

    ``sessions`` must contain only complete, already-selected futures sessions
    in chronological order. The target session is excluded by the caller, so
    the result is point-in-time safe for replay.
    """
    if window < 1:
        raise ValueError("ATR window must be positive")
    if window != 14:
        raise ValueError("orderflow daily ATR artifact requires a 14-session window")
    if len(sessions) < window:
        raise ValueError("insufficient completed sessions for daily ATR")

    input_prior_close = prior_close
    true_ranges: list[float] = []
    source_bar_ids: list[str] = []
    fingerprint_rows: list[dict[str, object]] = []
    for date_value, rows in sessions:
        if not rows:
            raise ValueError("daily ATR sessions must contain bars")
        ordered = sorted(rows, key=lambda row: str(row["minute"]))
        filtered_rows = [
            row for row in ordered
            if str(row["minute"])[11:16] < session_end_minute
        ]
        expected_count = _session_minute_count(session_end_minute)
        if len(filtered_rows) < expected_count:
            raise ValueError("daily ATR session is incomplete")
        ordered = filtered_rows
        if zone_boundaries is None:
            zone_rows = (ordered,)
        else:
            zone_rows = tuple(
                tuple(
                    row for row in ordered
                    if start <= str(row["minute"])[11:16] < end
                )
                for start, end in zone_boundaries
            )
            if any(not zone for zone in zone_rows):
                raise ValueError("daily ATR session is missing a strategy zone")
        high = max(
            _required_bar_number(row, "high")
            for zone in zone_rows for row in zone
        )
        low = min(
            _required_bar_number(row, "low")
            for zone in zone_rows for row in zone
        )
        close = _required_bar_number(zone_rows[-1][-1], "close")
        true_range = high - low
        if prior_close is not None:
            true_range = max(true_range, abs(high - prior_close), abs(low - prior_close))
        true_ranges.append(true_range)
        prior_close = close
        for row in ordered:
            minute = str(row["minute"])
            source_bar_ids.append(f"{date_value}:{minute[11:16]}")
            fingerprint_rows.append({
                "session_date": date_value,
                "minute": minute,
                "open": _required_bar_number(row, "open"),
                "high": _required_bar_number(row, "high"),
                "low": _required_bar_number(row, "low"),
                "close": _required_bar_number(row, "close"),
                "volume": row.get("volume"),
                "open_interest": row.get("open_interest"),
                "symbol": str(row.get("symbol", "NIFTY")),
                "expiry": str(row.get("expiry", "")),
            })

    digest = hashlib.sha256(
        json.dumps(
            {"prior_close": input_prior_close, "rows": fingerprint_rows},
            sort_keys=True, separators=(",", ":"), default=str,
        ).encode("utf-8")
    ).hexdigest()
    return FeatureSnapshot(
        key=FeatureCacheKey(
            instrument="NIFTY",
            session_date=session_date,
            timeframe="1d",
            cutoff_minute="00:00",
            feature_set=feature_set,
            calculator_version=calculator_version,
            parameters=feature_parameters,
            input_fingerprint=digest,
        ),
        values={"atr_14": sum(true_ranges[-window:]) / window},
        source_bar_ids=tuple(source_bar_ids),
        computed_at=computed_at or datetime.now(timezone.utc),
    )


def build_daily_atr_snapshot(
    session_date: str,
    sessions: Sequence[tuple[str, Sequence[Mapping[str, object]]]],
    *,
    window: int = 14,
    session_end_minute: str = "15:30",
    computed_at: datetime | None = None,
) -> "FeatureSnapshot":
    """Build a full-session daily ATR for a strategy-owned feature set."""
    return _build_daily_atr_snapshot(
        session_date, sessions, window=window, session_end_minute=session_end_minute,
        feature_set=DAILY_ATR_FEATURE_SET,
        calculator_version=DAILY_ATR_CALCULATOR_VERSION,
        feature_parameters=(
            ("series", "continuous"), ("window", str(window)),
            ("session_end", session_end_minute),
        ), computed_at=computed_at,
    )


def build_orderflow_daily_atr_snapshot(
    session_date: str,
    sessions: Sequence[tuple[str, Sequence[Mapping[str, object]]]],
    *,
    window: int = 14,
    prior_close: float | None = None,
    computed_at: datetime | None = None,
) -> "FeatureSnapshot":
    """Build the canonical orderflow ATR using the three strategy zones."""
    return _build_daily_atr_snapshot(
        session_date, sessions, window=window, session_end_minute="15:00",
        feature_set=ORDERFLOW_DAILY_ATR_FEATURE_SET,
        calculator_version=ORDERFLOW_DAILY_ATR_CALCULATOR_VERSION,
        feature_parameters=(
            ("series", "continuous"), ("window", str(window)),
            ("zones", "09:15-11:00,11:00-13:00,13:00-15:00"),
        ),
        zone_boundaries=(("09:15", "11:00"), ("11:00", "13:00"), ("13:00", "15:00")),
        prior_close=prior_close,
        computed_at=computed_at,
    )


def _session_minute_count(session_end_minute: str) -> int:
    hour, minute = (int(value) for value in session_end_minute.split(":"))
    return (hour * 60 + minute) - (9 * 60 + 15)


def _required_bar_number(row: Mapping[str, object], name: str) -> float:
    value = row.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"daily ATR bar field {name!r} must be numeric")
    return float(value)


def daily_atr_from_snapshot(snapshot: "FeatureSnapshot | None") -> float | None:
    """Read canonical prior-session ATR from an existing feature artifact."""
    if snapshot is None or snapshot.key.feature_set != ORDERFLOW_DAILY_ATR_FEATURE_SET:
        return None
    if snapshot.key.timeframe != "1d":
        return None
    if snapshot.key.calculator_version != ORDERFLOW_DAILY_ATR_CALCULATOR_VERSION:
        return None
    parameters = dict(snapshot.key.parameters)
    if (
        parameters.get("series") != "continuous"
        or parameters.get("window") != "14"
        or parameters.get("zones") != "09:15-11:00,11:00-13:00,13:00-15:00"
    ):
        return None
    if snapshot.availability != "available":
        return None
    value = snapshot.values.get("atr_14")
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        return None
    return float(value)


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
    "DAILY_ATR_CALCULATOR_VERSION", "DAILY_ATR_FEATURE_SET", "FEATURE_CALCULATOR_VERSION",
    "ORDERFLOW_DAILY_ATR_CALCULATOR_VERSION", "ORDERFLOW_DAILY_ATR_FEATURE_SET",
    "FeatureCacheKey", "FeatureMemoryCache", "FeatureRepository", "FeatureService",
    "FeatureSnapshot", "build_daily_atr_snapshot", "build_orderflow_daily_atr_snapshot",
]
