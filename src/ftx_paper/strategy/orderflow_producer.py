"""Native causal observation producer for completed market bars."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone, tzinfo
import math

from ftx_paper.contracts import MarketBar
from ftx_paper.config import IST

from .orderflow_contract import (
    OrderflowContext, OrderflowObservation, OrderflowObservationContext,
    UnavailableValue, stable_orderflow_id,
)
from .orderflow_location import LocationBand, LocationObservation, LocationRecord
from .orderflow_source import source_fingerprint


def _numeric_feature(features: Mapping[str, object], name: str) -> float:
    value = features.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"orderflow child feature {name!r} must be numeric")
    return float(value)


def source_row_is_complete(bar: MarketBar) -> bool:
    """Return whether a source row can produce published bar evidence."""
    values = (bar.open, bar.high, bar.low, bar.close, bar.volume)
    return all(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        for value in values
    )


def _source_identity(bar: MarketBar) -> str:
    instrument = bar.instrument
    return "|".join((
        instrument.symbol,
        instrument.exchange,
        instrument.instrument_type,
        instrument.expiry or "",
        "" if instrument.strike is None else str(instrument.strike),
        instrument.option_type.value if instrument.option_type else "",
    ))


class CausalOrderflowProducer:
    _PRODUCER_VERSION = "causal_orderflow_v2"

    def __init__(self, *, session_timezone: tzinfo = IST,
                 session_start: str = "09:15", session_end: str = "15:30") -> None:
        self._bars: dict[tuple[str, str], list[MarketBar]] = defaultdict(list)
        self._active_date: str | None = None
        self._last_timestamps: dict[str, datetime] = {}
        self._raw_last_timestamps: dict[str, datetime] = {}
        self._selected_identity: str | None = None
        self._prior_bars: tuple[MarketBar, ...] = ()
        self._prior_complete = False
        self._prior_close: float | None = None
        self._prior_value_high: float | None = None
        self._prior_value_low: float | None = None
        self._session_timezone = session_timezone
        self._session_start = datetime.strptime(session_start, "%H:%M").time()
        self._session_end = datetime.strptime(session_end, "%H:%M").time()

    def set_prior_close(self, close: float | None) -> None:
        """Provide the prior-session close when replay starts mid-history."""
        self._prior_close = close

    def set_prior_value_bounds(self, high: float | None, low: float | None) -> None:
        self._prior_value_high = high
        self._prior_value_low = low

    def ingest(self, bar: MarketBar) -> None:
        local = bar.timestamp.astimezone(self._session_timezone)
        date = local.date().isoformat()
        local_time = local.time().replace(second=0, microsecond=0)
        if bar.instrument.instrument_type.upper() not in {"FUT", "FUTURE", "FUTURES"}:
            return
        if not source_row_is_complete(bar):
            return
        if local_time < self._session_start or local_time >= self._session_end:
            return
        if self._active_date is not None and date < self._active_date:
            raise ValueError("orderflow source bars cannot move to an earlier session")
        if self._active_date is not None and date > self._active_date:
            self._prior_bars = tuple(
                bar for bars in self._bars.values() for bar in bars
            )
            expected = int((
                datetime.combine(datetime.min, self._session_end)
                - datetime.combine(datetime.min, self._session_start)
            ).total_seconds() // 60)
            prior_times = sorted(item.timestamp for item in self._prior_bars)
            self._prior_complete = len(prior_times) == expected and all(
                right - left == timedelta(minutes=1)
                for left, right in zip(prior_times, prior_times[1:])
            )
            self._bars.clear()
            self._last_timestamps.clear()
            self._raw_last_timestamps.clear()
            self._selected_identity = None
        timestamp = bar.timestamp.astimezone(timezone.utc)
        identity = _source_identity(bar)
        if self._selected_identity is None:
            self._selected_identity = identity
        elif identity != self._selected_identity:
            raise ValueError("orderflow source identity changed within a session")
        raw_last_timestamp = self._raw_last_timestamps.get(identity)
        if raw_last_timestamp == timestamp:
            return
        if raw_last_timestamp is not None and timestamp < raw_last_timestamp:
            raise ValueError("orderflow source bars must be strictly chronological")
        self._raw_last_timestamps[identity] = timestamp
        self._active_date = date
        key = (date, identity)
        bars = self._bars[key]
        last_timestamp = self._last_timestamps.get(identity)
        if last_timestamp == timestamp:
            return
        if last_timestamp is not None and timestamp < last_timestamp:
            raise ValueError("orderflow source bars must be strictly chronological")
        bars.append(bar)
        self._last_timestamps[identity] = timestamp

    def context(self, trading_date: str, minute: str) -> OrderflowContext | None:
        if self._active_date != trading_date:
            return None
        decision_cutoff = datetime.fromisoformat(
            f"{trading_date}T{minute}:00"
        ).replace(tzinfo=self._session_timezone).astimezone(timezone.utc) + timedelta(minutes=1)
        candidates = [
            bar for (date, _), bars in self._bars.items()
            if date == trading_date
            for bar in bars
            if self._session_start <= bar.timestamp.astimezone(self._session_timezone).time().replace(second=0, microsecond=0) < self._session_end
            and bar.timestamp + timedelta(minutes=1) <= decision_cutoff
        ]
        if not candidates:
            return None
        bars = sorted(candidates, key=lambda item: item.timestamp)
        stream_identity = _source_identity(bars[-1])
        instrument = bars[-1].instrument.symbol
        bars = [bar for bar in bars if _source_identity(bar) == stream_identity]
        prefix: list[OrderflowObservation] = []
        cumulative_volume = 0.0
        cumulative_dollar_volume = 0.0
        vwap_unavailable = False
        recovery_indices: set[int] = set()
        session_start = datetime.combine(
            datetime.fromisoformat(trading_date).date(), self._session_start,
        ).replace(
            tzinfo=self._session_timezone
        ).astimezone(timezone.utc)
        for gap_index in range(1, len(bars)):
            if bars[gap_index].timestamp - bars[gap_index - 1].timestamp <= timedelta(minutes=1):
                continue
            for end_index in range(gap_index + 4, len(bars)):
                window = bars[end_index - 4:end_index + 1]
                if (
                    len(window) == 5
                    and all(right.timestamp - left.timestamp == timedelta(minutes=1) for left, right in zip(window, window[1:]))
                    and ((window[0].timestamp.astimezone(self._session_timezone).hour * 60 + window[0].timestamp.astimezone(self._session_timezone).minute - (self._session_start.hour * 60 + self._session_start.minute)) % 5 == 0)
                ):
                    recovery_indices.add(end_index + 1)
                    break
        for bar_index, bar in enumerate(bars):
            boundary_gap = bar_index == 0 and bar.timestamp > session_start
            source_gap = bool(
                bar_index
                and bar.timestamp - bars[bar_index - 1].timestamp > timedelta(minutes=1)
            )
            bar_has_gap = boundary_gap or source_gap
            volume = float(bar.volume or 0.0)
            available_at = bar.timestamp + timedelta(minutes=1)
            if bar_index in recovery_indices:
                vwap_unavailable = False
                cumulative_volume = 0.0
                cumulative_dollar_volume = 0.0
            if bar_has_gap:
                vwap_unavailable = True
            gap_status = (
                "boundary_gap" if boundary_gap
                else "source_gap" if source_gap or vwap_unavailable
                else "none"
            )
            if not vwap_unavailable:
                cumulative_volume += max(0.0, volume)
                cumulative_dollar_volume += ((bar.high + bar.low + bar.close) / 3.0) * max(0.0, volume)
            vwap: float | UnavailableValue = (
                cumulative_dollar_volume / cumulative_volume
                if not vwap_unavailable and cumulative_volume > 0
                else UnavailableValue(gap_status if bar_has_gap else "source_gap", available_at)
            )
            prefix.append(OrderflowObservation(
                observation_id=stable_orderflow_id("observation", trading_date, stream_identity, "1m", bar.timestamp.isoformat()),
                source_observation_id=stable_orderflow_id("source", trading_date, stream_identity, bar.timestamp.isoformat()),
                feature_version="typical_price_volume_v1",
                timeframe="1m",
                instrument=instrument,
                observed_at=bar.timestamp,
                available_at=available_at,
                interval_start=bar.timestamp,
                interval_end=available_at,
                features={
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": volume,
                    "previous_close": (
                        bars[bar_index - 1].close if bar_index and not bar_has_gap
                        else self._prior_bars[-1].close if bar_index == 0 and self._prior_bars
                        else self._prior_close if bar_index == 0
                        else None
                    ),
                    "prior_value_high": self._prior_value_high,
                    "prior_value_low": self._prior_value_low,
                    "vwap": vwap,
                },
                context=OrderflowObservationContext(
                    session_id=trading_date, completeness="complete", availability_status="available",
                    replay_provenance={
                        "producer": "ftx-paper",
                        "producer_version": self._PRODUCER_VERSION,
                        "input_availability": "gap" if bar_has_gap else "complete",
                        "gap_status": gap_status,
                        "gap_origin": "boundary_gap" if boundary_gap else "source_gap" if source_gap else "none",
                        "source_fingerprint": source_fingerprint((bar,)),
                    }, generation=1,
                    causal_sequence=1,
                ),
            ))
        five_minute: list[OrderflowObservation] = []
        location_records: dict[str, LocationRecord] = {}
        location_generations: dict[str, int] = {}
        windows: dict[int, list[OrderflowObservation]] = defaultdict(list)
        for item in prefix:
            if item.interval_start is None:
                continue
            offset = int((item.interval_start - session_start).total_seconds() // 60)
            if offset >= 0:
                windows[offset // 5].append(item)
        for window_index, window_children in sorted(windows.items()):
            children = tuple(sorted(window_children, key=lambda item: item.observed_at))
            if len(children) != 5 or tuple(
                item.observed_at for item in children
            ) != tuple(children[0].observed_at + timedelta(minutes=index) for index in range(5)):
                continue
            if any(
                left.interval_end != right.interval_start
                for left, right in zip(children, children[1:])
            ):
                continue
            start, end = children[0].interval_start, children[-1].interval_end
            if start is None or end is None:
                continue
            child_intervals = tuple(
                (item.interval_start, item.interval_end)
                for item in children
                if item.interval_start is not None and item.interval_end is not None
            )
            if len(child_intervals) != 5:
                continue
            parent_has_gap = any(
                item.context is not None
                and item.context.replay_provenance.get("gap_origin", "none") != "none"
                for item in children
            )
            parent_id = stable_orderflow_id("aggregate", trading_date, stream_identity, "5m", start.isoformat())
            child_ids = tuple(item.observation_id for item in children)
            child_timestamps = {item.observed_at for item in children}
            source_rows = [
                bar for bar in bars
                if bar.timestamp in child_timestamps and _source_identity(bar) == stream_identity
            ]
            parent_source_fingerprint = source_fingerprint(source_rows)
            child_features = [item.features for item in children]
            total_volume = sum(_numeric_feature(item, "volume") for item in child_features)
            parent_features = {
                "open": _numeric_feature(child_features[0], "open"),
                "high": max(_numeric_feature(item, "high") for item in child_features),
                "low": min(_numeric_feature(item, "low") for item in child_features),
                "close": _numeric_feature(child_features[-1], "close"),
                "previous_close": child_features[0].get("previous_close"),
                "volume": total_volume,
                "vwap": (
                    sum(((_numeric_feature(item.features, "high") + _numeric_feature(item.features, "low") + _numeric_feature(item.features, "close")) / 3.0) * _numeric_feature(item.features, "volume") for item in children) / total_volume
                    if not parent_has_gap and total_volume > 0 else UnavailableValue("source_gap" if parent_has_gap else "missing_source", end)
                ),
            }
            session_children = [
                item for item in prefix if item.observed_at <= children[-1].observed_at
            ]
            ranges = [
                _numeric_feature(item.features, "high") - _numeric_feature(item.features, "low")
                for item in session_children
            ]
            atr = max(sum(ranges[-20:]) / len(ranges[-20:]), 1e-9)
            session_high = max(_numeric_feature(item.features, "high") for item in session_children)
            session_low = min(_numeric_feature(item.features, "low") for item in session_children)
            session_open = [
                item for item in session_children
                if item.observed_at < session_start + timedelta(minutes=15)
            ]
            levels: dict[str, float] = {
                "session_high": session_high,
                "session_low": session_low,
            }
            if len(session_open) >= 5 and tuple(
                item.observed_at for item in session_open[:5]
            ) == tuple(session_start + timedelta(minutes=index) for index in range(5)):
                levels.update({
                    "or_high": max(_numeric_feature(item.features, "high") for item in session_open[:5]),
                    "or_low": min(_numeric_feature(item.features, "low") for item in session_open[:5]),
                })
            if self._prior_bars and self._prior_complete:
                levels.update({
                    "pd_high": max(float(item.high) for item in self._prior_bars),
                    "pd_low": min(float(item.low) for item in self._prior_bars),
                })
            parent_vwap = parent_features.get("vwap")
            if isinstance(parent_vwap, (int, float)) and not isinstance(parent_vwap, bool):
                levels["vwap"] = float(parent_vwap)
            location_observations: list[LocationObservation] = []
            for alias, level in levels.items():
                alias = alias.upper()
                previous = location_records.get(alias)
                band = LocationBand(level - atr * 0.06, level + atr * 0.06)
                if (
                    previous is None
                    or previous.price != level
                    or previous.band != band
                ):
                    generation = location_generations.get(alias, 0) + 1
                    location_generations[alias] = generation
                    location_id = stable_orderflow_id(
                        "location", trading_date, stream_identity, alias, generation,
                    )
                    location_records[alias] = LocationRecord(
                        location_id, alias, level, band, generation, end, parent_id,
                        timeframe="5m", source_identity=stream_identity,
                        replaced_location_id=previous.location_id if previous else None,
                    )
                record = location_records[alias]
                parent_high = _numeric_feature(parent_features, "high")
                parent_low = _numeric_feature(parent_features, "low")
                location_observations.append(LocationObservation(
                    location_id=record.location_id,
                    location_alias=record.alias,
                    reference_generation=record.generation,
                    reference=record.price,
                    band_lower=record.band.lower,
                    band_upper=record.band.upper,
                    open=_numeric_feature(parent_features, "open"),
                    high=parent_high,
                    low=parent_low,
                    close=_numeric_feature(parent_features, "close"),
                    band_contact=record.band.lower <= parent_high and parent_low <= record.band.upper,
                    source_aggregate_id=parent_id,
                    observed_at=start,
                    available_at=end,
                    observation_id=stable_orderflow_id(
                        "location_observation", parent_id, record.location_id,
                    ),
                ))
            five_minute.append(OrderflowObservation(
                observation_id=parent_id,
                source_observation_id=parent_id,
                feature_version="typical_price_volume_v1",
                timeframe="5m",
                instrument=instrument,
                observed_at=start,
                available_at=end,
                interval_start=start,
                interval_end=end,
                features=parent_features,
                context=OrderflowObservationContext(
                    session_id=trading_date, completeness="complete", availability_status="available",
                    replay_provenance={
                        "producer": "ftx-paper",
                        "producer_version": self._PRODUCER_VERSION,
                        "input_availability": "gap" if parent_has_gap else "complete",
                        "gap_status": "source_gap" if parent_has_gap else "none",
                        "source_fingerprint": parent_source_fingerprint,
                    }, generation=1,
                    child_observation_ids=child_ids,
                    child_intervals=child_intervals,
                    causal_sequence=1,
                ),
                location_observations=tuple(location_observations),
            ))
        observations = (*prefix, *five_minute)
        cutoff = decision_cutoff
        return OrderflowContext(
            trading_date, cutoff, observations,
            {"1m": tuple(prefix), "5m": tuple(five_minute)},
        )


__all__ = ["CausalOrderflowProducer", "source_row_is_complete"]
