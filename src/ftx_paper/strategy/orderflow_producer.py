"""Native causal observation producer for completed market bars."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta, timezone, tzinfo
from bisect import bisect_right
import math
from typing import cast

from ftx_paper.contracts import MarketBar
from ftx_paper.config import IST

from .orderflow_contract import (
    OrderflowContext, OrderflowObservation, OrderflowObservationContext,
    stable_orderflow_id,
)
from .orderflow_location import LocationBand, LocationObservation, LocationRecord
from .orderflow_source import source_fingerprint
from .orderflow_scores import score_aggregate


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
        self._context_bars: tuple[MarketBar, ...] = ()
        self._context_prefix: tuple[OrderflowObservation, ...] = ()
        self._context_five_minute: tuple[OrderflowObservation, ...] = ()
        self._context_processed_windows: set[int] = set()
        self._context_location_records: dict[str, LocationRecord] = {}
        self._context_location_generations: dict[str, int] = {}
        self._frozen_atr: float | None = None
        self._source_issues: list[dict[str, object]] = []

    def _invalidate_context_cache(self) -> None:
        self._context_bars = ()
        self._context_prefix = ()
        self._context_five_minute = ()
        self._context_processed_windows.clear()
        self._context_location_records.clear()
        self._context_location_generations.clear()

    def set_prior_close(self, close: float | None) -> None:
        """Provide the prior-session close when replay starts mid-history."""
        self._prior_close = close
        self._invalidate_context_cache()

    def set_prior_value_bounds(self, high: float | None, low: float | None) -> None:
        self._prior_value_high = high
        self._prior_value_low = low
        self._invalidate_context_cache()

    def set_prior_bars(self, bars: Sequence[MarketBar]) -> None:
        """Provide completed prior-session bars for PD location publication."""
        self._invalidate_context_cache()
        self._prior_bars = tuple(sorted(
            (item for item in bars
             if item.instrument.instrument_type.upper() in {"FUT", "FUTURE", "FUTURES"}),
            key=lambda item: item.timestamp,
        ))
        expected = int((
            datetime.combine(datetime.min, self._session_end)
            - datetime.combine(datetime.min, self._session_start)
        ).total_seconds() // 60)
        times = [item.timestamp.astimezone(self._session_timezone) for item in self._prior_bars]
        self._prior_complete = len(times) == expected and all(
            right - left == timedelta(minutes=1)
            for left, right in zip(times, times[1:])
        )

    def set_frozen_atr(self, atr: float | None) -> None:
        """Provide the completed prior-session ATR for location geometry."""
        self._frozen_atr = atr if atr is None else float(atr)
        self._invalidate_context_cache()

    def ingest(self, bar: MarketBar) -> None:
        local = bar.timestamp.astimezone(self._session_timezone)
        date = local.date().isoformat()
        local_time = local.time().replace(second=0, microsecond=0)
        if bar.instrument.instrument_type.upper() not in {"FUT", "FUTURE", "FUTURES"}:
            return
        identity = _source_identity(bar)
        if self._active_date is not None and date > self._active_date:
            self._source_issues.clear()
        if self._selected_identity is None:
            self._selected_identity = identity
        elif identity != self._selected_identity and date == self._active_date:
            raise ValueError("orderflow source identity changed within a session")
        if not source_row_is_complete(bar):
            self._active_date = date
            timestamp = bar.timestamp.astimezone(timezone.utc)
            self._source_issues.append({
                "reason": "incomplete_source_row",
                "observed_at": timestamp.isoformat(),
                "available_at": (timestamp + timedelta(minutes=1)).isoformat(),
                "source_identity": _source_identity(bar),
                "source_fingerprint": source_fingerprint((bar,)),
            })
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
            self._selected_identity = identity
            self._invalidate_context_cache()
        timestamp = bar.timestamp.astimezone(timezone.utc)
        raw_last_timestamp = self._raw_last_timestamps.get(identity)
        if raw_last_timestamp == timestamp:
            self._source_issues.append({
                "reason": "duplicate_source_row",
                "observed_at": timestamp.isoformat(),
                "available_at": (timestamp + timedelta(minutes=1)).isoformat(),
                "source_identity": identity,
                "source_fingerprint": source_fingerprint((bar,)),
            })
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
        selected_identity = self._selected_identity
        if selected_identity is None:
            return None
        source_bars = self._bars.get((trading_date, selected_identity), [])
        cutoff_index = bisect_right(
            [bar.timestamp for bar in source_bars],
            decision_cutoff - timedelta(minutes=1),
        )
        bars = source_bars[:cutoff_index]
        if not bars and not self._source_issues:
            return None
        if not bars:
            return OrderflowContext(
                trading_date, decision_cutoff, (), {"1m": (), "5m": ()},
                tuple(self._source_issues),
            )
        stream_identity = selected_identity
        instrument = bars[-1].instrument.symbol
        cached_count = len(self._context_bars)
        append_only = (
            cached_count <= len(bars)
            and tuple(bars[:cached_count]) == self._context_bars
        )
        prefix: list[OrderflowObservation] = (
            list(self._context_prefix) if append_only else []
        )
        session_start = datetime.combine(
            datetime.fromisoformat(trading_date).date(), self._session_start,
        ).replace(
            tzinfo=self._session_timezone
        ).astimezone(timezone.utc)
        start_index = len(prefix)
        prior_1m: OrderflowObservation | None = prefix[-1] if prefix else None
        for bar_index, bar in enumerate(bars[start_index:], start=start_index):
            boundary_gap = bar_index == 0 and bar.timestamp > session_start
            source_gap = bool(
                bar_index
                and bar.timestamp - bars[bar_index - 1].timestamp > timedelta(minutes=1)
            )
            bar_has_gap = boundary_gap or source_gap
            volume = float(bar.volume or 0.0)
            available_at = bar.timestamp + timedelta(minutes=1)
            gap_status = (
                "boundary_gap" if boundary_gap
                else "source_gap" if source_gap
                else "none"
            )
            features = {
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
            }
            prior_features = prior_1m.features if prior_1m is not None and not bar_has_gap else None
            scores = score_aggregate(
                features, prior_features, atr=self._frozen_atr,
                volume_average=(
                    sum(_numeric_feature(cast(Mapping[str, object], item.features), "volume")
                        for item in prefix[-20:]) / min(len(prefix), 20)
                    if prefix else None
                ),
            )
            if scores is not None:
                features["long_score"], features["short_score"] = scores
            current = OrderflowObservation(
                observation_id=stable_orderflow_id("observation", trading_date, stream_identity, "1m", bar.timestamp.isoformat()),
                source_observation_id=stable_orderflow_id("source", trading_date, stream_identity, bar.timestamp.isoformat()),
                feature_version="typical_price_volume_v1",
                timeframe="1m",
                instrument=instrument,
                observed_at=bar.timestamp,
                available_at=available_at,
                interval_start=bar.timestamp,
                interval_end=available_at,
                features=features,
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
            )
            prefix.append(current)
            prior_1m = current
        self._context_bars = tuple(bars)
        self._context_prefix = tuple(prefix)
        five_minute: list[OrderflowObservation] = (
            list(self._context_five_minute) if append_only else []
        )
        location_records = (
            self._context_location_records if append_only else {}
        )
        location_generations = (
            self._context_location_generations if append_only else {}
        )
        processed_windows = (
            set(self._context_processed_windows) if append_only else set()
        )
        windows: dict[int, list[OrderflowObservation]] = defaultdict(list)
        for item in prefix:
            if item.interval_start is None:
                continue
            offset = int((item.interval_start - session_start).total_seconds() // 60)
            if offset >= 0:
                windows[offset // 5].append(item)
        for window_index, window_children in sorted(windows.items()):
            if window_index in processed_windows:
                continue
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
            source_rows = [bar for bar in bars if bar.timestamp in child_timestamps]
            parent_source_fingerprint = source_fingerprint(source_rows)
            child_features = [item.features for item in children]
            total_volume = sum(_numeric_feature(item, "volume") for item in child_features)
            atr = self._frozen_atr
            parent_features = {
                "open": _numeric_feature(child_features[0], "open"),
                "high": max(_numeric_feature(item, "high") for item in child_features),
                "low": min(_numeric_feature(item, "low") for item in child_features),
                "close": _numeric_feature(child_features[-1], "close"),
                "previous_close": child_features[0].get("previous_close"),
                "volume": total_volume,
            }
            prior_parent = next(
                (item for item in reversed(five_minute)
                 if item.observed_at < start), None,
            )
            child_scores_list: list[tuple[float, float]] = []
            for child in children:
                prior_child = next(
                    (item for item in reversed(prefix)
                     if item.observed_at < child.observed_at),
                    None,
                )
                prior_children = [
                    item for item in prefix if item.observed_at < child.observed_at
                ][-20:]
                child_score = score_aggregate(
                    child.features,
                    prior_child.features if prior_child is not None else None,
                    atr=atr,
                    volume_average=(
                        sum(_numeric_feature(cast(Mapping[str, object], item.features), "volume")
                            for item in prior_children) / min(len(prior_children), 20)
                        if prior_children else None
                    ),
                )
                if child_score is None:
                    child_scores_list = []
                    break
                child_scores_list.append(child_score)
            child_scores = tuple(child_scores_list)
            parent_scores = score_aggregate(
                parent_features,
                prior_parent.features if prior_parent is not None else None,
                atr=atr,
                child_scores=child_scores,
                volume_average=(
                    sum(_numeric_feature(cast(Mapping[str, object], item.features), "volume")
                        for item in five_minute[-20:])
                    / min(len(five_minute), 20) if five_minute else None
                ),
            )
            if parent_scores is not None:
                parent_features["long_score"], parent_features["short_score"] = parent_scores
            session_children = [
                item for item in prefix if item.observed_at <= children[-1].observed_at
            ]
            session_high = max(_numeric_feature(item.features, "high") for item in session_children)
            session_low = min(_numeric_feature(item.features, "low") for item in session_children)
            session_open = [
                item for item in session_children
                if item.observed_at < session_start + timedelta(minutes=15)
            ]
            levels: dict[str, float] = {} if atr is None else {
                "session_high": session_high,
                "session_low": session_low,
            }
            if len(session_open) >= 5 and tuple(
                item.observed_at for item in session_open[:5]
            ) == tuple(session_start + timedelta(minutes=index) for index in range(5)):
                if atr is not None:
                    levels.update({
                    "or_high": max(_numeric_feature(item.features, "high") for item in session_open[:5]),
                    "or_low": min(_numeric_feature(item.features, "low") for item in session_open[:5]),
                    })
            if self._prior_bars and self._prior_complete:
                if atr is not None:
                    levels.update({
                    "pd_high": max(float(item.high) for item in self._prior_bars),
                    "pd_low": min(float(item.low) for item in self._prior_bars),
                    })
            for alias, level in levels.items():
                alias = alias.upper()
                if atr is None:
                    continue
                previous = max(
                    (item for item in location_records.values() if item.alias == alias),
                    key=lambda item: item.generation,
                    default=None,
                )
                band = LocationBand(level - atr * 0.06, level + atr * 0.06)
                if previous is None or previous.price != level or previous.band != band:
                    generation = location_generations.get(alias, 0) + 1
                    location_generations[alias] = generation
                    location_id = stable_orderflow_id(
                        "location", trading_date, stream_identity, alias, generation,
                    )
                    location_records[location_id] = LocationRecord(
                        location_id, alias, level, band, generation, end, parent_id,
                        timeframe="5m", source_identity=stream_identity,
                        replaced_location_id=previous.location_id if previous else None,
                        location_type="session_reference",
                        source_observation_id=parent_id,
                        frozen_atr=atr,
                        session_id=trading_date,
                        calculation_version="orderflow_daily_atr_v1",
                    )
            location_observations: list[LocationObservation] = []
            latest_records = {
                alias: max(
                    (
                        item for item in location_records.values()
                        if item.alias == alias and item.timeframe == "5m"
                        and (item.alias != "VWAP" or item.source_observation_id is not None)
                        and item.available_at <= end
                    ),
                    key=lambda item: item.generation,
                )
                for alias in {
                    item.alias for item in location_records.values()
                    if item.timeframe == "5m"
                    and (item.alias != "VWAP" or item.source_observation_id is not None)
                    and item.available_at <= end
                }
            }
            for record in latest_records.values():
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
                    # Canonical location observations are observed at the
                    # completed source candle, not at its availability time.
                    observed_at=children[-1].observed_at,
                    available_at=end,
                    observation_id=stable_orderflow_id(
                        "location_observation", parent_id, record.location_id,
                    ),
                    location_type=record.location_type,
                    timeframe="5m",
                    child_observation_ids=child_ids,
                    method="causal_producer",
                    reference_source_aggregate_id=record.source_aggregate_id,
                    reference_source_timeframe=record.timeframe or "5m",
                    frozen_atr=record.frozen_atr,
                ))
            # Publish the same structural reference against each complete 1m
            # child that backs this 5m parent.  The child observations are
            # producer facts; the policy must not synthesize them later.
            child_indexes = {
                item.observation_id: index
                for index, item in enumerate(prefix)
                if item.observation_id in child_ids
            }
            for record in latest_records.values():
                child_locations: list[tuple[str, LocationObservation]] = []
                for child in children:
                    high = _numeric_feature(child.features, "high")
                    low = _numeric_feature(child.features, "low")
                    close = _numeric_feature(child.features, "close")
                    open_price = _numeric_feature(child.features, "open")
                    child_locations.append((
                        child.observation_id,
                        LocationObservation(
                            record.location_id, record.alias, record.generation, record.price,
                            record.band.lower, record.band.upper, open_price, high, low, close,
                            record.band.lower <= high and low <= record.band.upper, child.observation_id,
                            child.observed_at, max(child.available_at, record.available_at),
                            stable_orderflow_id(
                                "location_observation", child.observation_id, record.location_id,
                            ),
                            location_type=record.location_type,
                            source_observation_id=record.source_observation_id,
                            reference_source_aggregate_id=record.source_aggregate_id,
                            reference_source_timeframe=record.timeframe or "5m",
                            frozen_atr=record.frozen_atr,
                            timeframe="1m",
                        ),
                    ))
                for child_id, location in child_locations:
                    index = child_indexes.get(child_id)
                    if index is None:
                        continue
                    child_observation = prefix[index]
                    if any(
                        item.observation_id == location.observation_id
                        for item in child_observation.location_observations
                    ):
                        continue
                    prefix[index] = replace(
                        child_observation,
                        location_observations=child_observation.location_observations + (location,),
                    )
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
            processed_windows.add(window_index)
        observations = (*prefix, *five_minute)
        self._context_five_minute = tuple(five_minute)
        self._context_processed_windows = processed_windows
        self._context_location_records = location_records
        self._context_location_generations = location_generations
        cutoff = decision_cutoff
        return OrderflowContext(
            trading_date, cutoff, observations,
            {"1m": tuple(prefix), "5m": tuple(five_minute)},
            tuple(self._source_issues),
        )

__all__ = ["CausalOrderflowProducer", "source_row_is_complete"]
