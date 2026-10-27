"""Native causal VWAP observation producer for completed market bars."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone, tzinfo
import hashlib
import json

from ftx_paper.contracts import MarketBar
from ftx_paper.config import IST

from .orderflow_contract import (
    OrderflowContext, OrderflowObservation, OrderflowObservationContext,
    stable_orderflow_id,
)


def _numeric_feature(features: Mapping[str, object], name: str) -> float:
    value = features.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"orderflow child feature {name!r} must be numeric")
    return float(value)


def _feature_fingerprint(features: Mapping[str, object]) -> str:
    payload = json.dumps(dict(features), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _source_row_fingerprint(bars: list[MarketBar]) -> str:
    """Hash the canonical source rows, never derived child fingerprints."""
    payload = [{
        "identity": _source_identity(bar),
        "timestamp": bar.timestamp.isoformat(),
        "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
        "volume": bar.volume, "open_interest": bar.open_interest,
    } for bar in bars]
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


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
        self._session_timezone = session_timezone
        self._session_start = datetime.strptime(session_start, "%H:%M").time()
        self._session_end = datetime.strptime(session_end, "%H:%M").time()

    def ingest(self, bar: MarketBar) -> None:
        local = bar.timestamp.astimezone(self._session_timezone)
        date = local.date().isoformat()
        local_time = local.time().replace(second=0, microsecond=0)
        if local_time < self._session_start or local_time >= self._session_end:
            return
        if self._active_date is not None and date != self._active_date:
            self._bars.clear()
            self._last_timestamps.clear()
        timestamp = bar.timestamp.astimezone(timezone.utc)
        self._active_date = date
        identity = _source_identity(bar)
        key = (date, identity)
        bars = self._bars[key]
        last_timestamp = self._last_timestamps.get(identity)
        if last_timestamp == timestamp:
            raise ValueError("duplicate orderflow source bar")
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
            and bar.timestamp.astimezone(self._session_timezone).strftime("%H:%M") <= minute
            and bar.timestamp + timedelta(minutes=1) <= decision_cutoff
        ]
        if not candidates:
            return None
        bars = sorted(candidates, key=lambda item: item.timestamp)
        stream_identity = "|".join((
            bars[-1].instrument.symbol, bars[-1].instrument.exchange,
            bars[-1].instrument.instrument_type, bars[-1].instrument.expiry or "",
        ))
        instrument = bars[-1].instrument.symbol
        bars = [bar for bar in bars if "|".join((
            bar.instrument.symbol, bar.instrument.exchange,
            bar.instrument.instrument_type, bar.instrument.expiry or "",
        )) == stream_identity]
        prefix: list[OrderflowObservation] = []
        cumulative_value = 0.0
        cumulative_volume = 0.0
        for bar_index, bar in enumerate(bars):
            bar_has_gap = bool(
                bar_index
                and bar.timestamp - bars[bar_index - 1].timestamp > timedelta(minutes=1)
            )
            if bar_has_gap:
                cumulative_value = 0.0
                cumulative_volume = 0.0
            volume = float(bar.volume or 0.0)
            cumulative_value += ((bar.high + bar.low + bar.close) / 3.0) * volume
            cumulative_volume += volume
            available_at = bar.timestamp + timedelta(minutes=1)
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
                    "vwap": cumulative_value / cumulative_volume if cumulative_volume else None,
                    "volume": volume,
                    "previous_close": bars[bar_index - 1].close if bar_index and not bar_has_gap else None,
                },
                context=OrderflowObservationContext(
                    session_id=trading_date, completeness="complete", availability_status="available",
                    replay_provenance={
                        "producer": "ftx-paper",
                        "producer_version": self._PRODUCER_VERSION,
                        "input_availability": "gap" if bar_has_gap else "complete",
                        "gap_status": "source_gap" if bar_has_gap else "none",
                        "source_fingerprint": _feature_fingerprint({
                            "timestamp": bar.timestamp.isoformat(), "open": bar.open,
                            "high": bar.high, "low": bar.low, "close": bar.close,
                            "volume": bar.volume, "open_interest": bar.open_interest,
                        }),
                    }, generation=1,
                    causal_sequence=1,
                ),
            ))
        five_minute: list[OrderflowObservation] = []
        session_start = datetime.combine(
            datetime.fromisoformat(trading_date).date(), self._session_start,
        ).replace(
            tzinfo=self._session_timezone
        ).astimezone(timezone.utc)
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
                and item.context.replay_provenance.get("gap_status") != "none"
                for item in children
            )
            parent_id = stable_orderflow_id("aggregate", trading_date, stream_identity, "5m", start.isoformat())
            child_ids = tuple(item.observation_id for item in children)
            child_timestamps = {item.observed_at for item in children}
            source_rows = [
                bar for bar in bars
                if bar.timestamp in child_timestamps and _source_identity(bar) == stream_identity
            ]
            source_fingerprint = _source_row_fingerprint(source_rows)
            child_features = [item.features for item in children]
            total_volume = sum(_numeric_feature(item, "volume") for item in child_features)
            parent_features = {
                "open": _numeric_feature(child_features[0], "open"),
                "high": max(_numeric_feature(item, "high") for item in child_features),
                "low": min(_numeric_feature(item, "low") for item in child_features),
                "close": _numeric_feature(child_features[-1], "close"),
                "previous_close": child_features[0].get("previous_close"),
                "vwap": child_features[-1].get("vwap"),
                "volume": total_volume,
            }
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
                        "source_fingerprint": source_fingerprint,
                    }, generation=1,
                    child_observation_ids=child_ids,
                    child_intervals=child_intervals,
                    causal_sequence=1,
                ),
            ))
        observations = (*prefix, *five_minute)
        cutoff = decision_cutoff
        return OrderflowContext(
            trading_date, cutoff, observations,
            {"1m": tuple(prefix), "5m": tuple(five_minute)},
        )


__all__ = ["CausalOrderflowProducer"]
