"""Native causal VWAP observation producer for completed market bars."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import hashlib

from ftx_paper.contracts import MarketBar, market_minute_key
from ftx_paper.config import IST

from .orderflow_contract import OrderflowContext, OrderflowObservation, OrderflowObservationContext


def _numeric_feature(features: Mapping[str, object], name: str) -> float:
    value = features.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"orderflow child feature {name!r} must be numeric")
    return float(value)


class CausalOrderflowProducer:
    _PRODUCER_VERSION = "causal_orderflow_v2"

    def __init__(self) -> None:
        self._bars: dict[tuple[str, str], list[MarketBar]] = defaultdict(list)
        self._active_date: str | None = None

    def ingest(self, bar: MarketBar) -> None:
        date, _ = market_minute_key(bar.timestamp)
        if self._active_date is not None and date != self._active_date:
            self._bars.clear()
        self._active_date = date
        key = (date, bar.instrument.symbol)
        bars = self._bars[key]
        if not bars or bars[-1].timestamp < bar.timestamp:
            bars.append(bar)

    def context(self, trading_date: str, minute: str) -> OrderflowContext | None:
        if self._active_date != trading_date:
            return None
        decision_cutoff = datetime.fromisoformat(
            f"{trading_date}T{minute}:00"
        ).replace(tzinfo=IST).astimezone(timezone.utc) + timedelta(minutes=1)
        candidates = [
            bar for (date, _), bars in self._bars.items()
            if date == trading_date
            for bar in bars
            if market_minute_key(bar.timestamp)[1] <= minute
            and bar.timestamp + timedelta(minutes=1) <= decision_cutoff
        ]
        if not candidates:
            return None
        bars = sorted(candidates, key=lambda item: item.timestamp)
        instrument = bars[-1].instrument.symbol
        bars = [bar for bar in bars if bar.instrument.symbol == instrument]
        prefix: list[OrderflowObservation] = []
        cumulative_value = 0.0
        cumulative_volume = 0.0
        for bar_index, bar in enumerate(bars):
            volume = float(bar.volume or 0.0)
            cumulative_value += ((bar.high + bar.low + bar.close) / 3.0) * volume
            cumulative_volume += volume
            available_at = bar.timestamp + timedelta(minutes=1)
            prefix.append(OrderflowObservation(
                observation_id=f"{instrument}:1m:{bar.timestamp.isoformat()}",
                source_observation_id=f"{instrument}:bar:{bar.timestamp.isoformat()}",
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
                    "previous_close": bars[bar_index - 1].close if bar_index else None,
                },
                context=OrderflowObservationContext(
                    session_id=trading_date, completeness="complete", availability_status="available",
                    replay_provenance={
                        "producer": "ftx-paper",
                        "producer_version": self._PRODUCER_VERSION,
                        "input_availability": "complete",
                        "gap_status": "none",
                    }, generation=1,
                    causal_sequence=1,
                ),
            ))
        five_minute: list[OrderflowObservation] = []
        for index in range(4, len(prefix), 5):
            children = tuple(prefix[index - 4:index + 1])
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
            parent_id = f"{instrument}:5m:{start.isoformat()}"
            child_ids = tuple(item.observation_id for item in children)
            source_fingerprint = hashlib.sha256(
                "|".join(child_ids).encode("utf-8")
            ).hexdigest()
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
                        "input_availability": "complete",
                        "gap_status": "none",
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
