"""Canonical feature input preparation shared by replay and live publication."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
from pathlib import Path
from typing import Any

from ftx_paper.runtime import RuntimeStore
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.strategies.orderflow import observe_market
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.timestamps import (
    coerce_timestamp,
    nifty_label,
    session_minute,
)


@dataclass(frozen=True, slots=True)
class _AvailableAggregate:
    available_at: datetime
    timeframe: str
    index: int
    value: dict[str, Any]


def _aggregate(value: dict[str, Any], timeframe: str) -> Aggregate:
    return Aggregate(
        coerce_timestamp(str(value["time"])),
        coerce_timestamp(str(value["end_time"])),
        coerce_timestamp(str(value["available_at"])),
        timeframe,
        int(value["source_bar_count"]),
        float(value["open"]),
        float(value["high"]),
        float(value["low"]),
        float(value["close"]),
        int(value["volume"]),
        str(value["symbol"]),
        str(value.get("expiry", "")),
        str(value["session_date"]),
        str(value["source_fingerprint"]),
    )


def _normalize_rows(source: Any, session_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in source.bars:
        required = ("minute", "open", "high", "low", "close", "volume", "open_interest")
        if any(field not in row or row[field] is None for field in required):
            raise ValueError(f"source row is incomplete for {session_date}")
        try:
            minute = coerce_timestamp(str(row["minute"]), field="source minute")
        except ValueError as exc:
            raise ValueError(f"source row has an invalid minute for {session_date}") from exc
        numeric = [row[field] for field in ("open", "high", "low", "close", "volume", "open_interest")]
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            for value in numeric
        ):
            raise ValueError(f"source row has invalid numeric values for {session_date}")
        rows.append({
            "time": nifty_label(minute),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
            "oi": int(row["open_interest"]),
            "symbol": "NIFTY",
            "expiry": "",
            "session_date": session_date,
        })
    return rows


def _load_session_rows(
    session_date: str, runtime_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, tuple[dict[str, Any], ...]]]:
    store = RuntimeStore.open_read_only(runtime_dir)
    resolver = FuturesSessionSourceResolver(store)
    dates = sorted(
        str(value) for value in store.read_dates()
        if date.fromisoformat(str(value)).isoformat() <= session_date
    )
    rows_by_date: dict[str, tuple[dict[str, Any], ...]] = {}
    for value in dates:
        source = resolver.resolve(value)
        if source is not None:
            rows_by_date[value] = tuple(_normalize_rows(source, value))
    rows = rows_by_date.get(session_date)
    if not rows:
        raise ValueError(f"continuous futures source is unavailable for {session_date}")
    return list(rows), rows_by_date


def _aggregate_events(observation: Any) -> tuple[_AvailableAggregate, ...]:
    events: list[_AvailableAggregate] = []
    for timeframe in ("1m", "5m"):
        for index, aggregate in enumerate(observation.aggregates[timeframe]):
            if aggregate.available_at < aggregate.end_time:
                raise ValueError("aggregate is available before completion")
            expected_count = 1 if timeframe == "1m" else 5
            if aggregate.source_bar_count != expected_count:
                raise ValueError(f"incomplete {timeframe} aggregate at {aggregate.time}")
            events.append(_AvailableAggregate(
                aggregate.available_at,
                timeframe,
                index,
                aggregate.to_dict(),
            ))
    return tuple(sorted(events, key=lambda item: (item.available_at, item.timeframe, item.index)))


def _observation_input_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project replay input timestamps into the canonical source shape."""
    return [
        {**row, "time": session_minute(str(row["session_date"]), row["time"])}
        for row in rows
    ]


def build_observation_artifact(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], tuple[_AvailableAggregate, ...]]:
    observation = observe_market(_observation_input_rows(rows))
    return (
        {
            "schema_version": 1,
            "artifact_type": "canonical_p2_market_observation",
            "source_fingerprint": observation.source_fingerprint,
            "aggregates": {
                timeframe: [item.to_dict() for item in aggregates]
                for timeframe, aggregates in observation.aggregates.items()
            },
        },
        _aggregate_events(observation),
    )


__all__ = [
    "_aggregate",
    "_load_session_rows",
    "build_observation_artifact",
]
