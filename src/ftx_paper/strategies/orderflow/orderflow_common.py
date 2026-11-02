"""Canonical source-bar aggregation primitives for orderflow Layer 1."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Iterable, Mapping
from .timestamps import coerce_timestamp, nifty_label, parse_nifty_label, parse_utc, require_utc, serialize_utc, session_minute

SESSION_START = 9 * 60 + 15
parse_timestamp = parse_utc
serialize_timestamp = serialize_utc


def canonical_timestamp(value: str | datetime) -> datetime:
    """Normalize a causal value, accepting only explicit UTC or ingress labels."""
    if isinstance(value, datetime):
        return require_utc(value)
    text = str(value).strip()
    if text.endswith("Z") or "+" in text[10:] or "-" in text[10:]:
        return parse_utc(text)
    return parse_nifty_label(text)


def candle_reference(candle: "Aggregate") -> dict[str, str]:
    """Return the repository-neutral identity of a completed candle."""
    return {
        "session_date": candle.session_date,
        "timeframe": candle.timeframe,
        "start_at": serialize_utc(candle.time),
        "end_at": serialize_utc(candle.end_time),
    }


@dataclass(frozen=True, slots=True)
class Aggregate:
    time: datetime
    end_time: datetime
    available_at: datetime
    timeframe: str
    source_bar_count: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    symbol: str
    expiry: str
    contract_type: str
    session_date: str
    source_fingerprint: str

    def __post_init__(self) -> None:
        for name in ("time", "end_time", "available_at"):
            object.__setattr__(self, name, coerce_timestamp(getattr(self, name), field=f"aggregate.{name}"))
        if self.available_at < self.end_time:
            raise ValueError("aggregate availability cannot precede completion")

    def to_dict(self) -> dict[str, Any]:
        result = {name: getattr(self, name) for name in (
            "timeframe", "source_bar_count",
            "open", "high", "low", "close", "volume", "symbol", "expiry",
            "contract_type", "session_date", "source_fingerprint",
        )}
        result.update({
            "time": serialize_utc(self.time),
            "end_time": serialize_utc(self.end_time),
            "available_at": serialize_utc(self.available_at),
            "time_label": nifty_label(self.time),
            "end_time_label": nifty_label(self.end_time),
        })
        return result


class ObservationIssueKind(StrEnum):
    MISSING_SOURCE_BAR = "missing_source_bar"
    INCOMPLETE_AGGREGATE = "incomplete_aggregate"
    MISSING_VOLUME = "missing_volume"


@dataclass(frozen=True, slots=True)
class ObservationIssue:
    kind: ObservationIssueKind
    session_date: str
    timeframe: int | None
    group_start: str | None
    group_end: str | None
    details: str


@dataclass(frozen=True, slots=True)
class AggregateBuildResult:
    aggregates: tuple[Aggregate, ...]
    issues: tuple[ObservationIssue, ...]


def _clock(value: int) -> str:
    return f"{value // 60:02d}:{value % 60:02d}"


def _usable_volume(row: Mapping[str, Any]) -> bool:
    try:
        return row.get("volume") is not None and math.isfinite(float(row["volume"]))
    except (TypeError, ValueError):
        return False


def build_aggregate_result(rows: Iterable[Mapping[str, Any]], timeframe: int) -> AggregateBuildResult:
    if timeframe not in (1, 3, 5, 15):
        raise ValueError("timeframe must be 1, 3, 5, or 15 minutes")
    groups: dict[tuple[str, int], list[Mapping[str, Any]]] = {}
    for row in rows:
        session_date = str(row["session_date"])
        label = session_minute(session_date, row["time"])
        local_hour, local_minute = (int(part) for part in label[-5:].split(":"))
        offset = local_hour * 60 + local_minute - SESSION_START
        if offset >= 0:
            start = SESSION_START + (offset // timeframe) * timeframe
            groups.setdefault((session_date, start), []).append(row)
    aggregates: list[Aggregate] = []
    issues: list[ObservationIssue] = []
    for session_date, start in sorted(groups):
        items = groups[(session_date, start)]
        expected = [_clock(start + index) for index in range(timeframe)]
        observed = [session_minute(session_date, item["time"])[-5:] for item in items]
        missing_volume = any(not _usable_volume(item) for item in items)
        if missing_volume:
            issues.append(ObservationIssue(ObservationIssueKind.MISSING_VOLUME, session_date, timeframe,
                _clock(start), _clock(start + timeframe - 1),
                "source row lacks a usable volume; aggregate suppressed"))
        incomplete = len(items) != timeframe or observed != expected
        if incomplete:
            kind = ObservationIssueKind.INCOMPLETE_AGGREGATE
            details = "incomplete session-aligned aggregate"
            if len(items) < timeframe and observed != expected[:len(observed)]:
                kind = ObservationIssueKind.MISSING_SOURCE_BAR
                details = "source bar gap prevents aggregate completion"
            issues.append(ObservationIssue(kind, session_date, timeframe, _clock(start),
                _clock(start + timeframe - 1), details))
        if missing_volume or incomplete:
            continue
        first, last = items[0], items[-1]
        raw = "|".join(
            f"{session_minute(session_date, item['time'])[-5:]}:{item['open']}:{item['high']}:{item['low']}:{item['close']}:{item['volume']}"
            for item in items
        )
        aggregates.append(Aggregate(parse_nifty_label(f"{session_date}T{_clock(start)}"),
            parse_nifty_label(f"{session_date}T{_clock(start + timeframe - 1)}"), parse_nifty_label(f"{session_date}T{_clock(start + timeframe)}"),
            f"{timeframe}m", len(items), float(first["open"]), max(float(item["high"]) for item in items),
            min(float(item["low"]) for item in items), float(last["close"]), sum(int(item["volume"]) for item in items),
            str(first.get("symbol", "NIFTY")), str(first.get("expiry", "")), str(first.get("contract_type", "FUT")),
            session_date, hashlib.sha256(raw.encode()).hexdigest()))
    return AggregateBuildResult(tuple(aggregates), tuple(issues))
