"""Canonical source-bar aggregation primitives for orderflow Layer 1."""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Iterable, Mapping

SESSION_START = 9 * 60 + 15
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")


def canonical_timestamp(value: str) -> str:
    value = str(value).strip()
    if len(value) != 16 or not _TIMESTAMP.fullmatch(value):
        raise ValueError(f"invalid canonical timestamp: {value!r}")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M")
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value!r}") from exc
    return value


@dataclass(frozen=True, slots=True)
class Aggregate:
    time: str
    end_time: str
    available_at: str
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
            object.__setattr__(self, name, canonical_timestamp(getattr(self, name)))

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in (
            "time", "end_time", "available_at", "timeframe", "source_bar_count",
            "open", "high", "low", "close", "volume", "symbol", "expiry",
            "contract_type", "session_date", "source_fingerprint",
        )}


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
        timestamp = datetime.strptime(f"{session_date}T{str(row['time'])[:5]}", "%Y-%m-%dT%H:%M")
        offset = timestamp.hour * 60 + timestamp.minute - SESSION_START
        if offset >= 0:
            start = SESSION_START + (offset // timeframe) * timeframe
            groups.setdefault((session_date, start), []).append(row)
    aggregates: list[Aggregate] = []
    issues: list[ObservationIssue] = []
    for session_date, start in sorted(groups):
        items = groups[(session_date, start)]
        expected = [_clock(start + index) for index in range(timeframe)]
        observed = [str(item["time"])[:5] for item in items]
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
        raw = "|".join(f"{item['time']}:{item['open']}:{item['high']}:{item['low']}:{item['close']}:{item['volume']}" for item in items)
        aggregates.append(Aggregate(f"{session_date}T{_clock(start)}",
            f"{session_date}T{_clock(start + timeframe - 1)}", f"{session_date}T{_clock(start + timeframe)}",
            f"{timeframe}m", len(items), float(first["open"]), max(float(item["high"]) for item in items),
            min(float(item["low"]) for item in items), float(last["close"]), sum(int(item["volume"]) for item in items),
            str(first.get("symbol", "NIFTY")), str(first.get("expiry", "")), str(first.get("contract_type", "FUT")),
            session_date, hashlib.sha256(raw.encode()).hexdigest()))
    return AggregateBuildResult(tuple(aggregates), tuple(issues))
