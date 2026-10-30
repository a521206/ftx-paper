"""Validation and session-clock-aligned completed-bar construction."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Iterable
from zoneinfo import ZoneInfo

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar, SourceBar
from ftx_paper.strategies.orderflow.canonical.ict.domain.time import require_aware


@dataclass(frozen=True, slots=True)
class SessionSpec:
    timezone: str = "Asia/Kolkata"
    start: time = time(9, 15)
    end: time = time(15, 30)

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError("session end must be after session start")
        try:
            ZoneInfo(self.timezone)
        except KeyError as exc:
            raise ValueError(f"unknown session timezone: {self.timezone}") from exc


def _validate_bar(bar: SourceBar) -> None:
    if not bar.instrument or not bar.source_id or not bar.session_date:
        raise ValueError("source bars require instrument, source_id, and session_date")
    require_aware(bar.timestamp, name="source_bar.timestamp")
    require_aware(bar.available_at, name="source_bar.available_at")
    if bar.available_at < bar.timestamp:
        raise ValueError("source bar cannot be available before its timestamp")
    prices = (bar.open, bar.high, bar.low, bar.close, bar.volume)
    if not all(math.isfinite(value) for value in prices):
        raise ValueError("source bar prices and volume must be finite")
    if bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close):
        raise ValueError("source bar OHLC values are inconsistent")
    if bar.high < bar.low or bar.volume < 0:
        raise ValueError("source bar range and volume must be non-negative")
    if bar.timestamp.second or bar.timestamp.microsecond:
        raise ValueError("source bars must be minute aligned")


def _fingerprint(
    bars: list[SourceBar], *, session_timezone: str, timeframe_minutes: int, session: SessionSpec
) -> str:
    payload = "|".join(
        f"{bar.source_id}:{bar.timestamp.isoformat()}:{bar.available_at.isoformat()}:{bar.open}:{bar.high}:"
        f"{bar.low}:{bar.close}:{bar.volume}:{bar.session_date}:{bar.provenance}:{session_timezone}:"
        f"{timeframe_minutes}:{session.start.isoformat()}:{session.end.isoformat()}"
        for bar in bars
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_completed_bars(
    bars: Iterable[SourceBar],
    *,
    timeframe_minutes: int,
    session: SessionSpec | None = None,
) -> list[CompletedBar]:
    """Build complete aggregates; reject in-session source gaps and omit partial edges."""
    if timeframe_minutes < 1:
        raise ValueError("timeframe_minutes must be positive")
    session = session or SessionSpec()
    session_zone = ZoneInfo(session.timezone)
    source = sorted(bars, key=lambda bar: (bar.timestamp, bar.instrument, bar.source_id))
    for bar in source:
        _validate_bar(bar)

    groups: dict[tuple[str, str, datetime], list[SourceBar]] = {}
    previous_by_session: dict[tuple[str, str], datetime] = {}
    for bar in source:
        local_timestamp = bar.timestamp.astimezone(session_zone)
        if bar.session_date != local_timestamp.date().isoformat():
            raise ValueError("source bar session_date does not match session timezone")
        session_start = datetime.combine(local_timestamp.date(), session.start, tzinfo=session_zone)
        session_end = datetime.combine(local_timestamp.date(), session.end, tzinfo=session_zone)
        if local_timestamp < session_start or local_timestamp >= session_end:
            continue
        session_key = (bar.instrument, bar.session_date)
        previous_timestamp = previous_by_session.get(session_key)
        expected_timestamp = (
            previous_timestamp + timedelta(minutes=1)
            if previous_timestamp is not None else None
        )
        if expected_timestamp is not None and local_timestamp != expected_timestamp:
            raise ValueError(
                f"discontinuous source bars for {bar.instrument} on {bar.session_date}: "
                f"expected {expected_timestamp.isoformat()}, "
                f"got {local_timestamp.isoformat()}"
            )
        previous_by_session[session_key] = local_timestamp
        offset = int((local_timestamp - session_start).total_seconds() // 60)
        start = session_start + timedelta(minutes=(offset // timeframe_minutes) * timeframe_minutes)
        groups.setdefault((bar.instrument, local_timestamp.date().isoformat(), start), []).append(bar)

    result: list[CompletedBar] = []
    for (instrument, session_date, start), items in sorted(groups.items(), key=lambda item: item[0]):
        session_end = datetime.combine(start.date(), session.end, tzinfo=session_zone)
        if start + timedelta(minutes=timeframe_minutes) > session_end:
            continue
        expected = [start + timedelta(minutes=index) for index in range(timeframe_minutes)]
        timestamps = [bar.timestamp.astimezone(session_zone) for bar in items]
        if timestamps != expected:
            continue
        if len({bar.source_id for bar in items}) != len(items):
            raise ValueError(
                f"duplicate source bar in {timeframe_minutes}m aggregate for {instrument} "
                f"on {session_date} at {start.isoformat()}"
            )
        end = start + timedelta(minutes=timeframe_minutes)
        result.append(
            CompletedBar(
                instrument=instrument,
                timeframe_minutes=timeframe_minutes,
                timestamp=start,
                end_timestamp=end,
                available_at=max(bar.available_at for bar in items),
                open=items[0].open,
                high=max(bar.high for bar in items),
                low=min(bar.low for bar in items),
                close=items[-1].close,
                volume=sum(bar.volume for bar in items),
                source_ids=tuple(bar.source_id for bar in items),
                source_provenance=tuple(bar.provenance for bar in items),
                source_fingerprint=_fingerprint(
                    items,
                    session_timezone=session.timezone,
                    timeframe_minutes=timeframe_minutes,
                    session=session,
                ),
                session_date=session_date,
            )
        )
    return result
