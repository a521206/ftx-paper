"""Build and identify persisted session daily profiles."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

from ftx_paper.strategies.orderflow.canonical.ict.bars.aggregate import SessionSpec
from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.accumulator import ProfileAccumulator
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.config import ValueProfileConfig
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import DailyProfileQuality, ProfileScope, SnapshotStatus
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import DailyProfileRecord, ValueProfileSnapshot
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.temporal import validate_profile_stream


def aggregate_source_fingerprint(bars: Sequence[CompletedBar]) -> str:
    """Stable digest over the contributing bar identities."""
    payload = "|".join(bar.source_fingerprint for bar in bars)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def daily_config_fingerprint(config: ValueProfileConfig, spec: SessionSpec) -> str:
    """Identify profile rules together with the session window that scopes them."""
    payload = (
        config.fingerprint(),
        spec.timezone,
        spec.start.isoformat(),
        spec.end.isoformat(),
    )
    return hashlib.sha256("|".join(payload).encode("utf-8")).hexdigest()


def daily_snapshot_id(
    *,
    session_date: str,
    config_fingerprint: str,
    aggregate_source_fingerprint: str,
) -> str:
    digest = hashlib.sha256(
        f"{session_date}:{config_fingerprint}:{aggregate_source_fingerprint}".encode("utf-8")
    ).hexdigest()
    return f"vp:daily:{session_date}:{digest[:24]}"


def _session_close_timestamp(session_date: str, spec: SessionSpec) -> datetime:
    zone = ZoneInfo(spec.timezone)
    return datetime.combine(date.fromisoformat(session_date), spec.end, tzinfo=zone)


def expected_session_bar_count(timeframe_minutes: int, spec: SessionSpec = SessionSpec()) -> int:
    start_minutes = spec.start.hour * 60 + spec.start.minute
    end_minutes = spec.end.hour * 60 + spec.end.minute
    session_minutes = end_minutes - start_minutes
    if session_minutes <= 0 or session_minutes % timeframe_minutes != 0:
        raise ValueError("session length must be a positive multiple of the bar timeframe")
    return session_minutes // timeframe_minutes


def session_scope_closed(last_bar: CompletedBar, spec: SessionSpec = SessionSpec()) -> bool:
    """True when the last received bar reaches the configured session end."""
    close_ts = _session_close_timestamp(last_bar.session_date, spec)
    return last_bar.end_timestamp >= close_ts


def _session_gap_count(bars: Sequence[CompletedBar]) -> int:
    if len(bars) < 2:
        return 0
    session_date = bars[0].session_date
    validation = validate_profile_stream(bars)
    count = 0
    for gap in validation.gaps:
        for index in range(1, len(bars)):
            if bars[index].timestamp != gap.next_timestamp:
                continue
            if bars[index - 1].session_date == session_date and bars[index].session_date == session_date:
                count += 1
            break
    return count


def build_daily_profile_record(
    bars: Sequence[CompletedBar],
    *,
    config: ValueProfileConfig,
    session_spec: SessionSpec = SessionSpec(),
) -> DailyProfileRecord | None:
    """Materialize one daily profile when the session has closed; otherwise return None."""
    if not bars:
        return None
    session_date = bars[0].session_date
    if any(bar.session_date != session_date for bar in bars):
        raise ValueError("daily profile build requires a single session_date")
    validation = validate_profile_stream(bars)
    session_start = datetime.combine(
        date.fromisoformat(session_date),
        session_spec.start,
        tzinfo=ZoneInfo(session_spec.timezone),
    )
    session_close = _session_close_timestamp(session_date, session_spec)
    last_bar = bars[-1]
    if not session_scope_closed(last_bar, session_spec):
        return None

    acc = ProfileAccumulator(
        value_area_fraction=config.value_area_fraction,
        tick_size=config.tick_size,
    )
    for bar in bars:
        acc.add_bar(high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)
    levels = acc.levels()
    if levels is None:
        return None

    gap_count = len(validation.gaps)
    expected = expected_session_bar_count(bars[0].timeframe_minutes, session_spec)
    expected_timestamps = tuple(
        session_start + timedelta(minutes=index * bars[0].timeframe_minutes)
        for index in range(expected)
    )
    quality = (
        DailyProfileQuality.COMPLETE
        if (
            gap_count == 0
            and len(bars) == expected
            and tuple(bar.timestamp for bar in bars) == expected_timestamps
            and last_bar.end_timestamp == session_close
        )
        else DailyProfileQuality.INCOMPLETE
    )
    config_fingerprint = daily_config_fingerprint(config, session_spec)
    source_fp = aggregate_source_fingerprint(bars)
    close_ts = _session_close_timestamp(session_date, session_spec)
    available_at = max(last_bar.available_at, close_ts)
    return DailyProfileRecord(
        snapshot_id=daily_snapshot_id(
            session_date=session_date,
            config_fingerprint=config_fingerprint,
            aggregate_source_fingerprint=source_fp,
        ),
        instrument=last_bar.instrument,
        timeframe_minutes=last_bar.timeframe_minutes,
        session_date=session_date,
        poc=levels.poc,
        vah=levels.vah,
        val=levels.val,
        vwap=levels.vwap,
        window_first_bar_ts=bars[0].timestamp,
        window_last_bar_ts=last_bar.timestamp,
        bar_count=len(bars),
        total_volume=levels.total_volume,
        gap_count=gap_count,
        quality=quality,
        observed_at=last_bar.timestamp,
        available_at=available_at,
        config_fingerprint=config_fingerprint,
        profile_version=config.profile_version,
        aggregate_source_fingerprint=source_fp,
    )

def prior_session_snapshot_from_daily(
    record: DailyProfileRecord,
    *,
    available_at: datetime,
) -> ValueProfileSnapshot:
    """Published prior-session geometry for the next session (no histogram copy)."""
    return ValueProfileSnapshot(
        snapshot_id=record.snapshot_id,
        scope=ProfileScope.PRIOR_SESSION,
        status=SnapshotStatus.FROZEN,
        instrument=record.instrument,
        timeframe_minutes=record.timeframe_minutes,
        session_date=record.session_date,
        window_first_bar_ts=record.window_first_bar_ts,
        window_last_bar_ts=record.window_last_bar_ts,
        poc=record.poc,
        vah=record.vah,
        val=record.val,
        vwap=record.vwap,
        total_volume=record.total_volume,
        observed_at=record.observed_at,
        available_at=available_at,
        config_fingerprint=record.config_fingerprint,
        profile_version=record.profile_version,
        source_bar_fingerprints=(),
    )

