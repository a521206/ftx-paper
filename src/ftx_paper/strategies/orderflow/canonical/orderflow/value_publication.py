"""Causal publication of value-profile references for order-flow replay."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from ftx_paper.strategies.orderflow.canonical.ict.bars import SourceBar, build_completed_bars
from ftx_paper.strategies.orderflow.canonical.ict.features import MeasurementConfig
from ftx_paper.strategies.orderflow.canonical.ict.replay.reference_streams import (
    build_value_profiles_by_timeframe,
    value_references_from_snapshots,
)
from ftx_paper.strategies.orderflow.canonical.ict.value_profile import (
    DailyProfileQuality,
    ProfileScope,
    ValueProfileConfig,
    build_daily_profile_record,
)
from ftx_paper.strategies.orderflow.canonical.replay.value_references import ValueAreaReference


_SESSION_ZONE = ZoneInfo("Asia/Kolkata")


def _profile_source_bars(*sessions: list[dict[str, Any]]) -> tuple[SourceBar, ...]:
    source_bars: list[SourceBar] = []
    for rows in sessions:
        for row in rows:
            session_date = str(row["session_date"])
            timestamp = datetime.strptime(
                f"{session_date} {str(row['time'])[:5]}", "%Y-%m-%d %H:%M",
            ).replace(tzinfo=_SESSION_ZONE)
            symbol = str(row.get("symbol", "NIFTY"))
            source_bars.append(SourceBar(
                instrument=symbol,
                timestamp=timestamp,
                available_at=timestamp + timedelta(minutes=1),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
                source_id=f"{session_date}:{timestamp:%H:%M}:{symbol}",
                session_date=session_date,
                provenance="futures_repo.continuous",
            ))
    return tuple(source_bars)


def prior_profile_is_complete(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    bars = build_completed_bars(_profile_source_bars(rows), timeframe_minutes=1)
    record = build_daily_profile_record(bars, config=ValueProfileConfig())
    return record is not None and record.quality is DailyProfileQuality.COMPLETE


def publish_value_references(
    prior_rows: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    session_date: str,
    *,
    prior_profile_complete: bool,
) -> tuple[ValueAreaReference, ...]:
    """Calculate profile facts, then publish eligible references at session open."""
    source_bars = _profile_source_bars(prior_rows, rows)
    profiles = build_value_profiles_by_timeframe(
        source_bars,
        (1,),
        profile_config=ValueProfileConfig(),
        measurement_config=MeasurementConfig(),
        active_session_date=session_date,
    )
    profile = profiles.get(1)
    if profile is None:
        return ()
    refs = value_references_from_snapshots(
        tuple(
            snapshot for snapshot in profile.snapshots
            if (
                snapshot.scope is ProfileScope.PRIOR_SESSION
                and prior_profile_complete
            )
        ),
        tuple(
            observation for observation in profile.observations
            if observation.session_date == session_date
        ),
    )
    session_open = f"{session_date}T09:15"
    published: list[ValueAreaReference] = []
    for reference in refs:
        available_at = (
            session_open
            if reference.scope == ProfileScope.PRIOR_SESSION.value
            else datetime.fromisoformat(reference.available_at)
            .astimezone(_SESSION_ZONE)
            .strftime("%Y-%m-%dT%H:%M")
        )
        published.append(replace(
            reference,
            available_at=available_at,
            fingerprint=hashlib.sha256(
                f"{reference.fingerprint}:{available_at}".encode("utf-8")
            ).hexdigest(),
        ))
    return tuple(published)
