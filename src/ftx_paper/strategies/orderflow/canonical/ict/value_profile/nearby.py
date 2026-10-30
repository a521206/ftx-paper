"""Select nearby historical value areas for each decision bar."""

from __future__ import annotations

from typing import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.config import ValueProfileConfig
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import DailyProfileQuality, NearbyReferenceRole, ValuePosition
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import (
    DailyProfileRecord,
    NearbyValueAreaSelection,
    NearbyValueAreaReference,
)


def _interval_distance(close: float, val: float, vah: float) -> float:
    if val <= close <= vah:
        return 0.0
    if close > vah:
        return close - vah
    return val - close


def _range_overlaps_value_area(high: float, low: float, val: float, vah: float) -> bool:
    return high >= val and low <= vah


def _reference_from_record(
    record: DailyProfileRecord,
    *,
    role: NearbyReferenceRole,
    close: float,
    high: float,
    low: float,
    config: ValueProfileConfig,
) -> NearbyValueAreaReference:
    return NearbyValueAreaReference(
        session_date=record.session_date,
        snapshot_id=record.snapshot_id,
        poc=record.poc,
        val=record.val,
        vah=record.vah,
        distance=_interval_distance(close, record.val, record.vah),
        overlaps_range=_range_overlaps_value_area(high, low, record.val, record.vah),
        role=role,
        available_at=record.available_at,
        position=(
            ValuePosition.ABOVE_VAH if close > record.vah
            else ValuePosition.BELOW_VAL if close < record.val
            else ValuePosition.AT_POC if abs(close - record.poc) <= config.tick_size
            else ValuePosition.INSIDE
        ),
        near_poc=abs(close - record.poc) <= config.tick_size,
        instrument=record.instrument,
        timeframe_minutes=record.timeframe_minutes,
        quality=record.quality,
        config_fingerprint=record.config_fingerprint,
        profile_version=record.profile_version,
        aggregate_source_fingerprint=record.aggregate_source_fingerprint,
    )


def select_nearby_value_areas(
    bar: CompletedBar,
    *,
    history: Sequence[DailyProfileRecord],
    prior_session_date: str | None,
    config: ValueProfileConfig,
    lookback_session_dates: Sequence[str] | None = None,
) -> NearbyValueAreaSelection:
    """Pick containing / nearest / prior references visible at the bar's decision time."""
    eligible = [
        record
        for record in history
        if record.quality is DailyProfileQuality.COMPLETE
        and record.available_at <= bar.available_at
        and record.session_date < bar.session_date
    ]
    eligible.sort(key=lambda item: item.session_date)
    lookback = config.daily_history_lookback_sessions
    if lookback_session_dates is None:
        windowed = eligible[-lookback:] if len(eligible) > lookback else eligible
        outside_window = eligible[:-lookback] if len(eligible) > lookback else ()
    else:
        allowed = set(lookback_session_dates[-lookback:])
        windowed = [record for record in eligible if record.session_date in allowed]
        outside_window = [record for record in eligible if record.session_date not in allowed]

    close = bar.close
    high = bar.high
    low = bar.low

    containing_candidates = [
        record for record in windowed if record.val <= close <= record.vah
    ]
    containing_record = containing_candidates[-1] if containing_candidates else None
    containing = (
        _reference_from_record(
            containing_record,
            role=NearbyReferenceRole.CONTAINING,
            close=close,
            high=high,
            low=low,
            config=config,
        )
        if containing_record is not None
        else None
    )

    above_candidates = [
        (record.val - close, record)
        for record in windowed
        if record.val > close
    ]
    nearest_above_record = min(above_candidates, key=lambda item: item[0])[1] if above_candidates else None
    nearest_above = (
        _reference_from_record(
            nearest_above_record,
            role=NearbyReferenceRole.NEAREST_ABOVE,
            close=close,
            high=high,
            low=low,
            config=config,
        )
        if nearest_above_record is not None
        else None
    )

    below_candidates = [
        (close - record.vah, record)
        for record in windowed
        if record.vah < close
    ]
    nearest_below_record = min(below_candidates, key=lambda item: item[0])[1] if below_candidates else None
    nearest_below = (
        _reference_from_record(
            nearest_below_record,
            role=NearbyReferenceRole.NEAREST_BELOW,
            close=close,
            high=high,
            low=low,
            config=config,
        )
        if nearest_below_record is not None
        else None
    )

    prior_record = next(
        (record for record in windowed if record.session_date == prior_session_date),
        None,
    )
    if prior_record is None and prior_session_date is not None:
        prior_record = next(
            (record for record in eligible if record.session_date == prior_session_date),
            None,
        )
    prior_session = (
        _reference_from_record(
            prior_record,
            role=NearbyReferenceRole.PRIOR_SESSION,
            close=close,
            high=high,
            low=low,
            config=config,
        )
        if prior_record is not None
        else None
    )

    older_nearby = any(
        _interval_distance(close, record.val, record.vah) == 0.0
        or record.val > close
        or record.vah < close
        for record in outside_window
    )
    none_within_lookback = (
        containing is None
        and nearest_above is None
        and nearest_below is None
        and older_nearby
    )

    return NearbyValueAreaSelection(
        containing=containing,
        nearest_above=nearest_above,
        nearest_below=nearest_below,
        prior_session=prior_session,
        none_within_lookback=none_within_lookback,
        history_available=bool(eligible),
    )


def nearby_references_for_stream(
    bars: Sequence[CompletedBar],
    *,
    history: Sequence[DailyProfileRecord],
    prior_session_date: str | None,
    config: ValueProfileConfig,
    lookback_session_dates: Sequence[str] | None = None,
) -> tuple[NearbyValueAreaSelection | None, ...]:
    return tuple(
        select_nearby_value_areas(
            bar,
            history=history,
            prior_session_date=prior_session_date,
            config=config,
            lookback_session_dates=lookback_session_dates,
        )
        for bar in bars
    )
