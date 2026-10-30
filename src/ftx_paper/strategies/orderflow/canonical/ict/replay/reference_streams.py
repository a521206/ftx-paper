"""Multi-timeframe value-profile helpers for reference replay products."""

from __future__ import annotations

import hashlib
import json
from typing import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars import CompletedBar, SourceBar, build_completed_bars
from ftx_paper.strategies.orderflow.canonical.ict.features import MeasurementConfig, PriceMeasurement, build_measurements
from ftx_paper.strategies.orderflow.canonical.ict.value_profile import (
    ValueProfileConfig,
    ValueProfileResult,
    run_value_profile,
    run_value_profile_with_daily_history,
)
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import ProfileObservation, ValueProfileSnapshot
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.store import DailyProfileStore
from ftx_paper.strategies.orderflow.canonical.replay.value_references import ValueAreaReference


def value_references_from_snapshots(
    snapshots: Sequence[ValueProfileSnapshot],
    observations: Sequence[ProfileObservation] = (),
) -> tuple[ValueAreaReference, ...]:
    """Adapt frozen profiles and completed-bar developing observations."""
    def adapt(
        *,
        source_id: str,
        scope: str,
        session_date: str,
        poc: float,
        vah: float,
        val: float,
        available_at: str,
        config_fingerprint: str,
        profile_version: str,
        source_fingerprints: tuple[str, ...],
    ) -> ValueAreaReference:
        payload = (
            source_id, scope, session_date, poc, vah, val, available_at,
            config_fingerprint, profile_version, source_fingerprints,
        )
        fingerprint = hashlib.sha256(
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return ValueAreaReference(
            reference_id=f"{scope}:{source_id}",
            scope=scope,
            poc=poc,
            vah=vah,
            val=val,
            source_sessions=(session_date,),
            available_at=available_at,
            fingerprint=fingerprint,
        )

    references = [
        adapt(
            source_id=snapshot.snapshot_id,
            scope=snapshot.scope.value,
            session_date=snapshot.session_date,
            poc=snapshot.poc,
            vah=snapshot.vah,
            val=snapshot.val,
            available_at=snapshot.available_at.isoformat(),
            config_fingerprint=snapshot.config_fingerprint,
            profile_version=snapshot.profile_version,
            source_fingerprints=snapshot.source_bar_fingerprints,
        )
        for snapshot in snapshots
    ]
    references.extend(
        adapt(
            source_id=f"{item.snapshot_id}:{item.bar_fingerprint}",
            scope=item.scope.value,
            session_date=item.session_date,
            poc=item.poc,
            vah=item.vah,
            val=item.val,
            available_at=item.available_at.isoformat(),
            config_fingerprint=item.config_fingerprint,
            profile_version=item.profile_version,
            source_fingerprints=(item.bar_fingerprint,),
        )
        for item in observations
    )
    return tuple(sorted(references, key=lambda item: (item.available_at, item.reference_id)))


def _restrict_to_session(
    result: ValueProfileResult,
    *,
    bars: Sequence[CompletedBar],
    session_date: str,
) -> ValueProfileResult:
    """Keep current-session bindings while retaining prior snapshots for as-of joins."""
    first_current = next(
        (bar.available_at for bar in bars if bar.session_date == session_date),
        None,
    )
    if first_current is None:
        return result
    current_indices = [
        index for index, bar in enumerate(bars) if bar.session_date == session_date
    ]
    bindings_by_scope = tuple(
        (
            scope,
            tuple(bindings[index] for index in current_indices),
        )
        for scope, bindings in result.bindings_by_scope
    )
    return ValueProfileResult(
        snapshots=result.snapshots,
        levels=result.levels,
        observations=tuple(
            item for item in result.observations if item.session_date == session_date
        ),
        bindings=tuple(result.bindings[index] for index in current_indices),
        bindings_by_scope=bindings_by_scope,
        events=tuple(item for item in result.events if item.available_at >= first_current),
    )


def build_value_profiles_by_timeframe(
    source_bars: Sequence[SourceBar],
    timeframes: tuple[int, ...],
    *,
    profile_config: ValueProfileConfig,
    measurement_config: MeasurementConfig,
    active_session_date: str | None = None,
    daily_profile_store: DailyProfileStore | None = None,
    trading_session_dates: tuple[str, ...] = (),
    completed_bars_by_timeframe: dict[int, tuple[CompletedBar, ...]] | None = None,
    measurements_by_timeframe: dict[int, tuple[PriceMeasurement, ...]] | None = None,
) -> dict[int, ValueProfileResult]:
    """Build causal value profiles on multiple aggregated bar streams from one source."""
    if not timeframes:
        return {}
    profiles: dict[int, ValueProfileResult] = {}
    for timeframe in timeframes:
        if timeframe < 1:
            raise ValueError("timeframe minutes must be positive")
        bars = (
            completed_bars_by_timeframe[timeframe]
            if completed_bars_by_timeframe is not None
            and timeframe in completed_bars_by_timeframe
            else tuple(build_completed_bars(source_bars, timeframe_minutes=timeframe))
        )
        if not bars:
            continue
        measurements = (
            measurements_by_timeframe[timeframe]
            if measurements_by_timeframe is not None
            and timeframe in measurements_by_timeframe
            else build_measurements(bars, measurement_config)
        )
        if len(measurements) != len(bars):
            raise ValueError("precomputed measurements must align with completed bars")
        if daily_profile_store is not None and active_session_date is not None:
            result = run_value_profile_with_daily_history(
                tuple(bar for bar in bars if bar.session_date == active_session_date),
                tuple(
                    measurement for bar, measurement in zip(bars, measurements)
                    if bar.session_date == active_session_date
                ),
                config=profile_config,
                store=daily_profile_store,
                trading_dates=trading_session_dates,
                prior_session_bars=tuple(
                    bar for bar in bars if bar.session_date < active_session_date
                ),
            )
        else:
            result = run_value_profile(bars, measurements, config=profile_config)
            if active_session_date is not None:
                result = _restrict_to_session(
                    result,
                    bars=bars,
                    session_date=active_session_date,
                )
        profiles[timeframe] = result
    return profiles
