"""Causal value-profile builder."""

from __future__ import annotations

from typing import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.features.model import PriceMeasurement
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.accumulator import ProfileAccumulator
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.bins import HistogramLevels
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.config import ValueProfileConfig
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.context import run_profile_context, run_profile_context_by_scope
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import (
    ProfileEventKind,
    ProfileLevelRole,
    ProfileScope,
    SnapshotStatus,
)
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import (
    ProfileEvent,
    ProfileLevel,
    ProfileObservation,
    ValueProfileResult,
    ValueProfileSnapshot,
)
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.temporal import TemporalGap, validate_profile_stream
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.zones import session_zone_label


def _new_accumulator(config: ValueProfileConfig) -> ProfileAccumulator:
    return ProfileAccumulator(
        value_area_fraction=config.value_area_fraction,
        tick_size=config.tick_size,
    )


def _accumulator_for_bars(
    window: Sequence[CompletedBar],
    config: ValueProfileConfig,
) -> ProfileAccumulator:
    acc = _new_accumulator(config)
    for item in window:
        acc.add_bar(high=item.high, low=item.low, close=item.close, volume=item.volume)
    return acc


def _gap_before_indices(bars: Sequence[CompletedBar], gaps: tuple[TemporalGap, ...]) -> set[int]:
    indices: set[int] = set()
    for gap in gaps:
        for index in range(1, len(bars)):
            if bars[index].timestamp == gap.next_timestamp:
                indices.add(index)
                break
    return indices


def _developing_snapshot_id(session_date: str) -> str:
    return f"vp:developing:{session_date}"


def _frozen_snapshot_id(scope: ProfileScope, session_date: str, *, suffix: str = "") -> str:
    if suffix:
        return f"vp:{scope.value}:{session_date}:{suffix}"
    return f"vp:{scope.value}:{session_date}"


def _publish_levels(
    snapshot: ValueProfileSnapshot,
    *,
    config_fingerprint: str,
    profile_version: str,
) -> tuple[ProfileLevel, ...]:
    rows: list[ProfileLevel] = []
    for role, price in (
        (ProfileLevelRole.POC, snapshot.poc),
        (ProfileLevelRole.VAH, snapshot.vah),
        (ProfileLevelRole.VAL, snapshot.val),
    ):
        rows.append(
            ProfileLevel(
                level_id=f"{snapshot.snapshot_id}:{role.value}",
                snapshot_id=snapshot.snapshot_id,
                role=role,
                price=price,
                instrument=snapshot.instrument,
                timeframe_minutes=snapshot.timeframe_minutes,
                session_date=snapshot.session_date,
                available_at=snapshot.available_at,
                config_fingerprint=config_fingerprint,
                profile_version=profile_version,
            )
        )
    if snapshot.vwap is not None:
        rows.append(
            ProfileLevel(
                level_id=f"{snapshot.snapshot_id}:{ProfileLevelRole.VWAP.value}",
                snapshot_id=snapshot.snapshot_id,
                role=ProfileLevelRole.VWAP,
                price=snapshot.vwap,
                instrument=snapshot.instrument,
                timeframe_minutes=snapshot.timeframe_minutes,
                session_date=snapshot.session_date,
                available_at=snapshot.available_at,
                config_fingerprint=config_fingerprint,
                profile_version=profile_version,
            )
        )
    return tuple(rows)


def _snapshot_from_levels(
    *,
    snapshot_id: str,
    scope: ProfileScope,
    status: SnapshotStatus,
    bar: CompletedBar,
    session_date: str,
    window_first: CompletedBar,
    window_last: CompletedBar,
    levels: HistogramLevels,
    available_at: CompletedBar,
    config_fingerprint: str,
    profile_version: str,
    fingerprints: tuple[str, ...],
) -> ValueProfileSnapshot:
    return ValueProfileSnapshot(
        snapshot_id=snapshot_id,
        scope=scope,
        status=status,
        instrument=bar.instrument,
        timeframe_minutes=bar.timeframe_minutes,
        session_date=session_date,
        window_first_bar_ts=window_first.timestamp,
        window_last_bar_ts=window_last.timestamp,
        poc=levels.poc,
        vah=levels.vah,
        val=levels.val,
        vwap=levels.vwap,
        total_volume=levels.total_volume,
        observed_at=window_last.timestamp,
        available_at=available_at.available_at,
        config_fingerprint=config_fingerprint,
        profile_version=profile_version,
        source_bar_fingerprints=fingerprints,
    )


def _finalize_session(
    *,
    scope: ProfileScope,
    session_date: str,
    window_first: CompletedBar,
    window_last: CompletedBar,
    accumulator: ProfileAccumulator,
    available_bar: CompletedBar,
    fingerprints: tuple[str, ...],
    config: ValueProfileConfig,
    config_fingerprint: str,
    snapshots: list[ValueProfileSnapshot],
    levels: list[ProfileLevel],
    events: list[ProfileEvent],
    snapshot_id_suffix: str = "",
) -> None:
    histogram = accumulator.levels()
    if histogram is None:
        return
    snapshot_id = _frozen_snapshot_id(scope, session_date, suffix=snapshot_id_suffix)
    snapshot = _snapshot_from_levels(
        snapshot_id=snapshot_id,
        scope=scope,
        status=SnapshotStatus.FROZEN,
        bar=window_last,
        session_date=session_date,
        window_first=window_first,
        window_last=window_last,
        levels=histogram,
        available_at=available_bar,
        config_fingerprint=config_fingerprint,
        profile_version=config.profile_version,
        fingerprints=fingerprints,
    )
    snapshots.append(snapshot)
    levels.extend(
        _publish_levels(
            snapshot,
            config_fingerprint=config_fingerprint,
            profile_version=config.profile_version,
        )
    )
    events.append(
        ProfileEvent(
            kind=ProfileEventKind.PUBLISHED,
            instrument=snapshot.instrument,
            timeframe_minutes=snapshot.timeframe_minutes,
            session_date=snapshot.session_date,
            observed_at=snapshot.observed_at,
            available_at=snapshot.available_at,
            snapshot_id=snapshot.snapshot_id,
            config_fingerprint=config_fingerprint,
            profile_version=config.profile_version,
        )
    )


def run_value_profile(
    bars: Sequence[CompletedBar],
    measurements: Sequence[PriceMeasurement],
    *,
    config: ValueProfileConfig | None = None,
    injected_prior: ValueProfileSnapshot | None = None,
) -> ValueProfileResult:
    """Project volume-at-price snapshots and levels over a validated bar stream."""
    resolved = config or ValueProfileConfig()
    if len(bars) != len(measurements):
        raise ValueError("bars and measurements must be aligned")
    if not bars:
        return ValueProfileResult(snapshots=(), levels=(), observations=(), bindings=(), events=())

    validation = validate_profile_stream(bars)
    config_fingerprint = resolved.fingerprint()
    gap_indices = _gap_before_indices(bars, validation.gaps)

    snapshots: list[ValueProfileSnapshot] = []
    levels: list[ProfileLevel] = []
    observations: list[ProfileObservation] = []
    events: list[ProfileEvent] = []

    if injected_prior is not None and ProfileScope.PRIOR_SESSION in resolved.scopes:
        snapshots.append(injected_prior)
        levels.extend(
            _publish_levels(
                injected_prior,
                config_fingerprint=injected_prior.config_fingerprint,
                profile_version=injected_prior.profile_version,
            )
        )

    developing = _new_accumulator(resolved)
    session_first_bar: CompletedBar | None = None
    session_fingerprints: list[str] = []
    current_session: str | None = None
    zone_enabled = ProfileScope.ZONE in resolved.scopes
    rolling_enabled = ProfileScope.ROLLING_BARS in resolved.scopes
    current_zone: str | None = None
    zone_first_bar: CompletedBar | None = None
    zone_acc = _new_accumulator(resolved)
    zone_fingerprints: list[str] = []
    rolling_window: list[CompletedBar] = []

    def reset_zone() -> None:
        nonlocal zone_first_bar, zone_fingerprints, current_zone, zone_acc
        zone_first_bar = None
        zone_fingerprints = []
        current_zone = None
        zone_acc = _new_accumulator(resolved)

    def finalize_zone(window_last: CompletedBar, available_bar: CompletedBar, zone_label: str) -> None:
        if zone_first_bar is None:
            return
        _finalize_session(
            scope=ProfileScope.ZONE,
            session_date=window_last.session_date,
            window_first=zone_first_bar,
            window_last=window_last,
            accumulator=zone_acc,
            available_bar=available_bar,
            fingerprints=tuple(zone_fingerprints),
            config=resolved,
            config_fingerprint=config_fingerprint,
            snapshots=snapshots,
            levels=levels,
            events=events,
            snapshot_id_suffix=zone_label,
        )
        reset_zone()

    def finalize_rolling_window(window_bars: Sequence[CompletedBar], available_bar: CompletedBar) -> None:
        if len(window_bars) < resolved.rolling_window_bars:
            return
        window = tuple(window_bars)
        _finalize_session(
            scope=ProfileScope.ROLLING_BARS,
            session_date=window[-1].session_date,
            window_first=window[0],
            window_last=window[-1],
            accumulator=_accumulator_for_bars(window, resolved),
            available_bar=available_bar,
            fingerprints=tuple(item.source_fingerprint for item in window),
            config=resolved,
            config_fingerprint=config_fingerprint,
            snapshots=snapshots,
            levels=levels,
            events=events,
            snapshot_id_suffix=window[-1].source_fingerprint,
        )

    emit_developing = (
        ProfileScope.SESSION_DEVELOPING in resolved.scopes
        and resolved.developing_policy == "emit"
    )

    def reset_developing() -> None:
        nonlocal developing, session_first_bar, session_fingerprints
        developing = _new_accumulator(resolved)
        session_first_bar = None
        session_fingerprints = []

    for index, (bar, _measurement) in enumerate(zip(bars, measurements)):
        session_changed = current_session is not None and bar.session_date != current_session
        if session_changed and resolved.session_reset:
            if session_first_bar is not None and current_session is not None:
                if ProfileScope.SESSION_DEVELOPING in resolved.scopes:
                    _finalize_session(
                        scope=ProfileScope.SESSION_DEVELOPING,
                        session_date=current_session,
                        window_first=session_first_bar,
                        window_last=bars[index - 1],
                        accumulator=developing,
                        available_bar=bars[index - 1],
                        fingerprints=tuple(session_fingerprints),
                        config=resolved,
                        config_fingerprint=config_fingerprint,
                        snapshots=snapshots,
                        levels=levels,
                        events=events,
                    )
                if ProfileScope.PRIOR_SESSION in resolved.scopes:
                    prior_acc = _new_accumulator(resolved)
                    for prior_index in range(index):
                        prior_bar = bars[prior_index]
                        if prior_bar.session_date != current_session:
                            continue
                        prior_acc.add_bar(
                            high=prior_bar.high,
                            low=prior_bar.low,
                            close=prior_bar.close,
                            volume=prior_bar.volume,
                        )
                    _finalize_session(
                        scope=ProfileScope.PRIOR_SESSION,
                        session_date=current_session,
                        window_first=session_first_bar,
                        window_last=bars[index - 1],
                        accumulator=prior_acc,
                        available_bar=bar,
                        fingerprints=tuple(session_fingerprints),
                        config=resolved,
                        config_fingerprint=config_fingerprint,
                        snapshots=snapshots,
                        levels=levels,
                        events=events,
                    )
            reset_developing()
            if zone_enabled and current_zone is not None and zone_first_bar is not None:
                finalize_zone(bars[index - 1], bars[index - 1], current_zone)
            reset_zone()

        intraday_gap = (
            index in gap_indices
            and index > 0
            and bars[index].session_date == bars[index - 1].session_date
        )
        if intraday_gap and resolved.gap_policy == "invalidate_developing":
            if zone_enabled and current_zone is not None and zone_first_bar is not None:
                finalize_zone(bars[index - 1], bar, current_zone)
            reset_developing()
            reset_zone()
            rolling_window.clear()
        bar_zone = session_zone_label(bar) if zone_enabled else None
        if zone_enabled and bar_zone is not None:
            if current_zone is not None and bar_zone != current_zone and zone_first_bar is not None:
                finalize_zone(bars[index - 1], bar, current_zone)
            if zone_first_bar is None:
                zone_first_bar = bar
            current_zone = bar_zone
            zone_fingerprints.append(bar.source_fingerprint)
            zone_acc.add_bar(
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
            )

        if session_first_bar is None:
            session_first_bar = bar
        session_fingerprints.append(bar.source_fingerprint)
        current_session = bar.session_date

        developing.add_bar(
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
        )
        if rolling_enabled:
            rolling_window.append(bar)
            if len(rolling_window) > resolved.rolling_window_bars:
                rolling_window.pop(0)
            if len(rolling_window) == resolved.rolling_window_bars:
                finalize_rolling_window(rolling_window, bar)

        if emit_developing:
            histogram = developing.levels()
            if histogram is not None:
                observations.append(
                    ProfileObservation(
                        bar_fingerprint=bar.source_fingerprint,
                        snapshot_id=_developing_snapshot_id(bar.session_date),
                        scope=ProfileScope.SESSION_DEVELOPING,
                        instrument=bar.instrument,
                        timeframe_minutes=bar.timeframe_minutes,
                        session_date=bar.session_date,
                        observed_at=bar.timestamp,
                        available_at=bar.available_at,
                        poc=histogram.poc,
                        vah=histogram.vah,
                        val=histogram.val,
                        config_fingerprint=config_fingerprint,
                        profile_version=resolved.profile_version,
                    )
                )

    snapshot_tuple = tuple(snapshots)
    bindings, _ = run_profile_context(
        bars,
        measurements,
        snapshot_tuple,
        config=resolved,
        observations=tuple(observations),
    )
    bindings_by_scope, scoped_context_events = run_profile_context_by_scope(
        bars,
        measurements,
        snapshot_tuple,
        config=resolved,
        observations=tuple(observations),
    )
    return ValueProfileResult(
        snapshots=snapshot_tuple,
        levels=tuple(levels),
        observations=tuple(observations),
        bindings=bindings,
        bindings_by_scope=bindings_by_scope,
        events=tuple(events) + scoped_context_events,
    )
