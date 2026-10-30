"""Per-bar profile bindings and handoff events."""

from __future__ import annotations

from typing import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.features.model import PriceMeasurement
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.config import ValueProfileConfig
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import ProfileEventKind, ProfileScope, ValuePosition
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import (
    ProfileBinding,
    ProfileEvent,
    ProfileObservation,
    ValueProfileSnapshot,
)


def _value_membership(close: float, vah: float, val: float) -> ValuePosition:
    if close > vah:
        return ValuePosition.ABOVE_VAH
    if close < val:
        return ValuePosition.BELOW_VAL
    return ValuePosition.INSIDE


def _binding_position(
    close: float,
    vah: float,
    val: float,
    *,
    near_poc: bool,
) -> ValuePosition:
    membership = _value_membership(close, vah, val)
    if membership is ValuePosition.INSIDE and near_poc:
        return ValuePosition.AT_POC
    return membership


def _prior_snapshot_as_of(
    snapshots: Sequence[ValueProfileSnapshot],
    available_at: CompletedBar,
) -> ValueProfileSnapshot | None:
    candidates = [
        snapshot
        for snapshot in snapshots
        if snapshot.scope is ProfileScope.PRIOR_SESSION
        and snapshot.available_at <= available_at.available_at
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.available_at)


def _frozen_scope_as_of(
    snapshots: Sequence[ValueProfileSnapshot],
    bar: CompletedBar,
    scope: ProfileScope,
) -> ValueProfileSnapshot | None:
    candidates = [
        snapshot
        for snapshot in snapshots
        if snapshot.scope is scope and snapshot.available_at <= bar.available_at
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.available_at)


def _binding_snapshot_as_of(
    snapshots: Sequence[ValueProfileSnapshot],
    bar: CompletedBar,
    config: ValueProfileConfig,
    requested_scope: ProfileScope | None = None,
) -> ValueProfileSnapshot | None:
    """Select only frozen geometry from the explicitly requested scope."""
    if requested_scope is ProfileScope.SESSION_DEVELOPING:
        return None
    if requested_scope is not None:
        if requested_scope is ProfileScope.PRIOR_SESSION:
            return _prior_snapshot_as_of(snapshots, bar)
        return _frozen_scope_as_of(snapshots, bar, requested_scope)
    if ProfileScope.PRIOR_SESSION in config.scopes:
        snapshot = _prior_snapshot_as_of(snapshots, bar)
        if snapshot is not None:
            return snapshot
    if ProfileScope.ZONE in config.scopes:
        snapshot = _frozen_scope_as_of(snapshots, bar, ProfileScope.ZONE)
        if snapshot is not None:
            return snapshot
    if ProfileScope.ROLLING_BARS in config.scopes:
        snapshot = _frozen_scope_as_of(snapshots, bar, ProfileScope.ROLLING_BARS)
        if snapshot is not None:
            return snapshot
    return None


def run_profile_context(
    bars: Sequence[CompletedBar],
    measurements: Sequence[PriceMeasurement],
    snapshots: Sequence[ValueProfileSnapshot],
    *,
    config: ValueProfileConfig,
    observations: Sequence[ProfileObservation] = (),
    requested_scope: ProfileScope | None = None,
) -> tuple[tuple[ProfileBinding | None, ...], tuple[ProfileEvent, ...]]:
    """Attach as-of profile geometry and emit edge-triggered profile events."""
    if len(bars) != len(measurements):
        raise ValueError("bars and measurements must be aligned")

    config_fingerprint = config.fingerprint()
    use_developing = (
        ProfileScope.SESSION_DEVELOPING in config.scopes and config.developing_policy == "emit"
        and (requested_scope is None or requested_scope is ProfileScope.SESSION_DEVELOPING)
    )
    developing_by_index: dict[int, ProfileObservation] = {}
    for observation in observations:
        for index, bar in enumerate(bars):
            if bar.source_fingerprint != observation.bar_fingerprint:
                continue
            if observation.available_at > bar.available_at:
                continue
            if observation.session_date != bar.session_date:
                continue
            developing_by_index[index] = observation

    bindings: list[ProfileBinding | None] = []
    events: list[ProfileEvent] = []
    previous_membership: ValuePosition | None = None
    previous_near_poc = False

    for index, (bar, measurement) in enumerate(zip(bars, measurements)):
        snapshot = _binding_snapshot_as_of(snapshots, bar, config, requested_scope)
        first_bar_in_session = (
            index == 0 or bars[index - 1].session_date != bar.session_date
        )
        poc = vah = val = 0.0
        snapshot_id = ""
        if snapshot is not None:
            poc, vah, val = snapshot.poc, snapshot.vah, snapshot.val
            snapshot_id = snapshot.snapshot_id
        elif (
            use_developing
            and index in developing_by_index
            and not first_bar_in_session
        ):
            row = developing_by_index[index]
            poc, vah, val = row.poc, row.vah, row.val
            snapshot_id = row.snapshot_id
        else:
            bindings.append(None)
            continue

        binding_config_fingerprint = (
            snapshot.config_fingerprint if snapshot is not None else config_fingerprint
        )

        distance_to_poc = abs(bar.close - poc)
        near_tolerance = config.tick_size
        atr = measurement.atr
        if atr is not None and atr > 0:
            near_tolerance = max(near_tolerance, 0.5 * atr)
        near_poc = distance_to_poc <= near_tolerance
        membership = _value_membership(bar.close, vah, val)
        position = _binding_position(bar.close, vah, val, near_poc=near_poc)
        distance_atr = None if atr is None or atr <= 0 else (bar.close - poc) / atr

        bindings.append(
            ProfileBinding(
                snapshot_id=snapshot_id,
                position=position,
                poc=poc,
                vah=vah,
                val=val,
                instrument=bar.instrument,
                timeframe_minutes=bar.timeframe_minutes,
                session_date=bar.session_date,
                observed_at=bar.timestamp,
                available_at=bar.available_at,
                selected_at=bar.available_at,
                config_fingerprint=binding_config_fingerprint,
                profile_version=snapshot.profile_version if snapshot is not None else config.profile_version,
                distance_to_poc=distance_to_poc,
                distance_to_vah=abs(bar.close - vah),
                distance_to_val=abs(bar.close - val),
                distance_atr=distance_atr,
                near_poc=near_poc,
            )
        )

        if previous_membership is not None:
            if previous_membership is not ValuePosition.INSIDE and membership is ValuePosition.INSIDE:
                events.append(
                    ProfileEvent(
                        kind=ProfileEventKind.ENTERED_VALUE,
                        instrument=bar.instrument,
                        timeframe_minutes=bar.timeframe_minutes,
                        session_date=bar.session_date,
                        observed_at=bar.timestamp,
                        available_at=bar.available_at,
                        snapshot_id=snapshot_id,
                        config_fingerprint=binding_config_fingerprint,
                        profile_version=snapshot.profile_version if snapshot is not None else config.profile_version,
                    )
                )
            elif previous_membership is ValuePosition.INSIDE and membership is not ValuePosition.INSIDE:
                events.append(
                    ProfileEvent(
                        kind=ProfileEventKind.EXITED_VALUE,
                        instrument=bar.instrument,
                        timeframe_minutes=bar.timeframe_minutes,
                        session_date=bar.session_date,
                        observed_at=bar.timestamp,
                        available_at=bar.available_at,
                        snapshot_id=snapshot_id,
                        config_fingerprint=binding_config_fingerprint,
                        profile_version=snapshot.profile_version if snapshot is not None else config.profile_version,
                    )
                )
        if near_poc and not previous_near_poc:
            events.append(
                ProfileEvent(
                    kind=ProfileEventKind.POC_TOUCH,
                    instrument=bar.instrument,
                    timeframe_minutes=bar.timeframe_minutes,
                    session_date=bar.session_date,
                    observed_at=bar.timestamp,
                    available_at=bar.available_at,
                    snapshot_id=snapshot_id,
                    config_fingerprint=binding_config_fingerprint,
                    profile_version=snapshot.profile_version if snapshot is not None else config.profile_version,
                )
            )
        previous_membership = membership
        previous_near_poc = near_poc

    return tuple(bindings), tuple(events)


def run_profile_context_by_scope(
    bars: Sequence[CompletedBar],
    measurements: Sequence[PriceMeasurement],
    snapshots: Sequence[ValueProfileSnapshot],
    *,
    config: ValueProfileConfig,
    observations: Sequence[ProfileObservation] = (),
) -> tuple[
    tuple[tuple[ProfileScope, tuple[ProfileBinding | None, ...]], ...],
    tuple[ProfileEvent, ...],
]:
    """Return independent per-bar bindings for every configured profile scope."""
    results: list[tuple[ProfileScope, tuple[ProfileBinding | None, ...]]] = []
    events: list[ProfileEvent] = []
    for scope in config.scopes:
        bindings, scoped_events = run_profile_context(
            bars,
            measurements,
            snapshots,
            config=config,
            observations=observations,
            requested_scope=scope,
        )
        results.append((scope, bindings))
        events.extend(scoped_events)
    return tuple(results), tuple(events)
