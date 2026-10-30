"""Load, build, and attach reusable daily profile history."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.aggregate import SessionSpec
from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.features.model import PriceMeasurement
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.config import ValueProfileConfig
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.daily import (
    aggregate_source_fingerprint,
    build_daily_profile_record,
    daily_config_fingerprint,
    prior_session_snapshot_from_daily,
)
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import DailyProfileQuality
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.nearby import nearby_references_for_stream
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.clusters import build_reference_clusters, evaluate_cluster_reactions
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import DailyProfileRecord, PriorSessionResolution, ValueProfileResult
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.session_runner import run_current_session_value_profile
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.store import DailyProfileStore


def previous_trading_session(session_date: str, trading_dates: Sequence[str]) -> str | None:
    """Return the immediately preceding trading session date, if any."""
    ordered = sorted(trading_dates)
    if session_date not in ordered:
        prior = [item for item in ordered if item < session_date]
        return prior[-1] if prior else None
    index = ordered.index(session_date)
    if index == 0:
        return None
    return ordered[index - 1]


def ensure_daily_profile(
    store: DailyProfileStore,
    session_bars: Sequence[CompletedBar],
    *,
    config: ValueProfileConfig,
    session_spec: SessionSpec = SessionSpec(),
    rebuild_if_source_mismatch: bool = True,
    maximum_available_at: datetime | None = None,
) -> DailyProfileRecord | None:
    """Load a saved daily profile or build and persist it once from historical bars."""
    if not session_bars:
        return None
    first = session_bars[0]
    config_fingerprint = daily_config_fingerprint(config, session_spec)
    session_date = first.session_date
    existing = store.load(
        instrument=first.instrument,
        timeframe_minutes=first.timeframe_minutes,
        config_fingerprint=config_fingerprint,
        session_date=session_date,
    )
    source_fp = aggregate_source_fingerprint(session_bars)
    if existing is not None:
        if existing.aggregate_source_fingerprint == source_fp:
            return existing
        if not rebuild_if_source_mismatch:
            return existing
    computed = build_daily_profile_record(session_bars, config=config, session_spec=session_spec)
    if computed is None:
        return None
    if maximum_available_at is not None and computed.available_at > maximum_available_at:
        return computed
    store.save(computed)
    return computed


def resolve_prior_session(
    *,
    active_session_date: str,
    trading_dates: Sequence[str],
    store: DailyProfileStore,
    prior_session_bars: Sequence[CompletedBar] | None,
    first_bar_available_at: CompletedBar,
    config: ValueProfileConfig,
    session_spec: SessionSpec = SessionSpec(),
) -> PriorSessionResolution:
    prior_date = previous_trading_session(active_session_date, trading_dates)
    if prior_date is None:
        return PriorSessionResolution(
            session_date=None,
            snapshot=None,
            daily_record=None,
            reason="no_previous_trading_session",
        )

    bars = tuple(
        bar for bar in (prior_session_bars or ()) if bar.session_date == prior_date
    )
    record: DailyProfileRecord | None = None
    if bars:
        record = ensure_daily_profile(
            store,
            bars,
            config=config,
            session_spec=session_spec,
            maximum_available_at=first_bar_available_at.available_at,
        )
    if record is None and not bars:
        record = store.load(
            instrument=first_bar_available_at.instrument,
            timeframe_minutes=first_bar_available_at.timeframe_minutes,
            config_fingerprint=daily_config_fingerprint(config, session_spec),
            session_date=prior_date,
        )
    if record is None:
        return PriorSessionResolution(
            session_date=prior_date,
            snapshot=None,
            daily_record=None,
            reason="prior_session_profile_unavailable",
        )
    if config.prior_session_require_complete and record.quality is not DailyProfileQuality.COMPLETE:
        return PriorSessionResolution(
            session_date=prior_date,
            snapshot=None,
            daily_record=record,
            reason="prior_session_incomplete",
        )
    if record.available_at > first_bar_available_at.available_at:
        return PriorSessionResolution(
            session_date=prior_date,
            snapshot=None,
            daily_record=record,
            reason="prior_session_not_yet_available",
        )
    snapshot = prior_session_snapshot_from_daily(
        record,
        available_at=first_bar_available_at.available_at,
    )
    return PriorSessionResolution(
        session_date=prior_date,
        snapshot=snapshot,
        daily_record=record,
        reason=None,
    )


def list_history_for_lookback(
    store: DailyProfileStore,
    *,
    instrument: str,
    timeframe_minutes: int,
    config_fingerprint: str,
    trading_dates: Sequence[str],
    active_session_date: str,
    lookback_sessions: int,
) -> tuple[DailyProfileRecord, ...]:
    ordered = sorted(trading_dates)
    if active_session_date in ordered:
        index = ordered.index(active_session_date)
        candidates = ordered[:index]
    else:
        candidates = [item for item in ordered if item < active_session_date]
    # Load retained history; nearby selection applies the configured window and
    # needs older records to report ``none_within_lookback`` explicitly.
    records: list[DailyProfileRecord] = []
    complete_count = 0
    for session_date in reversed(candidates):
        loaded = store.load(
            instrument=instrument,
            timeframe_minutes=timeframe_minutes,
            config_fingerprint=config_fingerprint,
            session_date=session_date,
        )
        if loaded is not None:
            records.append(loaded)
            if loaded.quality is DailyProfileQuality.COMPLETE:
                complete_count += 1
        if complete_count >= lookback_sessions:
            break
    return tuple(records)


def run_value_profile_with_daily_history(
    bars: Sequence[CompletedBar],
    measurements: Sequence[PriceMeasurement],
    *,
    config: ValueProfileConfig,
    store: DailyProfileStore,
    trading_dates: Sequence[str],
    prior_session_bars: Sequence[CompletedBar] | None = None,
    session_spec: SessionSpec = SessionSpec(),
) -> ValueProfileResult:
    """Replay orchestration: history load, current session profile, optional persistence."""
    if not bars:
        return ValueProfileResult(snapshots=(), levels=(), observations=(), bindings=(), events=())

    active_session_date = bars[0].session_date
    if any(bar.session_date != active_session_date for bar in bars):
        raise ValueError("daily history replay expects one active session in the bar stream")

    prior_resolution = resolve_prior_session(
        active_session_date=active_session_date,
        trading_dates=trading_dates,
        store=store,
        prior_session_bars=prior_session_bars,
        first_bar_available_at=bars[0],
        config=config,
        session_spec=session_spec,
    )

    result = run_current_session_value_profile(
        bars,
        measurements,
        config=config,
        injected_prior=prior_resolution.snapshot,
    )
    result = ValueProfileResult(
        snapshots=result.snapshots,
        levels=result.levels,
        observations=result.observations,
        bindings=result.bindings,
        bindings_by_scope=result.bindings_by_scope,
        events=result.events,
        prior_session_resolution=prior_resolution,
    )

    history = list_history_for_lookback(
        store,
        instrument=bars[0].instrument,
        timeframe_minutes=bars[0].timeframe_minutes,
        config_fingerprint=daily_config_fingerprint(config, session_spec),
        trading_dates=trading_dates,
        active_session_date=active_session_date,
        lookback_sessions=config.daily_history_lookback_sessions,
    )
    if prior_resolution.daily_record is not None and all(
        item.session_date != prior_resolution.daily_record.session_date for item in history
    ):
        history = (*history, prior_resolution.daily_record)

    nearby = nearby_references_for_stream(
        bars,
        history=history,
        prior_session_date=prior_resolution.session_date,
        config=config,
        lookback_session_dates=tuple(
            sorted(date for date in trading_dates if date < active_session_date)
        ),
    )
    reference_clusters = build_reference_clusters(
        tuple(
            record for record in history
            if record.session_date in set(sorted(date for date in trading_dates if date < active_session_date)[-config.daily_history_lookback_sessions:])
        ),
        overlap_ratio=config.cluster_overlap_ratio,
        acceptance_half_life=config.cluster_acceptance_half_life,
        as_of=bars[0].available_at,
        trading_dates=trading_dates,
    )
    cluster_reactions = evaluate_cluster_reactions(
        bars,
        reference_clusters,
        confirmation_bars=config.cluster_break_confirmation_bars,
    )
    today_record = build_daily_profile_record(bars, config=config, session_spec=session_spec)
    if today_record is not None:
        existing = store.load(
            instrument=today_record.instrument,
            timeframe_minutes=today_record.timeframe_minutes,
            config_fingerprint=today_record.config_fingerprint,
            session_date=today_record.session_date,
        )
        if existing is None:
            store.save(today_record)
        elif existing.aggregate_source_fingerprint != today_record.aggregate_source_fingerprint:
            store.save(today_record)

    return ValueProfileResult(
        snapshots=result.snapshots,
        levels=result.levels,
        observations=result.observations,
        bindings=result.bindings,
        bindings_by_scope=result.bindings_by_scope,
        events=result.events,
        nearby_references=nearby,
        reference_clusters=reference_clusters,
        cluster_reaction_events=cluster_reactions.events,
        cluster_reaction_stats=cluster_reactions.stats,
        prior_session_resolution=prior_resolution,
    )
