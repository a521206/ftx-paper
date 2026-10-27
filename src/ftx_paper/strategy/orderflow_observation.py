"""Causal aggregate publication and source-gap diagnostics for orderflow."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from collections.abc import Iterable

from ftx_paper.contracts import MarketBar, normalize_exchange_timestamp
from ftx_paper.config import IST

from .orderflow_contract import OrderflowContext, OrderflowObservation
from .orderflow_producer import CausalOrderflowProducer, source_row_is_complete
from .orderflow_source import OrderflowReplayInput


@dataclass(frozen=True, slots=True)
class ObservationGap:
    """A missing completed source minute that invalidates parent publication."""

    start: datetime
    end: datetime
    reason: str = "missing_source_bar"

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError("observation gap must be increasing")


@dataclass(frozen=True, slots=True)
class ObservationBoundary:
    first_observed_at: datetime | None
    last_observed_at: datetime | None
    first_available_at: datetime | None
    last_available_at: datetime | None
    expected_start: datetime | None = None
    expected_end: datetime | None = None


@dataclass(frozen=True, slots=True)
class ObservationIssue:
    timeframe: str
    interval_start: datetime
    interval_end: datetime
    reason: str
    observed_child_times: tuple[datetime, ...] = ()


@dataclass(frozen=True, slots=True)
class MarketObservation:
    """Immutable source observation with all published aggregates and issues."""

    session_date: str
    source_fingerprint: str
    context: OrderflowContext
    gaps: tuple[ObservationGap, ...]
    boundary: ObservationBoundary
    issues: tuple[ObservationIssue, ...] = ()

    def available(self, cutoff: datetime) -> OrderflowContext:
        prefixes = {
            timeframe: tuple(item for item in values if item.available_by(cutoff))
            for timeframe, values in self.context.prefixes.items()
        }
        published_ids = {item.observation_id for values in prefixes.values() for item in values}
        observations = tuple(
            item for item in self.context.observations
            if item.observation_id in published_ids
        )
        return OrderflowContext(self.session_date, cutoff, observations, prefixes)


def _source_gaps(
    rows: tuple[MarketBar, ...], expected_start: datetime | None, expected_end: datetime | None,
) -> tuple[ObservationGap, ...]:
    timestamps = [normalize_exchange_timestamp(row.timestamp) for row in rows]
    gaps: list[ObservationGap] = []
    if not timestamps and expected_start is not None and expected_end is not None:
        return (ObservationGap(expected_start, expected_end),)
    if timestamps and expected_start is not None and timestamps[0] > expected_start:
        gaps.append(ObservationGap(expected_start, timestamps[0]))
    for previous, current in zip(timestamps, timestamps[1:]):
        if current - previous > timedelta(minutes=1):
            gaps.append(ObservationGap(previous + timedelta(minutes=1), current))
    if timestamps and expected_end is not None and timestamps[-1] + timedelta(minutes=1) < expected_end:
        gaps.append(ObservationGap(timestamps[-1] + timedelta(minutes=1), expected_end))
    return tuple(gaps)


def _expected_bounds(replay: OrderflowReplayInput) -> tuple[datetime, datetime]:
    start_raw, end_raw = replay.metadata.session_partition.split("-", maxsplit=1)
    session_start = datetime.fromisoformat(f"{replay.session_date}T{start_raw}").replace(tzinfo=IST)
    session_end = datetime.fromisoformat(f"{replay.session_date}T{end_raw}").replace(tzinfo=IST)
    return session_start.astimezone(timezone.utc), session_end.astimezone(timezone.utc)


def rows_available_for_observation(
    observations: Iterable[OrderflowObservation], cutoff: datetime,
) -> tuple[OrderflowObservation, ...]:
    """Expose only completed aggregates whose availability is causal."""
    return tuple(item for item in observations if item.available_by(cutoff))


def observe_market(replay: OrderflowReplayInput) -> MarketObservation:
    """Build complete 1m/5m evidence without fabricating incomplete parents."""
    expected_start, expected_end = _expected_bounds(replay)
    # The source contract normally already filters this stream. Keep the
    # aggregation boundary defensive so callers cannot leak out-of-session
    # rows into windows or fingerprints.
    session_rows = tuple(
        row for row in replay.rows
        if expected_start <= normalize_exchange_timestamp(row.timestamp) < expected_end
        and source_row_is_complete(row)
    )
    start_text, end_text = replay.metadata.session_partition.split("-", maxsplit=1)
    producer = CausalOrderflowProducer(
        session_timezone=IST, session_start=start_text, session_end=end_text,
    )
    for row in session_rows:
        producer.ingest(row)
    gaps = _source_gaps(session_rows, expected_start, expected_end)
    if session_rows:
        context = producer.context(replay.session_date, "23:59")
        if context is None:
            raise ValueError("source rows did not produce an observation context")
    else:
        context = OrderflowContext(replay.session_date, datetime.min.replace(tzinfo=timezone.utc))
    issues: list[ObservationIssue] = []
    window_start = expected_start
    while window_start < expected_end:
        window_end = min(window_start + timedelta(minutes=5), expected_end)
        children = tuple(
            normalize_exchange_timestamp(row.timestamp)
            for row in session_rows
            if window_start <= normalize_exchange_timestamp(row.timestamp) < window_end
        )
        expected_children = tuple(
            window_start + timedelta(minutes=index)
            for index in range(int((window_end - window_start).total_seconds() // 60))
        )
        if children and children != expected_children:
            issues.append(ObservationIssue(
                "5m", window_start, window_end, "incomplete_session_aligned_parent", children,
            ))
        window_start = window_end
    for parent in context.observations_for("5m"):
        if not context.validate_complete_parent(parent):
            raise ValueError(f"incomplete 5m parent publication: {parent.observation_id}")
    # Preserve the canonical fingerprint of the selected input rows through
    # every aggregate; derived child hashes are not a substitute for lineage.
    if replay.metadata.source_fingerprint:
        observations = tuple(
            replace(item, context=replace(
                item.context,
                replay_provenance={
                    **dict(item.context.replay_provenance),
                    "source_fingerprint": replay.metadata.source_fingerprint,
                },
            )) if item.context is not None else item
            for item in context.observations
        )
        context = OrderflowContext(
            context.session_id, context.cutoff, observations,
            {timeframe: tuple(item for item in observations if item.timeframe == timeframe)
             for timeframe in context.prefixes},
        )
    ordered = tuple(sorted(context.observations, key=lambda item: item.observed_at))
    boundary = ObservationBoundary(
        ordered[0].observed_at if ordered else None,
        ordered[-1].observed_at if ordered else None,
        min((item.available_at for item in ordered), default=None),
        max((item.available_at for item in ordered), default=None),
        expected_start, expected_end,
    )
    return MarketObservation(
        replay.session_date, replay.metadata.source_fingerprint, context,
        gaps, boundary, tuple(issues),
    )


__all__ = [
    "MarketObservation", "ObservationBoundary", "ObservationGap", "ObservationIssue",
    "observe_market", "rows_available_for_observation",
]
