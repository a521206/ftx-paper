"""Causal timeout accounting for unfilled opportunities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .interactions import InteractionSnapshot, InteractionStatus
from .lifecycle import SetupOpportunity
from .orderflow_common import Aggregate, canonical_timestamp

DEFAULT_OPPORTUNITY_TIMEOUT_UNITS = 6


@dataclass(frozen=True, slots=True)
class TimeoutAdvance:
    added_units: int
    expiry_at: str | None
    counted_source_ids: tuple[str, ...]
    counted_snapshot_ids: tuple[str, ...]


def advance_opportunity_timeout(opportunity: SetupOpportunity, *, interaction: InteractionSnapshot | None, evidence: Sequence[Aggregate], as_of: str) -> TimeoutAdvance:
    if not opportunity.active or opportunity.timeout_evidence_timeframe != "5m":
        return TimeoutAdvance(0, None, (), ())
    as_of = canonical_timestamp(as_of)
    snapshots = dict(interaction.synthetic_snapshots).get("5m", ()) if interaction is not None else ()
    active = interaction is not None and interaction.status is InteractionStatus.ACTIVE
    counted_sources = set(opportunity.timeout_counted_source_ids)
    counted_snapshots = set(opportunity.timeout_counted_snapshot_ids)
    sources: list[str] = []
    snapshots_added: list[str] = []
    expiry: str | None = None
    for snapshot in sorted(snapshots, key=lambda item: item.available_at):
        if not opportunity.formation_available_at < snapshot.available_at <= as_of:
            continue
        snapshot_id = ":".join((interaction.interaction_id if interaction else "", "5m", ",".join(snapshot.source_candle_ids)))
        if snapshot_id in counted_snapshots:
            continue
        counted_snapshots.add(snapshot_id)
        opportunity.timeout_counted_snapshot_ids.append(snapshot_id)
        snapshots_added.append(snapshot_id)
        for source_id in snapshot.source_candle_ids:
            if source_id and source_id not in counted_sources:
                counted_sources.add(source_id)
                opportunity.timeout_counted_source_ids.append(source_id)
                sources.append(source_id)
        opportunity.timeout_count += 1
        expiry = snapshot.available_at
        if opportunity.timeout_count >= opportunity.timeout_limit:
            return TimeoutAdvance(len(snapshots_added), expiry, tuple(sources), tuple(snapshots_added))
    if active:
        return TimeoutAdvance(len(snapshots_added), expiry, tuple(sources), tuple(snapshots_added))
    for candle in sorted(evidence, key=lambda item: item.available_at):
        if candle.timeframe != "5m" or not opportunity.formation_available_at < candle.available_at <= as_of:
            continue
        if candle.time in counted_sources:
            continue
        counted_sources.add(candle.time)
        opportunity.timeout_counted_source_ids.append(candle.time)
        sources.append(candle.time)
        opportunity.timeout_count += 1
        expiry = candle.available_at
        if opportunity.timeout_count >= opportunity.timeout_limit:
            break
    return TimeoutAdvance(len(snapshots_added) + len(sources), expiry, tuple(sources), tuple(snapshots_added))


__all__ = ["DEFAULT_OPPORTUNITY_TIMEOUT_UNITS", "TimeoutAdvance", "advance_opportunity_timeout"]
