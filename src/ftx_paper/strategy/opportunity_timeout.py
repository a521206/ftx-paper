"""Causal evidence-time timeout helpers for opportunities."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
from typing import Any, Sequence


DEFAULT_OPPORTUNITY_TIMEOUT_UNITS = 6


@dataclass(frozen=True, slots=True)
class TimeoutAdvance:
    added_units: int
    expiry_at: Any | None
    counted_source_ids: tuple[str, ...]
    counted_snapshot_ids: tuple[str, ...]
    opportunity: Any | None = None


def advance_opportunity_timeout(opportunity: Any, *, interaction: Any | None, evidence: Sequence[Any], as_of: Any) -> TimeoutAdvance:
    """Count cumulative projections, then unprojected source candles after departure."""
    status = getattr(opportunity, "status", None)
    if getattr(status, "value", status) != "ACTIVE" or getattr(opportunity, "evidence_timeframe", None) != "5m":
        return TimeoutAdvance(0, None, (), (), opportunity)
    counted: set[str] = set(getattr(opportunity, "timeout_evidence_ids", ()))
    counted_snapshots: set[str] = set(getattr(opportunity, "timeout_counted_snapshot_ids", ()))
    counted_sources: set[str] = set(getattr(opportunity, "timeout_counted_source_ids", ()))
    added: list[str] = []
    added_snapshots: list[str] = []
    added_sources: list[str] = []
    expiry = None
    snapshots = dict(getattr(interaction, "synthetic_snapshots", ())).get("5m", ()) if interaction is not None else ()
    for snapshot in sorted(snapshots, key=lambda item: item.available_at):
        if snapshot.available_at > as_of or snapshot.available_at <= opportunity.setup.formed_at:
            continue
        source_ids = tuple(str(item) for item in getattr(snapshot, "source_observation_ids", ()))
        snapshot_id = ":".join((str(getattr(interaction, "interaction_id", "")), "5m", ",".join(source_ids)))
        if snapshot_id in counted_snapshots:
            continue
        counted_snapshots.add(snapshot_id)
        added_snapshots.append(snapshot_id)
        for source_id in source_ids:
            if source_id and source_id not in counted_sources:
                counted_sources.add(source_id)
                added_sources.append(source_id)
        added.append(snapshot_id)
        expiry = snapshot.available_at
        if getattr(opportunity, "timeout_count", 0) + len(added_snapshots) >= DEFAULT_OPPORTUNITY_TIMEOUT_UNITS:
            break
    updated = opportunity
    for snapshot_id in added_snapshots:
        snapshot = next(
            item for item in snapshots
            if ":".join((str(getattr(interaction, "interaction_id", "")), "5m", ",".join(str(value) for value in item.source_observation_ids))) == snapshot_id
        )
        updated = updated.advance_timeout(timeframe="5m", at=snapshot.available_at, evidence_id=snapshot_id)
        updated = replace(
            updated,
            timeout_counted_source_ids=tuple(
                (*(item for item in updated.timeout_counted_source_ids if item != snapshot_id), *(
                    item for item in added_sources
                    if item not in updated.timeout_counted_source_ids
                ))
            ),
            timeout_counted_snapshot_ids=(*updated.timeout_counted_snapshot_ids, snapshot_id),
        )
        if updated.status.value != "ACTIVE" or updated.timeout_count >= DEFAULT_OPPORTUNITY_TIMEOUT_UNITS:
            return TimeoutAdvance(
                len(added_snapshots), expiry, tuple(added_sources), tuple(added_snapshots), updated,
            )
    active = getattr(getattr(interaction, "status", None), "value", getattr(interaction, "status", None)) == "ACTIVE"
    if active:
        return TimeoutAdvance(len(added_snapshots), expiry, tuple(added_sources), tuple(added_snapshots), updated)
    for item in sorted(evidence, key=lambda value: value.available_at):
        if item.timeframe != "5m" or item.available_at > as_of or item.available_at <= opportunity.setup.formed_at:
            continue
        token = str(item.observation_id)
        if token in counted_sources or token in counted:
            continue
        counted.add(token)
        counted_sources.add(token)
        added.append(token)
        added_sources.append(token)
        expiry = item.available_at
        if len(counted) >= DEFAULT_OPPORTUNITY_TIMEOUT_UNITS:
            break
    if added and hasattr(opportunity, "advance_timeout"):
        for token in added:
            item = next((value for value in evidence if str(value.observation_id) == token), None)
            if item is not None:
                updated = updated.advance_timeout(timeframe="5m", at=item.available_at, evidence_id=token)
    return TimeoutAdvance(len(added_snapshots) + len(added_sources), expiry, tuple(added_sources), tuple(added_snapshots), updated)


__all__ = ["DEFAULT_OPPORTUNITY_TIMEOUT_UNITS", "TimeoutAdvance", "advance_opportunity_timeout"]
