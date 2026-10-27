"""Causal evidence-time timeout helpers for opportunities."""

from __future__ import annotations

from dataclasses import dataclass
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
    """Count only new completed evidence on the opportunity timeframe."""
    status = getattr(opportunity, "status", None)
    if getattr(status, "value", status) != "ACTIVE" or getattr(opportunity, "evidence_timeframe", None) != "5m":
        return TimeoutAdvance(0, None, (), (), opportunity)
    counted: set[str] = set(getattr(opportunity, "timeout_evidence_ids", ()))
    added: list[str] = []
    expiry = None
    for item in sorted(evidence, key=lambda value: value.available_at):
        if item.timeframe != "5m" or item.available_at > as_of or item.available_at <= opportunity.setup.formed_at:
            continue
        token = str(item.observation_id)
        if token in counted:
            continue
        counted.add(token)
        added.append(token)
        expiry = item.available_at
        if len(counted) >= DEFAULT_OPPORTUNITY_TIMEOUT_UNITS:
            break
    updated = opportunity
    if added and hasattr(opportunity, "advance_timeout"):
        for token in added:
            item = next((value for value in evidence if str(value.observation_id) == token), None)
            if item is not None:
                updated = updated.advance_timeout(timeframe="5m", at=item.available_at, evidence_id=token)
    return TimeoutAdvance(len(added), expiry, tuple(added), (), updated)


__all__ = ["DEFAULT_OPPORTUNITY_TIMEOUT_UNITS", "TimeoutAdvance", "advance_opportunity_timeout"]
