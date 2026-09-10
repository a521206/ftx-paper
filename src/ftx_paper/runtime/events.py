"""Small, shared vocabulary for runtime event presentation."""

from __future__ import annotations


DECISION_EVENT_TYPES = frozenset({
    "CANDIDATEDECISION",
    "ACCEPTEDDECISION",
    "REJECTEDDECISION",
})

# These names are migration-only compatibility inputs.  New decision events
# must use ``minute`` and application code must never read these fields.
LEGACY_DECISION_TIME_FIELDS = (
    "bar_datetime", "bar_timestamp", "event_time", "decision_minute", "timestamp",
)

RISK_EVENT_TYPES = frozenset({"SIZING_REJECTED"})

EXECUTION_EVENT_TYPES = frozenset({
    "ORDER_SUPPRESSED",
    "ORDER_ACK",
    "FILL",
    "ORDER_UNFILLED",
    "EXECUTION_ERROR",
    # Kept for reading old paper-trading records.
    "EXECUTEDDECISION",
})


def event_category(event_type: str) -> str:
    """Return the simple UI category for an event type."""
    normalized = str(event_type).upper()
    if normalized in DECISION_EVENT_TYPES:
        return "decision"
    if normalized in RISK_EVENT_TYPES:
        return "risk"
    if normalized in EXECUTION_EVENT_TYPES:
        return "execution"
    return "runtime"


def is_decision_event(event_type: str) -> bool:
    return str(event_type).upper() in DECISION_EVENT_TYPES


__all__ = [
    "DECISION_EVENT_TYPES",
    "EXECUTION_EVENT_TYPES",
    "RISK_EVENT_TYPES",
    "event_category",
    "is_decision_event",
]
