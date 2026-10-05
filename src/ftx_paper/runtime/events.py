"""Small, shared vocabulary and timestamp contract for runtime events."""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


DECISION_EVENT_TYPES = frozenset({
    "CANDIDATEDECISION",
    "EXITDECISION",
})

# These names are rejected on new decision events. They are retained only so
# callers get a clear validation error instead of silently storing old fields.
LEGACY_DECISION_TIME_FIELDS = (
    "minute", "bar_datetime", "bar_timestamp", "event_time", "timestamp",
)


class DecisionTimestampError(ValueError):
    """Raised when a decision event has no valid timezone-aware decision_at."""


def parse_decision_at(value: object) -> datetime:
    """Parse a decision timestamp and normalize it to IST."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise DecisionTimestampError(f"invalid decision_at: {value!r}") from exc
    else:
        raise DecisionTimestampError("decision event requires a non-empty decision_at")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DecisionTimestampError("decision_at must be timezone-aware")
    return parsed.astimezone(IST)


def serialize_datetime(value: object) -> str:
    """Serialize an aware datetime only at an external boundary."""
    if not isinstance(value, datetime):
        raise TypeError(f"expected datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("cannot serialize a timezone-naive datetime")
    return value.isoformat()


def decision_session_bucket(value: datetime) -> str:
    """Classify an already parsed decision timestamp using IST boundaries."""
    local = parse_decision_at(value)
    current = local.time()
    if current < time(10, 15):
        return "Pre"
    if current < time(11, 15):
        return "Morning"
    if current < time(13, 30):
        return "Mid"
    if current < time(14, 15):
        return "Afternoon"
    return "Post"

RISK_EVENT_TYPES = frozenset({"SIZING_REJECTED", "RISK_REJECTED"})

EXECUTION_EVENT_TYPES = frozenset({
    "ORDER_INTENT",
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
    "DecisionTimestampError",
    "EXECUTION_EVENT_TYPES",
    "IST",
    "LEGACY_DECISION_TIME_FIELDS",
    "RISK_EVENT_TYPES",
    "decision_session_bucket",
    "event_category",
    "is_decision_event",
    "parse_decision_at",
    "serialize_datetime",
]
