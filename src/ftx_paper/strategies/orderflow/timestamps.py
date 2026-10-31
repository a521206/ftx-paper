"""One timestamp convention for Paper orderflow causal data."""

from __future__ import annotations

from datetime import datetime, timezone
import re
from zoneinfo import ZoneInfo


UTC = timezone.utc
NIFTY_ZONE = ZoneInfo("Asia/Kolkata")
_NIFTY_LABEL = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")


def require_utc(value: datetime, *, field: str = "timestamp") -> datetime:
    """Return an aware UTC instant, rejecting naive values."""
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def parse_utc(value: str, *, field: str = "timestamp") -> datetime:
    """Parse an external UTC ISO-8601 timestamp; naive values are rejected."""
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an ISO-8601 timestamp string")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp string") from exc
    return require_utc(parsed, field=field)


def parse_nifty_label(value: str, *, field: str = "NiftyZoning timestamp") -> datetime:
    """Parse a NiftyZoning local minute label at the sole ingress boundary."""
    if not isinstance(value, str) or not _NIFTY_LABEL.fullmatch(value):
        raise ValueError(f"{field} must be YYYY-MM-DDTHH:MM")
    try:
        local = datetime.strptime(value, "%Y-%m-%dT%H:%M").replace(tzinfo=NIFTY_ZONE)
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYY-MM-DDTHH:MM") from exc
    return local.astimezone(UTC)


def parse_session_timestamp(
    session_date: str, value: datetime | str, *, field: str = "session timestamp"
) -> datetime:
    """Parse a session-local minute, full local label, or persisted UTC value."""
    if isinstance(value, datetime):
        timestamp = require_utc(value, field=field)
    elif isinstance(value, str):
        text = value.strip()
        if len(text) == 5 and text[2] == ":":
            text = f"{session_date}T{text}"
        if _NIFTY_LABEL.fullmatch(text):
            timestamp = parse_nifty_label(text, field=field)
        else:
            timestamp = parse_utc(text, field=field)
    else:
        raise TypeError(f"{field} must be a datetime or timestamp string")
    if nifty_label(timestamp)[:10] != session_date:
        raise ValueError(f"{field} does not belong to session {session_date}")
    return timestamp


def session_minute(session_date: str, value: datetime | str) -> str:
    """Return a session-local minute label for any accepted session timestamp."""
    return nifty_label(parse_session_timestamp(session_date, value))


def parse_causal_timestamp(value: datetime | str, *, field: str = "timestamp") -> datetime:
    """Parse a persisted causal timestamp without accepting local labels."""
    if isinstance(value, datetime):
        return require_utc(value, field=field)
    return parse_utc(value, field=field)


def serialize_utc(value: datetime, *, field: str = "timestamp") -> str:
    """Serialize a causal instant as a UTC ISO-8601 string ending in ``Z``."""
    return require_utc(value, field=field).isoformat(timespec="microseconds").replace("+00:00", "Z")


def coerce_timestamp(value: datetime | str, *, field: str = "timestamp") -> datetime:
    """Normalize an explicit UTC value or a NiftyZoning ingress label."""
    if isinstance(value, datetime):
        return require_utc(value, field=field)
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a datetime or ISO timestamp")
    text = value.strip()
    if text.endswith("Z") or "+" in text[10:] or "-" in text[10:]:
        return parse_utc(text, field=field)
    return parse_nifty_label(text, field=field)


def nifty_label(value: datetime) -> str:
    """Return the preserved NiftyZoning session-local label for an instant."""
    return require_utc(value).astimezone(NIFTY_ZONE).strftime("%Y-%m-%dT%H:%M")


__all__ = [
    "UTC", "NIFTY_ZONE", "coerce_timestamp", "nifty_label", "parse_nifty_label",
    "parse_causal_timestamp", "parse_session_timestamp", "parse_utc", "require_utc",
    "serialize_utc", "session_minute",
]
