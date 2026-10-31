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


__all__ = ["UTC", "NIFTY_ZONE", "coerce_timestamp", "nifty_label", "parse_nifty_label", "parse_utc", "require_utc", "serialize_utc"]
