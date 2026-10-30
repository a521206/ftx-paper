"""Timestamp validation for causal ICT contracts."""

from datetime import datetime


def require_aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


def require_order(*, observed_at: datetime, available_at: datetime, subject: str) -> None:
    require_aware(observed_at, name=f"{subject}.observed_at")
    require_aware(available_at, name=f"{subject}.available_at")
    if available_at < observed_at:
        raise ValueError(f"{subject} cannot be available before observation")
