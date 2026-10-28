"""Typed execution notifications delivered back to strategies."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExecutionNotification:
    event_type: str
    client_order_id: str
    status: str
    quantity: int = 0
    filled_quantity: int = 0
    price: float | None = None
    reason: str | None = None
    broker_order_id: str | None = None
    event_id: str | None = None
    occurred_at: str | None = None
    source: str | None = None
    strategy_id: str | None = None
    account_id: str | None = None
    observed_at: str | None = None
    available_at: str | None = None

    def __post_init__(self) -> None:
        if self.observed_at is not None and self.available_at is not None:
            from datetime import datetime
            observed = datetime.fromisoformat(self.observed_at.replace("Z", "+00:00"))
            available = datetime.fromisoformat(self.available_at.replace("Z", "+00:00"))
            if available < observed:
                raise ValueError("execution notification availability cannot precede observation")


__all__ = ["ExecutionNotification"]
