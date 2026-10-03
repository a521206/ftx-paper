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


__all__ = ["ExecutionNotification"]
