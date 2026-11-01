"""Canonical portfolio admission gates for orderflow execution."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from .lifecycle import SetupOpportunity


def entry_block_reason(
    *,
    setups: Iterable[SetupOpportunity],
    executions: Mapping[str, Any],
    events: Iterable[Mapping[str, object]],
    constraints: Any,
    direction: str | None = None,
    timestamp: str | None = None,
    session_date: str | None = None,
    exclude_lifecycle_id: str | None = None,
) -> str | None:
    """Return canonical first-match capacity/cooldown reason."""
    setup_list = tuple(setups)
    def state(setup: SetupOpportunity) -> Any:
        return executions[setup.lifecycle_id]
    open_or_reserved = sum(
        setup.lifecycle_id != exclude_lifecycle_id
        and ((state(setup).position is not None and state(setup).position.status == "open")
             or state(setup).reservation is not None)
        for setup in setup_list
    )
    if open_or_reserved >= constraints.max_open_positions:
        return "portfolio_position_limit"
    entries = sum(
        state(setup).entry_at is not None or state(setup).reservation is not None
        for setup in setup_list
        if session_date is None or setup.session_date == session_date
    )
    if constraints.max_session_entries is not None and entries >= constraints.max_session_entries:
        return "session_entry_limit"
    if direction is not None and constraints.max_direction_entries is not None:
        event_list = tuple(events)
        filled = sum(
            item.get("direction") == direction
            for item in event_list
            if item.get("kind") == "filled"
            and (session_date is None or item.get("session_date") == session_date)
        )
        pending = sum(
            setup.direction.value == direction and state(setup).reservation is not None
            for setup in setup_list
            if session_date is None or setup.session_date == session_date
        )
        if filled + pending >= constraints.max_direction_entries:
            return "direction_entry_limit"
    cooldown = constraints.post_trade_cooldown_minutes
    if timestamp is not None and cooldown:
        current = datetime.fromisoformat(timestamp)
        closes = [
            datetime.fromisoformat(str(item["available_at"]))
            for item in events
            if item.get("kind") == "position_closed" and item.get("available_at") is not None
        ]
        if closes and 0 <= (current - max(closes)).total_seconds() / 60 < cooldown:
            return "post_trade_cooldown"
    return None


__all__ = ["entry_block_reason"]
