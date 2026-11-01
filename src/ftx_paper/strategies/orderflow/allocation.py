"""Canonical portfolio admission gates for orderflow execution."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .lifecycle import SetupOpportunity
from .timestamps import parse_causal_timestamp


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
        and ((state(setup).position is not None and str(state(setup).position.status) in {"open", "PositionStatus.OPEN"})
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
        filled = 0
        for item in event_list:
            values = item.get("values")
            event_direction = (
                values.get("direction") if isinstance(values, Mapping) else None
            )
            if (
                item.get("kind") in {"filled", "fill"}
                and (session_date is None or item.get("session_date") == session_date)
                and (item.get("direction") == direction or event_direction == direction)
            ):
                filled += 1
        pending = sum(
            setup.direction.value == direction and state(setup).reservation is not None
            for setup in setup_list
            if session_date is None or setup.session_date == session_date
        )
        if filled + pending >= constraints.max_direction_entries:
            return "direction_entry_limit"
    cooldown = constraints.post_trade_cooldown_minutes
    if timestamp is not None and cooldown:
        current = parse_causal_timestamp(timestamp, field="allocation timestamp")
        closes = [
            parse_causal_timestamp(str(item["available_at"]), field="position close availability")
            for item in events
            if item.get("kind") == "position_closed"
            and item.get("available_at") is not None
            and (session_date is None or item.get("session_date") == session_date)
        ]
        if closes and 0 <= (current - max(closes)).total_seconds() / 60 < cooldown:
            return "post_trade_cooldown"
    return None


__all__ = ["entry_block_reason"]
