"""Portfolio capacity predicates used by diagnostic admission."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any


def entry_block_reason(*, setups: Iterable[Any], executions: Mapping[str, Any], events: Iterable[Mapping[str, object]], constraints: Any, direction: str | None = None, timestamp: str | None = None, session_date: str | None = None, exclude_lifecycle_id: str | None = None) -> str | None:
    setups = tuple(setups)
    active = [item for item in setups if item.lifecycle_id != exclude_lifecycle_id and (getattr(executions[item.lifecycle_id], "position", None) is not None or getattr(executions[item.lifecycle_id], "reservation", None) is not None)]
    if len(active) >= int(constraints.max_open_positions):
        return "portfolio_position_limit"
    entries = [item for item in setups if session_date is None or item.session_date == session_date if getattr(executions[item.lifecycle_id], "entry_at", None) is not None or getattr(executions[item.lifecycle_id], "reservation", None) is not None]
    if constraints.max_session_entries is not None and len(entries) >= constraints.max_session_entries:
        return "session_entry_limit"
    if direction is not None and constraints.max_direction_entries is not None:
        filled = sum(event.get("kind") == "filled" and event.get("direction") == direction for event in events)
        pending = sum(item.direction == direction and getattr(executions[item.lifecycle_id], "reservation", None) is not None for item in setups)
        if filled + pending >= constraints.max_direction_entries:
            return "direction_entry_limit"
    if timestamp is not None and constraints.post_trade_cooldown_minutes:
        current = datetime.fromisoformat(timestamp)
        closes = [datetime.fromisoformat(str(event["available_at"])) for event in events if event.get("kind") == "position_closed" and event.get("available_at") is not None]
        if closes and 0 <= (current - max(closes)).total_seconds() / 60 < constraints.post_trade_cooldown_minutes:
            return "post_trade_cooldown"
    return None


__all__ = ["entry_block_reason"]
