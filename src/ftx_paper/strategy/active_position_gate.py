"""Pure active-position admission gate projection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ActivePositionBlock:
    session_date: str
    candidate_location: str
    blocking_timestamp: str
    active_position_id: str
    active_position_location: str | None
    active_position_direction: str
    candidate_direction: str
    candidate_lifecycle_id: str
    source_completed_bar_timestamp: str
    available_at: str
    vwap: float
    close: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "active_position_id": self.active_position_id,
            "active_position_location": self.active_position_location,
            "active_position_direction": self.active_position_direction,
            "candidate_location": self.candidate_location,
            "candidate_direction": self.candidate_direction,
            "candidate_lifecycle_id": self.candidate_lifecycle_id,
            "source_completed_bar_timestamp": self.source_completed_bar_timestamp,
            "available_at": self.available_at,
            "vwap": self.vwap,
            "close": self.close,
            "blocking_timestamp": self.blocking_timestamp,
            "block_reason": "active_position_session_extreme_gate",
        }


@dataclass
class ActivePositionGate:
    blocks: dict[tuple[str, str], list[ActivePositionBlock]] = field(default_factory=dict)
    blocked_lifecycles: set[str] = field(default_factory=set)

    def update(self, context: Any) -> None:
        positions = tuple(context.execution_engine.open_positions()) if hasattr(context, "execution_engine") else ()
        if not positions:
            self.blocks.clear()
            self.blocked_lifecycles.clear()

    def check(self, context: Any, opportunity: Any, timestamp: Any) -> dict[str, Any] | None:
        self.update(context)
        positions = tuple(context.execution_engine.open_positions()) if hasattr(context, "execution_engine") else ()
        location = str(getattr(opportunity, "location", ""))
        if not positions or location not in {"SESSION_LOW", "SESSION_HIGH"}:
            return None
        bars = tuple(getattr(context, "aggregates", {}).get("5m", ()))
        bars = tuple(item for item in bars if item.available_at <= timestamp)
        if not bars:
            return None
        volume = sum(max(0.0, float(item.volume)) for item in bars)
        vwap = sum(((float(item.high) + float(item.low) + float(item.close)) / 3.0) * max(0.0, float(item.volume)) for item in bars)
        latest = bars[-1]
        vwap = vwap / volume if volume else float(latest.close)
        active = positions[0]
        block = ActivePositionBlock(
            str(getattr(opportunity, "session_date", "")), location, str(timestamp),
            str(active.position_id), getattr(active, "location", None), str(active.direction),
            str(opportunity.direction), str(opportunity.lifecycle_id), str(latest.end_time),
            str(latest.available_at), vwap, float(latest.close),
        )
        self.blocks.setdefault((block.session_date, location), []).append(block)
        self.blocked_lifecycles.add(block.candidate_lifecycle_id)
        return block.as_dict()

    def is_blocked(self, lifecycle_id: str) -> bool:
        return lifecycle_id in self.blocked_lifecycles


__all__ = ["ActivePositionBlock", "ActivePositionGate"]
