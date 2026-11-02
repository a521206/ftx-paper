"""Causal gate for session-extreme entries while a position is open."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .timestamps import coerce_timestamp


@dataclass
class ActivePositionGate:
    blocks: dict[tuple[str, str], list[dict[str, Any]]] = field(default_factory=dict)
    blocked_lifecycles: set[str] = field(default_factory=set)
    _cursor_session: str | None = field(default=None, init=False, repr=False)
    _cursor_index: int = field(default=0, init=False, repr=False)
    _volume: float = field(default=0.0, init=False, repr=False)
    _dollar_volume: float = field(default=0.0, init=False, repr=False)
    _latest_bar: Any | None = field(default=None, init=False, repr=False)
    _latest_vwap: float | None = field(default=None, init=False, repr=False)

    def update(self, context: Any, timestamp: str) -> None:
        timestamp_at = coerce_timestamp(timestamp, field="active-position gate timestamp")
        if not tuple(context.execution_engine.open_positions()):
            self.blocks.clear()
            self.blocked_lifecycles.clear()
            context.session_extreme_gate_blocked_lifecycles.clear()
            return
        bars = context.aggregates.get("5m", ())
        if not bars:
            return
        if self._cursor_session != bars[0].session_date:
            self._cursor_session = bars[0].session_date
            self._cursor_index = 0
            self._volume = 0.0
            self._dollar_volume = 0.0
            self._latest_bar = None
            self._latest_vwap = None
        while self._cursor_index < len(bars):
            bar = bars[self._cursor_index]
            if (
                bar.session_date != self._cursor_session
                or coerce_timestamp(bar.available_at) > timestamp_at
            ):
                break
            volume = max(0.0, float(bar.volume))
            self._volume += volume
            self._dollar_volume += ((float(bar.high) + float(bar.low) + float(bar.close)) / 3.0) * volume
            self._latest_bar = bar
            self._cursor_index += 1
        if self._latest_bar is not None:
            self._latest_vwap = self._dollar_volume / self._volume if self._volume else float(self._latest_bar.close)

    def check(self, context: Any, life: Any, timestamp: str) -> dict[str, Any] | None:
        self.update(context, timestamp)
        if str(life.location) not in {"SESSION_LOW", "SESSION_HIGH"} or not tuple(context.execution_engine.open_positions()):
            return None
        if self._latest_bar is None or self._latest_vwap is None:
            return None
        key = (str(life.session_date), str(life.location))
        existing = self.blocks.get(key, [])
        if existing and life.lifecycle_id in self.blocked_lifecycles:
            return dict(existing[-1])
        active = tuple(context.execution_engine.open_positions())[0]
        bar = self._latest_bar
        evidence = {
            "active_position_id": active.position_id,
            "active_position_location": getattr(
                context.opportunities.get(getattr(active, "lifecycle_id", None)),
                "location", None,
            ),
            "active_position_direction": active.direction,
            "candidate_lifecycle_id": life.lifecycle_id,
            "candidate_location": str(life.location),
            "candidate_direction": life.direction.value,
            "vwap": self._latest_vwap,
            "close": float(bar.close),
            "source_completed_bar_timestamp": bar.end_time,
            "available_at": bar.available_at,
            "blocking_timestamp": timestamp,
            "block_reason": "active_position_session_extreme_gate",
            "active_position_lifecycle_id": getattr(active, "lifecycle_id", None),
            "active_position_status": getattr(
                getattr(active, "status", None), "value", getattr(active, "status", None)
            ),
            "active_position_quantity": getattr(active, "quantity", None),
        }
        self.blocks.setdefault(key, []).append(evidence)
        self.blocked_lifecycles.add(life.lifecycle_id)
        context.session_extreme_gate_blocked_lifecycles.add(life.lifecycle_id)
        return dict(evidence)

    def is_blocked(self, lifecycle_id: str) -> bool:
        return lifecycle_id in self.blocked_lifecycles


__all__ = ["ActivePositionGate"]
