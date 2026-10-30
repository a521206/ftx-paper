"""Causal active-position gate for session-extreme entries."""

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
    """Block session-extreme candidates while any position is open."""

    blocks: dict[tuple[str, str], list[ActivePositionBlock]] = field(default_factory=dict)
    blocked_lifecycles: set[str] = field(default_factory=set)
    _cursor_session: str | None = field(default=None, init=False, repr=False)
    _cursor_index: int = field(default=0, init=False, repr=False)
    _volume: float = field(default=0.0, init=False, repr=False)
    _dollar_volume: float = field(default=0.0, init=False, repr=False)
    _latest_bar: Any | None = field(default=None, init=False, repr=False)
    _latest_vwap: float | None = field(default=None, init=False, repr=False)

    @staticmethod
    def _text(value: Any) -> str:
        raw = getattr(value, "value", value)
        return str(raw)

    @staticmethod
    def _position_location(context: Any, lifecycle_id: str | None) -> str | None:
        opportunities = getattr(context, "opportunities", {})
        lifecycle = opportunities.get(lifecycle_id) if lifecycle_id else None
        location = getattr(lifecycle, "location", None) if lifecycle is not None else None
        return str(location) if location is not None else None

    @staticmethod
    def _active_position(context: Any) -> Any | None:
        engine = getattr(context, "execution_engine", None)
        return next(iter(engine.open_positions()), None) if engine is not None else None

    def _reset(self, context: Any) -> None:
        self.blocks.clear()
        self.blocked_lifecycles.clear()
        blocked = getattr(context, "session_extreme_gate_blocked_lifecycles", None)
        if isinstance(blocked, set):
            blocked.clear()
        self._cursor_session = None
        self._cursor_index = 0
        self._volume = 0.0
        self._dollar_volume = 0.0
        self._latest_bar = None
        self._latest_vwap = None

    def update(self, context: Any, timestamp: Any | None = None) -> None:
        if self._active_position(context) is None:
            self._reset(context)
            return
        bars = tuple(getattr(context, "aggregates", {}).get("5m", ()))
        if not bars:
            return
        session = str(getattr(bars[0], "session_date", ""))
        if self._cursor_session != session:
            self._cursor_session = session
            self._cursor_index = 0
            self._volume = 0.0
            self._dollar_volume = 0.0
            self._latest_bar = None
            self._latest_vwap = None
        while self._cursor_index < len(bars):
            bar = bars[self._cursor_index]
            if str(getattr(bar, "session_date", "")) != self._cursor_session:
                break
            if timestamp is not None and getattr(bar, "available_at") > timestamp:
                break
            volume = max(0.0, float(getattr(bar, "volume", 0.0) or 0.0))
            typical_price = (
                float(getattr(bar, "high")) + float(getattr(bar, "low"))
                + float(getattr(bar, "close"))
            ) / 3.0
            self._volume += volume
            self._dollar_volume += typical_price * volume
            self._latest_bar = bar
            self._cursor_index += 1
        if self._latest_bar is not None:
            self._latest_vwap = self._dollar_volume / self._volume if self._volume else float(self._latest_bar.close)

    def check(self, context: Any, opportunity: Any, timestamp: Any) -> dict[str, Any] | None:
        self.update(context, timestamp)
        location = str(getattr(opportunity, "location", ""))
        active = self._active_position(context)
        if active is None or location not in {"SESSION_LOW", "SESSION_HIGH"}:
            return None
        if self._latest_bar is None or self._latest_vwap is None:
            return None
        lifecycle_id = str(getattr(opportunity, "lifecycle_id"))
        key = (str(getattr(opportunity, "session_date", "")), location)
        existing = self.blocks.get(key)
        if existing and lifecycle_id in self.blocked_lifecycles:
            return existing[-1].as_dict()
        block = ActivePositionBlock(
            str(getattr(opportunity, "session_date", "")), location, str(timestamp),
            str(getattr(active, "position_id")),
            self._position_location(context, getattr(active, "lifecycle_id", None)),
            self._text(getattr(active, "direction")), self._text(getattr(opportunity, "direction")),
            lifecycle_id, str(self._latest_bar.end_time), str(self._latest_bar.available_at),
            self._latest_vwap, float(self._latest_bar.close),
        )
        self.blocks.setdefault(key, []).append(block)
        self.blocked_lifecycles.add(lifecycle_id)
        blocked = getattr(context, "session_extreme_gate_blocked_lifecycles", None)
        if isinstance(blocked, set):
            blocked.add(lifecycle_id)
        return block.as_dict()

    def is_blocked(self, lifecycle_id: str) -> bool:
        return lifecycle_id in self.blocked_lifecycles


__all__ = ["ActivePositionBlock", "ActivePositionGate"]
