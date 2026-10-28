"""The single causal execution lifecycle shared by live and replay."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Protocol


class ExecutionPhase:
    REFERENCE_PUBLICATION = "reference_publication"
    EXISTING_POSITION_MANAGEMENT = "existing_position_management"
    PERMISSION_AND_GAP = "permission_and_gap"
    INTERACTION_ADVANCEMENT = "interaction_advancement"
    OPPORTUNITY_TIMEOUT = "opportunity_timeout"
    OPPORTUNITY_FORMATION = "opportunity_formation"
    PROPOSAL_ARBITRATION = "proposal_arbitration"
    EXECUTION_SETTLEMENT = "execution_settlement"
    EVENT_RECORDING = "event_recording"


EXECUTION_PHASES = (
    ExecutionPhase.REFERENCE_PUBLICATION,
    ExecutionPhase.EXISTING_POSITION_MANAGEMENT,
    ExecutionPhase.PERMISSION_AND_GAP,
    ExecutionPhase.INTERACTION_ADVANCEMENT,
    ExecutionPhase.OPPORTUNITY_TIMEOUT,
    ExecutionPhase.OPPORTUNITY_FORMATION,
    ExecutionPhase.PROPOSAL_ARBITRATION,
    ExecutionPhase.EXECUTION_SETTLEMENT,
    ExecutionPhase.EVENT_RECORDING,
)


def _parse(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("execution timestamps must be timezone-aware")
    return parsed


@dataclass(frozen=True, slots=True)
class ExecutionTurn:
    turn_id: str
    source: str
    observed_at: str
    available_at: str
    decision_at: str
    evidence: tuple[Any, ...] = ()
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        observed, available, decision = map(_parse, (self.observed_at, self.available_at, self.decision_at))
        if observed > available:
            raise ValueError("execution availability cannot precede observation")
        if available > decision:
            raise ValueError("execution decision cannot precede availability")
        if self.source not in {"live", "replay"}:
            raise ValueError("execution source must be live or replay")


@dataclass(frozen=True, slots=True)
class ExecutionPhaseEvent:
    turn_id: str
    source: str
    phase: str
    sequence: int
    observed_at: str
    available_at: str
    decision_at: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PendingExecution:
    order: Any
    available_at: str
    reason: str

    def is_due(self, observed_at: str) -> bool:
        return _parse(observed_at) > _parse(self.available_at)


class ExecutionLifecycleAdapter(Protocol):
    def reference_publication(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...
    def existing_position_management(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...
    def permission_and_gap(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...
    def interaction_advancement(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...
    def opportunity_timeout(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...
    def opportunity_formation(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...
    def proposal_arbitration(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...
    def execution_settlement(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...
    def event_recording(self, turn: ExecutionTurn) -> Mapping[str, Any]: ...


class ExecutionLifecycleCoordinator:
    """Run one complete turn and retain only coordinator-owned lifecycle state."""

    def __init__(self) -> None:
        self.pending_entries: dict[str, PendingExecution] = {}
        self.pending_exits: dict[str, PendingExecution] = {}
        self.seen_fill_ids: set[str] = set()
        self.history: list[ExecutionPhaseEvent] = []

    def defer_entry(self, order: Any, *, available_at: str, reason: str) -> None:
        self._defer(self.pending_entries, order, available_at=available_at, reason=reason)

    def due_entries(self, observed_at: str) -> tuple[Any, ...]:
        return self._take_due(self.pending_entries, observed_at)

    def defer_exit(self, order: Any, *, available_at: str, reason: str) -> None:
        self._defer(self.pending_exits, order, available_at=available_at, reason=reason)

    def due_exits(self, observed_at: str) -> tuple[Any, ...]:
        return self._take_due(self.pending_exits, observed_at)

    @staticmethod
    def _defer(target: dict[str, PendingExecution], order: Any, *, available_at: str, reason: str) -> None:
        order_id = str(order.client_order_id)
        pending = PendingExecution(order, available_at, reason)
        existing = target.get(order_id)
        if existing is not None and existing != pending:
            raise ValueError(f"pending order identity already exists: {order_id}")
        target[order_id] = pending

    @staticmethod
    def _take_due(target: dict[str, PendingExecution], observed_at: str) -> tuple[Any, ...]:
        due = tuple(item.order for item in target.values() if item.is_due(observed_at))
        for order in due:
            target.pop(str(order.client_order_id), None)
        return due

    def apply_fill(self, fill_id: str, operation) -> Any:
        """Apply one fill exactly once and retain the idempotency key."""
        if fill_id in self.seen_fill_ids:
            return None
        result = operation()
        self.seen_fill_ids.add(fill_id)
        return result

    def run(
        self,
        turn: ExecutionTurn,
        operations: Mapping[str, Callable[[ExecutionTurn], Mapping[str, Any] | None]],
    ) -> tuple[ExecutionPhaseEvent, ...]:
        missing = [phase for phase in EXECUTION_PHASES if phase not in operations]
        extra = [phase for phase in operations if phase not in EXECUTION_PHASES]
        if missing or extra:
            raise ValueError(f"lifecycle operations must explicitly cover all phases; missing={missing}, extra={extra}")
        events: list[ExecutionPhaseEvent] = []
        for sequence, phase in enumerate(EXECUTION_PHASES, start=len(self.history)):
            operation = operations[phase]
            if not callable(operation):
                raise TypeError(f"lifecycle operation {phase!r} is not callable")
            result = operation(turn)
            events.append(ExecutionPhaseEvent(
                turn.turn_id, turn.source, phase, sequence,
                turn.observed_at, turn.available_at, turn.decision_at,
                dict(result) if result is not None else {},
            ))
        self.history.extend(events)
        return tuple(events)


__all__ = ["EXECUTION_PHASES", "ExecutionPhase", "ExecutionLifecycleAdapter", "ExecutionLifecycleCoordinator", "ExecutionPhaseEvent", "ExecutionTurn", "PendingExecution"]
