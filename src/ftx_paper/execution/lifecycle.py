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
        if observed > available or available > decision:
            raise ValueError("execution timestamps must satisfy observed_at <= available_at <= decision_at")
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


class FunctionLifecycleAdapter:
    """Bind concrete phase operations for an owner without changing phase order."""

    def __init__(self, operations: Mapping[str, Callable[[ExecutionTurn], Mapping[str, Any] | None]]) -> None:
        self.operations = operations

    def _run(self, phase: str, turn: ExecutionTurn) -> Mapping[str, Any]:
        operation = self.operations.get(phase)
        if operation is None:
            return {}
        result = operation(turn)
        return dict(result) if result is not None else {}

    def reference_publication(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.REFERENCE_PUBLICATION, turn)
    def existing_position_management(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.EXISTING_POSITION_MANAGEMENT, turn)
    def permission_and_gap(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.PERMISSION_AND_GAP, turn)
    def interaction_advancement(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.INTERACTION_ADVANCEMENT, turn)
    def opportunity_timeout(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.OPPORTUNITY_TIMEOUT, turn)
    def opportunity_formation(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.OPPORTUNITY_FORMATION, turn)
    def proposal_arbitration(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.PROPOSAL_ARBITRATION, turn)
    def execution_settlement(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.EXECUTION_SETTLEMENT, turn)
    def event_recording(self, turn: ExecutionTurn) -> Mapping[str, Any]:
        return self._run(ExecutionPhase.EVENT_RECORDING, turn)


class ExecutionLifecycleCoordinator:
    """Run one complete turn and retain only coordinator-owned lifecycle state."""

    def __init__(self) -> None:
        self.pending_entries: list[Any] = []
        self.pending_exits: list[Any] = []
        self.seen_fill_ids: set[str] = set()
        self.history: list[ExecutionPhaseEvent] = []

    def apply_fill(self, fill_id: str, operation) -> Any:
        """Apply one fill exactly once and retain the idempotency key."""
        if fill_id in self.seen_fill_ids:
            return None
        result = operation()
        self.seen_fill_ids.add(fill_id)
        return result

    def run(self, turn: ExecutionTurn, adapter: ExecutionLifecycleAdapter) -> tuple[ExecutionPhaseEvent, ...]:
        operations = (
            adapter.reference_publication,
            adapter.existing_position_management,
            adapter.permission_and_gap,
            adapter.interaction_advancement,
            adapter.opportunity_timeout,
            adapter.opportunity_formation,
            adapter.proposal_arbitration,
            adapter.execution_settlement,
            adapter.event_recording,
        )
        events: list[ExecutionPhaseEvent] = []
        for sequence, (phase, operation) in enumerate(zip(EXECUTION_PHASES, operations, strict=True), start=len(self.history)):
            events.append(ExecutionPhaseEvent(
                turn.turn_id, turn.source, phase, sequence,
                turn.observed_at, turn.available_at, turn.decision_at,
                dict(operation(turn)),
            ))
        self.history.extend(events)
        return tuple(events)


__all__ = ["EXECUTION_PHASES", "ExecutionPhase", "ExecutionLifecycleAdapter", "FunctionLifecycleAdapter", "ExecutionLifecycleCoordinator", "ExecutionPhaseEvent", "ExecutionTurn"]
