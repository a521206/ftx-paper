"""Explicitly authorized paper-execution boundary for orderflow proposals."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
import math


class ExecutionStatus(StrEnum):
    PROPOSED = "PROPOSED"
    FILLED = "FILLED"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class ExecutionAuthorization:
    approved: bool
    reason: str


@dataclass(frozen=True, slots=True)
class ExecutionProposal:
    intent_id: str
    instrument: str
    direction: str
    quantity: int
    entry_price: float
    stop_price: float
    target_price: float

    def __post_init__(self) -> None:
        if not self.intent_id.strip() or not self.instrument.strip() or self.quantity <= 0:
            raise ValueError("execution proposal identity and quantity are required")


@dataclass(frozen=True, slots=True)
class ExecutionState:
    status: ExecutionStatus
    proposal: ExecutionProposal
    filled_price: float | None = None
    exit_price: float | None = None
    filled_at: datetime | None = None
    closed_at: datetime | None = None
    exit_reason: str | None = None


class OrderflowExecutionCoordinator:
    """Paper-only state owner; diagnostic submissions cannot mutate state."""

    def __init__(self, authorization: ExecutionAuthorization) -> None:
        self.authorization = authorization
        self._states: dict[str, ExecutionState] = {}

    def submit(self, proposal: ExecutionProposal) -> ExecutionState | None:
        if not self.authorization.approved:
            return None
        if proposal.intent_id in self._states:
            raise ValueError("duplicate execution intent")
        state = ExecutionState(ExecutionStatus.PROPOSED, proposal)
        self._states[proposal.intent_id] = state
        return state

    def fill(self, intent_id: str, price: float) -> ExecutionState:
        if not math.isfinite(price) or price <= 0:
            raise ValueError("fill price must be finite and positive")
        state = self._states[intent_id]
        if state.status is ExecutionStatus.FILLED:
            return state
        if state.status is not ExecutionStatus.PROPOSED:
            raise ValueError("only proposed execution can fill")
        updated = replace(state, status=ExecutionStatus.FILLED, filled_price=price)
        self._states[intent_id] = updated
        return updated

    def close(self, intent_id: str, price: float, *, reason: str) -> ExecutionState:
        if not math.isfinite(price) or price <= 0 or not reason.strip():
            raise ValueError("close requires a finite price and reason")
        state = self._states[intent_id]
        if state.status is ExecutionStatus.CLOSED:
            return state
        if state.status is not ExecutionStatus.FILLED:
            raise ValueError("only filled execution can close")
        updated = replace(state, status=ExecutionStatus.CLOSED, exit_price=price, exit_reason=reason)
        self._states[intent_id] = updated
        return updated

    @property
    def states(self) -> tuple[ExecutionState, ...]:
        return tuple(self._states.values())


__all__ = [
    "ExecutionAuthorization", "ExecutionProposal", "ExecutionState",
    "ExecutionStatus", "OrderflowExecutionCoordinator",
]
