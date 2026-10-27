"""Explicitly authorized paper-execution boundary for orderflow proposals."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
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


def next_available_bar(available_at: datetime, *, minutes: int = 1) -> datetime:
    """Return the first bar boundary on which completed evidence can execute."""
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        raise ValueError("available_at must be timezone-aware")
    if minutes <= 0:
        raise ValueError("minutes must be positive")
    return available_at + timedelta(minutes=minutes)


def entry_size_against_failure_boundary(
    *, direction: str, entry_price: float, failure_boundary: float,
    risk_budget_bp: float = 20.0,
) -> tuple[float, dict[str, float]]:
    """Normalize size so actual fill risk remains within the fixed budget."""
    if direction not in {"long", "short"} or entry_price <= 0 or risk_budget_bp <= 0:
        return 0.0, {}
    protective = (
        failure_boundary < entry_price if direction == "long"
        else failure_boundary > entry_price
    )
    actual = abs(entry_price - failure_boundary) / entry_price * 10000.0
    if not protective or actual <= 0:
        return 0.0, {"actual_risk_bp": actual}
    quantity = min(1.0, risk_budget_bp / actual)
    return quantity, {
        "actual_risk_bp": actual,
        "risk_budget_bp": risk_budget_bp,
        "risk_adjusted_quantity": quantity,
        "effective_risk_bp": actual * quantity,
    }


def arbitrate_execution_proposals(
    proposals: tuple[ExecutionProposal, ...],
) -> tuple[ExecutionProposal, ...]:
    """Keep one deterministic proposal per causal intent/instrument boundary."""
    selected: dict[tuple[str, str, str, float], ExecutionProposal] = {}
    for proposal in sorted(proposals, key=lambda item: (item.intent_id, item.instrument, item.entry_price)):
        key = (proposal.intent_id, proposal.instrument, proposal.direction, proposal.entry_price)
        selected.setdefault(key, proposal)
    return tuple(selected.values())


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
    "ExecutionStatus", "OrderflowExecutionCoordinator", "arbitrate_execution_proposals",
    "entry_size_against_failure_boundary", "next_available_bar",
]
