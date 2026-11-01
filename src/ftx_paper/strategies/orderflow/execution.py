"""Canonical intent, admission, reservation, and entry-fill state owner."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any

from .hierarchy import EntryPolicy, IntentStatus, TradeIntent, arm_intent, fill_intent, record_trigger
from .lifecycle import SetupOpportunity


class AdmissionStatus(StrEnum):
    NOT_EVALUATED = "NOT_EVALUATED"
    ALLOWED = "ALLOWED"
    DENIED = "DENIED"


class ReservationStatus(StrEnum):
    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"


@dataclass(frozen=True, slots=True)
class ExecutionAdmission:
    status: AdmissionStatus = AdmissionStatus.NOT_EVALUATED
    reason: str | None = None
    decided_at: str | None = None
    intent_id: str | None = None


@dataclass(frozen=True, slots=True)
class EntryReservation:
    reservation_id: str
    opportunity_id: str
    direction: str
    session_date: str
    reserved_at: str
    status: ReservationStatus = ReservationStatus.ACTIVE
    released_at: str | None = None
    release_reason: str | None = None

    def release(self, *, at: str, reason: str) -> "EntryReservation":
        if self.status is ReservationStatus.RELEASED:
            return self
        return replace(self, status=ReservationStatus.RELEASED, released_at=at, release_reason=reason)


@dataclass
class ExecutionState:
    opportunity_id: str
    intent: TradeIntent | None = None
    admission: ExecutionAdmission = field(default_factory=ExecutionAdmission)
    admission_history: tuple[ExecutionAdmission, ...] = ()
    reservation: EntryReservation | None = None
    reservation_history: tuple[EntryReservation, ...] = ()
    entry_at: str | None = None
    entry_attempted: bool = False
    fill_price: float | None = None
    quantity: float = 0.0


@dataclass
class ExecutionEngine:
    """Idempotent mutable execution state, before any Paper accounting call."""

    _states: dict[str, ExecutionState] = field(default_factory=dict)

    def register(self, lifecycle_id: str) -> None:
        self._states.setdefault(lifecycle_id, ExecutionState(lifecycle_id))

    def state_for(self, lifecycle_id: str) -> ExecutionState:
        try:
            return self._states[lifecycle_id]
        except KeyError as exc:
            raise KeyError(f"no execution attempt registered for {lifecycle_id}") from exc

    def current(self) -> dict[str, ExecutionState]:
        return dict(self._states)

    def update_intent(self, life: SetupOpportunity, intent: TradeIntent) -> None:
        state = self.state_for(life.lifecycle_id)
        if not life.active or intent.lifecycle_id != life.lifecycle_id:
            raise ValueError("intent does not belong to the active lifecycle")
        state.intent = intent

    def decide_admission(self, life: SetupOpportunity, admission: ExecutionAdmission) -> None:
        state = self.state_for(life.lifecycle_id)
        state.admission = admission
        if not state.admission_history or state.admission_history[-1] != admission:
            state.admission_history += (admission,)

    def admit_opportunity(self, life: SetupOpportunity, setup: Any, *, regime: Any, decided_at: str) -> TradeIntent | None:
        if setup is None:
            self.decide_admission(life, ExecutionAdmission(AdmissionStatus.DENIED, "setup_unavailable", decided_at))
            return None
        if regime is None:
            self.decide_admission(life, ExecutionAdmission(reason="regime_unresolved", decided_at=decided_at))
            return None
        if not regime.permits(life.direction.value):
            self.decide_admission(life, ExecutionAdmission(AdmissionStatus.DENIED, "regime_permission_denied", decided_at))
            return None
        direction = life.direction.value
        intent = arm_intent(intent=TradeIntent(
            intent_id=f"intent-{life.lifecycle_id}", setup_id=setup.setup_id,
            regime_version=setup.regime_version, direction=direction,
            status=IntentStatus.CONFIRMATION_FOUND, created_at=decided_at,
            trigger_price=setup.entry_zone_high if direction == "long" else setup.entry_zone_low,
            failure_boundary=setup.failure_boundary, expiry=setup.expires_at,
            confirmation=setup.required_confirmation, setup_version=setup.version,
            entry_policy=EntryPolicy.INTRABAR_CROSS, required_confirmation=setup.required_confirmation,
            lifecycle_id=life.lifecycle_id, formation_context=life.parent_context,
        ), at=decided_at)
        self.update_intent(life, intent)
        self.decide_admission(life, ExecutionAdmission(intent_id=intent.intent_id, decided_at=decided_at))
        return intent

    def select_entry(self, life: SetupOpportunity, *, price: float, available_at: str) -> TradeIntent:
        state = self.state_for(life.lifecycle_id)
        if state.entry_attempted:
            if state.intent is None:
                raise ValueError("entry intent is unavailable")
            return state.intent
        if state.intent is None or state.intent.status is not IntentStatus.ARMED:
            raise ValueError("entry selection requires an armed intent")
        triggered = record_trigger(intent=state.intent, at=available_at, price=price)
        state.intent = triggered
        state.entry_attempted = True
        reservation = EntryReservation(f"reservation:{life.lifecycle_id}", life.lifecycle_id, life.direction.value, life.session_date, available_at)
        state.reservation = reservation
        state.reservation_history += (reservation,)
        return triggered

    def fill_entry(self, life: SetupOpportunity, *, price: float, quantity: float, filled_at: str) -> TradeIntent:
        state = self.state_for(life.lifecycle_id)
        if state.intent is None or state.intent.status is not IntentStatus.TRIGGERED or state.reservation is None:
            raise ValueError("entry fill requires a triggered reserved intent")
        if quantity <= 0:
            raise ValueError("fill quantity must be positive")
        filled = fill_intent(intent=state.intent, at=filled_at)
        state.intent = filled
        state.entry_at = filled_at
        state.fill_price = price
        state.quantity = quantity
        state.reservation = None
        state.reservation_history += (state.reservation_history[-1].release(at=filled_at, reason="entry_filled"),)
        return filled


__all__ = ["AdmissionStatus", "EntryReservation", "ExecutionAdmission", "ExecutionEngine", "ExecutionState", "ReservationStatus"]
