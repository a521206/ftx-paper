"""Canonical orderflow execution state, calculations, and coordination."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields, is_dataclass, replace
from copy import copy
from datetime import datetime
from enum import StrEnum
from collections.abc import Mapping
from typing import Any, Protocol

from .hierarchy import (
    ExitIntent,
    IntentStatus,
    PositionManagement,
    PositionStatus,
    ResolvedExitRule,
    TradeIntent,
    TriggerObservation,
    ContextVersionRef,
    arm_intent,
    cancel_intent,
    close_position,
    fill_intent,
    open_position,
    record_trigger,
    observe_trigger,
    trigger_exit_intent,
)
from .lifecycle import OpportunityStatus, SetupOpportunity
from .templates import Direction, OpeningInteractionKind, StopPolicy
from .state_machine_types import CanonicalExecutionProposal, ReplayContext
from .timestamps import coerce_timestamp, nifty_label, parse_session_timestamp
from .allocation import entry_block_reason
from .active_position_gate import ActivePositionGate
from .response import (
    ResponseCandle,
    ResponseEvaluation,
    ResponseOutcomeStatus,
    ResponseSpec,
    evaluate_response_window,
)
from .features import daily_atr
from .interactions import InteractionSnapshot
from .state_machine_evidence import acceptance_trigger_evidence
from .state_machine_types import (
    DISPLACEMENT_RULES,
    ORDERFLOW_TRAIL_ACTIVATE_BP,
    ORDERFLOW_TRAIL_BP,
)
from .orderflow_common import build_aggregate_result
from ftx_paper.risk.trail_stop import TrailConfig, TrailTracker

SESSION_EXIT_DEADLINE = "15:10"
STRUCTURE_TIMEFRAME = "5m"
EXHAUSTION_RISK = "exhaustion_risk"


def _diagnostic_value(value: Any) -> Any:
    """Convert immutable canonical values without deepcopying mapping proxies."""
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _diagnostic_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _diagnostic_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_diagnostic_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [_diagnostic_value(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return getattr(value, "value", value)


def _opposite_open_exposure(
    context: ReplayContext, direction: Direction
) -> SetupOpportunity | None:
    return next(
        (
            item
            for item in context.opportunities.values()
            if item.direction is not direction and context.position_is_open(item)
        ),
        None,
    )


class ExecutionHost(Protocol):
    """Causal context boundary required by execution coordination."""

    context: Any
    mode: Any
    location_registry: Any

    def refresh_decision_context(
        self,
        life: SetupOpportunity,
        *,
        timestamp: str,
    ) -> Any | None: ...

    def scores_for_context_ref(
        self,
        reference: Any | None,
    ) -> tuple[float | None, float | None]: ...

    def local_scores_at(self, timestamp: str) -> tuple[float | None, float | None]: ...

    def decision_context_for_life(
        self, life: SetupOpportunity, as_of: str
    ) -> Any | None: ...

    def context_relation_for_life(
        self, life: SetupOpportunity, context: Any
    ) -> Any: ...

    def decision_context_for_location(
        self, location: str | None, *, as_of: str
    ) -> Any | None: ...

    def record_exit_decision_context(
        self,
        life: SetupOpportunity,
        rule: ResolvedExitRule,
        *,
        timestamp: str,
        reason: str,
    ) -> Any | None: ...

    def promote_episode(
        self,
        life: SetupOpportunity,
        *,
        observed_at: str,
        available_at: str,
        bar_events: list[dict[str, Any]],
    ) -> None: ...


def _target_interaction(
    machine: Any, location: str, as_of: str, *, not_before: str | None = None
) -> Any:
    as_of_at = coerce_timestamp(as_of, field="interaction as_of")
    candidates = [
        item
        for item in machine.context.interactions.values()
        if item.location == location
        and item.timeframe == "1m"
        and item.available_at <= as_of_at
    ]
    if not_before is None:
        return max(candidates, key=lambda item: item.available_at, default=None)
    not_before_at = coerce_timestamp(not_before, field="interaction not_before")
    eligible = [
        item
        for item in candidates
        if any(
            not_before_at <= candle.candle.available_at <= as_of_at
            for candle in item.candles
        )
    ]
    return min(
        eligible,
        key=lambda item: (item.available_at, item.interaction_id),
        default=None,
    )


def evaluate_exit_rule(
    machine: Any,
    life: SetupOpportunity,
    rule: ResolvedExitRule,
    *,
    timestamp: str,
    rule_valid: bool = True,
) -> str | None:
    position = machine.context.execution_engine.state_for(life.lifecycle_id).position
    if position is None or position.status is not PositionStatus.OPEN:
        return "position_not_open"
    context = machine.decision_context_for_location(rule.location, as_of=timestamp)
    scores = (context.long_score, context.short_score) if context is not None else None
    from .intent_policy import exit_admission_reason

    reason = exit_admission_reason(
        score_direction=rule.score_direction,
        score_margin=rule.score_margin,
        decision_scores=scores,
        rule_valid=rule_valid and context is not None,
    )
    machine.record_exit_decision_context(
        life, rule, timestamp=timestamp, reason=reason or "exit_admission_evaluated"
    )
    return reason


@dataclass(frozen=True, slots=True)
class EntryOrder:
    intent: TradeIntent
    available_at: str
    fill_price: float
    session_date: str | None = None


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
        if coerce_timestamp(at) < coerce_timestamp(self.reserved_at):
            raise ValueError("reservation release cannot precede reservation")
        return replace(
            self,
            status=ReservationStatus.RELEASED,
            released_at=at,
            release_reason=reason,
        )


@dataclass(frozen=True, slots=True)
class Fill:
    fill_id: str
    opportunity_id: str
    kind: str
    quantity: float
    price: float
    filled_at: str
    cost: float = 0.0

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("fill quantity must be positive")


@dataclass(frozen=True, slots=True)
class ExitExecution:
    reason: str
    position_before: PositionManagement
    position_after: PositionManagement
    price: float
    quantity: float
    order: "ExitOrder"


@dataclass(frozen=True, slots=True)
class ExitOrder:
    reason: str
    available_at: str
    requested_price: float | None = None
    execution_policy: str = "next_available_bar_open"
    quantity: float | None = None
    session_date: str | None = None
    position_id: str | None = None


@dataclass
class ExecutionState:
    opportunity_id: str
    setup: Any | None = None
    setup_available_at: str | None = None
    exit_plan_frozen: bool = False
    intent: TradeIntent | None = None
    admission: ExecutionAdmission = field(default_factory=ExecutionAdmission)
    admission_history: tuple[ExecutionAdmission, ...] = ()
    reservation: EntryReservation | None = None
    reservation_history: tuple[EntryReservation, ...] = ()
    pending_entry: EntryOrder | None = None
    pending_exit: ExitOrder | None = None
    position: PositionManagement | None = None
    entry_at: str | None = None
    exit_at: str | None = None
    exit_price: float | None = None
    realized_points: float | None = None
    fills: list[Fill] = field(default_factory=list)
    entry_attempted: bool = False
    entry_aliases: tuple[str, ...] = ()
    retest_touched: bool = False
    retest_touch_observed_at: str | None = None
    retest_touch_available_at: str | None = None
    retest_touch_evidence: dict[str, Any] | None = None
    trail_tracker: Any | None = field(default=None, repr=False)


@dataclass
class ExecutionEngine:
    _states: dict[str, ExecutionState] = field(default_factory=dict)
    _entry_owners: dict[tuple[str, str, str, float, str], str] = field(
        default_factory=dict, repr=False
    )
    _execution_group_claims: dict[str, tuple[str, str]] = field(
        default_factory=dict, repr=False
    )

    def register(self, lifecycle_id: str) -> None:
        self._states.setdefault(lifecycle_id, ExecutionState(lifecycle_id))

    @staticmethod
    def _entry_identity(
        life: SetupOpportunity,
        available_at: str,
        entry_price: float | None,
    ) -> tuple[str, str, str, float, str] | None:
        if entry_price is None:
            return None
        return (
            str(life.session_date),
            life.instrument,
            life.direction.value,
            round(float(entry_price), 6),
            str(coerce_timestamp(available_at)),
        )

    def entry_owner(
        self,
        life: SetupOpportunity,
        *,
        available_at: str,
        entry_price: float | None,
    ) -> str | None:
        identity = self._entry_identity(life, available_at, entry_price)
        return self._entry_owners.get(identity) if identity is not None else None

    def alias_entry(self, owner_id: str, alias_id: str) -> None:
        state = self._state(owner_id)
        if alias_id != owner_id and alias_id not in state.entry_aliases:
            state.entry_aliases += (alias_id,)

    def execution_group_claim(self, group_id: str | None) -> tuple[str, str] | None:
        return (
            self._execution_group_claims.get(group_id) if group_id is not None else None
        )

    def _assert_execution_group_unclaimed(self, life: SetupOpportunity) -> None:
        if self.execution_group_claim(life.execution_group_id) is not None:
            raise ValueError("execution group claim was already consumed")

    def _state(self, lifecycle_id: str) -> ExecutionState:
        try:
            return self._states[lifecycle_id]
        except KeyError as exc:
            raise KeyError(
                f"no execution attempt registered for {lifecycle_id}"
            ) from exc

    def state_for(self, lifecycle_id: str) -> ExecutionState:
        state = self._state(lifecycle_id)
        return self._detached_state(state)

    @staticmethod
    def _detached_state(state: ExecutionState) -> ExecutionState:
        return replace(
            state,
            fills=list(state.fills),
            reservation_history=tuple(state.reservation_history),
            trail_tracker=copy(state.trail_tracker)
            if state.trail_tracker is not None
            else None,
        )

    def current(self) -> dict[str, ExecutionState]:
        return {key: self.state_for(key) for key in self._states}

    def open_positions(self) -> tuple[PositionManagement, ...]:
        return tuple(
            state.position
            for state in self._states.values()
            if state.position is not None
            and state.position.status is PositionStatus.OPEN
        )

    def snapshot(self) -> dict[str, ExecutionState]:
        return self.current()

    def setup_for(self, lifecycle_id: str) -> Any | None:
        return self._state(lifecycle_id).setup

    def attach_setup(
        self, life: SetupOpportunity, setup: Any, *, available_at: str
    ) -> None:
        state = self._state(life.lifecycle_id)
        if (
            not life.active
            or getattr(setup, "lifecycle_id", life.lifecycle_id) != life.lifecycle_id
            or getattr(setup, "direction", life.direction.value) != life.direction.value
        ):
            raise ValueError("setup does not belong to the active lifecycle")
        if state.setup is not None:
            raise ValueError("an opportunity setup can only be attached once")
        if coerce_timestamp(available_at) < coerce_timestamp(
            life.formation_available_at
        ):
            raise ValueError(
                "setup cannot become available before opportunity formation"
            )
        state.setup = setup
        state.setup_available_at = available_at

    def freeze_exit_plan(
        self,
        life: SetupOpportunity,
        exit_plan: tuple[ResolvedExitRule, ...],
        *,
        available_at: str,
    ) -> None:
        state = self._state(life.lifecycle_id)
        if state.setup is None:
            raise ValueError("cannot freeze exits before attaching a setup")
        if state.exit_plan_frozen:
            raise ValueError("opportunity exit plan is already frozen")
        if coerce_timestamp(available_at) < coerce_timestamp(
            state.setup_available_at or life.formation_available_at
        ):
            raise ValueError("exit plan cannot become available before the setup")
        state.setup = replace(state.setup, exit_plan=exit_plan)
        state.exit_plan_frozen = True

    def update_decision_context(
        self, life: SetupOpportunity, reference: Any | None
    ) -> None:
        state = self._state(life.lifecycle_id)
        if state.intent is not None and state.intent.decision_context != reference:
            state.intent = replace(state.intent, decision_context=reference)

    def set_exit_intent(
        self, life: SetupOpportunity, intent: ExitIntent | None
    ) -> None:
        state = self._state(life.lifecycle_id)
        if state.position is None:
            raise ValueError("exit intent requires a position")
        if intent is not None and intent.position_id != state.position.position_id:
            raise ValueError("exit intent does not belong to the current position")
        state.position = replace(state.position, exit_intent=intent)

    def clear_exit_intent(self, life: SetupOpportunity) -> None:
        state = self._state(life.lifecycle_id)
        if state.position is not None:
            state.position = replace(state.position, exit_intent=None)

    def mark_retest_touched(
        self,
        life: SetupOpportunity,
        *,
        observed_at: str,
        available_at: str,
        evidence: dict[str, Any] | None = None,
    ) -> None:
        state = self._state(life.lifecycle_id)
        if not state.retest_touched:
            state.retest_touched = True
            state.retest_touch_observed_at = observed_at
            state.retest_touch_available_at = available_at
            state.retest_touch_evidence = evidence

    def update_position(
        self, life: SetupOpportunity, position: PositionManagement
    ) -> None:
        state = self._state(life.lifecycle_id)
        if (
            state.intent is None
            or position.lifecycle_id != life.lifecycle_id
            or position.intent_id != state.intent.intent_id
        ):
            raise ValueError("position does not belong to the lifecycle opportunity")
        state.position = position

    def update_intent(self, life: SetupOpportunity, intent: TradeIntent) -> None:
        if not life.active or intent.lifecycle_id != life.lifecycle_id:
            raise ValueError("intent does not belong to the active lifecycle")
        state = self._state(life.lifecycle_id)
        if state.intent is None or state.intent.intent_id != intent.intent_id:
            self.decide_admission(life, ExecutionAdmission())
        state.intent = intent

    def decide_admission(
        self, life: SetupOpportunity, admission: ExecutionAdmission
    ) -> None:
        state = self._state(life.lifecycle_id)
        state.admission = admission
        if not state.admission_history or state.admission_history[-1] != admission:
            state.admission_history += (admission,)

    def release_reservation(
        self, life: SetupOpportunity, *, at: str, reason: str
    ) -> None:
        state = self._state(life.lifecycle_id)
        if state.reservation is None:
            return
        released = state.reservation.release(at=at, reason=reason)
        state.reservation_history = tuple(
            released if item.reservation_id == released.reservation_id else item
            for item in state.reservation_history
        )
        state.reservation = None

    def clear_pending_entry(
        self, life: SetupOpportunity, *, at: str | None = None
    ) -> None:
        state = self._state(life.lifecycle_id)
        state.pending_entry = None
        if state.position is None and state.reservation is not None:
            self.release_reservation(
                life,
                at=max(
                    coerce_timestamp(at or life.clock or life.formation_available_at),
                    coerce_timestamp(state.reservation.reserved_at),
                ).isoformat(),
                reason="entry_cancelled",
            )

    def cancel_exit_intent(
        self, life: SetupOpportunity, *, reason: str
    ) -> ExitIntent | None:
        state = self._state(life.lifecycle_id)
        if state.position is None or state.position.exit_intent is None:
            return None
        cancelled = replace(
            state.position.exit_intent,
            status=IntentStatus.CANCELLED,
            cancel_reason=reason,
        )
        self.set_exit_intent(life, cancelled)
        return cancelled

    def apply_entry_admission(
        self,
        life: SetupOpportunity,
        admission: ExecutionAdmission,
    ) -> tuple[TradeIntent | None, bool]:
        state = self._state(life.lifecycle_id)
        previous = state.intent
        had_pending = state.pending_entry is not None
        self.decide_admission(life, admission)
        if admission.status is AdmissionStatus.DENIED:
            self.clear_pending_entry(life)
            if previous is not None and (
                previous.status.value == "ARMED" or had_pending
            ):
                self.update_intent(
                    life,
                    replace(
                        previous,
                        status=IntentStatus.CANCELLED,
                        cancel_reason=admission.reason,
                    )
                    if had_pending
                    else cancel_intent(
                        intent=previous, reason=admission.reason or "admission_denied"
                    ),
                )
        return previous, had_pending

    def reserve_entry(
        self, life: SetupOpportunity, *, reserved_at: str
    ) -> EntryReservation:
        state = self._state(life.lifecycle_id)
        if state.reservation is not None:
            return state.reservation
        reservation = EntryReservation(
            f"reservation:{life.lifecycle_id}",
            life.lifecycle_id,
            life.direction.value,
            str(life.session_date),
            reserved_at,
        )
        state.reservation = reservation
        state.reservation_history += (reservation,)
        return reservation

    def admit_entry(
        self,
        life: SetupOpportunity,
        *,
        observation: TriggerObservation,
        blocked_reason: str | None,
        available_at: str,
    ) -> TradeIntent | None:
        state = self._state(life.lifecycle_id)
        if state.intent is None:
            raise ValueError("entry admission requires an intent")
        if blocked_reason is not None:
            self.apply_entry_admission(
                life,
                ExecutionAdmission(
                    AdmissionStatus.DENIED,
                    blocked_reason,
                    available_at,
                    state.intent.intent_id,
                ),
            )
            return None
        selected = record_trigger(intent=state.intent, observation=observation)
        self.select_entry(
            life,
            EntryOrder(
                intent=selected,
                available_at=available_at,
                fill_price=observation.price or selected.trigger_price,
                session_date=str(life.session_date),
            ),
            intent=selected,
        )
        return selected

    def set_trail_tracker(self, life: SetupOpportunity, tracker: Any) -> None:
        self._state(life.lifecycle_id).trail_tracker = tracker

    def update_trail_candidate(
        self,
        life: SetupOpportunity,
        *,
        bar_open: float,
        bar_high: float,
        bar_low: float,
        bar_close: float,
    ) -> tuple[float | None, float | None, bool]:
        tracker = self._state(life.lifecycle_id).trail_tracker
        if tracker is None:
            raise ValueError("cannot update trailing state before entry")
        previous = tracker.current_trail_price
        was_active = tracker.is_active
        tracker.update(
            bar_open=bar_open,
            bar_high=bar_high,
            bar_low=bar_low,
            bar_close=bar_close,
            evaluate_exit=False,
        )
        return previous, tracker.current_trail_price, was_active

    def clear_pending_exit(self, life: SetupOpportunity) -> None:
        self._state(life.lifecycle_id).pending_exit = None

    def select_entry(
        self,
        life: SetupOpportunity,
        pending: EntryOrder,
        *,
        intent: TradeIntent | None = None,
    ) -> None:
        state = self._state(life.lifecycle_id)
        if state.entry_attempted:
            raise ValueError("execution attempt cannot select another entry")
        if state.position is not None:
            raise ValueError("execution attempt cannot select another entry")
        if intent is not None:
            self.update_intent(life, intent)
        if state.intent is None or state.intent.status is not IntentStatus.TRIGGERED:
            raise ValueError("entry selection requires a triggered intent")
        if pending.intent.intent_id != state.intent.intent_id:
            raise ValueError("pending entry does not belong to intent")
        if pending.session_date is not None and pending.session_date != str(
            life.session_date
        ):
            raise ValueError("pending entry does not belong to lifecycle session")
        if coerce_timestamp(pending.available_at) < coerce_timestamp(life.formation_available_at):
            raise ValueError(
                "entry cannot become available before opportunity formation"
            )
        state.pending_entry = pending
        state.entry_attempted = True
        self.reserve_entry(life, reserved_at=pending.available_at)
        identity = self._entry_identity(life, pending.available_at, pending.fill_price)
        if identity is not None:
            self._entry_owners.setdefault(identity, life.lifecycle_id)

    def fill_entry(
        self,
        life: SetupOpportunity,
        *,
        price: float,
        quantity: float,
        filled_at: str,
        protective_stop: float | None = None,
        exit_plan: tuple[ResolvedExitRule, ...] = (),
    ) -> PositionManagement:
        state = self._state(life.lifecycle_id)
        pending = state.pending_entry
        if (
            state.intent is None
            or state.intent.status is not IntentStatus.TRIGGERED
            or pending is None
        ):
            raise ValueError("entry fill requires a triggered pending entry")
        if state.position is not None:
            return state.position
        if quantity <= 0:
            raise ValueError("position quantity must be positive")
        self._assert_execution_group_unclaimed(life)
        filled = fill_intent(intent=state.intent, at=filled_at)
        state.intent = filled
        position = open_position(
            intent=filled,
            position_id=f"position-{filled.intent_id}",
            fill_price=price,
            quantity=quantity,
            protective_stop=protective_stop,
            exit_plan=exit_plan,
        )
        self.record_entry_fill(
            life,
            position,
            price=price,
            quantity=quantity,
            filled_at=filled_at,
        )
        return position

    def queue_exit(self, life: SetupOpportunity, order: ExitOrder) -> bool:
        state = self._state(life.lifecycle_id)
        if state.pending_exit is not None and self._exit_priority(
            order.reason
        ) >= self._exit_priority(state.pending_exit.reason):
            return False
        if order.quantity is not None and order.quantity <= 0:
            raise ValueError("exit quantity must be positive")
        if (
            order.quantity is not None
            and state.position is not None
            and order.quantity > state.position.quantity
        ):
            raise ValueError("exit quantity exceeds open position")
        if state.position is None or state.position.status is not PositionStatus.OPEN:
            raise ValueError("exit requires an open position")
        if (
            order.position_id is not None
            and order.position_id != state.position.position_id
        ):
            raise ValueError("exit order does not belong to the open position")
        if order.session_date is not None and order.session_date != str(
            life.session_date
        ):
            raise ValueError("exit order does not belong to the lifecycle session")
        state.pending_exit = order
        return True

    @staticmethod
    def _exit_priority(reason: str) -> int:
        return {
            "session_close": 0,
            "hard_stop": 1,
            "vwap_stop": 1,
            "failure_boundary": 1,
            "trail_stop": 2,
            "failed_continuation": 3,
            "partial_3r": 4,
            "fixed_target": 10,
            "conditional_exit": 20,
        }.get(reason, 99)

    def execute_pending_exit(
        self, life: SetupOpportunity, *, filled_at: str, open_price: float
    ) -> ExitExecution:
        state = self._state(life.lifecycle_id)
        order, position = state.pending_exit, state.position
        if (
            order is None
            or position is None
            or position.status is not PositionStatus.OPEN
        ):
            raise ValueError("no open pending exit to execute")
        quantity = order.quantity if order.quantity is not None else position.quantity
        if quantity <= 0 or quantity > position.quantity:
            raise ValueError("pending exit quantity is outside the open position")
        if quantity < position.quantity:
            updated = self.record_partial_exit(
                life,
                position,
                quantity=quantity,
                price=open_price,
                filled_at=filled_at,
            )
            price = open_price
            return ExitExecution(
                order.reason, position, updated, price, quantity, order
            )
        price = (
            open_price
            if order.requested_price is None
            or order.reason in {"fixed_target", "conditional_exit"}
            else order.requested_price
        )
        closed = close_position(position=position, at=filled_at, reason=order.reason)
        self.record_exit_fill(life, position, closed, price=price, filled_at=filled_at)
        return ExitExecution(
            order.reason, position, closed, price, position.quantity, order
        )

    def record_partial_exit(
        self,
        life: SetupOpportunity,
        position: PositionManagement,
        *,
        quantity: float,
        price: float,
        filled_at: str,
    ) -> PositionManagement:
        state = self._state(life.lifecycle_id)
        if (
            state.position is None
            or state.position.position_id != position.position_id
            or position.status is not PositionStatus.OPEN
            or quantity <= 0
            or quantity >= position.quantity
        ):
            raise ValueError("partial exit does not match the open lifecycle position")
        updated = replace(
            position,
            status=PositionStatus.OPEN,
            quantity=position.quantity - quantity,
            partial_exit_taken=True,
        )
        state.position = updated
        state.fills.append(
            Fill(
                f"{position.position_id}:partial:{nifty_label(coerce_timestamp(filled_at))}",
                life.lifecycle_id,
                "partial_exit",
                quantity,
                price,
                filled_at,
            )
        )
        state.pending_exit = None
        self._recalculate_realized(state, life.direction.value)
        return updated

    def record_entry_fill(
        self,
        life: SetupOpportunity,
        position: PositionManagement,
        *,
        price: float,
        quantity: float,
        filled_at: str,
    ) -> None:
        state = self._state(life.lifecycle_id)
        if (
            state.intent is None
            or state.intent.status is not IntentStatus.FILLED
            or state.pending_entry is None
            or state.reservation is None
            or state.position is not None
            or quantity <= 0
            or position.lifecycle_id != life.lifecycle_id
            or position.intent_id != state.intent.intent_id
            or position.direction != life.direction.value
        ):
            raise ValueError("entry fill does not match the pending lifecycle attempt")
        self._assert_execution_group_unclaimed(life)
        state.position = position
        state.entry_at = filled_at
        state.pending_entry = None
        self.release_reservation(life, at=filled_at, reason="entry_filled")
        state.fills.append(
            Fill(
                f"{position.position_id}:entry",
                life.lifecycle_id,
                "entry",
                quantity,
                price,
                filled_at,
            )
        )
        if life.execution_group_id is not None:
            self._execution_group_claims[life.execution_group_id] = (
                life.lifecycle_id,
                str(coerce_timestamp(filled_at)),
            )

    def record_exit_fill(
        self,
        life: SetupOpportunity,
        position: PositionManagement,
        closed_position: PositionManagement,
        *,
        price: float,
        filled_at: str,
    ) -> float | None:
        state = self._state(life.lifecycle_id)
        if (
            state.position is None
            or state.position.position_id != position.position_id
            or position.status is not PositionStatus.OPEN
            or closed_position.position_id != position.position_id
            or closed_position.status is not PositionStatus.CLOSED
        ):
            raise ValueError("exit fill does not match the open lifecycle position")
        state.position = closed_position
        state.exit_at = filled_at
        state.exit_price = price
        state.pending_exit = None
        state.fills.append(
            Fill(
                f"{position.position_id}:exit",
                life.lifecycle_id,
                "exit",
                position.quantity,
                price,
                filled_at,
            )
        )
        self.release_reservation(life, at=filled_at, reason="exit_filled")
        return self._recalculate_realized(state, life.direction.value)

    def _recalculate_realized(
        self, state: ExecutionState, direction: str
    ) -> float | None:
        entry = next((fill for fill in state.fills if fill.kind == "entry"), None)
        if entry is None:
            return None
        sign = 1 if direction == "long" else -1
        state.realized_points = sum(
            sign * (fill.price - entry.price) * fill.quantity
            for fill in state.fills
            if fill.kind in {"partial_exit", "exit"}
        )
        return state.realized_points


@dataclass(frozen=True, slots=True)
class EntryFillDecision:
    fill_price: float
    hard_stop: float | None
    hard_stop_source: str
    quantity: float
    risk_metrics: dict[str, float]


def protective_stop_price(
    direction: str, entry_price: float, failure_boundary: float
) -> tuple[float | None, str]:
    if (
        failure_boundary < entry_price
        if direction == "long"
        else failure_boundary > entry_price
    ) and entry_price != failure_boundary:
        return float(failure_boundary), "failure_boundary"
    return None, "trail_distance_fallback"


def protective_stop_distance_bp(
    direction: str,
    entry_price: float,
    failure_boundary: float,
    fallback_bp: float,
) -> tuple[float, str]:
    stop, source = protective_stop_price(direction, entry_price, failure_boundary)
    if stop is not None:
        return abs(entry_price - stop) / entry_price * 10000.0, source
    return fallback_bp, "trail_distance_fallback"


def risk_adjustment(
    *, trigger_price: float, fill_price: float, hard_stop: float | None
) -> tuple[float, dict[str, float]]:
    if hard_stop is None or trigger_price <= 0 or fill_price <= 0:
        return 0.0, {}
    actual = abs(hard_stop - fill_price) / fill_price * 10000.0
    trigger = abs(hard_stop - trigger_price) / trigger_price * 10000.0
    quantity = 0.0 if actual == 0 else min(1.0, 20.0 / actual)
    return quantity, {
        "trigger_risk_bp": trigger,
        "actual_risk_bp": actual,
        "risk_budget_bp": 20.0,
        "risk_adjusted_quantity": quantity,
        "effective_risk_bp": actual * quantity,
    }


def entry_fill_decision(
    *,
    direction: str,
    trigger_price: float,
    requested_fill_price: float,
    bar_open: float,
    failure_boundary: float,
) -> EntryFillDecision:
    fill = (
        max(requested_fill_price, bar_open)
        if direction == "long"
        else min(requested_fill_price, bar_open)
    )
    stop, source = protective_stop_price(direction, fill, failure_boundary)
    quantity, metrics = risk_adjustment(
        trigger_price=trigger_price, fill_price=fill, hard_stop=stop
    )
    return EntryFillDecision(fill, stop, source, quantity, metrics)


def validate_exit_plan(
    rules: tuple[ResolvedExitRule, ...], *, direction: str, fill_price: float
) -> tuple[tuple[ResolvedExitRule, ...], tuple[ResolvedExitRule, ...], tuple[ResolvedExitRule, ...]]:
    """Partition exits and reject a plan missing any required valid rule."""
    accepted = tuple(
        rule for rule in rules
        if rule.validation_reason(direction=direction, fill_price=fill_price) is None
    )
    rejected = tuple(rule for rule in rules if rule not in accepted)
    return accepted, rejected, tuple(rule for rule in rejected if rule.required)


def resolve_setup_exit_plan(
    host: ExecutionHost,
    life: SetupOpportunity,
    *,
    as_of: str,
) -> tuple[ResolvedExitRule, ...]:
    """Resolve template exits against context available at the decision clock."""
    setup = host.context.execution_engine.setup_for(life.lifecycle_id)
    template = getattr(setup, "template", None) if setup is not None else None
    if template is None:
        return ()
    resolved: list[ResolvedExitRule] = []
    for rule in getattr(template, "exits", ()):
        location = getattr(rule.location, "value", rule.location)
        target_level = _target_level(
            host, str(location), as_of, direction=life.direction
        )
        context = host.decision_context_for_location(location, as_of=as_of)
        source = None
        if context is not None and hasattr(context, "context_id"):
            source = ContextVersionRef(context.context_id, context.version)
        resolved.append(
            ResolvedExitRule(
                rule_id=rule.rule_id,
                location=str(location),
                mode=getattr(rule.mode, "value", str(rule.mode)),
                level=target_level,
                source_context=source,
                response=getattr(rule, "response", None),
                execution=getattr(rule, "execution", None),
                score_direction=getattr(rule, "score_direction", None),
                score_margin=getattr(rule, "score_margin", None),
                required=getattr(rule, "required", False),
                trigger_policy="touch"
                if getattr(rule, "trigger", None) == "touch_after_confirmation"
                else getattr(rule, "trigger", "touch"),
            )
        )
    return tuple(resolved)


def _target_level(
    host: ExecutionHost,
    location: str,
    as_of: str,
    *,
    direction: Direction,
) -> float | None:
    """Resolve the canonical location-band edge at the decision clock."""
    registry = host.location_registry
    if registry is None:
        return None
    record = registry.resolve_as_of(location, as_of)
    if record is None:
        return None
    return record.band_low if direction is Direction.LONG else record.band_high


def _session_vwap(
    completed_5m: list[Any], session_date: str, available_at: str
) -> tuple[float | None, Any | None]:
    bars = [
        item
        for item in completed_5m
        if item.session_date == session_date and item.available_at <= coerce_timestamp(available_at)
    ]
    if not bars:
        return None, None
    volume = sum(max(0.0, float(item.volume)) for item in bars)
    return (
        sum(
            ((float(item.high) + float(item.low) + float(item.close)) / 3.0)
            * max(0.0, float(item.volume))
            for item in bars
        )
        / volume
        if volume
        else float(bars[-1].close)
    ), bars[-1]


def _or_vwap_stop_price(
    direction: str, entry_price: float, vwap: float | None, atr: float | None
) -> tuple[float | None, str]:
    if vwap is None:
        return None, "vwap_unavailable"
    stop = (
        vwap + (0.05 * atr if atr is not None and atr > 0 else 0.0)
        if direction == "short"
        else vwap - (0.05 * atr if atr is not None and atr > 0 else 0.0)
    )
    if (direction == "long" and stop >= entry_price) or (
        direction == "short" and stop <= entry_price
    ):
        return None, "vwap_stop_invalid"
    return stop, "vwap_stop"


@dataclass(frozen=True, slots=True)
class StopUpdate:
    candidate: float | None
    source: str
    changed: bool


def _favorable_move(position: PositionManagement, row: dict[str, Any]) -> float:
    assert position.fill_price is not None
    return (
        float(row["high"]) - position.fill_price
        if position.direction == "long"
        else position.fill_price - float(row["low"])
    )


def _three_r_reached(position: PositionManagement, row: dict[str, Any]) -> bool:
    return (
        position.initial_risk_points > 0
        and _favorable_move(position, row) >= 3.0 * position.initial_risk_points
    )


def update_position_stop(
    position: PositionManagement,
    setup: Any,
    row: dict[str, Any],
    current_trailing_stop: float | None,
    current_vwap_stop: float | None,
) -> StopUpdate:
    entry = position.fill_price
    if entry is None:
        return StopUpdate(None, "unavailable", False)
    is_or_vwap = (
        setup is not None
        and getattr(getattr(setup, "template", None), "stop_policy", None)
        == StopPolicy.OR_VWAP
    )
    candidate = current_vwap_stop if is_or_vwap else current_trailing_stop
    source = "vwap" if is_or_vwap else "trailing"
    if position.post_3r_activated:
        one_r = (
            entry + position.initial_risk_points
            if position.direction == "long"
            else entry - position.initial_risk_points
        )
        locations = {
            str(item)
            for item in getattr(getattr(setup, "template", None), "locations", ())
        }
        if "OR_LOW" in locations and position.direction == "short":
            candidate = min(one_r, candidate) if candidate is not None else one_r
        elif "OR_HIGH" in locations and position.direction == "long":
            candidate = max(one_r, candidate) if candidate is not None else one_r
        else:
            candidate = (
                max(one_r, candidate)
                if position.direction == "long" and candidate is not None
                else min(one_r, candidate)
                if position.direction == "short" and candidate is not None
                else one_r
            )
        source = "post_3r_1r" if candidate == one_r else source
    current = position.protective_stop
    if candidate is None:
        return StopUpdate(current, "existing_stop_retained", False)
    tightens = current is None or (
        candidate > current if position.direction == "long" else candidate < current
    )
    return StopUpdate(
        candidate if tightens else current,
        source if tightens else "existing_stop_retained",
        tightens,
    )


def failed_continuation(
    prior: list[dict[str, Any]],
    completed1: list[Any],
    completed5: list[Any],
    *,
    direction: str,
    entry_price: float,
    failure_boundary: float,
    entry_time: str | None,
    synthetic: list[Any] | None = None,
) -> dict[str, Any] | None:
    """Detect canonical post-entry continuation failure from causal evidence."""
    if direction not in {"long", "short"} or entry_time is None or len(prior) < 2:
        return None
    risk = abs(entry_price - failure_boundary)
    if risk <= 0:
        return None
    post_entry = [item for item in prior if str(item["time"]) >= entry_time]
    if not post_entry:
        return None
    favorable = (
        max(float(item["high"]) for item in post_entry) - entry_price
        if direction == "long"
        else entry_price - min(float(item["low"]) for item in post_entry)
    )
    if favorable < risk:
        return None
    if synthetic is not None:
        if len(synthetic) < 2:
            return None
        previous, latest = synthetic[-2], synthetic[-1]
        boundary = previous.high if direction == "short" else previous.low
        broke = (
            latest.close > boundary if direction == "short" else latest.close < boundary
        )
        if not broke:
            return None
        return {
            "direction": direction,
            "synthetic_boundary": boundary,
            "synthetic_break": broke,
            "synthetic_close": latest.close,
            "favorable_excursion": favorable,
            "risk": risk,
            "evidence_time": str(latest.last_source_end),
            "evidence_available_at": str(latest.available_at),
        }
    if len(completed1) < 2 or len(completed5) < 2:
        return None
    previous1, latest1 = completed1[-2], completed1[-1]
    previous5, latest5 = completed5[-2], completed5[-1]
    if len(completed1) >= 3:
        previous1 = completed1[-3]
    if len(completed5) >= 3:
        previous5 = completed5[-3]
    one_boundary = previous1.high if direction == "short" else previous1.low
    five_boundary = previous5.high if direction == "short" else previous5.low
    one_break = (
        latest1.close > one_boundary
        if direction == "short"
        else latest1.close < one_boundary
    )
    five_break = (
        latest5.close > five_boundary
        if direction == "short"
        else latest5.close < five_boundary
    )
    if not (one_break and five_break):
        return None
    return {
        "direction": direction,
        "one_minute_boundary": one_boundary,
        "one_minute_break": one_break,
        "one_minute_close": latest1.close,
        "five_minute_boundary": five_boundary,
        "five_minute_break": five_break,
        "five_minute_close": latest5.close,
        "favorable_excursion": favorable,
        "risk": risk,
        "evidence_time": str(latest5.available_at),
        "evidence_available_at": str(latest5.available_at),
    }


@dataclass
class ExecutionCoordinator:
    """Canonical execution lifecycle coordinator."""

    machine: ExecutionHost | None = None
    engine: ExecutionEngine = field(default_factory=ExecutionEngine)
    active_position_gate: ActivePositionGate = field(default_factory=ActivePositionGate)

    @property
    def parity_streams(self) -> dict[str, list[dict[str, Any]]]:
        return self.context.parity_streams

    def _record_fill(self, fill: Fill) -> None:
        self.context.record_event({
            "kind": "filled",
            "lifecycle_id": fill.opportunity_id,
            "observed_at": fill.filled_at,
            "available_at": fill.filled_at,
            "fill_price": fill.price,
            "quantity": fill.quantity,
            "requested_price": fill.price,
        }, record_lifecycle=False)
        self.context.record_parity_fact("execution_fills", {
            "kind": fill.kind,
            "fill_id": fill.fill_id,
            "opportunity_id": fill.opportunity_id,
            "causal_parent_id": fill.opportunity_id,
            "occurred_at": fill.filled_at,
            "observed_at": fill.filled_at,
            "available_at": fill.filled_at,
            "values": {
                "fill_id": fill.fill_id,
                "opportunity_id": fill.opportunity_id,
                 "kind": fill.kind,
                 "filled_at": fill.filled_at,
                 "quantity": fill.quantity,
                "price": fill.price,
                "cost": fill.cost,
            },
            "unavailable": [],
        })

    def _initialize_trail_tracker(self, life: SetupOpportunity) -> None:
        state = self.engine.state_for(life.lifecycle_id)
        position = state.position
        setup = state.setup
        if position is None or position.fill_price is None:
            raise ValueError("trail tracker requires a filled position")
        if (
            setup is not None
            and getattr(getattr(setup, "template", None), "stop_policy", None)
            == StopPolicy.OR_VWAP
        ):
            return
        hard_stop_bp, _ = protective_stop_distance_bp(
            position.direction,
            position.fill_price,
            position.failure_boundary,
            ORDERFLOW_TRAIL_BP,
        )
        self.engine.set_trail_tracker(
            life,
            TrailTracker(
                entry_price=position.fill_price,
                direction=position.direction,
                hard_stop_bp=hard_stop_bp,
                config=TrailConfig(
                    activation_bp=ORDERFLOW_TRAIL_ACTIVATE_BP,
                    distance_bp=ORDERFLOW_TRAIL_BP,
                    breakeven_lock=True,
                ),
            ),
        )

    def settle_exit(
        self, life: SetupOpportunity, *, filled_at: str, open_price: float,
    ) -> ExitExecution:
        result = self.engine.execute_pending_exit(
            life, filled_at=filled_at, open_price=open_price
        )
        self._record_fill(self.engine.state_for(life.lifecycle_id).fills[-1])
        self.context.record_parity_fact("ledger_mutations", {
            "kind": "position_closed" if result.position_after.status is PositionStatus.CLOSED else "position_reduced",
            "identity": f"position:{life.lifecycle_id}:{filled_at}:{result.reason}",
            "causal_parent_id": life.lifecycle_id,
            "occurred_at": filled_at,
            "observed_at": filled_at,
            "available_at": filled_at,
            "values": {"lifecycle_id": life.lifecycle_id, "reason": result.reason, "quantity": result.quantity},
            "unavailable": [],
        })
        return result

    @property
    def context(self) -> ReplayContext:
        if self.machine is None:
            raise RuntimeError(
                "execution coordinator is not attached to a transition owner"
            )
        return self.machine.context

    def attach_setup(self, formation: Any) -> None:
        life, setup = formation.lifecycle, formation.setup
        if life is None or setup is None:
            raise ValueError(
                "lifecycle formation is missing canonical execution objects"
            )
        self.engine.register(life.lifecycle_id)
        self.engine.attach_setup(life, setup, available_at=str(life.formation_available_at))
        if self.machine is not None:
            self.engine.freeze_exit_plan(
                life,
                resolve_setup_exit_plan(
                    self.machine, life, as_of=str(life.formation_available_at)
                ),
                available_at=str(life.formation_available_at),
            )

    def update_decision_context(
        self, life: SetupOpportunity, reference: Any | None
    ) -> None:
        self.engine.update_decision_context(life, reference)

    def update_exit_intent(
        self, life: SetupOpportunity, intent: ExitIntent | None
    ) -> None:
        self.engine.set_exit_intent(life, intent)

    def _failed_reversal_block_reason(
        self, life: SetupOpportunity, timestamp: str, bar_events: list[dict[str, Any]]
    ) -> str | None:
        constraints = self.context.execution_constraints
        session = self.context.session
        location = str(life.location) if life.location is not None else None
        if (
            not getattr(constraints, "failed_reversal_lockout", False)
            or session is None
            or location not in {"SESSION_HIGH", "SESSION_LOW"}
        ):
            return None
        expected = "short" if location == "SESSION_HIGH" else "long"
        locked_at = getattr(session, "failed_reversal_locked_at", {}).get(location)
        locked = getattr(session, "failed_reversal_locked", set())
        if (
            location in locked
            and life.direction.value != expected
            and getattr(life, "response_evidence", None)
            and locked_at is not None
            and life.formation_available_at > locked_at
        ):
            locked.discard(location)
            session.failed_reversal_counts[location] = 0
            event = {
                "kind": "failed_reversal_lockout_reenabled",
                "location": location,
                "observed_at": timestamp,
                "available_at": life.formation_available_at,
                "lifecycle_id": life.lifecycle_id,
                "reason": "fresh_opposing_structural_response",
            }
            self.context.record_event(event)
            bar_events.append(event)
        return (
            "failed_reversal_lockout"
            if location in locked and life.direction.value == expected
            else None
        )

    def _record_failed_reversal_close(
        self, life: SetupOpportunity, reason: str, timestamp: str
    ) -> None:
        constraints = self.context.execution_constraints
        session = self.context.session
        location = str(life.location) if life.location is not None else None
        if (
            not getattr(constraints, "failed_reversal_lockout", False)
            or session is None
            or location not in {"SESSION_HIGH", "SESSION_LOW"}
            or reason != "hard_stop"
        ):
            return
        count = session.failed_reversal_counts.get(location, 0) + 1
        session.failed_reversal_counts[location] = count
        if count >= 2:
            session.failed_reversal_locked.add(location)
            session.failed_reversal_locked_at[location] = timestamp

    def admit_opportunity(
        self, life: SetupOpportunity, next_time: str, bar_events: list[dict[str, Any]]
    ) -> None:
        decision_at = nifty_label(life.formation_available_at)
        setup = self.context.execution_engine.setup_for(life.lifecycle_id)
        reason = None
        if setup is None:
            reason = "setup_unavailable"
        elif self.context.regime is None:
            self.record_admission(
                life,
                ExecutionAdmission(
                    reason="regime_unresolved",
                    decided_at=decision_at,
                ),
                bar_events,
            )
            return
        elif not self.context.regime.permits(life.direction.value):
            reason = "regime_permission_denied"
        if reason is not None:
            self.record_admission(
                life,
                ExecutionAdmission(
                    AdmissionStatus.DENIED, reason, decision_at
                ),
                bar_events,
            )
            return
        assert setup is not None
        regime = self.context.regime
        intent = arm_intent(
            intent=TradeIntent(
                f"intent-{life.lifecycle_id}",
                setup.setup_id,
                setup.regime_version,
                life.direction.value,
                IntentStatus.CONFIRMATION_FOUND,
                 decision_at,
                setup.entry_zone_high
                if life.direction is Direction.LONG
                else setup.entry_zone_low,
                setup.failure_boundary,
                setup.expires_at,
                setup.required_confirmation,
                setup_version=setup.version,
                required_confirmation=setup.required_confirmation,
                lifecycle_id=life.lifecycle_id,
                formation_context=life.parent_context,
                decision_context=life.parent_context,
            ),
            regime=regime,
            setup=setup,
            at=decision_at,
            confirmations=setup.required_confirmation,
            effective_at=decision_at,
        )
        self.engine.update_intent(life, intent)
        self.record_admission(
            life,
            ExecutionAdmission(
                reason="awaiting_entry_policy",
                intent_id=intent.intent_id,
                decided_at=decision_at,
            ),
            bar_events,
        )
        self.emit_intent_armed(
            decision_at, next_time, bar_events, life
        )

    def record_admission(
        self,
        life: SetupOpportunity,
        admission: ExecutionAdmission,
        bar_events: list[dict[str, Any]],
    ) -> None:
        self.engine.decide_admission(life, admission)
        event = {
            "kind": "admission_decided",
            "time": admission.decided_at,
            "admission": asdict(admission),
        }
        self.context.record_event(event)
        bar_events.append(event)

    def emit_intent_armed(
        self,
        timestamp: str,
        next_time: str,
        bar_events: list[dict[str, Any]],
        life: SetupOpportunity,
    ) -> None:
        intent = self.engine.state_for(life.lifecycle_id).intent
        if intent is None or self.machine is None:
            return
        long_score, short_score = self.machine.scores_for_context_ref(
            intent.formation_context
        )
        event = {
            "kind": "intent_armed",
            "level": "1m",
            "time": timestamp,
            "available_at": next_time,
            "intent_id": intent.intent_id,
            "setup_version": intent.setup_version,
            "regime_version": intent.regime_version,
            "trigger_available_at": intent.effective_at,
            "effective_at": intent.effective_at,
            "formation_context": asdict(intent.formation_context)
            if intent.formation_context is not None
            else None,
            "intent_long_score": long_score,
            "intent_short_score": short_score,
            "long_score": long_score,
            "short_score": short_score,
            "lifecycle_id": life.lifecycle_id,
            "owner": STRUCTURE_TIMEFRAME,
            "evidence_timeframe": STRUCTURE_TIMEFRAME,
        }
        self.context.record_event(event)
        bar_events.append(event)

    def advance(
        self,
        formations: tuple[Any, ...],
        *,
        decision_at: str,
        regime: Any = None,
        regime_publications: Any = (),
        triggers: Any = None,
        bar_opens: Any = None,
        decision_scores: Any = None,
        constraints: Any = None,
    ) -> dict[str, list[dict[str, Any]]]:
        for formation in formations:
            life, setup = formation.lifecycle, formation.setup
            if life is None or setup is None:
                raise ValueError(
                    "lifecycle formation is missing canonical execution objects"
                )
            if not life.active:
                continue
            formation_at = coerce_timestamp(life.formation_available_at)
            if formation_at >= coerce_timestamp(setup.expires_at):
                continue
            self.engine.register(life.lifecycle_id)
            if self.engine.setup_for(life.lifecycle_id) is None:
                self.engine.attach_setup(
                    life, setup, available_at=str(life.formation_available_at)
                )
            events: list[dict[str, Any]] = []
            self.admit_opportunity(life, decision_at, events)
            state = self.engine.state_for(life.lifecycle_id)
            decided_at = state.admission.decided_at or str(formation.available_at)
            self.context.record_parity_fact("intent_admission", {
                    "kind": "intent_admission",
                    "identity": (
                        f"intent-admission:{life.lifecycle_id}:"
                        f"{nifty_label(coerce_timestamp(decided_at))}"
                    ),
                    "causal_parent_id": life.lifecycle_id,
                    "occurred_at": decided_at,
                    "observed_at": decided_at,
                    "available_at": decided_at,
                    "values": {
                        "lifecycle_id": life.lifecycle_id,
                        "intent_id": state.admission.intent_id,
                        "status": state.admission.status.value,
                        "reason": state.admission.reason,
                    },
                    "unavailable": []
                    if state.admission.reason is None
                    else [state.admission.reason],
                })
        return self.context.parity_snapshot()

    def queue_exit_order(
        self, formation: Any, order: ExitOrder, *, observed_at: str
    ) -> bool:
        life = formation.lifecycle
        if life is None or coerce_timestamp(order.available_at) < coerce_timestamp(observed_at):
            if coerce_timestamp(order.available_at) < coerce_timestamp(observed_at):
                raise ValueError("exit order cannot be available before observation")
            return False
        queued = self.engine.queue_exit(life, order)
        if queued:
            self.context.record_parity_fact("trigger_exit", {
                    "kind": "exit_queued",
                    "identity": f"exit:{life.lifecycle_id}:{order.available_at}:{order.reason}",
                    "causal_parent_id": life.lifecycle_id,
                    "occurred_at": observed_at,
                    "observed_at": observed_at,
                    "available_at": order.available_at,
                    "values": {
                        "reason": order.reason,
                        "requested_price": order.requested_price,
                        "quantity": order.quantity,
                    },
                    "unavailable": [],
                })
            if order.reason in {
                "hard_stop",
                "vwap_stop",
                "trail_stop",
                "session_close",
                "failure_boundary",
            }:
                position = self.engine.state_for(life.lifecycle_id).position
                if position is not None and position.exit_intent is not None:
                    self.engine.clear_exit_intent(life)
        return queued

    def observe_triggers(
        self, formations: tuple[Any, ...], observations: Any, **kwargs: Any
    ) -> None:
        for formation in formations:
            life = formation.lifecycle
            if life is None:
                continue
            observation = observations.get(life.lifecycle_id)
            state = self.engine.state_for(life.lifecycle_id)
            if (
                observation is None
                or state.intent is None
                or state.entry_attempted
                or not observation.eligible
                or not observation.crossed
            ):
                continue
            self.engine.admit_entry(
                life,
                observation=observation,
                blocked_reason=None,
                available_at=str(observation.available_at),
            )

    def _complete_pending_entry_fill(
        self,
        life: SetupOpportunity,
        *,
        timestamp: str,
        causal_now: str,
        row: Mapping[str, Any],
        bar_events: list[dict[str, Any]],
        pending: EntryOrder,
    ) -> None:
        """Apply canonical pending-entry validation, RR gate, and fill."""
        triggered = pending.intent
        execution = self.engine.state_for(life.lifecycle_id)
        setup = self.engine.setup_for(life.lifecycle_id)
        fill_decision = entry_fill_decision(
            direction=life.direction.value,
            trigger_price=triggered.trigger_price,
            requested_fill_price=pending.fill_price,
            bar_open=float(row["open"]),
            failure_boundary=triggered.failure_boundary,
        )
        fill_price = fill_decision.fill_price
        hard_stop = fill_decision.hard_stop
        quantity = fill_decision.quantity
        risk_metrics = fill_decision.risk_metrics
        if hard_stop is None:
            self.engine.update_intent(
                life,
                cancel_intent(
                    intent=triggered, reason="failure_boundary_not_protective"
                ),
            )
            self.engine.clear_pending_entry(life, at=causal_now)
            event = {
                "kind": "entry_blocked_risk",
                "time": timestamp,
                "intent_id": triggered.intent_id,
                "lifecycle_id": life.lifecycle_id,
                "reason": "failure_boundary_not_protective",
            }
            self.context.record_event(event)
            bar_events.append(event)
            return
        if quantity <= 0.0:
            self.engine.update_intent(
                life,
                cancel_intent(intent=triggered, reason="zero_initial_risk"),
            )
            self.engine.clear_pending_entry(life, at=causal_now)
            event = {
                "kind": "entry_blocked_risk",
                "time": timestamp,
                "intent_id": triggered.intent_id,
                "lifecycle_id": life.lifecycle_id,
                "reason": "zero_initial_risk",
                "actual_risk_bp": risk_metrics.get("actual_risk_bp", 0.0),
            }
            self.context.record_event(event)
            bar_events.append(event)
            return
        if setup is not None and not execution.exit_plan_frozen:
            if self.machine is None:
                raise RuntimeError(
                    "execution coordinator is not attached to a transition owner"
                )
            self.engine.freeze_exit_plan(
                life,
                resolve_setup_exit_plan(self.machine, life, as_of=timestamp),
                available_at=timestamp,
            )
            setup = self.engine.setup_for(life.lifecycle_id)
        configured_exit_plan = setup.exit_plan if setup is not None else ()
        accepted, rejected, required_rejections = validate_exit_plan(
            configured_exit_plan,
            direction=life.direction.value,
            fill_price=fill_price,
        )
        for rule in rejected:
            event = {
                "kind": "exit_plan_rule_rejected",
                "rule_id": rule.rule_id,
                "location": rule.location,
                "reason": rule.validation_reason(
                    direction=life.direction.value, fill_price=fill_price
                ),
                "observed_at": timestamp,
                "available_at": timestamp,
                "lifecycle_id": life.lifecycle_id,
            }
            self.context.record_event(event)
            bar_events.append(event)
        if required_rejections:
            current = self.engine.state_for(life.lifecycle_id).intent
            if current is not None:
                self.engine.update_intent(
                    life,
                    replace(
                        current,
                        status=IntentStatus.CANCELLED,
                        cancel_reason="required_exit_plan_invalid",
                    ),
                )
            self.engine.clear_pending_entry(life, at=causal_now)
            event = {
                "kind": "entry_blocked_exit_plan",
                "intent_id": triggered.intent_id,
                "lifecycle_id": life.lifecycle_id,
                "reason": "required_exit_plan_invalid",
                "rejected_rule_ids": tuple(
                    rule.rule_id for rule in required_rejections
                ),
                "observed_at": timestamp,
                "available_at": timestamp,
            }
            self.context.record_event(event)
            bar_events.append(event)
            return
        exit_plan = accepted
        for rule in exit_plan:
            if rule.mode != "conditional" or hard_stop is None:
                continue
            target_level = rule.level
            if target_level is None:
                continue
            reward_points = (
                target_level - fill_price
                if life.direction is Direction.LONG
                else fill_price - target_level
            )
            risk_points = abs(fill_price - hard_stop)
            risk_reward = (
                reward_points / risk_points if risk_points > 0.0 else None
            )
            if risk_reward is None or risk_reward >= 1.0:
                continue
            current = self.engine.state_for(life.lifecycle_id).intent or triggered
            self.engine.update_intent(
                life,
                replace(
                    current,
                    status=IntentStatus.CANCELLED,
                    cancel_reason="conditional_exit_rr_below_one",
                ),
            )
            self.engine.clear_pending_entry(life, at=causal_now)
            event = {
                "kind": "entry_blocked_rr",
                "owner": "1m",
                "evidence_timeframe": "1m",
                "intent_id": triggered.intent_id,
                "lifecycle_id": life.lifecycle_id,
                "rule_id": rule.rule_id,
                "location": rule.location,
                "reason": "conditional_exit_rr_below_one",
                "reward_points": reward_points,
                "risk_points": risk_points,
                "risk_reward": risk_reward,
                "observed_at": timestamp,
                "available_at": timestamp,
            }
            self.context.record_event(event)
            bar_events.append(event)
            return
        self.engine.fill_entry(
            life,
            price=fill_price,
            quantity=quantity,
            protective_stop=hard_stop,
            filled_at=causal_now,
            exit_plan=exit_plan,
        )
        self._initialize_trail_tracker(life)
        self._record_fill(self.engine.state_for(life.lifecycle_id).fills[-1])
        self.context.record_parity_fact("ledger_mutations", {
            "kind": "position_opened",
            "identity": f"position:{life.lifecycle_id}:{causal_now}",
            "causal_parent_id": life.lifecycle_id,
            "occurred_at": causal_now,
            "observed_at": causal_now,
            "available_at": causal_now,
            "values": {
                "lifecycle_id": life.lifecycle_id,
                "quantity": quantity,
            },
            "unavailable": [],
        })
        for sibling in self.context.unfilled_execution_group_siblings(life):
            if sibling.active:
                sibling.supersede(
                    at=causal_now,
                    reason="execution_group_claimed",
                )
                self.context.record_event({
                    "kind": "setup_superseded",
                    "lifecycle_id": sibling.lifecycle_id,
                    "observed_at": causal_now,
                    "available_at": causal_now,
                    "reason": "execution_group_claimed",
                })

    def fill_triggered(
        self, formations: tuple[Any, ...], *, bar_open: Any, fill_at: str
    ) -> None:
        for formation in formations:
            life = formation.lifecycle
            if life is None or life.lifecycle_id not in bar_open:
                continue
            state = self.engine.state_for(life.lifecycle_id)
            pending = state.pending_entry
            if (
                pending is None
                or state.position is not None
                or coerce_timestamp(pending.available_at) > coerce_timestamp(fill_at)
                or pending.intent.status is not IntentStatus.TRIGGERED
            ):
                continue
            self._complete_pending_entry_fill(
                life,
                timestamp=fill_at,
                causal_now=fill_at,
                row={
                    "open": bar_open[life.lifecycle_id],
                    "session_date": str(life.session_date),
                },
                bar_events=[],
                pending=pending,
            )

    def fill_exit(self, formation: Any, *, filled_at: str, open_price: float) -> None:
        life = formation.lifecycle
        if life is None:
            return
        state = self.engine.state_for(life.lifecycle_id)
        if (
            state.pending_exit is not None
            and state.pending_exit.available_at <= filled_at
        ):
            self.settle_exit(life, filled_at=filled_at, open_price=open_price)

    def expire_opportunity(
        self,
        life: SetupOpportunity,
        *,
        timestamp: str,
        bar_events: list[dict[str, Any]],
        reason: str = "expired",
        timeout_event: dict[str, Any] | None = None,
    ) -> None:
        state = self.engine.state_for(life.lifecycle_id)
        intent, pending = state.intent, state.pending_entry
        cancelled = intent is not None and (
            intent.status is IntentStatus.ARMED or pending is not None
        )
        if cancelled and intent is not None:
            self.engine.update_intent(
                life, cancel_intent(intent=intent, reason="expired")
            )
        self.engine.clear_pending_entry(life)
        life.invalidate(at=timestamp, expired=True)
        event = {
            "kind": "setup_expired",
            "time": timestamp,
            "lifecycle_id": life.lifecycle_id,
            "reason": reason,
            **(timeout_event or {}),
            "response_episode_id": life.response_episode_id,
            "execution_group_id": life.execution_group_id,
        }
        self.context.record_event(event)
        bar_events.append(event)
        self._emit_entry_cancellation(
            life,
            intent,
            cancelled,
            pending,
            "expired",
            timestamp,
            timestamp,
            bar_events,
        )

    def opportunity_expired(self, life: SetupOpportunity, *, timestamp: str) -> bool:
        setup = self.engine.setup_for(life.lifecycle_id)
        return bool(
            life.active
            and setup is not None
            and coerce_timestamp(timestamp) >= coerce_timestamp(setup.expires_at)
            and not self.context.position_is_open(life)
        )

    def apply_session_deadline(
        self,
        life: SetupOpportunity,
        *,
        session_date: str,
        timestamp: str,
        bar_events: list[dict[str, Any]],
    ) -> None:
        if (
            timestamp < f"{session_date}T{SESSION_EXIT_DEADLINE}"
            or not self.context.position_is_open(life)
        ):
            return
        self._queue_exit("session_close", timestamp, life)
        event = {
            "kind": "exit_signal",
            "level": "session",
            "reason": "session_close",
            "observed_at": timestamp,
            "available_at": timestamp,
        }
        self.context.record_event(event)
        bar_events.append(event)

    def apply_permission_change(
        self, *, available_at: str, bar_events: list[dict[str, Any]]
    ) -> None:
        regime = self.context.regime
        if regime is None:
            return
        for life in tuple(self.context.opportunities.values()):
            state = self.engine.state_for(life.lifecycle_id)
            if (
                not life.active
                or
                state.intent is None
                or state.intent.status is not IntentStatus.ARMED
                or regime.permits(life.direction.value)
            ):
                continue
            self.engine.update_intent(
                life,
                cancel_intent(
                    intent=state.intent, reason="opening_gap_permission_changed"
                ),
            )
            event = {
                "kind": "intent_cancelled",
                "time": available_at,
                "available_at": available_at,
                "intent_id": state.intent.intent_id,
                "reason": "opening_gap_permission_changed",
            }
            self.context.record_event(event)
            bar_events.append(event)

    def setup_invalidated_by_close(
        self, life: SetupOpportunity, *, close: float
    ) -> bool:
        setup = self.engine.setup_for(life.lifecycle_id)
        return bool(
            life.active and setup is not None and setup.invalidated_by(close=close)
        )

    def setup_valid_for_life(self, life: SetupOpportunity, *, as_of: str) -> bool:
        setup = self.engine.setup_for(life.lifecycle_id)
        if (
            not life.active
            or life.candidate_invalidated
            or setup is None
            or not setup.active
            or coerce_timestamp(as_of) >= coerce_timestamp(setup.expires_at)
        ):
            return False
        latest = getattr(self.context, "latest_completed_5m", None)
        return (
            latest is None
            or latest.available_at > coerce_timestamp(as_of)
            or not setup.invalidated_by(close=latest.close)
        )

    def invalidate_opportunity(
        self,
        life: SetupOpportunity,
        *,
        timestamp: str,
        observed_at: str,
        available_at: str,
        bar_events: list[dict[str, Any]],
        reason: str = "setup_invalidated",
    ) -> None:
        state, setup = (
            self.engine.state_for(life.lifecycle_id),
            self.engine.setup_for(life.lifecycle_id),
        )
        boundary = getattr(setup, "failure_boundary", None) or life.failure_boundary
        if boundary is None:
            raise ValueError(
                "invalidated opportunity has no resolved invalidation level"
            )
        intent, pending = state.intent, state.pending_entry
        cancelled = intent is not None and (
            intent.status is IntentStatus.ARMED or pending is not None
        )
        if cancelled and intent is not None:
            self.engine.update_intent(
                life,
                replace(intent, status=IntentStatus.CANCELLED, cancel_reason=reason)
                if pending is not None
                else cancel_intent(intent=intent, reason=reason),
            )
        self.engine.clear_pending_entry(life)
        if self.context.position_is_open(life):
            self._queue_exit(
                "failure_boundary",
                available_at,
                life,
                requested_price=boundary,
                execution_policy="failure_boundary",
            )
        life.invalidate(at=timestamp)
        event = {
            "kind": "setup_invalidated",
            "observed_at": observed_at,
            "available_at": available_at,
            "lifecycle_id": life.lifecycle_id,
            "boundary": boundary,
            "reason": reason,
            "response_episode_id": life.response_episode_id,
            "execution_group_id": life.execution_group_id,
        }
        self.context.record_event(event)
        bar_events.append(event)
        self._emit_entry_cancellation(
            life,
            intent,
            cancelled,
            pending,
            reason,
            timestamp,
            available_at,
            bar_events,
        )

    def _emit_entry_cancellation(
        self,
        life: SetupOpportunity,
        intent: TradeIntent | None,
        cancelled: bool,
        pending: EntryOrder | None,
        reason: str,
        observed_at: str,
        available_at: str,
        bar_events: list[dict[str, Any]],
    ) -> None:
        for event in (
            (
                {
                    "kind": "intent_cancelled",
                    "time": observed_at,
                    "intent_id": intent.intent_id,
                    "reason": reason,
                }
                if intent is not None and cancelled
                else None
            ),
            (
                {
                    "kind": "entry_cancelled",
                    "intent_id": pending.intent.intent_id,
                    "observed_at": observed_at,
                    "available_at": available_at,
                    "reason": reason,
                }
                if pending is not None
                else None
            ),
        ):
            if event is not None:
                self.context.record_event(event)
                bar_events.append(event)

    def _queue_exit(
        self,
        reason: str,
        available_at: str,
        life: SetupOpportunity,
        *,
        requested_price: float | None = None,
        execution_policy: str = "next_available_bar_open",
        quantity: float | None = None,
    ) -> None:
        position = self.engine.state_for(life.lifecycle_id).position
        self.engine.queue_exit(
            life,
            ExitOrder(
                reason,
                available_at,
                requested_price,
                execution_policy,
                quantity,
                str(life.session_date),
                position.position_id if position else None,
            ),
        )
        if reason in {
            "hard_stop",
            "vwap_stop",
            "trail_stop",
            "session_close",
            "failure_boundary",
        }:
            current = self.engine.state_for(life.lifecycle_id).position
            if current is not None and current.exit_intent is not None:
                self.engine.clear_exit_intent(life)

    def queue_exit(
        self,
        reason: str,
        available_at: str,
        life: SetupOpportunity,
        *,
        requested_price: float | None = None,
        execution_policy: str = "next_available_bar_open",
        quantity: float | None = None,
    ) -> None:
        self._queue_exit(
            reason,
            available_at,
            life,
            requested_price=requested_price,
            execution_policy=execution_policy,
            quantity=quantity,
        )

    def exit_response_evaluation(
        self,
        rule: ResolvedExitRule,
        *,
        interaction: InteractionSnapshot,
        entry_at: str,
        as_of: str,
    ) -> ResponseEvaluation[Any]:
        response = getattr(rule, "response", None)
        if response is None:
            return ResponseEvaluation(ResponseOutcomeStatus.CONFIRMED, False, (), None)
        candles = tuple(
            item.candle
            for item in interaction.candles
            if item.candle.available_at >= coerce_timestamp(entry_at)
            and item.candle.available_at <= coerce_timestamp(as_of)
        )
        return evaluate_response_window(
            candles,
            to_candle=lambda c: ResponseCandle(c.open, c.high, c.low, c.close),
            spec_for=lambda _: ResponseSpec(
                response.direction, response.response_type, interaction.band
            ),
            armed=False,
        )

    @staticmethod
    def exit_rule_triggered(
        rule: ResolvedExitRule,
        *,
        interaction: InteractionSnapshot,
        direction: Direction,
        row: dict[str, Any],
    ) -> bool:
        return (
            float(row["close"]) >= interaction.band_low
            if rule.trigger_policy == "close_confirmation"
            and direction is Direction.LONG
            else float(row["close"]) <= interaction.band_high
            if rule.trigger_policy == "close_confirmation"
            else float(row["high"]) >= interaction.band_low
            if direction is Direction.LONG
            else float(row["low"]) <= interaction.band_high
        )

    def handle_execution(
        self,
        index: int,
        timestamp: str,
        next_time: str,
        row: dict[str, Any],
        predicates: list[Any],
        bar_events: list[dict[str, Any]],
        life: SetupOpportunity,
        *,
        evaluate_entries: bool = True,
    ) -> None:
        state = self.engine.state_for(life.lifecycle_id)
        if not life.active and state.position is None:
            return
        if self.machine is None or not evaluate_entries:
            if (
                state.pending_entry is not None
                and state.position is None
                and coerce_timestamp(state.pending_entry.available_at) <= coerce_timestamp(timestamp)
            ):
                pending = state.pending_entry
                if (
                    pending.session_date not in {None, str(row.get("session_date", life.session_date))}
                    or coerce_timestamp(pending.intent.expiry) <= coerce_timestamp(timestamp)
                ):
                    return
                self._complete_pending_entry_fill(
                    life,
                    timestamp=timestamp,
                    causal_now=timestamp,
                    row=row,
                    bar_events=bar_events,
                    pending=pending,
                )
            return
        if (
            state.intent is None
            or state.intent.status is not IntentStatus.ARMED
            or state.position is not None
        ):
            return
        if coerce_timestamp(timestamp) >= coerce_timestamp(state.intent.expiry):
            cancelled = cancel_intent(intent=state.intent, reason="expired")
            self.engine.update_intent(life, cancelled)
            event = {
                "kind": "intent_cancelled",
                "intent_id": cancelled.intent_id,
                "lifecycle_id": life.lifecycle_id,
                "time": timestamp,
                "reason": "expired",
            }
            self.context.record_event(event)
            bar_events.append(event)
            return
        setup = self.engine.setup_for(life.lifecycle_id)
        if setup is None or not self.setup_valid_for_life(life, as_of=timestamp):
            return
        if (
            life.opening_policy_version is not None
            and life.opening_interaction_kind
            not in {
                OpeningInteractionKind.OPENING_BREAKOUT.value,
                OpeningInteractionKind.OPENING_RETEST.value,
            }
        ):
            return
        if (
            not getattr(life, "episode_confirmed", True)
            and coerce_timestamp(timestamp) <= coerce_timestamp(life.formation_available_at)
        ):
            return
        decision_context = self.machine.decision_context_for_life(life, next_time)
        decision_relation = (
            self.machine.context_relation_for_life(life, decision_context)
            if decision_context is not None
            else None
        )
        observation = observe_trigger(
            intent=state.intent,
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            at=timestamp,
            available_at=next_time,
            regime=self.context.regime,
            setup=setup,
            setup_available_at=state.setup_available_at or life.formation_available_at,
            setup_valid=(
                not setup.invalidated_by(close=float(row["close"]))
                if not getattr(life, "episode_confirmed", True)
                else self.setup_valid_for_life(life, as_of=timestamp)
            ),
            context_relation=decision_relation,
            template=setup.template,
            retest_touched=state.retest_touched,
        )
        trigger_evidence = [
            bar.to_dict() for bar in build_aggregate_result([row], 1).aggregates
        ]
        if (
            observation.eligible
            and setup.template is not None
            and setup.template.requires_displacement
            and life.opening_interaction_kind is None
        ):
            prior_rows = self.context.rows[
                max(0, index - DISPLACEMENT_RULES.baseline_bars):index
            ]
            displacement = acceptance_trigger_evidence(
                row,
                prior_rows,
                self.context.prior_session_rows,
                life.direction,
                include_diagnostics=True,
            )
            event = {
                "kind": "acceptance_trigger_observed",
                "time": timestamp,
                "available_at": next_time,
                "qualified": displacement["qualified"],
                "evidence": displacement,
            }
            self.context.record_event(event)
            bar_events.append(event)
            if not displacement["qualified"]:
                observation = replace(
                    observation,
                    eligible=False,
                    reason="acceptance displacement did not qualify",
                    ineligible_reason="acceptance_displacement_unqualified",
                )
        if observation.retest_touched:
            self.engine.mark_retest_touched(
                life,
                observed_at=timestamp,
                available_at=next_time,
                evidence=trigger_evidence[0],
            )
            life.mark_retest(
                at=next_time,
                confirmed=observation.retest_confirmed,
                evidence={
                    "intent_id": state.intent.intent_id,
                    "reason": observation.reason,
                },
            )
        event = {
            "kind": "trigger_observation",
            "evidence": trigger_evidence,
            "level": "1m",
            "intent_id": state.intent.intent_id,
            "lifecycle_id": life.lifecycle_id,
            "setup_id": state.intent.setup_id,
            "location": life.location,
            "template_id": getattr(setup.template, "template_id", None),
            "stop_policy": getattr(setup.template, "stop_policy", ""),
            "observed_at": timestamp,
            "available_at": next_time,
            "crossed": observation.crossed,
            "eligible": observation.eligible,
            "ineligible_reason": observation.ineligible_reason,
            "retest_touched": observation.retest_touched,
            "retest_confirmed": observation.retest_confirmed,
            "retest_touch_observed_at": state.retest_touch_observed_at,
            "retest_touch_available_at": state.retest_touch_available_at,
            "retest_touch_evidence": state.retest_touch_evidence,
            "reason": observation.reason,
            "owner": "1m",
            "evidence_timeframe": "1m",
        }
        self.context.record_event(event)
        bar_events.append(event)
        if observation.eligible:
            if not life.episode_confirmed and self.machine is not None:
                self.machine.promote_episode(
                    life,
                    observed_at=timestamp,
                    available_at=next_time,
                    bar_events=bar_events,
                )
            self.update_decision_context(
                life, getattr(decision_context, "reference", None)
            )
            self.context.record_decision_context({
                "lifecycle_id": life.lifecycle_id,
                "intent_id": state.intent.intent_id,
                "observed_at": timestamp,
                "available_at": next_time,
                "context": _diagnostic_value(decision_context)
                if decision_context is not None else None,
                "relation": getattr(decision_relation, "value", decision_relation),
            })
            proposal_event = {
                "kind": "execution_proposal_created",
                "lifecycle_id": life.lifecycle_id,
                "intent_id": state.intent.intent_id,
                "observed_at": timestamp,
                "available_at": next_time,
                "decision_context": (
                    _diagnostic_value(decision_context)
                    if decision_context is not None else None
                ),
                "context_relation": getattr(
                    decision_relation, "value", decision_relation
                ),
            }
            self.context.record_event(proposal_event)
            bar_events.append(proposal_event)
            self.context.execution_proposals.append(
                CanonicalExecutionProposal(
                    life.lifecycle_id,
                    state.intent.intent_id,
                    observation,
                    state.setup_available_at or life.formation_available_at,
                    life.direction,
                    0 if life.status is OpportunityStatus.FORMED else 1,
                )
            )

    def select_execution_proposals(
        self, timestamp: str, next_time: str, bar_events: list[dict[str, Any]]
    ) -> None:
        proposals = sorted(
            self.context.execution_proposals,
            key=lambda p: (
                str(p.setup_available_at),
                p.structural_rank,
                p.lifecycle_id,
                p.intent_id,
            ),
        )
        for proposal in proposals:
            life = self.context.opportunities.get(proposal.lifecycle_id)
            if life is None or not life.active:
                continue
            state = self.engine.state_for(life.lifecycle_id)
            setup = self.engine.setup_for(life.lifecycle_id)
            if (
                life.candidate_invalidated
                or not getattr(life, "episode_confirmed", True)
                or setup is None
                or not setup.active
                or state.entry_attempted
                or state.intent is None
                or state.intent.intent_id != proposal.intent_id
                or state.intent.status is not IntentStatus.ARMED
                or life.lifecycle_id
                in getattr(self.context, "regime_gate_blocked_lifecycles", set())
                or life.lifecycle_id
                in getattr(
                    self.context, "session_extreme_gate_blocked_lifecycles", set()
                )
                or self.active_position_gate.is_blocked(life.lifecycle_id)
            ):
                continue
            duplicate_owner = self.engine.entry_owner(
                life,
                available_at=next_time,
                entry_price=proposal.observation.price
                or state.intent.trigger_price,
            )
            if duplicate_owner is not None and duplicate_owner != life.lifecycle_id:
                self.engine.alias_entry(duplicate_owner, life.lifecycle_id)
                self.engine.apply_entry_admission(
                    life,
                    ExecutionAdmission(
                        AdmissionStatus.DENIED,
                        "duplicate_causal_identity",
                        next_time,
                        proposal.intent_id,
                    ),
                )
                event = {
                    "kind": "execution_proposal_suppressed",
                    "intent_id": proposal.intent_id,
                    "lifecycle_id": life.lifecycle_id,
                    "execution_owner_lifecycle_id": duplicate_owner,
                    "observed_at": timestamp,
                    "available_at": next_time,
                    "reason": "duplicate_causal_identity",
                }
                self.context.record_event(event)
                bar_events.append(event)
                continue
            blocked = None
            gate_evidence = None
            if not getattr(life, "episode_confirmed", True):
                blocked = "response_episode_unconfirmed"
            elif self.engine.entry_owner(
                life,
                available_at=next_time,
                entry_price=proposal.observation.price,
            ) not in {None, life.lifecycle_id}:
                blocked = "entry_already_owned"
            elif (gate_evidence := self.active_position_gate.check(
                self.context, life, timestamp
            )) is not None:
                blocked = "active_position_session_extreme_gate"
            if str(life.location) in {"SESSION_LOW", "SESSION_HIGH"}:
                self.context.execution_diagnostics.append({
                    "kind": "active_position_gate_evaluated",
                    "lifecycle_id": life.lifecycle_id,
                    "location": str(life.location),
                    "observed_at": timestamp,
                    "available_at": next_time,
                    "active_positions": [
                        {
                            "position_id": position.position_id,
                            "lifecycle_id": getattr(position, "lifecycle_id", None),
                            "direction": position.direction,
                        }
                        for position in self.engine.open_positions()
                    ],
                    "gate_result": blocked,
                    "blocking_reason": (
                        gate_evidence.get("block_reason")
                        if gate_evidence is not None else None
                    ),
                    "gate_evidence": gate_evidence,
                    "gate_state": {
                        "bars_count": len(self.context.aggregates.get("5m", ())),
                        "latest_bar_end_time": getattr(
                            getattr(self.active_position_gate, "_latest_bar", None),
                            "end_time", None,
                        ),
                        "latest_bar_available_at": getattr(
                            getattr(self.active_position_gate, "_latest_bar", None),
                            "available_at", None,
                        ),
                        "latest_vwap": getattr(
                            self.active_position_gate, "_latest_vwap", None
                        ),
                    },
                })
            if blocked is None:
                blocked = self._failed_reversal_block_reason(life, timestamp, bar_events)
            regime_gate = getattr(self.context, "regime_gate", None)
            if blocked is None and regime_gate is not None and regime_gate.blocks(
                getattr(self.context, "rows", ()),
                timestamp,
                life.direction.value,
                str(life.location) if life.location is not None else None,
            ):
                blocked = "causal_trend_mean_reversion_gate"
                blocked_lifecycles = getattr(
                    self.context, "regime_gate_blocked_lifecycles", None
                )
                if blocked_lifecycles is not None:
                    blocked_lifecycles.add(life.lifecycle_id)
            if blocked is None:
                blocked = entry_block_reason(
                    setups=self.context.opportunities.values(),
                    executions=self.engine.current(),
                    events=self.context.events,
                    constraints=self.context.execution_constraints,
                    direction=life.direction.value,
                    timestamp=timestamp,
                    session_date=str(life.session_date),
                )
            reversal_from: SetupOpportunity | None = None
            if blocked == "portfolio_position_limit":
                opposite = _opposite_open_exposure(self.context, life.direction)
                if opposite is not None:
                    blocked = entry_block_reason(
                        setups=self.context.opportunities.values(),
                        executions=self.engine.current(),
                        events=self.context.events,
                        constraints=self.context.execution_constraints,
                        direction=life.direction.value,
                        timestamp=timestamp,
                        session_date=str(life.session_date),
                        exclude_lifecycle_id=opposite.lifecycle_id,
                    )
                    if blocked is None:
                        reversal_from = opposite
            selected = self.engine.admit_entry(
                life,
                observation=proposal.observation,
                blocked_reason=blocked,
                available_at=next_time,
            )
            if selected is not None:
                if reversal_from is not None:
                    self.queue_exit(
                        "position_reversal",
                        next_time,
                        reversal_from,
                        execution_policy="next_available_bar_open",
                    )
                    reversal_event = {
                        "kind": "position_reversal_requested",
                        "time": timestamp,
                        "available_at": next_time,
                        "lifecycle_id": reversal_from.lifecycle_id,
                        "from_lifecycle_id": reversal_from.lifecycle_id,
                        "to_lifecycle_id": life.lifecycle_id,
                        "direction": life.direction.value,
                    }
                    self.context.record_event(reversal_event)
                    bar_events.append(reversal_event)
                for opposing in self.context.conflicting_unfilled_opportunities(life):
                    if opposing.active:
                        opposing.supersede(at=timestamp, reason="conflicting_entry_selected")
                        self.context.record_event({
                            "kind": "setup_superseded",
                            "lifecycle_id": opposing.lifecycle_id,
                            "observed_at": timestamp,
                            "available_at": timestamp,
                            "reason": "conflicting_entry_selected",
                        })
                event = {
                    "kind": "entry_pending",
                    "time": timestamp,
                    "available_at": next_time,
                    "intent_id": selected.intent_id,
                    "lifecycle_id": life.lifecycle_id,
                    "requested_price": selected.trigger_price,
                }
                self.context.record_event(event)
                bar_events.append(event)
            else:
                event = {
                    "kind": "admission_decided",
                    "time": timestamp,
                    "available_at": next_time,
                    "admission": asdict(
                        self.engine.state_for(life.lifecycle_id).admission
                    ),
                }
                self.context.record_event(event)
                bar_events.append(event)
        self.context.execution_proposals.clear()

    def handle_management(
        self,
        index: int,
        timestamp: str,
        next_time: str,
        row: dict[str, Any],
        available5: list[Any],
        bar_events: list[dict[str, Any]],
        life: SetupOpportunity,
        defer_discretionary_execution: bool = False,
    ) -> None:
        state = self.engine.state_for(life.lifecycle_id)
        if not life.active and state.position is None:
            return
        context = self.context
        pending = state.pending_exit
        if (
            pending is not None
            and context.position_is_open(life)
            and coerce_timestamp(pending.available_at) <= coerce_timestamp(timestamp)
            and (
                not defer_discretionary_execution
                or pending.reason != "conditional_exit"
            )
        ):
            reason = pending.reason
            if (
                reason == "conditional_exit"
                and state.position is not None
                and state.position.exit_intent is not None
            ):
                intent = state.position.exit_intent
                rule = next(
                    (
                        item
                        for item in state.position.exit_plan
                        if item.rule_id == intent.rule_id
                    ),
                    None,
                )
                if rule is None:
                    self.engine.clear_pending_exit(life)
                    return self.handle_management(
                        index, timestamp, next_time, row, available5, bar_events, life
                    )
                if coerce_timestamp(timestamp) >= coerce_timestamp(intent.expiry):
                    cancelled = self.engine.cancel_exit_intent(life, reason="expired")
                    self.engine.clear_pending_exit(life)
                    if cancelled is not None:
                        event = {
                            "kind": "exit_intent_cancelled",
                            "intent_id": cancelled.intent_id,
                            "position_id": cancelled.position_id,
                            "rule_id": cancelled.rule_id,
                            "reason": "expired",
                            "lifecycle_id": life.lifecycle_id,
                            "observed_at": timestamp,
                            "available_at": timestamp,
                        }
                        context.record_event(event, record_lifecycle=False)
                        bar_events.append(event)
                    return self.handle_management(
                        index, timestamp, next_time, row, available5, bar_events, life
                    )
                admission = (
                    evaluate_exit_rule(self.machine, life, rule, timestamp=timestamp)
                    if self.machine is not None
                    else None
                )
                if admission is not None:
                    self.engine.set_exit_intent(
                        life,
                        replace(
                            intent,
                            status=IntentStatus.CONFIRMATION_FOUND,
                            triggered_at=None,
                        ),
                    )
                    self.engine.clear_pending_exit(life)
                    event = {
                        "kind": "exit_decision",
                        "action": "hold",
                        "intent_id": intent.intent_id,
                        "position_id": intent.position_id,
                        "rule_id": intent.rule_id,
                        "reason": f"delayed_revalidation:{admission}",
                        "lifecycle_id": life.lifecycle_id,
                        "observed_at": timestamp,
                        "available_at": timestamp,
                    }
                    context.record_event(event, record_lifecycle=False)
                    bar_events.append(event)
                    return self.handle_management(
                        index, timestamp, next_time, row, available5, bar_events, life
                    )
            execution = self.settle_exit(
                life, filled_at=timestamp, open_price=float(row["open"])
            )
            self._record_failed_reversal_close(life, reason, timestamp)
            event = {
                "kind": "exit_order",
                "time": timestamp,
                "reason": reason,
                "requested_price": execution.order.requested_price,
                "fill_price": execution.price,
                "lifecycle_id": life.lifecycle_id,
                "execution_assumptions": execution.order.execution_policy,
            }
            context.record_event(event, record_lifecycle=False)
            bar_events.append(event)
            after = self.engine.state_for(life.lifecycle_id)
            if execution.position_after.status is PositionStatus.OPEN:
                partial = {
                    "kind": "partial_exit_order",
                    "position_id": execution.position_before.position_id,
                    "time": timestamp,
                    "reason": reason,
                    "fill_price": execution.price,
                    "quantity": execution.quantity,
                    "remaining_quantity": execution.position_after.quantity,
                    "risk_reward": 3.0,
                    "execution_assumptions": execution.order.execution_policy,
                    "lifecycle_id": life.lifecycle_id,
                }
                context.record_event(partial, record_lifecycle=False)
                bar_events.append(partial)
            if execution.position_after.status is PositionStatus.CLOSED:
                closed = {
                    "kind": "position_closed",
                    "position_id": execution.position_after.position_id,
                    "time": timestamp,
                    "reason": reason,
                    "exit_price": execution.price,
                    "entry_price": execution.position_before.fill_price,
                    "quantity": execution.position_before.quantity,
                    "protective_stop": execution.position_before.protective_stop,
                    "risk_points": execution.position_before.initial_risk_points,
                    "reward_points": after.realized_points,
                    "lifecycle_id": life.lifecycle_id,
                }
                context.record_event(closed, record_lifecycle=False)
                bar_events.append(closed)
                position_intent = after.position.exit_intent if after.position is not None else None
                if position_intent is not None:
                    if position_intent.status is IntentStatus.TRIGGERED:
                        self.engine.clear_exit_intent(life)
                    else:
                        cancelled = self.engine.cancel_exit_intent(
                            life, reason="position_closed"
                        )
                        if cancelled is not None:
                            intent_event = {
                                "kind": "exit_intent_cancelled",
                                "intent_id": cancelled.intent_id,
                                "position_id": cancelled.position_id,
                                "rule_id": cancelled.rule_id,
                                "reason": "position_closed",
                                "lifecycle_id": life.lifecycle_id,
                                "observed_at": timestamp,
                                "available_at": timestamp,
                            }
                            context.record_event(intent_event, record_lifecycle=False)
                            bar_events.append(intent_event)
                self.engine.clear_pending_exit(life)
            return
        self.evaluate_position_exit_plan(
            life=life,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            bar_events=bar_events,
        )
        state = self.engine.state_for(life.lifecycle_id)
        pending = state.pending_exit
        if (
            defer_discretionary_execution
            and pending is not None
            and pending.reason == "conditional_exit"
            and coerce_timestamp(pending.available_at) <= coerce_timestamp(timestamp)
            and context.position_is_open(life)
        ):
            return self.handle_management(
                index, timestamp, next_time, row, available5, bar_events, life
            )
        if not context.position_is_open(life):
            return
        assert state.position is not None
        position = state.position
        setup = state.setup
        prior_stop = position.protective_stop
        assert position.fill_price is not None
        is_or_vwap = (
            setup is not None
            and getattr(getattr(setup, "template", None), "stop_policy", None)
            == StopPolicy.OR_VWAP
        )
        if is_or_vwap:
            previous_trail, trailing_stop, trail_was_active = None, None, False
        else:
            previous_trail, trailing_stop, trail_was_active = (
                self.engine.update_trail_candidate(
                    life,
                    bar_open=float(row["open"]),
                    bar_high=float(row["high"]),
                    bar_low=float(row["low"]),
                    bar_close=float(row["close"]),
                )
            )
        vwap_stop = None
        source_bar = None
        vwap = None
        atr = None
        if is_or_vwap:
            vwap, source_bar = _session_vwap(
                context.aggregates.get("5m", []), str(life.session_date), timestamp
            )
            if source_bar is not None:
                atr = daily_atr(
                    source_bar.session_date,
                    context.rows_by_date
                    or {str(life.session_date): context.rows},
                )
                vwap_stop, _ = _or_vwap_stop_price(
                    position.direction, position.fill_price, vwap, atr
                )
        activated = False
        if not position.post_3r_activated and _three_r_reached(position, row):
            activated = True
            self.engine.update_position(life, replace(position, post_3r_activated=True))
            state = self.engine.state_for(life.lifecycle_id)
            assert state.position is not None
            position = state.position
        if (
            position.post_3r_activated
            and not position.partial_exit_taken
            and state.pending_exit is None
            and position.quantity * 0.5 > 0
        ):
            quantity = position.quantity * 0.5
            self._queue_exit("partial_3r", next_time, life, quantity=quantity)
            event = {
                "kind": "exit_signal",
                "level": "3R",
                "reason": "partial_3r",
                "lifecycle_id": life.lifecycle_id,
                "observed_at": timestamp,
                "available_at": next_time,
                "quantity": quantity,
                "remaining_quantity": position.quantity - quantity,
                "risk_reward": 3.0,
                "owner": "position_management",
                "evidence_timeframe": "1m",
            }
            context.record_event(event, record_lifecycle=False)
            bar_events.append(event)
        stop_update = update_position_stop(
            position, setup, row, trailing_stop, vwap_stop
        )
        if stop_update.changed:
            self.engine.update_position(
                life, replace(position, protective_stop=stop_update.candidate)
            )
            state = self.engine.state_for(life.lifecycle_id)
            assert state.position is not None
            position = state.position
        if stop_update.changed or (
            previous_trail != trailing_stop and trailing_stop is not None
        ):
            event = {
                "kind": "stop_updated",
                "observed_at": timestamp,
                "available_at": source_bar.available_at
                if source_bar is not None
                else next_time,
                "lifecycle_id": life.lifecycle_id,
                "position_id": position.position_id,
                "previous_stop": prior_stop,
                "trailing_stop": trailing_stop,
                "vwap": vwap,
                "atr": atr,
                "vwap_stop_reference": vwap_stop,
                "active_stop": position.protective_stop,
                "stop_source": stop_update.source,
            }
            context.record_event(event, record_lifecycle=False)
            bar_events.append(event)
        protective_stop = position.protective_stop
        can_trigger = (
            is_or_vwap or trail_was_active or activated or position.post_3r_activated
        )
        has_candidate = is_or_vwap or trailing_stop is not None or activated
        if (
            can_trigger
            and has_candidate
            and protective_stop is not None
            and (
                (float(row["high"]) >= protective_stop)
                if position.direction == "short"
                else (float(row["low"]) <= protective_stop)
            )
        ):
            trigger_stop, reason = (
                protective_stop,
                ("vwap_stop" if is_or_vwap else "trail_stop"),
            )
        elif prior_stop is not None and (
            (float(row["high"]) >= prior_stop)
            if position.direction == "short"
            else (float(row["low"]) <= prior_stop)
        ):
            trigger_stop, reason = prior_stop, "hard_stop"
        else:
            trigger_stop, reason = None, ""
        if trigger_stop is not None:
            fill_price = (
                max(float(row["open"]), trigger_stop)
                if position.direction == "long"
                else min(float(row["open"]), trigger_stop)
            )
            self._queue_exit(
                reason,
                next_time,
                life,
                requested_price=fill_price,
                execution_policy="protective_stop_decision",
            )
            event = {
                "kind": "exit_signal",
                "level": "1m",
                "reason": reason,
                "lifecycle_id": life.lifecycle_id,
                "observed_at": timestamp,
                "available_at": next_time,
                "fill_price": fill_price,
                "stop": trigger_stop,
                "stop_trigger": "wick_or_touch",
                "owner": "1m",
                "evidence_timeframe": "1m",
            }
            context.record_event(event, record_lifecycle=False)
            bar_events.append(event)

        synthetic_snapshots = None
        if state.intent is not None:
            available_interactions = [
                [
                    snapshot
                    for snapshot in dict(interaction.synthetic_snapshots).get("5m", ())
                    if snapshot.available_at <= coerce_timestamp(timestamp)
                ]
                for interaction in context.interactions.values()
            ]
            synthetic_snapshots = max(
                (items for items in available_interactions if items),
                key=lambda items: items[-1].available_at,
                default=None,
            )
            if position.fill_price is None:
                return
            entry_price = position.fill_price
            continuation = failed_continuation(
                [
                    {
                        **item,
                        "time": nifty_label(
                            parse_session_timestamp(
                                str(life.session_date), item["time"]
                            )
                        )[11:],
                    }
                    for item in context.rows[:index]
                ],
                [
                    item
                    for item in context.aggregates.get("1m", [])
                    if str(getattr(item, "session_date", life.session_date))
                    == str(life.session_date)
                    and item.available_at <= coerce_timestamp(timestamp)
                ],
                [item for item in available5 if item.available_at <= coerce_timestamp(timestamp)],
                direction=life.direction.value,
                entry_price=entry_price,
                failure_boundary=position.failure_boundary,
                # ``prior`` contains session-local source-row labels while
                # execution state stores UTC instants. Compare like-for-like
                # so pre-entry rows cannot satisfy continuation evidence.
                entry_time=(
                    nifty_label(coerce_timestamp(state.intent.triggered_at))[11:]
                    if state.intent.triggered_at is not None else None
                ),
                synthetic=synthetic_snapshots,
            )
            context.execution_diagnostics.append({
                "kind": "failed_continuation_evaluated",
                "lifecycle_id": life.lifecycle_id,
                "observed_at": timestamp,
                "available_at": next_time,
                "location_interaction_id": life.location_interaction_id,
                "entry_at": state.entry_at,
                "completed_1m_count": len(context.aggregates.get("1m", ())),
                "completed_5m_count": len(available5),
                "synthetic_snapshot_count": len(synthetic_snapshots or ()),
                "synthetic_snapshots": [
                    {
                        "available_at": item.available_at,
                        "last_source_end": item.last_source_end,
                        "source_candle_ids": list(item.source_candle_ids),
                        "high": item.high,
                        "low": item.low,
                        "close": item.close,
                    }
                    for item in (synthetic_snapshots or ())[-3:]
                ],
                "continuation": continuation,
            })
            if continuation is not None:
                self._queue_exit("failed_continuation", next_time, life)
                event = {
                    "kind": "exit_signal",
                    "level": "synthetic"
                    if synthetic_snapshots is not None
                    else "1m/5m",
                    "reason": "failed_continuation",
                    "observed_at": continuation["evidence_time"],
                    "available_at": continuation["evidence_available_at"],
                    "components": continuation,
                    "owner": "position_management",
                    "evidence_timeframe": "synthetic"
                    if synthetic_snapshots is not None
                    else "1m/5m",
                }
                context.record_event(event)
                bar_events.append(event)

    def evaluate_position_exit_plan(
        self,
        *,
        life: SetupOpportunity,
        timestamp: str,
        next_time: str,
        row: dict[str, Any],
        bar_events: list[dict[str, Any]],
    ) -> None:
        state = self.engine.state_for(life.lifecycle_id)
        if not life.active and state.position is None:
            return
        if (
            state.position is None
            or state.position.status is not PositionStatus.OPEN
            or state.pending_exit is not None
        ):
            return
        for rule in state.position.exit_plan:
            interaction = (
                _target_interaction(
                    self.machine,
                    rule.location,
                    timestamp,
                    not_before=str(state.entry_at or life.formation_available_at),
                )
                if self.machine is not None
                else None
            )
            if rule.interaction_id is not None:
                interaction = next(
                    (
                        item
                        for item in self.context.interactions.values()
                        if item.interaction_id == rule.interaction_id
                        and item.available_at <= coerce_timestamp(timestamp)
                        and any(
                            coerce_timestamp(state.entry_at or life.formation_available_at)
                            <= candle.available_at
                            <= coerce_timestamp(timestamp)
                            for candle in item.candles
                        )
                    ),
                    None,
                )
            if interaction is None:
                continue
            if rule.level is None:
                level = (
                    interaction.band_high
                    if life.direction is Direction.LONG
                    else interaction.band_low
                )
                rule = replace(
                    rule, level=level, interaction_id=interaction.interaction_id
                )
                position = replace(
                    state.position,
                    exit_plan=tuple(
                        rule if item.rule_id == rule.rule_id else item
                        for item in state.position.exit_plan
                    ),
                )
                self.engine.update_position(life, position)
            existing_intent = state.position.exit_intent
            if (
                existing_intent is not None
                and existing_intent.rule_id == rule.rule_id
                and coerce_timestamp(timestamp) >= coerce_timestamp(existing_intent.expiry)
            ):
                cancelled = self.engine.cancel_exit_intent(life, reason="expired")
                self.engine.clear_pending_exit(life)
                if cancelled is not None:
                    event = {
                        "kind": "exit_intent_cancelled",
                        "intent_id": cancelled.intent_id,
                        "position_id": cancelled.position_id,
                        "rule_id": cancelled.rule_id,
                        "reason": "expired",
                        "observed_at": timestamp,
                        "available_at": next_time,
                    }
                    self.context.record_event(event)
                    bar_events.append(event)
                continue
            if rule.mode == "fixed_target" and self.exit_rule_triggered(
                rule, interaction=interaction, direction=life.direction, row=row
            ):
                if rule.execution is None:
                    raise ValueError("resolved exit rules require execution timing")
                self._queue_exit(
                    "fixed_target", next_time, life, requested_price=rule.level
                )
                return
            if (
                rule.mode == "conditional"
                and self.exit_response_evaluation(
                    rule,
                    interaction=interaction,
                    entry_at=str(state.entry_at or life.formation_available_at),
                    as_of=timestamp,
                ).status
                is ResponseOutcomeStatus.CONFIRMED
            ):
                existing = state.position.exit_intent
                if existing is None or existing.rule_id != rule.rule_id:
                    intent = ExitIntent(
                        intent_id=f"exit-{state.position.position_id}-{rule.rule_id}",
                        position_id=state.position.position_id,
                        rule_id=rule.rule_id,
                        location=rule.location,
                        status=IntentStatus.CONFIRMATION_FOUND,
                        created_at=timestamp,
                        expiry=f"{timestamp[:10]}T{SESSION_EXIT_DEADLINE}",
                        interaction_id=rule.interaction_id,
                        decision_context=getattr(rule, "source_context", None),
                    )
                    self.engine.set_exit_intent(life, intent)
                    event = {
                        "kind": "exit_intent_created",
                        "intent_id": intent.intent_id,
                        "position_id": intent.position_id,
                        "rule_id": intent.rule_id,
                        "observed_at": timestamp,
                        "available_at": next_time,
                    }
                    self.context.record_event(event)
                    bar_events.append(event)
                    existing = intent
                admission = (
                    evaluate_exit_rule(self.machine, life, rule, timestamp=timestamp)
                    if self.machine is not None
                    else None
                )
                if admission is not None:
                    return
                triggered = trigger_exit_intent(
                    intent=existing,
                    at=timestamp,
                    decision_context=existing.decision_context,
                )
                self.engine.set_exit_intent(life, triggered)
                if rule.execution is None:
                    raise ValueError("resolved exit rules require execution timing")
                self._queue_exit(
                    "conditional_exit", next_time, life, requested_price=rule.level
                )
                event = {
                    "kind": "exit_decision",
                    "action": "close",
                    "intent_id": triggered.intent_id,
                    "position_id": triggered.position_id,
                    "rule_id": triggered.rule_id,
                    "reason": "exit_triggered",
                    "observed_at": timestamp,
                    "available_at": next_time,
                }
                self.context.record_event(event)
                bar_events.append(event)
                return


__all__ = [
    "AdmissionStatus",
    "EntryFillDecision",
    "EntryOrder",
    "EntryReservation",
    "ExecutionAdmission",
    "ExecutionEngine",
    "ExecutionHost",
    "ExecutionState",
    "ExitExecution",
    "ExitIntent",
    "ExitOrder",
    "Fill",
    "PositionManagement",
    "PositionStatus",
    "ReservationStatus",
    "ResolvedExitRule",
    "entry_fill_decision",
    "protective_stop_price",
    "resolve_setup_exit_plan",
    "risk_adjustment",
]
