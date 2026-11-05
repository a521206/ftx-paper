"""Canonical execution-bar phase for the orderflow transition."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from ftx_paper.contracts import Instrument
from ftx_paper.execution.entry_fill import entry_fill_decision

from .execution import AdmissionStatus, IntentStatus
from .hierarchy import RegimePermission, TriggerObservation
from .opportunity_coordinator import LifecycleFormation
from .timestamps import coerce_timestamp


def _trace(state: Any) -> dict[str, Any]:
    position = state.position
    pending = state.pending_exit
    intent = state.intent

    def value(item: Any) -> Any:
        return getattr(item, "value", item)

    return {
        "intent": {
            "status": value(intent.status),
            "triggered_at": getattr(intent, "triggered_at", None),
        } if intent is not None else None,
        "pending_exit": {
            "reason": pending.reason,
            "available_at": pending.available_at,
            "requested_price": pending.requested_price,
            "execution_policy": pending.execution_policy,
            "quantity": pending.quantity,
        } if pending is not None else None,
        "position": {
            "status": value(position.status),
            "quantity": position.quantity,
            "fill_price": position.fill_price,
            "failure_boundary": position.failure_boundary,
            "protective_stop": position.protective_stop,
            "post_3r_activated": position.post_3r_activated,
            "partial_exit_taken": position.partial_exit_taken,
        } if position is not None else None,
    }


def intent_allows_requalification(state: Any) -> bool:
    """Return whether the execution policy may reassess an active intent."""
    intent = state.intent
    if intent is None:
        return state.admission.status is AdmissionStatus.NOT_EVALUATED
    return (
        intent.status is IntentStatus.CANCELLED
        and intent.cancel_reason in {
            "opening_gap_permission_changed",
            "decision_context_unavailable",
            "decision_context_scores_unavailable",
            "fill_permission_mismatch",
            "active_position_session_extreme_gate",
        }
    )


class ExecutionClock:
    """Run execution policy in the transition's established causal order."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def advance_transition(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        regime: RegimePermission | None,
        regime_publications: Sequence[tuple[str, RegimePermission]],
        triggers: Mapping[str, TriggerObservation] | None,
        bar_opens: Mapping[str, float] | None,
        decision_scores: Mapping[str, tuple[float | None, float | None]] | None,
        constraints: Any,
        decision_at: str,
        index: int,
        timestamp: str,
        next_time: str,
        row: Mapping[str, Any],
        available5: Sequence[Any] = (),
        instrument: Instrument | None = None,
    ) -> Mapping[str, tuple[dict[str, Any], ...]]:
        """Coordinate formation admission and the post-structure execution pass."""
        owner = self.engine.execution_owner
        if formations:
            owner.advance(
                tuple(formations),
                regime=regime,
                regime_publications=regime_publications,
                triggers=triggers,
                bar_opens=bar_opens,
                decision_scores=decision_scores,
                constraints=constraints,
                decision_at=decision_at,
            )
        return self.advance_post_structure(
            tuple(formations),
            index=index,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available5=available5,
            instrument=instrument,
        )

    def advance_post_structure(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        index: int,
        timestamp: str,
        next_time: str,
        row: Mapping[str, Any],
        available5: Sequence[Any] = (),
        instrument: Instrument | None = None,
    ) -> Mapping[str, tuple[dict[str, Any], ...]]:
        owner = self.engine.execution_owner
        context = self.engine.context
        if available5:
            context.aggregates["5m"] = list(available5)
        events = list(context.pending_bar_events)
        context.pending_bar_events.clear()
        context.execution_proposals.clear()
        managed: list[Any] = list(formations)
        formation_ids = {
            item.lifecycle.lifecycle_id
            for item in formations
            if item.lifecycle is not None
        }
        managed.extend(
            SimpleNamespace(
                lifecycle=life,
                setup=owner.engine.setup_for(life.lifecycle_id),
                available_at=life.formation_available_at,
            )
            for life in context.opportunities.values()
            if life.lifecycle_id not in formation_ids
        )
        for formation in managed:
            life = formation.lifecycle
            if life is None:
                continue
            if coerce_timestamp(timestamp) < coerce_timestamp(life.formation_available_at):
                continue
            state = owner.engine.state_for(life.lifecycle_id)
            if not life.active and state.position is None:
                continue
            life.clock = timestamp
            if owner.opportunity_expired(life, timestamp=timestamp):
                owner.expire_opportunity(
                    life,
                    timestamp=timestamp,
                    bar_events=events,
                )
                continue
            if (
                life.active
                and context.regime is not None
                and intent_allows_requalification(state)
                and not owner.opportunity_expired(life, timestamp=timestamp)
            ):
                owner.admit_opportunity(life, next_time, events)
            trace_before = _trace(owner.engine.state_for(life.lifecycle_id))
            owner.apply_session_deadline(
                life, session_date=str(life.session_date),
                timestamp=timestamp, bar_events=events,
            )
            owner.handle_management(
                index, timestamp, next_time, dict(row), list(available5), events, life,
                defer_discretionary_execution=True,
            )
            if life.active:
                owner.handle_execution(
                    index, timestamp, next_time, dict(row), [], events, life,
                    evaluate_entries=True,
                )
            context.execution_diagnostics.append({
                "kind": "execution_lifecycle_tick",
                "lifecycle_id": life.lifecycle_id,
                "observed_at": timestamp,
                "available_at": next_time,
                "bar": {key: row.get(key) for key in ("time", "open", "high", "low", "close")},
                "available_5m": [
                    {"time": item.time, "available_at": item.available_at}
                    for item in available5[-2:]
                ],
                "before": trace_before,
                "after": _trace(owner.engine.state_for(life.lifecycle_id)),
            })
        owner.select_execution_proposals(timestamp, next_time, events)
        if self.engine.execution_adapter is not None and instrument is not None:
            self._publish_proposals(
                managed, row=dict(row), timestamp=timestamp,
                next_time=next_time, instrument=instrument,
            )
        return {
            name: tuple(values)
            for name, values in context.parity_snapshot().items()
        }

    def settle_pending(
        self, *, index: int, timestamp: str, next_time: str,
        row: Mapping[str, Any], events: list[dict[str, Any]],
    ) -> None:
        owner = self.engine.execution_owner
        for life in tuple(self.engine.context.opportunities.values()):
            if life.active or self.engine.context.position_is_open(life):
                owner.handle_execution(
                    index, timestamp, next_time, dict(row), [], events, life,
                    evaluate_entries=False,
                )

    def _publish_proposals(
        self,
        formations: Sequence[Any],
        *,
        row: Mapping[str, Any],
        timestamp: str,
        next_time: str,
        instrument: Instrument,
    ) -> None:
        adapter = self.engine.execution_adapter
        assert adapter is not None
        for formation in formations:
            life = formation.lifecycle
            if life is None:
                continue
            state = self.engine.execution_owner.engine.state_for(life.lifecycle_id)
            intent = state.intent
            if (
                intent is None
                or intent.status.value != "TRIGGERED"
                or self.engine._proposal_buffer.contains_intent(intent.intent_id)
            ):
                continue
            decision = entry_fill_decision(
                direction=life.direction.value,
                trigger_price=intent.trigger_price,
                requested_fill_price=intent.trigger_price,
                bar_open=float(row["open"]),
                failure_boundary=intent.failure_boundary,
            )
            if decision.quantity <= 0:
                continue
            proposal = adapter.proposal(
                intent, instrument=instrument, quantity=decision.quantity,
                observed_at=timestamp, available_at=next_time,
            )
            self.engine._proposal_buffer.publish(intent.intent_id, proposal)
