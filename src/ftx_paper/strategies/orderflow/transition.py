"""Shared chronological owner for live and bounded orderflow transitions.

This module is deliberately an orchestration boundary.  Policy remains in the
canonical-compatible coordinators; this owner only gives live and replay one
mutable clock, one interaction owner, and one execution owner.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from ftx_paper.execution.events import ExecutionNotification

from .coordinator import OrderflowAdvance, OrderflowClockInput, OrderflowCoordinator
from .execution import AdmissionStatus, ExecutionCoordinator, ExecutionEngine, IntentStatus
from .features import daily_atr_cache
from .hierarchy import ContextVersionRef, TriggerObservation
from .opportunity_coordinator import LifecycleFormation
from .opportunity_timeout import advance_opportunity_timeout
from .state_machine_types import ExecutionConstraints, LocationRegistry, ReplayContext
from .timestamps import coerce_timestamp, nifty_label, serialize_utc


def _execution_trace_state(state: Any) -> dict[str, Any]:
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


@dataclass(frozen=True, slots=True)
class TransitionAdvance:
    """Facts emitted by one idempotent shared transition."""

    observation: OrderflowAdvance
    execution: Mapping[str, tuple[dict[str, Any], ...]]


@dataclass(slots=True)
class OrderflowTransitionOwner:
    """Own the single chronological state boundary used by both modes."""

    location_registry: LocationRegistry
    observation_owner: OrderflowCoordinator = field(init=False)
    execution_owner: ExecutionCoordinator = field(default_factory=ExecutionCoordinator)
    execution_adapter: Any | None = None
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]] = field(default_factory=dict, repr=False)
    atr_by_session: Mapping[str, float | None] | None = field(default=None, repr=False)
    context: ReplayContext = field(init=False)
    mode: str = "FULL"
    _clock: datetime | None = field(default=None, init=False, repr=False)
    _published_intents: set[str] = field(default_factory=set, init=False, repr=False)
    _execution_notifications: set[str] = field(default_factory=set, init=False, repr=False)

    @staticmethod
    def _intent_allows_requalification(state: Any) -> bool:
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

    def __post_init__(self) -> None:
        self.observation_owner = OrderflowCoordinator(self.location_registry)
        self.context = ReplayContext(
            rows_by_date=self.rows_by_date,
            atr_by_session=(
                self.atr_by_session
                if self.atr_by_session is not None
                else daily_atr_cache(self.rows_by_date)
            ),
            location_registry=self.location_registry,
            execution_engine=ExecutionEngine(),
        )
        # ReplayContext is the canonical mutable execution owner.  The
        # coordinator is only a service facade over that same instance.
        self.execution_owner.engine = self.context.execution_engine
        self.execution_owner.machine = self

    def promote_episode(
        self,
        life: Any,
        *,
        observed_at: str,
        available_at: str,
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Promote a provisional lifecycle after its final trigger checks."""
        if life.episode_confirmed:
            return
        life.episode_confirmed = True
        for competing in self.context.conflicting_unfilled_opportunities(life):
            if competing.active:
                self.execution_owner.invalidate_opportunity(
                    competing,
                    timestamp=observed_at,
                    observed_at=observed_at,
                    available_at=available_at,
                    bar_events=bar_events,
                    reason="opposing_episode_confirmed",
                )
        event = {
            "kind": "episode_confirmed",
            "lifecycle_id": life.lifecycle_id,
            "observed_at": observed_at,
            "available_at": available_at,
            "direction": life.direction.value,
            "conflict_key": life.conflict_key,
            "owner": "1m",
            "evidence_timeframe": "1m",
        }
        self.context.record_event(event)
        bar_events.append(event)

    @property
    def clock(self) -> datetime | None:
        return self._clock

    @property
    def parity_streams(self) -> dict[str, list[dict[str, Any]]]:
        return self.context.parity_snapshot()

    def snapshot(self) -> dict[str, Any]:
        """Return the canonical producer prefix needed for recovery."""
        return {
            "schema_version": 1,
            "clock": serialize_utc(self._clock) if self._clock is not None else None,
            "events": deepcopy(self.context.events),
            "transitions": deepcopy(self.context.transitions),
            "parity_streams": deepcopy(self.context.parity_streams),
            "parity_observed": sorted(self.context.parity_observed),
            "parity_layers": deepcopy(self.context.parity_layers),
            "execution_diagnostics": deepcopy(self.context.execution_diagnostics),
        }

    def restore_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        """Restore producer-owned facts and the transition clock idempotently."""
        if snapshot.get("schema_version") != 1:
            raise ValueError("unsupported orderflow snapshot schema")
        raw_clock = snapshot.get("clock")
        if raw_clock is not None and not isinstance(raw_clock, str):
            raise ValueError("orderflow snapshot clock must be a string or null")
        for name in ("events", "transitions", "parity_streams", "parity_layers", "execution_diagnostics"):
            value = snapshot.get(name, [] if name in {"events", "transitions", "execution_diagnostics"} else {})
            if not isinstance(value, (list, dict)):
                raise ValueError(f"orderflow snapshot {name} is invalid")
        observed = snapshot.get("parity_observed", [])
        if not isinstance(observed, list) or any(not isinstance(item, str) for item in observed):
            raise ValueError("orderflow snapshot parity_observed is invalid")
        self._clock = coerce_timestamp(raw_clock) if raw_clock else None
        self.context.events = deepcopy(snapshot.get("events", []))
        self.context.transitions = deepcopy(snapshot.get("transitions", []))
        self.context.parity_streams = deepcopy(snapshot.get("parity_streams", {}))
        self.context.parity_observed = set(observed)
        self.context.parity_layers = deepcopy(snapshot.get("parity_layers", {}))
        self.context.execution_diagnostics = deepcopy(snapshot.get("execution_diagnostics", []))

    @property
    def engine(self) -> Any:
        """Expose the canonical execution state for lifecycle adapters."""
        return self.execution_owner.engine

    def attach_setup(self, formation: LifecycleFormation) -> None:
        self.execution_owner.attach_setup(formation)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        """Record an account-owner notification without re-running policy."""
        identity = event.event_id or (
            f"{event.event_type}:{event.client_order_id}:"
            f"{event.occurred_at or event.available_at or ''}"
        )
        if identity in self._execution_notifications:
            return
        self._execution_notifications.add(identity)
        occurred_at = event.occurred_at or event.available_at or self._clock
        available_at = event.available_at or occurred_at
        if occurred_at is None or available_at is None:
            return
        kind = {
            "FILLED": "filled",
            "CANCELLED": "entry_cancelled",
            "REJECTED": "entry_cancelled",
        }.get(event.event_type.upper())
        if kind is None:
            return
        if any(
            fact.get("kind") == kind
            and fact.get("values", {}).get("client_order_id") == event.client_order_id
            for fact in self.context.parity_streams.get("execution_transitions", ())
        ):
            return
        self.context.record_event({
            "kind": kind,
            "identity": f"execution_notification:{identity}",
            "client_order_id": event.client_order_id,
            "status": event.status,
            "quantity": event.quantity,
            "filled_quantity": event.filled_quantity,
            "price": event.price,
            "reason": event.reason,
            "observed_at": occurred_at,
            "available_at": available_at,
            "causal_parent_id": event.client_order_id,
            "parity_stream": "execution_transitions",
        })

    def _accept_formations(
        self, formations: Sequence[LifecycleFormation],
    ) -> tuple[LifecycleFormation, ...]:
        accepted: list[LifecycleFormation] = []
        for formation in formations:
            life = formation.lifecycle
            if life is None:
                continue
            existing = self.context.opportunities.get(life.lifecycle_id)
            if existing is None and not self.context.register_opportunity(life):
                continue
            if existing is not None and existing is not life:
                continue
            if formation.setup is not None and self.engine.setup_for(
                life.lifecycle_id
            ) is None:
                self.attach_setup(formation)
            accepted.append(formation)
        return tuple(accepted)

    def advance(
        self,
        item: OrderflowClockInput,
        *,
        formations: Sequence[LifecycleFormation] = (),
        regime: Any = None,
        regime_publications: Sequence[tuple[str, Any]] = (),
        triggers: Mapping[str, TriggerObservation] | None = None,
        bar_opens: Mapping[str, float] | None = None,
        decision_scores: Mapping[str, tuple[float | None, float | None]] | None = None,
        constraints: ExecutionConstraints | None = None,
        instrument: Any | None = None,
    ) -> TransitionAdvance:
        """Advance one completed bar through the shared causal lifecycle.

        Existing requests are settled and positions are managed before the
        current bar is allowed to create a new entry proposal.  This mirrors
        the canonical replay boundary and keeps live/replay on one path.
        """
        available_at = coerce_timestamp(item.one_minute.available_at)
        if self._clock is not None and available_at < self._clock:
            raise ValueError("orderflow transition clock cannot move backwards")
        observation = self.observation_owner.advance(item)
        if regime is not None:
            self.context.regime = regime
        if constraints is not None:
            self.context.execution_constraints = constraints
        timestamp = nifty_label(item.one_minute.time)
        next_time = nifty_label(item.one_minute.available_at)
        row = item.one_minute.to_dict()
        available5 = list(self.observation_owner.five_minute_aggregates)
        self.context.aggregates["5m"] = available5
        timeout_events: list[dict[str, Any]] = []
        self.advance_opportunity_timeouts(timestamp, available5, timeout_events)
        self.context.pending_bar_events.extend(timeout_events)
        accepted_formations = self._accept_formations(formations)
        if accepted_formations:
            self.execution_owner.advance(
                tuple(accepted_formations),
                regime=regime,
                regime_publications=regime_publications,
                triggers=triggers,
                bar_opens=bar_opens,
                decision_scores=decision_scores,
                constraints=constraints,
                decision_at=next_time,
            )
        self.process_execution_bar(
            tuple(accepted_formations),
            index=0,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available5=available5,
            instrument=instrument,
        )
        self._clock = available_at
        return TransitionAdvance(
            observation=observation,
            execution={
                name: tuple(values)
                for name, values in self.context.parity_snapshot().items()
            },
        )

    def advance_execution(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        decision_at: str,
        regime: Any = None,
        regime_publications: Sequence[tuple[str, Any]] = (),
        triggers: Mapping[str, TriggerObservation] | None = None,
        bar_opens: Mapping[str, float] | None = None,
        decision_scores: Mapping[str, tuple[float | None, float | None]] | None = None,
        constraints: ExecutionConstraints | None = None,
    ) -> Mapping[str, tuple[dict[str, Any], ...]]:
        """Advance lifecycle execution without advancing market observation."""
        if regime is not None:
            self.context.regime = regime
        if constraints is not None:
            self.context.execution_constraints = constraints
        accepted_formations = self._accept_formations(formations)
        self.execution_owner.advance(
            tuple(accepted_formations),
            regime=regime,
            regime_publications=regime_publications,
            triggers=triggers,
            bar_opens=bar_opens,
            decision_scores=decision_scores,
            constraints=constraints,
            decision_at=decision_at,
        )
        return {
            name: tuple(values)
            for name, values in self.context.parity_snapshot().items()
        }

    def advance_replay_bar(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        index: int,
        timestamp: str,
        next_time: str,
        row: Mapping[str, Any],
        available5: Sequence[Any] = (),
        instrument: Any | None = None,
    ) -> Mapping[str, tuple[dict[str, Any], ...]]:
        """Advance replay execution through the shared chronological boundary."""
        available_at = coerce_timestamp(next_time)
        if self._clock is not None and available_at < self._clock:
            raise ValueError("orderflow transition clock cannot move backwards")
        result = self.process_execution_bar(
            formations,
            index=index,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available5=available5,
            instrument=instrument,
        )
        self._clock = available_at
        return result

    def advance_opportunity_timeouts(
        self,
        timestamp: str,
        available5: Sequence[Any],
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Expire existing opportunities before current-bar structure admission."""
        for life in tuple(self.context.opportunities.values()):
            if not life.active or self.context.position_is_open(life):
                continue
            interaction = (
                self.context.interactions.get(life.location_interaction_id)
                if life.location_interaction_id is not None else None
            )
            timeout = advance_opportunity_timeout(
                life,
                interaction=interaction,
                evidence=available5,
                as_of=timestamp,
            )
            if life.timeout_count >= life.timeout_limit:
                self.execution_owner.expire_opportunity(
                    life,
                    timestamp=timeout.expiry_at or timestamp,
                    bar_events=bar_events,
                    reason="evidence_timeout",
                    timeout_event={
                        "timeout_count": life.timeout_count,
                        "timeout_limit": life.timeout_limit,
                        "evidence_timeframe": life.timeout_evidence_timeframe,
                        "counted_source_ids": list(life.timeout_counted_source_ids),
                        "counted_snapshot_ids": list(life.timeout_counted_snapshot_ids),
                    },
                )

    def process_execution_bar(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        index: int,
        timestamp: str,
        next_time: str,
        row: Mapping[str, Any],
        available5: Sequence[Any] = (),
        instrument: Any | None = None,
    ) -> Mapping[str, tuple[dict[str, Any], ...]]:
        """Run the canonical execution clock for one completed 1m bar.

        Pending fills are settled before position management.  New trigger
        observations are evaluated only after that pass and are selected for
        the next bar, preventing same-bar trigger/fill transitions.
        """
        # The gate and position-management policies consume the same completed
        # 5m evidence that drove this execution clock. Keep it visible on the
        # shared context before any proposal is evaluated.
        if available5:
            self.context.aggregates["5m"] = list(available5)
        self.execution_owner.active_position_gate.update(self.context, timestamp)
        events = list(self.context.pending_bar_events)
        self.context.pending_bar_events.clear()
        self.context.execution_proposals.clear()
        if self.mode == "FULL":
            row_dict = dict(row)
            for life in tuple(self.context.opportunities.values()):
                if life.active or self.context.position_is_open(life):
                    self.execution_owner.handle_execution(
                        index,
                        timestamp,
                        next_time,
                        row_dict,
                        [],
                        events,
                        life,
                        evaluate_entries=False,
                    )
        managed: list[Any] = list(formations)
        formation_ids = {
            item.lifecycle.lifecycle_id
            for item in formations
            if item.lifecycle is not None
        }
        managed.extend(
            SimpleNamespace(
                lifecycle=life,
                setup=self.execution_owner.engine.setup_for(life.lifecycle_id),
                available_at=life.formation_available_at,
            )
            for life in self.context.opportunities.values()
            if life.lifecycle_id not in formation_ids
        )
        for formation in managed:
            life = formation.lifecycle
            if life is None:
                continue
            if coerce_timestamp(timestamp) < coerce_timestamp(life.formation_available_at):
                continue
            state = self.execution_owner.engine.state_for(life.lifecycle_id)
            if not life.active and state.position is None:
                continue
            self.execution_owner.apply_session_deadline(
                life,
                session_date=str(life.session_date),
                timestamp=timestamp,
                bar_events=events,
            )
            state = self.execution_owner.engine.state_for(life.lifecycle_id)
            if (
                self.mode == "FULL"
                and
                life.active
                and self.context.regime is not None
                and self._intent_allows_requalification(state)
                and not self.execution_owner.opportunity_expired(
                    life, timestamp=timestamp
                )
            ):
                self.execution_owner.admit_opportunity(life, next_time, events)
            trace_before = _execution_trace_state(
                self.execution_owner.engine.state_for(life.lifecycle_id)
            )
            self.execution_owner.handle_management(
                index, timestamp, next_time, dict(row), list(available5), events, life,
                defer_discretionary_execution=True,
            )
            if self.mode == "FULL" and life.active:
                self.execution_owner.handle_execution(
                    index, timestamp, next_time, dict(row), [], events, life,
                    evaluate_entries=True,
                )
            self.context.execution_diagnostics.append({
                "kind": "execution_lifecycle_tick",
                "lifecycle_id": life.lifecycle_id,
                "observed_at": timestamp,
                "available_at": next_time,
                "bar": {
                    "time": row.get("time"), "open": row.get("open"),
                    "high": row.get("high"), "low": row.get("low"),
                    "close": row.get("close"),
                },
                "available_5m": [
                    {"time": item.time, "available_at": item.available_at}
                    for item in available5[-2:]
                ],
                "before": trace_before,
                "after": _execution_trace_state(
                    self.execution_owner.engine.state_for(life.lifecycle_id)
                ),
            })
        self.execution_owner.select_execution_proposals(timestamp, next_time, events)
        if self.execution_adapter is not None and instrument is not None:
            self._publish_execution_proposals(
                managed, row=dict(row), timestamp=timestamp,
                next_time=next_time, instrument=instrument,
            )
        return {
            name: tuple(values)
            for name, values in self.context.parity_snapshot().items()
        }

    def _publish_execution_proposals(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        row: Mapping[str, Any],
        timestamp: str,
        next_time: str,
        instrument: Any,
    ) -> None:
        from .execution import entry_fill_decision

        for formation in formations:
            life = formation.lifecycle
            if life is None:
                continue
            state = self.execution_owner.engine.state_for(life.lifecycle_id)
            intent = state.intent
            if (
                intent is None
                or intent.status.value != "TRIGGERED"
                or intent.intent_id in self._published_intents
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
            adapter = self.execution_adapter
            assert adapter is not None
            proposal = adapter.proposal(
                intent,
                instrument=instrument,
                quantity=decision.quantity,
                observed_at=timestamp,
                available_at=next_time,
            )
            adapter.authorize(proposal, timestamp=next_time)
            self._published_intents.add(intent.intent_id)

    def observe_triggers(
        self,
        formations: Sequence[LifecycleFormation],
        observations: Mapping[str, TriggerObservation],
        **kwargs: Any,
    ) -> None:
        self.execution_owner.observe_triggers(tuple(formations), observations, **kwargs)

    def fill_triggered(
        self, formations: Sequence[LifecycleFormation], **kwargs: Any
    ) -> None:
        self.execution_owner.fill_triggered(tuple(formations), **kwargs)

    def queue_exit(
        self, formation: LifecycleFormation, order: Any, *, observed_at: str
    ) -> bool:
        return self.execution_owner.queue_exit_order(
            formation, order, observed_at=observed_at
        )

    def fill_exit(
        self, formation: LifecycleFormation, *, filled_at: str, open_price: float
    ) -> None:
        self.execution_owner.fill_exit(
            formation, filled_at=filled_at, open_price=open_price
        )

    def refresh_decision_context(self, life: Any, *, timestamp: str) -> Any | None:
        reference = self.context.market_contexts.ordinary_context
        context = self.context.market_contexts.resolve(reference) if reference is not None else None
        if context is not None and context.available_at > coerce_timestamp(timestamp):
            context = None
        if context is None:
            return self.decision_context_for_life(life, timestamp)
        self.execution_owner.engine.update_decision_context(
            life, ContextVersionRef(context.context_id, context.version)
        )
        return context

    def decision_context_for_life(self, life: Any, as_of: str) -> Any | None:
        reference = getattr(life, "parent_context", None)
        context = self.context.context(reference) if reference is not None else None
        if context is not None and context.available_at <= coerce_timestamp(as_of):
            return context
        reference = self.context.market_contexts.ordinary_context
        context = self.context.market_contexts.resolve(reference) if reference is not None else None
        return context if context is not None and context.available_at <= coerce_timestamp(as_of) else None

    def decision_context_for_location(
        self, location: str | None, *, as_of: str
    ) -> Any | None:
        if location is None:
            return None
        return self.context.market_contexts.latest_as_of(
            str(location), coerce_timestamp(as_of)
        )

    def scores_for_context_ref(
        self, reference: Any | None
    ) -> tuple[float | None, float | None]:
        context = self.context.context(reference) if reference is not None else None
        return (
            getattr(context, "long_score", None),
            getattr(context, "short_score", None),
        )

    def local_scores_at(self, timestamp: str) -> tuple[float | None, float | None]:
        context = self.decision_context_for_life(None, timestamp)
        return (
            getattr(context, "long_score", None),
            getattr(context, "short_score", None),
        )

    def context_relation_for_life(self, life: Any, context: Any) -> Any:
        setup = self.execution_owner.engine.setup_for(life.lifecycle_id)
        return getattr(setup, "context_relation", getattr(context, "relation", "neutral"))

    def record_exit_decision_context(
        self, life: Any, rule: Any, *, timestamp: str, reason: str
    ) -> Any | None:
        reference = self.decision_context_for_location(
            getattr(rule, "location", None), as_of=timestamp
        )
        self.context.record_decision_context(
            {
                "lifecycle_id": life.lifecycle_id,
                "rule_id": getattr(rule, "rule_id", None),
                "observed_at": timestamp,
                "reason": reason,
            }
        )
        return getattr(reference, "reference", None)


__all__ = ["OrderflowTransitionOwner", "TransitionAdvance"]
