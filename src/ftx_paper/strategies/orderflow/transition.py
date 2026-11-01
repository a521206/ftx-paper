"""Shared chronological owner for live and bounded orderflow transitions.

This module is deliberately an orchestration boundary.  Policy remains in the
canonical-compatible coordinators; this owner only gives live and replay one
mutable clock, one interaction owner, and one execution owner.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from .coordinator import OrderflowAdvance, OrderflowClockInput, OrderflowCoordinator
from .execution import ExecutionCoordinator, ExecutionEngine
from .hierarchy import ContextVersionRef, TriggerObservation
from .opportunity_coordinator import LifecycleFormation
from .opportunity_timeout import advance_opportunity_timeout
from .state_machine_types import ExecutionConstraints, LocationRegistry, ReplayContext
from .timestamps import coerce_timestamp


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
    context: ReplayContext = field(init=False)
    mode: str = "FULL"
    _clock: datetime | None = field(default=None, init=False, repr=False)
    _published_intents: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        self.observation_owner = OrderflowCoordinator(self.location_registry)
        self.context = ReplayContext(
            location_registry=self.location_registry,
            execution_engine=ExecutionEngine(),
        )
        # ReplayContext is the canonical mutable execution owner.  The
        # coordinator is only a service facade over that same instance.
        self.execution_owner.engine = self.context.execution_engine
        self.execution_owner.machine = self

    @property
    def clock(self) -> datetime | None:
        return self._clock

    @property
    def parity_streams(self) -> dict[str, list[dict[str, Any]]]:
        return self.context.parity_snapshot()

    @property
    def engine(self) -> Any:
        """Expose the canonical execution state for lifecycle adapters."""
        return self.execution_owner.engine

    def attach_setup(self, formation: LifecycleFormation) -> None:
        self.execution_owner.attach_setup(formation)

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
            )
        timestamp = str(item.one_minute.time)
        next_time = str(item.one_minute.available_at)
        row = item.one_minute.to_dict()
        available5 = list(self.observation_owner.five_minute_aggregates)
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
        events: list[dict[str, Any]] = []
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
                as_of=next_time,
            )
            if life.timeout_count >= life.timeout_limit:
                self.execution_owner.expire_opportunity(
                    life,
                    timestamp=timeout.expiry_at or timestamp,
                    bar_events=events,
                    reason="evidence_timeout",
                    timeout_event={
                        "timeout_count": life.timeout_count,
                        "timeout_limit": life.timeout_limit,
                        "evidence_timeframe": life.timeout_evidence_timeframe,
                        "counted_source_ids": list(life.timeout_counted_source_ids),
                        "counted_snapshot_ids": list(life.timeout_counted_snapshot_ids),
                    },
                )
        for formation in managed:
            life = formation.lifecycle
            if life is None:
                continue
            self.execution_owner.handle_execution(
                index, timestamp, next_time, dict(row), [], events, life,
                evaluate_entries=False,
            )
            self.execution_owner.apply_session_deadline(
                life,
                session_date=str(life.session_date),
                timestamp=timestamp,
                bar_events=events,
            )
            self.execution_owner.handle_management(
                index, timestamp, next_time, dict(row), list(available5), events, life,
                defer_discretionary_execution=True,
            )
            self.execution_owner.handle_execution(
                index, timestamp, next_time, dict(row), [], events, life,
                evaluate_entries=True,
            )
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
            adapter.proposal(
                intent,
                instrument=instrument,
                quantity=decision.quantity,
                observed_at=timestamp,
                available_at=next_time,
            )
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
        context = self.context.market_contexts.latest_as_of(
            "ordinary", coerce_timestamp(timestamp)
        )
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
        return self.context.market_contexts.latest_as_of(
            "ordinary", coerce_timestamp(as_of)
        )

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
        context = self.context.market_contexts.latest_as_of(
            "ordinary", coerce_timestamp(timestamp)
        )
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
