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

from .coordinator import OrderflowAdvance, OrderflowInput, OrderflowCoordinator
from .execution import (
    AdmissionStatus,
    ExecutionCoordinator,
    ExecutionEngine,
    ExecutionState,
    IntentStatus,
)
from .contextual import build_context_scores
from .hierarchy import (
    MarketContext,
    Permission,
    TriggerObservation,
)
from .lifecycle import SetupOpportunity
from .opportunity_coordinator import LifecycleFormation, OpportunityCoordinator
from .opportunity_timeout import advance_opportunity_timeout
from .state_machine_types import (
    ExecutionConstraints,
    LocationRegistry,
    OrderflowState,
)
from .interactions import (
    InteractionCloseReason,
    InteractionStatus,
)
from .orderflow_common import Aggregate
from .timestamps import coerce_timestamp, nifty_label, serialize_utc
from . import snapshot_codec
from .session_policy import classify_opening_gap


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
    host_proposals: tuple[Any, ...] = ()


@dataclass(slots=True)
class OrderflowEngine:
    """Own the single chronological state boundary used by both modes."""

    location_registry: LocationRegistry
    observation_owner: OrderflowCoordinator = field(init=False)
    opportunity_owner: OpportunityCoordinator = field(init=False)
    execution_owner: ExecutionCoordinator = field(default_factory=ExecutionCoordinator)
    execution_adapter: Any | None = None
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]] = field(default_factory=dict, repr=False)
    atr_by_session: Mapping[str, float | None] | None = field(default=None, repr=False)
    context: OrderflowState = field(init=False)
    _clock: datetime | None = field(default=None, init=False, repr=False)
    _published_intents: set[str] = field(default_factory=set, init=False, repr=False)
    _execution_notifications: set[str] = field(default_factory=set, init=False, repr=False)
    _host_proposals: tuple[Any, ...] = field(default=(), init=False, repr=False)

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
        self.context = OrderflowState(
            rows_by_date=self.rows_by_date,
            atr_by_session=self.atr_by_session or {},
            location_registry=self.location_registry,
            execution_engine=ExecutionEngine(),
        )
        # OrderflowState is the canonical mutable execution owner.  The
        # coordinator is only a service facade over that same instance.
        self.execution_owner.engine = self.context.execution_engine
        self.execution_owner.machine = self
        from .opportunity_replay_host import build_opportunity_coordinator
        self.opportunity_owner = build_opportunity_coordinator(
            self,
            detector=self.observation_owner.detector,
        )

    def bind_observation_owner(self, owner: OrderflowCoordinator) -> None:
        """Install one observation owner and reconnect all dependent services."""
        from .opportunity_replay_host import build_opportunity_coordinator
        self.observation_owner = owner
        self.location_registry = owner.location_registry
        self.context.location_registry = owner.location_registry
        self.opportunity_owner = build_opportunity_coordinator(
            self,
            detector=owner.detector,
        )

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

    def drain_host_proposals(self) -> tuple[Any, ...]:
        """Return host proposals emitted by the most recent transition."""
        proposals = self._host_proposals
        self._host_proposals = ()
        return proposals

    def snapshot(self) -> dict[str, Any]:
        """Return the complete canonical mutable state and producer prefix."""
        context = self.context
        # Execution proposals are a synchronous transition buffer.  They are
        # drained by the host before a completed-input checkpoint is written;
        # persisting them would make recovery depend on a transient proposal.
        if context.execution_proposals or self._host_proposals:
            raise RuntimeError("cannot snapshot orderflow state with pending execution proposals")
        return {
            "schema_version": 2,
            "clock": serialize_utc(self._clock) if self._clock is not None else None,
            "canonical_state": {
                "source": {
                    "rows": snapshot_codec.snapshot_value(context.rows),
                    "rows_by_date": snapshot_codec.snapshot_value(context.rows_by_date),
                    "prior_session_rows": snapshot_codec.snapshot_value(context.prior_session_rows),
                    "atr_by_session": snapshot_codec.snapshot_value(context.atr_by_session),
                    "aggregates": snapshot_codec.snapshot_value(context.aggregates),
                    "current_session_date": context.current_session_date,
                },
                "observation_owner": snapshot_codec.snapshot_value(
                    self.observation_owner.snapshot_state()
                ),
                "context_store": snapshot_codec.context_snapshot(context),
                "location_registry": snapshot_codec.location_snapshot(self.location_registry),
                "interactions": snapshot_codec.snapshot_value(context.interactions),
                "opportunities": snapshot_codec.snapshot_value(context.opportunities),
                "execution_states": snapshot_codec.snapshot_value(context.execution_engine.snapshot()),
                "execution_claims": snapshot_codec.snapshot_value(
                    context.execution_engine.snapshot_claims()
                ),
                "regime": snapshot_codec.snapshot_value(context.regime),
                "execution_constraints": snapshot_codec.snapshot_value(context.execution_constraints),
                "opening_context": snapshot_codec.snapshot_value(context.opening_context),
                "continuous": snapshot_codec.snapshot_value(context.continuous),
                "session": snapshot_codec.snapshot_value(context.session),
                "regime_gate": snapshot_codec.snapshot_value(context.regime_gate),
                "market_state": context.market_state,
                "last_confirmation_context_time": snapshot_codec.snapshot_value(
                    context.last_confirmation_context_time,
                ),
                "child_observation": snapshot_codec.snapshot_value(context.child_observation),
                "regime_gate_blocked_lifecycles": snapshot_codec.snapshot_value(
                    context.regime_gate_blocked_lifecycles,
                ),
                "session_extreme_gate_blocked_lifecycles": snapshot_codec.snapshot_value(
                    context.session_extreme_gate_blocked_lifecycles,
                ),
                "pending_bar_events": snapshot_codec.snapshot_value(context.pending_bar_events),
                "decision_context_evidence": snapshot_codec.snapshot_value(context.decision_context_evidence),
                "opportunity_identities": snapshot_codec.snapshot_value(context.opportunity_identities),
                "published_resolved_responses": snapshot_codec.snapshot_value(
                    context.published_resolved_responses,
                ),
                "opportunity_outcomes": snapshot_codec.tuple_mapping_snapshot(context.opportunity_outcomes),
                "or_seed_candles": [serialize_utc(value) for value in context.or_seed_candles],
                "input_diagnostics": snapshot_codec.snapshot_value(context.input_diagnostics),
                "previous5_available_at": snapshot_codec.snapshot_value(context.previous5_available_at),
                "setup_number": context.setup_number,
                "neutral_event_ordinals": snapshot_codec.tuple_mapping_snapshot(context.neutral_event_ordinals),
                "neutral_keys_by_causal_id": snapshot_codec.tuple_mapping_snapshot(
                    context.neutral_keys_by_causal_id,
                ),
                "published_intents": sorted(self._published_intents),
                "execution_notifications": sorted(self._execution_notifications),
            },
            "events": deepcopy(context.events),
            "transitions": deepcopy(context.transitions),
            "parity_streams": deepcopy(context.parity_streams),
            "parity_observed": sorted(context.parity_observed),
            "parity_layers": deepcopy(context.parity_layers),
            "execution_diagnostics": deepcopy(context.execution_diagnostics),
        }

    def restore_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        """Restore producer-owned facts and the transition clock idempotently."""
        if snapshot.get("schema_version") not in {1, 2}:
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
        canonical = snapshot.get("canonical_state")
        if not isinstance(canonical, Mapping):
            return
        source = canonical.get("source")
        if isinstance(source, Mapping):
            if isinstance(source.get("rows"), list):
                self.context.rows = deepcopy(source["rows"])
            if isinstance(source.get("rows_by_date"), Mapping):
                self.context.rows_by_date = deepcopy(source["rows_by_date"])
            if isinstance(source.get("prior_session_rows"), list):
                self.context.prior_session_rows = tuple(
                    tuple(dict(item) for item in values)
                    for values in source["prior_session_rows"]
                    if isinstance(values, list)
                )
            if isinstance(source.get("atr_by_session"), Mapping):
                self.context.atr_by_session = deepcopy(source["atr_by_session"])
            if isinstance(source.get("aggregates"), Mapping):
                self.context.aggregates = deepcopy(source["aggregates"])
            current_session = source.get("current_session_date")
            self.context.current_session_date = (
                str(current_session) if current_session is not None else None
            )
        context_store = canonical.get("context_store")
        if isinstance(context_store, Mapping):
            self.context.market_contexts = snapshot_codec.restore_context_store(context_store)
        location = canonical.get("location_registry")
        if isinstance(location, Mapping):
            restored_registry = snapshot_codec.restore_location_registry(location)
            self.location_registry = restored_registry
            self.context.location_registry = restored_registry
            self.observation_owner.bind_registry(restored_registry)
        observation_owner = canonical.get("observation_owner")
        if isinstance(observation_owner, Mapping):
            self.observation_owner.restore_state(observation_owner)
        for name in (
            "pending_bar_events", "decision_context_evidence",
            "opportunity_identities", "opportunity_outcomes", "neutral_event_ordinals",
            "neutral_keys_by_causal_id",
        ):
            value = canonical.get(name)
            if value is not None:
                setattr(self.context, name, deepcopy(value))
        if "input_diagnostics" in canonical:
            self.context.input_diagnostics = deepcopy(canonical["input_diagnostics"])
        for name in ("opportunity_outcomes", "neutral_event_ordinals", "neutral_keys_by_causal_id"):
            if isinstance(canonical.get(name), list):
                setattr(self.context, name, snapshot_codec.tuple_mapping_restore(canonical[name]))
        raw_previous5 = canonical.get("previous5_available_at")
        self.context.previous5_available_at = (
            coerce_timestamp(raw_previous5) if isinstance(raw_previous5, str) else raw_previous5
        )
        raw_interactions = canonical.get("interactions")
        if isinstance(raw_interactions, Mapping):
            self.context.interactions = {
                str(key): snapshot_codec.restore_interaction_snapshot(value)
                for key, value in raw_interactions.items()
                if isinstance(value, Mapping)
            }
        raw_opportunities = canonical.get("opportunities")
        if isinstance(raw_opportunities, Mapping):
            restored_opportunities: dict[str, SetupOpportunity] = {}
            for lifecycle_id, value in raw_opportunities.items():
                if not isinstance(value, Mapping):
                    continue
                restored = snapshot_codec.restore_opportunity(value)
                restored_opportunities[str(lifecycle_id)] = restored
                self.context.execution_engine.register(restored.lifecycle_id)
            self.context.opportunities = restored_opportunities
        raw_execution_states = canonical.get("execution_states")
        restored_execution_states: dict[str, ExecutionState] | None = None
        if isinstance(raw_execution_states, Mapping):
            restored_execution_states = {
                str(lifecycle_id): snapshot_codec.restore_execution_state(value)
                for lifecycle_id, value in raw_execution_states.items()
                if isinstance(value, Mapping)
            }
        claims = canonical.get("execution_claims")
        if restored_execution_states is not None or isinstance(claims, Mapping):
            self.context.execution_engine.restore_state(
                restored_execution_states or self.context.execution_engine.current(),
                claims if isinstance(claims, Mapping) else None,
            )
        if isinstance(canonical.get("opening_context"), Mapping):
            self.context.opening_context = deepcopy(canonical["opening_context"])
        if isinstance(canonical.get("continuous"), Mapping):
            self.context.continuous = snapshot_codec.restore_continuous(
                canonical["continuous"], self.location_registry,
            )
        for name in ("session", "regime_gate"):
            if name in canonical:
                setattr(self.context, name, snapshot_codec.restore_namespace(canonical[name]))
        session = self.context.session
        if session is not None:
            for name in ("failed_reversal_counts", "failed_reversal_locked_at"):
                value = getattr(session, name, None)
                if isinstance(value, SimpleNamespace):
                    setattr(session, name, vars(value))
        regime = canonical.get("regime")
        if isinstance(regime, Mapping):
            self.context.regime = snapshot_codec.restore_regime(regime)
        elif regime is None:
            self.context.regime = None
        if canonical.get("market_state") is not None:
            self.context.market_state = canonical["market_state"]
        raw_confirmation_time = canonical.get("last_confirmation_context_time")
        self.context.last_confirmation_context_time = (
            coerce_timestamp(raw_confirmation_time)
            if isinstance(raw_confirmation_time, str) else None
        )
        child_observation = canonical.get("child_observation")
        self.context.child_observation = (
            snapshot_codec.restore_context_observation(child_observation)
            if isinstance(child_observation, Mapping) else None
        )
        for name in (
            "regime_gate_blocked_lifecycles",
            "session_extreme_gate_blocked_lifecycles",
        ):
            value = canonical.get(name)
            if isinstance(value, list):
                setattr(self.context, name, set(value))
        constraints = canonical.get("execution_constraints")
        if isinstance(constraints, Mapping):
            self.context.execution_constraints = ExecutionConstraints(
                max_open_positions=int(constraints.get("max_open_positions", 1)),
                max_session_entries=constraints.get("max_session_entries"),
                max_direction_entries=constraints.get("max_direction_entries"),
                post_trade_cooldown_minutes=int(
                    constraints.get("post_trade_cooldown_minutes", 0),
                ),
                fill_permission_margin=constraints.get("fill_permission_margin"),
                stop_atr_buffer=float(constraints.get("stop_atr_buffer", 0.25)),
                failed_reversal_lockout=bool(constraints.get("failed_reversal_lockout", False)),
            )
        if isinstance(canonical.get("published_resolved_responses"), list):
            self.context.published_resolved_responses = {
                tuple(item) for item in canonical["published_resolved_responses"]
                if isinstance(item, list) and len(item) == 2
            }
        if isinstance(canonical.get("or_seed_candles"), list):
            self.context.or_seed_candles = {
                coerce_timestamp(value)
                for value in canonical["or_seed_candles"]
                if isinstance(value, str)
            }
        if isinstance(canonical.get("setup_number"), int):
            self.context.setup_number = canonical["setup_number"]
        self._published_intents = set(canonical.get("published_intents", ()))
        self._execution_notifications = set(canonical.get("execution_notifications", ()))

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
        item: OrderflowInput,
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
        if self.observation_owner.has_seen(item):
            return TransitionAdvance(
                observation=OrderflowAdvance(),
                execution={
                    name: tuple(values)
                    for name, values in self.parity_streams.items()
                },
            )
        if self._clock is not None and available_at < self._clock:
            raise ValueError("orderflow transition clock cannot move backwards")
        self._host_proposals = ()
        previous_session = self.context.current_session_date
        self.context.input_diagnostics.append({
            "kind": "orderflow_clock_input",
            "time": nifty_label(item.one_minute.time),
            "available_at": nifty_label(item.one_minute.available_at),
            **dict(item.input_diagnostic),
        })
        if (
            item.atr is not None
            and item.atr.value is not None
            and item.atr.available_at is not None
            and coerce_timestamp(item.atr.available_at) <= available_at
        ):
            self.context.atr_by_session = {
                **self.context.atr_by_session,
                item.one_minute.session_date: item.atr.value,
            }
        observation = self.observation_owner.advance(item)
        row = item.one_minute.to_dict()
        if not any(
            str(existing.get("time")) == str(item.one_minute.time)
            for existing in self.context.rows
        ):
            self.context.rows.append(row)
        if regime is not None:
            self.context.regime = regime
        if constraints is not None:
            self.context.execution_constraints = constraints
        timestamp = nifty_label(item.one_minute.time)
        next_time = nifty_label(item.one_minute.available_at)
        if item.prior_value is not None and self.context.continuous is not None:
            self.context.continuous.publish_prior_value(item.prior_value)
        bar_events = list(observation.events)
        if previous_session is not None and previous_session != item.one_minute.session_date:
            self.handle_session_boundary(
                previous_session,
                item.one_minute.session_date,
                timestamp,
                item.one_minute,
                bar_events,
            )
        else:
            self.context.current_session_date = item.one_minute.session_date
        available5 = list(self.observation_owner.five_minute_aggregates)
        self.context.aggregates["1m"] = list(self.observation_owner.one_minute_aggregates)
        self.context.aggregates["5m"] = available5
        pre_execution_events = self._settle_pending_execution(
            index=0,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
        )
        self.context.pending_bar_events.extend(pre_execution_events)
        for transition in observation.transitions:
            self.context.interactions[transition.interaction.interaction_id] = (
                transition.interaction
            )
            self.opportunity_owner.publish_interaction_context(
                transition.interaction, bar_events,
            )
        for event in observation.events:
            self.context.record_event(event)
        self._record_phase4_source_observations(
            (*self.observation_owner.one_minute_aggregates, *available5),
        )
        new_5m = bool(
            available5
            and (
                self.context.previous5_available_at is None
                or available5[-1].available_at > self.context.previous5_available_at
            )
        )
        if item.five_minute is not None:
            latest = item.five_minute
            if self.context.last_confirmation_context_time != latest.time:
                self._publish_ordinary_context(latest)
        self.update_gap_state(
            item.one_minute.session_date,
            list(self.observation_owner.one_minute_aggregates),
            timestamp,
            bar_events,
        )
        if new_5m and available5:
            scores = build_context_scores(
                available5[-1],
                raw_rows=self.context.rows,
                aggregates=self.context.aggregates,
                atr=self.context.atr_by_session.get(available5[-1].session_date),
            )
            long_score, short_score = scores if scores is not None else (None, None)
            self.handle_permission(available5[-1], long_score, short_score, bar_events)
        timeout_events: list[dict[str, Any]] = []
        self.advance_opportunity_timeouts(timestamp, available5, timeout_events)
        self.context.pending_bar_events.extend(timeout_events)
        self.opportunity_owner.handle_structure(
            timestamp, next_time, row, available5, bar_events,
        )
        self.opportunity_owner.handle_opening_interaction(
            row, timestamp, next_time, bar_events,
        )
        self._record_phase4_context_location(item)
        generated_formations = self.opportunity_owner.formations_for(next_time)
        accepted_formations = self._accept_formations(
            (*formations, *generated_formations),
        )
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
            pre_execution_done=True,
        )
        host_proposals = self._host_proposals
        self._clock = available_at
        return TransitionAdvance(
            observation=observation,
            execution={
                name: tuple(values)
                for name, values in self.context.parity_snapshot().items()
            },
            host_proposals=host_proposals,
        )

    def _record_phase4_context_location(self, item: OrderflowInput) -> None:
        """Publish canonical context/location facts for completed aggregates."""
        context = self.context
        ordinary = (
            context.market_contexts.resolve(context.market_contexts.ordinary_context)
            if context.market_contexts.ordinary_context is not None else None
        )
        observations_by_timeframe = {
            "1m": tuple(
                observation for observation in item.observations_1m
                if observation.timeframe == "1m"
                and observation.source_aggregate_id == nifty_label(item.one_minute.time)
                and observation.reference_source_timeframe in {None, "1m"}
            ),
        }
        aggregates = [(item.one_minute, observations_by_timeframe["1m"])]
        if item.five_minute is not None:
            observations_by_timeframe["5m"] = tuple(
                observation for observation in item.observations_5m
                if observation.timeframe == "5m"
                and observation.source_aggregate_id == nifty_label(item.five_minute.time)
            )
            aggregates.append((item.five_minute, observations_by_timeframe["5m"]))
        known = {
            str(fact.get("identity"))
            for fact in context.parity_streams.get("context_location", ())
        }
        for aggregate, observations in aggregates:
            identity = f"context-location:{aggregate.timeframe}:{nifty_label(aggregate.time)}"
            if identity in known:
                continue
            context.record_parity_fact("context_location", {
                "kind": "context_location_observed",
                "occurred_at": aggregate.end_time,
                "observed_at": aggregate.end_time,
                "available_at": aggregate.available_at,
                "identity": identity,
                "causal_parent_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
                "values": {
                    "timeframe": aggregate.timeframe,
                    "source_observation_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
                    "context": {
                        "context_id": ordinary.context_id,
                        "version": ordinary.version,
                        "location": ordinary.location,
                        "market_state": ordinary.market_state,
                        "available_at": ordinary.available_at,
                        "parent_context_id": ordinary.parent_context_id,
                        "definition_version": ordinary.definition_version,
                        "source_provenance": list(ordinary.source_provenance),
                    } if ordinary is not None else None,
                    "locations": [observation.to_dict() for observation in observations],
                },
                "unavailable": [] if ordinary is not None and observations else ["missing_context"],
            })
            known.add(identity)

    def _record_phase4_source_observations(
        self, aggregates: Sequence[Aggregate],
    ) -> None:
        """Record source facts once, at the owner that advances causal time."""
        known = {
            str(fact.get("identity"))
            for fact in self.context.parity_streams.get("source_observation", ())
        }
        one_minute = self.observation_owner.one_minute_aggregates
        for aggregate in aggregates:
            identity = f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}"
            if identity in known:
                continue
            children = tuple(
                child for child in one_minute
                if aggregate.timeframe == "5m"
                and aggregate.time <= child.time <= aggregate.end_time
                and child.available_at <= aggregate.available_at
            )
            self.context.record_parity_fact("source_observation", {
                "kind": "source_observation",
                "occurred_at": aggregate.end_time,
                "observed_at": aggregate.end_time,
                "available_at": aggregate.available_at,
                "sequence": len(self.context.parity_streams.get("source_observation", ())),
                "identity": identity,
                "causal_parent_id": (
                    f"aggregate:1m:{nifty_label(children[-1].time)}"
                    if children else None
                ),
                "values": {
                    "aggregate": aggregate.to_dict(),
                    "source_fingerprint": aggregate.source_fingerprint,
                },
                "unavailable": [],
                "lineage": [
                    f"aggregate:1m:{nifty_label(child.time)}" for child in children
                ],
            })
            known.add(identity)

    def _publish_ordinary_context(self, latest: Aggregate) -> None:
        """Publish the canonical 5m decision context at bar availability."""
        context = self.context
        scores = build_context_scores(
            latest,
            raw_rows=context.rows,
            aggregates=context.aggregates,
            atr=context.atr_by_session.get(latest.session_date),
        )
        long_score, short_score = scores if scores is not None else (None, None)
        context_id = (
            f"5m-context-{latest.symbol}-{latest.session_date}-1m_5m"
        )
        published = MarketContext(
            context_id=context_id,
            version=f"v{len(context.market_contexts.versions.get(context_id, ())) + 1}",
            timestamp=latest.time,
            available_at=latest.available_at,
            location=None,
            market_state=context.market_state,
            definition_version="1m_5m",
            source_timeframe="5m",
            long_score=long_score,
            short_score=short_score,
            evidence=(
                {
                    "completed_bar": True,
                    "open": latest.open,
                    "high": latest.high,
                    "low": latest.low,
                    "close": latest.close,
                },
            ),
            source_provenance=(
                {
                    "source_candle_id": nifty_label(latest.time),
                    "available_at": latest.available_at,
                },
            ),
        )
        reference = context.publish_context(published)
        context.market_contexts.ordinary_context = reference
        context.last_confirmation_context_time = latest.time

    def handle_session_boundary(
        self,
        previous_session: str | None,
        session_date: str,
        timestamp: str,
        opening: Aggregate,
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Switch the daily opening-gap context without resetting the stream."""
        context = self.context
        context.current_session_date = session_date
        continuous = context.continuous
        context.opening_context = classify_opening_gap(
            opening=opening,
            registry=self.location_registry,
            prior_value=(
                continuous.prior_value_as_of(opening.available_at)
                if continuous is not None else None
            ),
        )
        context.emit({
            "kind": "session_boundary",
            "previous_session": previous_session,
            "session_date": session_date,
            "observed_at": timestamp,
            "available_at": timestamp,
        }, bar_events)
        detector = self.observation_owner.detector
        for interaction in tuple(detector.interactions.values()):
            if interaction.status is not InteractionStatus.ACTIVE:
                continue
            templates = tuple(
                item for item in (self.opportunity_owner.host.templates.values())
                if interaction.location in item.locations
            )
            updated = detector.advance_session(
                interaction.interaction_id,
                available_at=timestamp,
                carry_sessions=max(
                    (item.carry_sessions for item in templates), default=-1,
                ),
            )
            if updated.status is InteractionStatus.ENDED:
                context.interactions[updated.interaction_id] = updated
                context.emit(
                    {
                        "kind": "interaction_ended",
                        "interaction_id": updated.interaction_id,
                        "reason": InteractionCloseReason.SESSION_END.value,
                        "observed_at": timestamp,
                        "available_at": timestamp,
                    },
                    bar_events,
                )
        self._start_opening_gap({
            **context.opening_context,
            "session_date": session_date,
        })

    def handle_permission(
        self,
        latest: Aggregate,
        long_score: float | None,
        short_score: float | None,
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Publish the completed-5m permission regime (canonical handle_permission)."""
        context = self.context
        context.previous5_available_at = latest.available_at
        current = self._current_opening_gap(latest.session_date)
        ordinary_reference = context.market_contexts.ordinary_context
        context.regime = context.opening_gap_policy.regime_for(
            state=str(current.get("state")) if current is not None else None,
            direction=str(current.get("direction")) if current is not None else None,
            version="5m-permission-allowed",
            context="5m_directional_context",
            restrictions=("completed_5m_only",),
        )
        context.emit(
            {
                "kind": "permission",
                "level": "5m",
                "time": nifty_label(latest.time),
                "decision_at": nifty_label(latest.time),
                "observed_at": latest.end_time,
                "aggregate_time": latest.time,
                "available_at": latest.available_at,
                "version": context.regime.version,
                "status": Permission.ALLOWED.value,
                "owner": "5m",
                "evidence_timeframe": "5m",
                "context_id": (
                    ordinary_reference.context_id
                    if ordinary_reference is not None else None
                ),
                "market_state": context.market_state,
                "directional": context.regime.long != context.regime.short,
                "long_permission": context.regime.long.value,
                "short_permission": context.regime.short.value,
                "opening_gap_state": context.regime.opening_gap_state,
                "long_score": long_score,
                "short_score": short_score,
                "reference_level": None,
                "opening_context": dict(context.opening_context),
            },
            bar_events,
        )
        for life in context.opportunities.values():
            if life.active:
                context.emit(
                    {
                        "kind": "context_observed",
                        "observed_at": latest.end_time,
                        "available_at": latest.available_at,
                        "context": context.snapshot(
                            context.market_contexts.resolve(ordinary_reference)
                            if ordinary_reference is not None else None
                        ),
                        "long_score": long_score,
                        "short_score": short_score,
                    },
                    bar_events,
                )

    def _start_opening_gap(self, opening: Mapping[str, Any]) -> None:
        continuous = self.context.continuous
        if continuous is None or opening.get("direction") not in {"up", "down"}:
            return
        continuous.open_gaps.append({
            **opening,
            "state": opening.get("state", opening.get("classification")),
            "completed_bars": 0,
        })

    def _current_opening_gap(self, session_date: str) -> Mapping[str, Any] | None:
        continuous = self.context.continuous
        if continuous is None:
            return None
        return next(
            (
                gap for gap in reversed(continuous.open_gaps)
                if gap.get("session_date") == session_date
            ),
            None,
        )

    def update_gap_state(
        self,
        session_date: str,
        available1: Sequence[Aggregate],
        timestamp: str,
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Advance the current opening gap from completed one-minute evidence."""
        continuous = self.context.continuous
        if continuous is None:
            return
        current = [item for item in available1 if item.session_date == session_date]
        if not current:
            return
        latest = current[-1]
        for gap in reversed(continuous.open_gaps):
            if gap.get("session_date", session_date) != session_date or gap.get("state") in {
                "GAP_DOWN_RECLAIMED", "GAP_UP_RECLAIMED", "RETURN_TO_VALUE", "EXPIRED",
            }:
                continue
            if gap.get("last_evaluated") == latest.available_at:
                continue
            gap["last_evaluated"] = latest.available_at
            gap["completed_bars"] = int(gap.get("completed_bars", 0)) + 1
            reference = float(gap["reference_level"])
            prior_value = continuous.prior_value_as_of(latest.available_at)
            prior_reference = getattr(prior_value, "reference", None)
            state = self.context.opening_gap_policy.transition(
                direction=str(gap["direction"]),
                completed_bars=int(gap["completed_bars"]),
                close=latest.close,
                reference_level=reference,
                prior_value_low=(
                    prior_reference.value_area_low
                    if prior_reference is not None else None
                ),
                prior_value_high=(
                    prior_reference.value_area_high
                    if prior_reference is not None else None
                ),
            )
            if state is None or state == gap.get("state"):
                continue
            previous_state = str(gap.get("state", "GAP_CANDIDATE"))
            gap["state"] = state
            reference_id = getattr(prior_reference, "reference_id", None)
            reference_scope = getattr(prior_reference, "scope", None)
            reference_identity = (
                {"value_reference_id": reference_id, "value_scope": reference_scope}
                if reference_id is not None else {}
            )
            self.context.emit({
                "kind": "opening_gap_transition",
                "session_date": session_date,
                "from_state": previous_state,
                "state": state,
                "observed_at": timestamp,
                "available_at": latest.available_at,
                "reference_level": reference,
                **reference_identity,
            }, bar_events)
            self._publish_opening_gap_regime(
                state=state,
                direction=str(gap["direction"]),
                available_at=latest.available_at,
                bar_events=bar_events,
            )
            break

    def _publish_opening_gap_regime(
        self,
        *,
        state: str,
        direction: str,
        available_at: Any,
        bar_events: list[dict[str, Any]],
    ) -> None:
        context = self.context
        context.regime = context.opening_gap_policy.regime_for(
            state=state,
            direction=direction,
            version=f"opening-gap-permission-{available_at}",
            context="opening_gap",
            restrictions=("causal_opening_gap",),
        )
        context.emit(
            {
                "kind": "opening_gap_permission",
                "state": state,
                "direction": direction,
                "available_at": available_at,
                "version": context.regime.version,
                "long": context.regime.long.value,
                "short": context.regime.short.value,
            },
            bar_events,
        )

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
        pre_execution_done: bool = False,
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
        row_dict = dict(row)
        if not pre_execution_done:
            self._settle_pending_execution(
                index=index,
                timestamp=timestamp,
                next_time=next_time,
                row=row_dict,
                events=events,
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
            if life.active:
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

    def _settle_pending_execution(
        self,
        *,
        index: int,
        timestamp: str,
        next_time: str,
        row: Mapping[str, Any],
        events: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Settle existing execution state before phase4 publication."""
        emitted = events if events is not None else []
        for life in tuple(self.context.opportunities.values()):
            if life.active or self.context.position_is_open(life):
                self.execution_owner.handle_execution(
                    index,
                    timestamp,
                    next_time,
                    dict(row),
                    [],
                    emitted,
                    life,
                    evaluate_entries=False,
                )
        return emitted

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
            self._host_proposals = (*self._host_proposals, proposal)
            self._published_intents.add(intent.intent_id)

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


__all__ = ["OrderflowEngine", "TransitionAdvance"]
