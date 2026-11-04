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
from typing import Any, Callable, Mapping, Sequence, cast

from ftx_paper.contracts import Instrument
from ftx_paper.execution.events import ExecutionNotification

from .coordinator import OrderflowAdvance, OrderflowInput, OrderflowCoordinator
from .execution import (
    AdmissionStatus,
    ExecutionHost,
    ExecutionCoordinator,
    ExecutionState,
    IntentStatus,
)
from .execution_adapter import OrderflowExecutionAdapter, PaperOrderProposal
from .contextual import build_context_scores
from .context_access import DecisionContextAccess
from .hierarchy import Permission, TriggerObservation
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
from .proposal_buffer import ProposalBuffer
from .opening_gap import OpeningGapOwner
from .execution_clock import ExecutionClock
from .phase4 import ContextPublisher, Phase4Recorder


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
    host_proposals: tuple[PaperOrderProposal, ...] = ()


@dataclass(frozen=True, slots=True)
class _ExecutionHost:
    """Narrow execution boundary detached from the transition owner."""

    context: OrderflowState
    location_registry: LocationRegistry
    _scores_for_context_ref: Callable[[Any | None], tuple[float | None, float | None]]
    _decision_context_for_life: Callable[[Any, str], Any | None]
    _context_relation_for_life: Callable[[Any, Any], Any]
    _decision_context_for_location: Callable[..., Any | None]
    _record_exit_decision_context: Callable[..., Any | None]
    _promote_episode: Callable[..., None]

    def scores_for_context_ref(
        self, reference: Any | None,
    ) -> tuple[float | None, float | None]:
        return self._scores_for_context_ref(reference)

    def decision_context_for_life(self, life: Any, as_of: str) -> Any | None:
        return self._decision_context_for_life(life, as_of)

    def context_relation_for_life(self, life: Any, context: Any) -> Any:
        return self._context_relation_for_life(life, context)

    def decision_context_for_location(
        self, location: str | None, *, as_of: str,
    ) -> Any | None:
        return self._decision_context_for_location(location, as_of=as_of)

    def record_exit_decision_context(
        self, life: Any, rule: Any, *, timestamp: str, reason: str,
    ) -> Any | None:
        return self._record_exit_decision_context(
            life, rule, timestamp=timestamp, reason=reason,
        )

    def promote_episode(self, life: Any, **kwargs: Any) -> None:
        self._promote_episode(life, **kwargs)


@dataclass(slots=True)
class OrderflowEngine:
    """Own the single chronological state boundary used by both modes."""

    location_registry: LocationRegistry
    observation_owner: OrderflowCoordinator = field(init=False)
    opportunity_owner: OpportunityCoordinator = field(init=False)
    execution_owner: ExecutionCoordinator = field(default_factory=ExecutionCoordinator)
    execution_adapter: OrderflowExecutionAdapter | None = None
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]] = field(default_factory=dict, repr=False)
    atr_by_session: Mapping[str, float | None] | None = field(default=None, repr=False)
    context: OrderflowState = field(init=False)
    _clock: datetime | None = field(default=None, init=False, repr=False)
    _execution_notifications: set[str] = field(default_factory=set, init=False, repr=False)
    _formation_cursor: set[str] = field(default_factory=set, init=False, repr=False)
    _proposal_buffer: ProposalBuffer = field(default_factory=ProposalBuffer, init=False, repr=False)
    _context_access: DecisionContextAccess = field(init=False, repr=False)
    _opening_gap: OpeningGapOwner = field(init=False, repr=False)
    _execution_clock: ExecutionClock = field(init=False, repr=False)
    _execution_host: ExecutionHost = field(init=False, repr=False)
    _phase4: Phase4Recorder = field(init=False, repr=False)
    _context_publisher: ContextPublisher = field(init=False, repr=False)

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
            execution_engine=self.execution_owner.engine,
        )
        if self.context.execution_engine is not self.execution_owner.engine:
            raise RuntimeError("orderflow execution ownership is not shared")
        self._context_access = DecisionContextAccess(self)
        self._phase4 = Phase4Recorder(self.context, self.observation_owner)
        self._context_publisher = ContextPublisher(self.context)
        self._execution_host = self._build_execution_host()
        self.execution_owner.host = self._execution_host
        self._opening_gap = OpeningGapOwner(self)
        self._execution_clock = ExecutionClock(self)
        from .opportunity_replay_host import build_opportunity_coordinator
        self.opportunity_owner = build_opportunity_coordinator(
            context=self.context,
            execution=self.execution_owner,
            location_registry=self.location_registry,
            detector=self.observation_owner.detector,
            selected_context=self.selected_interaction_context,
            structure_interactions=self.structure_interactions,
            sync_interaction=self.sync_interaction_snapshot,
        )

    def bind_observation_owner(self, owner: OrderflowCoordinator) -> None:
        """Install one observation owner and reconnect all dependent services."""
        from .opportunity_replay_host import build_opportunity_coordinator
        self.observation_owner = owner
        self.location_registry = owner.location_registry
        self.context.location_registry = owner.location_registry
        self._phase4 = Phase4Recorder(self.context, owner)
        self._execution_host = self._build_execution_host()
        self.execution_owner.host = self._execution_host
        self.opportunity_owner = build_opportunity_coordinator(
            context=self.context,
            execution=self.execution_owner,
            location_registry=self.location_registry,
            detector=owner.detector,
            selected_context=self.selected_interaction_context,
            structure_interactions=self.structure_interactions,
            sync_interaction=self.sync_interaction_snapshot,
        )

    def _build_execution_host(self) -> ExecutionHost:
        return cast(ExecutionHost, _ExecutionHost(
            context=self.context,
            location_registry=self.location_registry,
            _scores_for_context_ref=self.scores_for_context_ref,
            _decision_context_for_life=self.decision_context_for_life,
            _context_relation_for_life=self.context_relation_for_life,
            _decision_context_for_location=self.decision_context_for_location,
            _record_exit_decision_context=self.record_exit_decision_context,
            _promote_episode=self.promote_episode,
        ))

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

    def drain_host_proposals(self) -> tuple[PaperOrderProposal, ...]:
        """Return host proposals emitted by the most recent transition."""
        return self._proposal_buffer.drain()

    def snapshot(self) -> dict[str, Any]:
        """Return the complete canonical mutable state and producer prefix."""
        context = self.context
        # Execution proposals are a synchronous transition buffer.  They are
        # drained by the host before a completed-input checkpoint is written;
        # persisting them would make recovery depend on a transient proposal.
        if context.execution_proposals or self._proposal_buffer.has_pending():
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
                "published_intents": list(self._proposal_buffer.published_intents()),
                "execution_notifications": sorted(self._execution_notifications),
                "formation_cursor": sorted(self._formation_cursor),
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
        canonical = snapshot.get("canonical_state")
        if canonical is not None and not isinstance(canonical, Mapping):
            raise ValueError("orderflow snapshot canonical_state is invalid")
        if isinstance(canonical, Mapping):
            for name in ("published_intents", "execution_notifications", "formation_cursor"):
                value = canonical.get(name, [])
                if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                    raise ValueError(f"orderflow snapshot {name} is invalid")
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
        self._proposal_buffer.restore(canonical.get("published_intents", ()))
        self._execution_notifications = set(canonical.get("execution_notifications", ()))
        self._formation_cursor = set(canonical.get("formation_cursor", ()))

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
            formation_key = (
                f"{life.lifecycle_id}:{serialize_utc(life.formation_available_at)}"
            )
            if formation_key in self._formation_cursor:
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
            self._formation_cursor.add(formation_key)
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
        instrument: Instrument | None = None,
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
        self._proposal_buffer.begin_transition()
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
        source, row, available5 = self._advance_observation(item)
        if regime is not None:
            self.context.regime = regime
        if constraints is not None:
            self.context.execution_constraints = constraints
        timestamp = nifty_label(item.one_minute.time)
        next_time = nifty_label(item.one_minute.available_at)
        if item.prior_value is not None and self.context.continuous is not None:
            self.context.continuous.publish_prior_value(item.prior_value)
        bar_events = list(source.events)
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
        bar_index = len(self.context.rows) - 1
        pre_execution_events: list[dict[str, Any]] = []
        self._execution_clock.settle_pending(
            index=bar_index,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            events=pre_execution_events,
        )
        self.context.pending_bar_events.extend(pre_execution_events)
        new_5m = bool(
            available5
            and (
                self.context.previous5_available_at is None
                or available5[-1].available_at > self.context.previous5_available_at
            )
        )
        self.update_gap_state(
            item.one_minute.session_date,
            list(self.observation_owner.one_minute_aggregates),
            timestamp,
            bar_events,
        )
        if new_5m and available5:
            market_state_before_permission = self.context.market_state
            permission_event_start = len(bar_events)
            scores = build_context_scores(
                available5[-1],
                raw_rows=self.context.rows,
                aggregates=self.context.aggregates,
                atr=self.context.atr_by_session.get(available5[-1].session_date),
            )
            long_score, short_score = scores if scores is not None else (None, None)
            self.handle_permission(available5[-1], long_score, short_score, bar_events)
            permission_events = bar_events[permission_event_start:]
            self.context.input_diagnostics.append({
                "kind": "orderflow_permission_result",
                "time": timestamp,
                "available_at": next_time,
                "market_state_before": market_state_before_permission,
                "market_state_after": self.context.market_state,
                "changed": market_state_before_permission != self.context.market_state,
                "latest_structure_time": nifty_label(available5[-1].time),
                "long_score": long_score,
                "short_score": short_score,
                "event_kinds": [
                    str(event.get("kind"))
                    for event in permission_events
                    if isinstance(event, dict) and event.get("kind") is not None
                ],
            })
        interaction = self.observation_owner.observe_interactions(
            item,
            new_five_minute=new_5m,
        )
        for transition in interaction.transitions:
            self._apply_interaction_transition(transition, bar_events)
        for event in interaction.events:
            self.context.record_event(event)
        self._phase4.source_observations(
            (*self.observation_owner.one_minute_aggregates, *available5),
        )
        self._phase4.context_location(item)
        timeout_events: list[dict[str, Any]] = []
        self.advance_opportunity_timeouts(timestamp, available5, timeout_events)
        self.context.pending_bar_events.extend(timeout_events)
        opportunities_before_structure = frozenset(self.context.opportunities)
        self.context.input_diagnostics.append({
            "kind": "orderflow_structure_boundary",
            "time": timestamp,
            "available_at": next_time,
            "latest_structure_time": (
                nifty_label(available5[-1].time) if available5 else None
            ),
            "market_state_before": self.context.market_state,
            "last_confirmation_context_time": (
                nifty_label(self.context.last_confirmation_context_time)
                if self.context.last_confirmation_context_time is not None else None
            ),
        })
        market_state_before_structure = self.context.market_state
        structure_event_start = len(bar_events)
        self.opportunity_owner.handle_structure(
            timestamp, next_time, row, available5, bar_events,
        )
        structure_events = bar_events[structure_event_start:]
        self.context.input_diagnostics.append({
            "kind": "orderflow_structure_result",
            "time": timestamp,
            "available_at": next_time,
            "market_state_after": self.context.market_state,
            "last_confirmation_context_time": (
                nifty_label(self.context.last_confirmation_context_time)
                if self.context.last_confirmation_context_time is not None else None
            ),
            "market_state_before": market_state_before_structure,
            "changed": bool(
                any(
                    isinstance(event, dict)
                    and event.get("market_state") is not None
                    for event in structure_events
                )
            ),
            "event_kinds": [
                str(event.get("kind"))
                for event in structure_events
                if isinstance(event, dict) and event.get("kind") is not None
            ],
            "new_opportunity_ids": sorted(
                set(self.context.opportunities) - opportunities_before_structure
            ),
        })
        self.opportunity_owner.handle_opening_interaction(
            row, timestamp, next_time, bar_events,
        )
        generated_formations = self.opportunity_owner.new_formations_for(
            next_time,
            emitted=self._formation_cursor,
        )
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
        self._execution_clock.advance(
            tuple(accepted_formations),
            index=bar_index,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available5=available5,
            instrument=instrument,
            pre_execution_done=True,
        )
        self.context.input_diagnostics.append({
            "kind": "orderflow_execution_result",
            "time": timestamp,
            "available_at": next_time,
            "market_state_after": self.context.market_state,
            "accepted_lifecycle_ids": [
                formation.lifecycle_id for formation in accepted_formations
            ],
            "bar_event_kinds": [
                str(event.get("kind"))
                for event in bar_events
                if isinstance(event, dict) and event.get("kind") is not None
            ],
        })
        host_proposals = self._proposal_buffer.drain()
        self._clock = available_at
        return TransitionAdvance(
            observation=OrderflowAdvance(
                (*source.events, *interaction.events),
                interaction.transitions,
            ),
            execution={
                name: tuple(values)
                for name, values in self.context.parity_snapshot().items()
            },
            host_proposals=host_proposals,
        )

    def _advance_observation(
        self, item: OrderflowInput,
    ) -> tuple[OrderflowAdvance, dict[str, Any], list[Aggregate]]:
        """Ingest source evidence and expose completed aggregate evidence."""
        source = self.observation_owner.ingest(item)
        row = item.one_minute.to_dict()
        if not any(
            str(existing.get("time")) == str(item.one_minute.time)
            for existing in self.context.rows
        ):
            self.context.rows.append(row)
        available5 = list(self.observation_owner.five_minute_aggregates)
        self.context.aggregates["1m"] = list(self.observation_owner.one_minute_aggregates)
        self.context.aggregates["5m"] = available5
        return source, row, available5

    def _apply_interaction_transition(
        self,
        transition: Any,
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Apply one detector transition before opportunity evaluation."""
        interaction = transition.interaction
        self.opportunity_owner.publish_interaction_context(interaction, bar_events)
        self.sync_interaction_snapshot(interaction)

    def sync_interaction_snapshot(self, interaction: Any) -> None:
        """Project detector-owned interaction state into the context snapshot."""
        detector_state = self.observation_owner.detector.interactions_for(
            interaction.timeframe,
        ).get(interaction.interaction_id)
        self.context.interactions[interaction.interaction_id] = (
            detector_state.evaluation_snapshot()
            if detector_state is not None else interaction
        )

    def selected_interaction_context(
        self, interaction_id: str, *, as_of: datetime | str,
    ) -> Any | None:
        available_at = coerce_timestamp(as_of)
        return max(
            (
                item for item in self.context.market_contexts.versions.get(
                    f"interaction:{interaction_id}", (),
                )
                if item.available_at <= available_at
            ),
            key=lambda item: item.available_at,
            default=None,
        )

    def structure_interactions(self) -> tuple[Any, ...]:
        return tuple(
            state.evaluation_snapshot()
            for state in self.observation_owner.detector.interactions.values()
            if state.timeframe == "5m"
        )

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
                self.sync_interaction_snapshot(updated)
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
        self._opening_gap.start({
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
        self._context_publisher.ordinary_context(latest, (long_score, short_score))
        current = self._opening_gap.current(latest.session_date)
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

    def update_gap_state(
        self,
        session_date: str,
        available1: Sequence[Aggregate],
        timestamp: str,
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Advance the current opening gap from completed one-minute evidence."""
        self._opening_gap.advance(session_date, available1, timestamp, bar_events)

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

    def decision_context_for_life(self, life: Any, as_of: str) -> Any | None:
        return self._context_access.for_life(life, as_of)

    def decision_context_for_location(
        self, location: str | None, *, as_of: str
    ) -> Any | None:
        return self._context_access.for_location(location, as_of=as_of)

    def scores_for_context_ref(
        self, reference: Any | None
    ) -> tuple[float | None, float | None]:
        return self._context_access.scores_for(reference)

    def context_relation_for_life(self, life: Any, context: Any) -> Any:
        return self._context_access.relation_for(life, context)

    def record_exit_decision_context(
        self, life: Any, rule: Any, *, timestamp: str, reason: str
    ) -> Any | None:
        return self._context_access.record_exit(
            life, rule, timestamp=timestamp, reason=reason,
        )


__all__ = ["OrderflowEngine", "TransitionAdvance"]
