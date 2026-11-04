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
from typing import Any, Callable, Mapping, Sequence, TypeVar, cast

from ftx_paper.contracts import Instrument
from ftx_paper.execution.events import ExecutionNotification

from .coordinator import OrderflowAdvance, OrderflowInput, OrderflowCoordinator
from .execution import ExecutionHost, ExecutionCoordinator, ExecutionState
from .execution_adapter import OrderflowExecutionAdapter, PaperOrderProposal
from .contextual import build_context_scores
from .context_access import DecisionContextAccess
from .hierarchy import TriggerObservation
from .lifecycle import SetupOpportunity
from .opportunity_coordinator import LifecycleFormation, OpportunityCoordinator
from .state_machine_types import (
    ExecutionConstraints,
    LocationRegistry,
    OrderflowState,
)
from .orderflow_common import Aggregate
from .timestamps import coerce_timestamp, nifty_label, serialize_utc
from . import snapshot_codec
from .proposal_buffer import ProposalBuffer
from .opening_gap import OpeningGapOwner
from .execution_clock import ExecutionClock
from .phase4 import ContextPublisher, Phase4Recorder
from .permission import PermissionOwner
from .session_boundary import SessionBoundaryOwner


T = TypeVar("T")


def _execution_lifecycle_snapshot(state: Any) -> dict[str, Any]:
    def value(item: Any) -> Any:
        return getattr(item, "value", item)

    position = state.position
    pending_exit = state.pending_exit
    intent = state.intent
    return {
        "intent": {
            "status": value(intent.status),
            "triggered_at": getattr(intent, "triggered_at", None),
        } if intent is not None else None,
        "pending_exit": {
            "reason": pending_exit.reason,
            "available_at": pending_exit.available_at,
            "quantity": pending_exit.quantity,
        } if pending_exit is not None else None,
        "position": {
            "status": value(position.status),
            "quantity": position.quantity,
            "fill_price": position.fill_price,
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
    _permission: PermissionOwner = field(init=False, repr=False)
    _session_boundary: SessionBoundaryOwner = field(init=False, repr=False)

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
        self._permission = PermissionOwner(self)
        self._session_boundary = SessionBoundaryOwner(self)
        from .opportunity_replay_host import build_opportunity_coordinator
        self.opportunity_owner = build_opportunity_coordinator(
            context=self.context,
            execution=self.execution_owner,
            location_registry=self.location_registry,
            detector=self.observation_owner.detector,
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
        """Delegate lifecycle promotion to the opportunity owner."""
        self.opportunity_owner.promote_episode(
            life,
            observed_at=observed_at,
            available_at=available_at,
            bar_events=bar_events,
        )

    @property
    def clock(self) -> datetime | None:
        return self._clock

    @property
    def parity_streams(self) -> dict[str, list[dict[str, Any]]]:
        return self.context.parity_snapshot()

    def _execution_snapshot(self) -> Mapping[str, tuple[dict[str, Any], ...]]:
        return {
            name: tuple(values)
            for name, values in self.context.parity_snapshot().items()
        }

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
        self._validate_snapshot(snapshot)
        raw_clock = snapshot.get("clock")
        self._clock = coerce_timestamp(raw_clock) if raw_clock else None
        self.context.events = deepcopy(snapshot.get("events", []))
        self.context.transitions = deepcopy(snapshot.get("transitions", []))
        self.context.parity_streams = deepcopy(snapshot.get("parity_streams", {}))
        self.context.parity_observed = set(snapshot.get("parity_observed", []))
        self.context.parity_layers = deepcopy(snapshot.get("parity_layers", {}))
        self.context.execution_diagnostics = deepcopy(snapshot.get("execution_diagnostics", []))
        canonical = snapshot.get("canonical_state")
        if not isinstance(canonical, Mapping):
            return
        self._restore_snapshot_canonical(canonical)

    @staticmethod
    def _validate_snapshot(snapshot: Mapping[str, Any]) -> None:
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

    def _restore_snapshot_canonical(self, canonical: Mapping[str, Any]) -> None:
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
                execution=self._execution_snapshot(),
            )
        if self._clock is not None and available_at < self._clock:
            raise ValueError("orderflow transition clock cannot move backwards")
        source, row, available5, timestamp, next_time, bar_events, bar_index = (
            self._prepare_transition(
                item,
                available_at=available_at,
                regime=regime,
                constraints=constraints,
            )
        )
        available5 = [
            aggregate for aggregate in available5
            if coerce_timestamp(aggregate.available_at) <= coerce_timestamp(timestamp)
        ]
        available1 = [
            aggregate for aggregate in self.observation_owner.one_minute_aggregates
            if coerce_timestamp(aggregate.available_at) <= coerce_timestamp(timestamp)
        ]
        new_5m = self._advance_pre_structure(
            item,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available1=available1,
            available5=available5,
            bar_index=bar_index,
            bar_events=bar_events,
        )
        interaction = self._process_interactions(
            item,
            new_5m=new_5m,
            available1=available1,
            available5=available5,
            timestamp=timestamp,
            bar_events=bar_events,
        )
        self._record_phase4(
            item,
            available1=available1,
            available5=available5,
            timestamp=timestamp,
            available_at=next_time,
        )
        self._process_timeouts(
            timestamp=timestamp,
            available_at=next_time,
            available5=available5,
        )
        self._advance_structure(
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available5=available5,
            bar_events=bar_events,
        )
        generated_formations = self.opportunity_owner.new_formations_for(
            next_time,
            emitted=self._formation_cursor,
        )
        accepted_formations = self.opportunity_owner.accept_formations(
            (*formations, *generated_formations),
            emitted=self._formation_cursor,
        )
        self._trace_phase(
            "post_structure_execution",
            timestamp,
            next_time,
            lambda: self._advance_execution(
                accepted_formations,
                regime=regime,
                regime_publications=regime_publications,
                triggers=triggers,
                bar_opens=bar_opens,
                decision_scores=decision_scores,
                constraints=constraints,
                instrument=instrument,
                index=bar_index,
                timestamp=timestamp,
                next_time=next_time,
                row=row,
                available5=available5,
            ),
        )
        self._record_execution_result(
            timestamp=timestamp,
            next_time=next_time,
            accepted_formations=accepted_formations,
            bar_events=bar_events,
        )
        return self._finish_transition(
            available_at=available_at,
            source=source,
            interaction=interaction,
        )

    def _prepare_transition(
        self,
        item: OrderflowInput,
        *,
        available_at: datetime,
        regime: Any,
        constraints: ExecutionConstraints | None,
    ) -> tuple[
        OrderflowAdvance,
        dict[str, Any],
        list[Aggregate],
        str,
        str,
        list[dict[str, Any]],
        int,
    ]:
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
        timestamp = nifty_label(item.one_minute.time)
        source, row, available5 = self._ingest_observation(item)
        if regime is not None:
            self.context.regime = regime
        if constraints is not None:
            self.context.execution_constraints = constraints
        next_time = nifty_label(item.one_minute.available_at)
        if item.prior_value is not None and self.context.continuous is not None:
            self.context.continuous.publish_prior_value(item.prior_value)
        bar_events = list(source.events)
        if previous_session is not None and previous_session != item.one_minute.session_date:
            self._session_boundary.advance(
                previous_session,
                item.one_minute.session_date,
                timestamp,
                item.one_minute,
                bar_events,
            )
        else:
            self.context.current_session_date = item.one_minute.session_date
        return source, row, available5, timestamp, next_time, bar_events, len(self.context.rows) - 1

    def _advance_pre_structure(
        self,
        item: OrderflowInput,
        *,
        timestamp: str,
        next_time: str,
        row: dict[str, Any],
        available1: list[Aggregate],
        available5: list[Aggregate],
        bar_index: int,
        bar_events: list[dict[str, Any]],
    ) -> bool:
        pre_execution_events: list[dict[str, Any]] = []
        self._trace_phase("pending_execution_settlement", timestamp, next_time, lambda: self._execution_clock.settle_pending(
            index=bar_index,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            events=pre_execution_events,
        ))
        self.context.pending_bar_events.extend(pre_execution_events)
        new_5m = bool(
            available5
            and (
                self.context.previous5_available_at is None
                or available5[-1].available_at > self.context.previous5_available_at
            )
        )
        self._trace_phase("opening_gap_update", timestamp, next_time, lambda: self._opening_gap.advance(
            item.one_minute.session_date,
            available1,
            timestamp,
            bar_events,
        ))
        self._trace_phase("active_position_gate", timestamp, next_time, lambda: self.execution_owner.active_position_gate.update(self.context, timestamp))
        if new_5m and available5:
            market_state_before_permission = self.context.market_state
            previous5_before_permission = self.context.previous5_available_at
            permission_event_start = len(bar_events)
            scores = build_context_scores(
                available5[-1],
                raw_rows=self.context.rows,
                aggregates=self.context.aggregates,
                atr=self.context.atr_by_session.get(available5[-1].session_date),
            )
            long_score, short_score = scores if scores is not None else (None, None)
            self._trace_phase("permission", timestamp, next_time, lambda: self._permission.publish(available5[-1], long_score, short_score, bar_events))
            permission_events = bar_events[permission_event_start:]
            self.context.input_diagnostics.append({
                "kind": "orderflow_permission_result",
                "time": timestamp,
                "available_at": next_time,
                "market_state_before": market_state_before_permission,
                "market_state_after": self.context.market_state,
                "changed": market_state_before_permission != self.context.market_state,
                "latest_structure_time": nifty_label(available5[-1].time),
                "decision_timestamp": timestamp,
                "decision_available_at": next_time,
                "latest_5m_time": nifty_label(available5[-1].time),
                "latest_5m_available_at": available5[-1].available_at,
                "previous5_available_at_before": previous5_before_permission,
                "previous5_available_at_after": self.context.previous5_available_at,
                "new_5m": new_5m,
                "long_score": long_score,
                "short_score": short_score,
                "event_kinds": [
                    str(event.get("kind"))
                    for event in permission_events
                    if isinstance(event, dict) and event.get("kind") is not None
                ],
            })
        return new_5m

    def _process_interactions(
        self,
        item: OrderflowInput,
        *,
        new_5m: bool,
        available1: list[Aggregate],
        available5: list[Aggregate],
        timestamp: str,
        bar_events: list[dict[str, Any]],
    ) -> OrderflowAdvance:
        available_at = nifty_label(item.one_minute.available_at)
        def handle_interactions() -> OrderflowAdvance:
            interaction = self.observation_owner.observe_interactions(
                item,
                new_five_minute=new_5m,
                available1=available1,
                available5=available5,
                input_diagnostics=self.context.input_diagnostics,
            )
            for transition in interaction.transitions:
                self.opportunity_owner.apply_interaction_transition(transition, bar_events)
            for event in interaction.events:
                self.context.record_event(event)
            return interaction

        interaction = self._trace_phase(
            "interaction_handling", timestamp, available_at, handle_interactions,
        )

        return interaction

    def _process_timeouts(
        self,
        *,
        timestamp: str,
        available_at: str,
        available5: list[Aggregate],
    ) -> None:
        timeout_events: list[dict[str, Any]] = []
        self._trace_phase(
            "timeout_processing",
            timestamp,
            available_at,
            lambda: self.opportunity_owner.advance_timeouts(
                timestamp, available5, timeout_events,
            ),
        )
        self.context.pending_bar_events.extend(timeout_events)

    def _record_phase4(
        self,
        item: OrderflowInput,
        *,
        available1: list[Aggregate],
        available5: list[Aggregate],
        timestamp: str,
        available_at: str,
    ) -> None:
        self._trace_phase(
            "phase4_source_observations",
            timestamp,
            available_at,
            lambda: self._phase4.source_observations(
                (*available1, *available5),
            ),
        )
        self._trace_phase(
            "phase4_context_location",
            timestamp,
            available_at,
            lambda: self._phase4.context_location(item, timeframe="1m"),
        )
        if available5 and item.five_minute is not None:
            self._trace_phase(
                "phase4_context_location",
                timestamp,
                available_at,
                lambda: self._phase4.context_location(item, timeframe="5m"),
            )

    def _advance_structure(
        self,
        *,
        timestamp: str,
        next_time: str,
        row: dict[str, Any],
        available5: list[Aggregate],
        bar_events: list[dict[str, Any]],
    ) -> None:
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
        self._trace_phase("structure", timestamp, next_time, lambda: self.opportunity_owner.handle_structure(
            timestamp, next_time, row, available5, bar_events,
        ))
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
        self._trace_phase("opening_interaction", timestamp, next_time, lambda: self.opportunity_owner.handle_opening_interaction(
            row, timestamp, next_time, bar_events,
        ))

    def _trace_phase(
        self,
        phase: str,
        observed_at: str,
        available_at: str,
        action: Callable[[], T],
    ) -> T:
        if phase not in {
            "permission", "interaction_handling", "pending_execution_settlement",
            "timeout_processing", "phase4_source_observations",
            "phase4_context_location", "structure", "post_structure_execution",
        }:
            return action()
        before = self._trace_snapshot()
        event_start = len(self.context.events)
        result = action()
        after = self._trace_snapshot()
        phase_events = self.context.events[event_start:]
        self.context.input_diagnostics.append({
            "kind": "phase4_clock_trace",
            "ordinal": sum(
                1 for item in self.context.input_diagnostics
                if item.get("kind") == "phase4_clock_trace"
            ),
            "phase": phase,
            "observed_at": observed_at,
            "available_at": available_at,
            "before": before,
            "after": after,
            "event_range": [event_start, len(self.context.events)],
            "event_kinds": sorted({
                str(event.get("kind")) for event in phase_events
                if event.get("kind")
            }),
            "event_links": [
                {
                    "event_id": event.get("event_id", event.get("id")),
                    "sequence": event.get("sequence", event_start + offset),
                    "kind": event.get("kind"),
                }
                for offset, event in enumerate(phase_events)
            ],
        })
        return result

    def _trace_snapshot(self) -> dict[str, Any]:
        ordinary_ref = self.context.market_contexts.ordinary_context
        ordinary = (
            self.context.market_contexts.resolve(ordinary_ref)
            if ordinary_ref is not None else None
        )
        return {
            "market_state": self.context.market_state,
            "ordinary_context": (
                {"context_id": ordinary.context_id, "version": ordinary.version,
                 "available_at": ordinary.available_at,
                 "market_state": ordinary.market_state}
                if ordinary is not None else None
            ),
            "active_interaction_count": sum(
                1 for interaction in self.context.interactions.values()
                if str(getattr(interaction, "status", "")) in {"ACTIVE", "InteractionStatus.ACTIVE"}
            ),
            "opportunity_count": len(self.context.opportunities),
            "execution_lifecycles": {
                life.lifecycle_id: {
                    "active": life.active,
                    "episode_confirmed": getattr(life, "episode_confirmed", None),
                    "state": _execution_lifecycle_snapshot(
                        self.execution_owner.engine.state_for(life.lifecycle_id),
                    ),
                }
                for life in self.context.opportunities.values()
            },
            "parity_counts": {
                stream: len(self.context.parity_streams.get(stream, ()))
                for stream in ("context_location", "interaction_response", "opportunity_lifecycle")
            },
        }

    def _advance_execution(
        self,
        accepted_formations: Sequence[LifecycleFormation],
        *,
        regime: Any,
        regime_publications: Sequence[tuple[str, Any]],
        triggers: Mapping[str, TriggerObservation] | None,
        bar_opens: Mapping[str, float] | None,
        decision_scores: Mapping[str, tuple[float | None, float | None]] | None,
        constraints: ExecutionConstraints | None,
        instrument: Instrument | None,
        index: int,
        timestamp: str,
        next_time: str,
        row: dict[str, Any],
        available5: list[Aggregate],
    ) -> None:
        self._execution_clock.advance_transition(
            tuple(accepted_formations),
            regime=regime,
            regime_publications=regime_publications,
            triggers=triggers,
            bar_opens=bar_opens,
            decision_scores=decision_scores,
            constraints=constraints,
            decision_at=next_time,
            index=index,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available5=available5,
            instrument=instrument,
        )

    def _record_execution_result(
        self,
        *,
        timestamp: str,
        next_time: str,
        accepted_formations: Sequence[LifecycleFormation],
        bar_events: list[dict[str, Any]],
    ) -> None:
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

    def _finish_transition(
        self,
        *,
        available_at: datetime,
        source: OrderflowAdvance,
        interaction: Any,
    ) -> TransitionAdvance:
        host_proposals = self._proposal_buffer.drain()
        self._clock = available_at
        return TransitionAdvance(
            observation=OrderflowAdvance(
                (*source.events, *interaction.events),
                interaction.transitions,
            ),
            execution=self._execution_snapshot(),
            host_proposals=host_proposals,
        )

    def _ingest_observation(
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
