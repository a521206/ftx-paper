"""Canonical context/location state contracts required by Layers 1 and 2."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields, is_dataclass, replace
from enum import StrEnum
from enum import Enum
import hashlib
import json
from typing import Any, Iterable, Mapping, Sequence, cast
from datetime import datetime

from .hierarchy import ContextVersionRef, MarketContext, ContextObservation
from .parity_artifacts import PHASE_STREAMS
from .location_geometry import BAND_ATR_FRACTION, LocationBand
from .orderflow_common import (
    Aggregate,
    BALANCE,
    EXHAUSTION_CANDIDATE,
    EXHAUSTION_RISK,
    REJECTION_CANDIDATE,
)
from .timestamps import coerce_timestamp, nifty_label, serialize_utc

from .lifecycle import EvidenceRef, LifecycleEvent, LifecycleStage, OpportunityStatus, SetupOpportunity  # noqa: F401
from .opportunity import DirectionalResponse, OpportunityOutcome, OpportunityOutcomeStatus  # noqa: F401
from .templates import Direction
from .hierarchy import TriggerObservation

_EXECUTION_DIAGNOSTIC_KINDS = frozenset({
    "intent_armed", "trigger_observation", "execution_proposal_created",
    "execution_proposal_selected", "execution_proposal_suppressed",
    "admission_decided", "entry_pending", "filled", "position_closed",
    "position_ownership_transferred",
    "intent_cancelled", "entry_cancelled", "entry_blocked_risk",
    "entry_blocked_rr", "entry_blocked_exit_plan", "trigger_denied_portfolio",
    "exit_signal", "exit_order", "exit_trigger_observed", "exit_decision",
    "stop_updated",
    "position_reversal_requested",
})

_INTERACTION_RESPONSE_KINDS = frozenset({
    "interaction_created", "location_interaction_observed",
    "interaction_departure", "interaction_ended", "interaction_closed",
    "response_confirmed", "response_invalidated", "response_expired",
    "role_transition", "hypothesis_arbitrated",
})

OPPORTUNITY_LIFECYCLE_KINDS = frozenset({
    "lifecycle_formed",
})

_EXECUTION_TRANSITION_KINDS = frozenset({
    "entry_pending", "admission_decided", "intent_effective",
    "intent_cancelled", "entry_cancelled", "exit_signal",
    "exit_decision", "filled", "position_closed",
    "position_ownership_transferred",
})


def _parity_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return serialize_utc(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _parity_safe(asdict(cast(Any, value)))
    if isinstance(value, Mapping):
        return {str(key): _parity_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_parity_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_parity_safe(item) for item in value), key=str)
    return value


def _lifecycle_parity_values(
    event: Mapping[str, Any], lifecycle: SetupOpportunity | None,
) -> tuple[str, Any]:
    if lifecycle is None:
        return str(event.get("neutral_event_key", event.get("event_id", ""))), _parity_safe(dict(event))
    location = getattr(lifecycle.location, "value", lifecycle.location)
    values = {
        **dict(event),
        "candidate_id": event.get("candidate_id") or lifecycle.candidate_id,
        "lifecycle_id": lifecycle.lifecycle_id,
        "direction": lifecycle.direction.value,
        "location": location,
        "status": lifecycle.status.value,
        "stage": lifecycle.stage.value,
        "formation_at": lifecycle.formation_at,
        "formation_available_at": lifecycle.formation_available_at,
        "policy_version": lifecycle.policy_version,
        "response_episode_id": lifecycle.response_episode_id,
        "execution_group_id": lifecycle.execution_group_id,
    }
    return lifecycle.causal_identity or lifecycle.lifecycle_id, _parity_safe(values)


ORDERFLOW_TRAIL_ACTIVATE_BP = 20.0
ORDERFLOW_TRAIL_BP = 20.0


@dataclass(frozen=True, slots=True)
class SessionLevels:
    opening_range_low: float
    opening_range_high: float
    prior_day_low: float | None
    prior_day_high: float | None
    opening_range_available_at: datetime
    prior_day_available_at: datetime


@dataclass(frozen=True, slots=True)
class DisplacementRules:
    min_volume_multiple: float = 3.0
    min_range_multiple: float = 1.0
    min_body_fraction: float = 0.3
    baseline_bars: int = 20
    minimum_prior_sessions: int = 5


DISPLACEMENT_RULES = DisplacementRules()


class LocationStatus(StrEnum):
    ACTIVE = "active"
    TESTED = "tested"
    ACCEPTED_THROUGH = "accepted-through"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class LocationRecord:
    location_id: str
    alias: str
    price: float
    source_session: str
    created_at: datetime
    available_at: datetime
    stream_id: str
    band: LocationBand
    generation: int = 1
    status: LocationStatus = LocationStatus.ACTIVE
    frozen_atr: float | None = None
    parent_extreme_generation: int | None = None
    source_aggregate_id: str | None = None
    source_observation_id: str | None = None
    timeframe: str = ""
    source_method: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", coerce_timestamp(self.created_at, field="location.created_at"))
        object.__setattr__(self, "available_at", coerce_timestamp(self.available_at, field="location.available_at"))
        if self.available_at < self.created_at:
            raise ValueError("location availability cannot precede publication")
        object.__setattr__(self, "status", LocationStatus(self.status))

    @property
    def band_low(self) -> float:
        return self.band.low

    @property
    def band_high(self) -> float:
        return self.band.high

    def with_status(self, status: str | LocationStatus) -> "LocationRecord":
        return replace(self, status=LocationStatus(status))


@dataclass
class LocationRegistry:
    stream_id: str
    records: dict[str, LocationRecord] = field(default_factory=dict)
    aliases: dict[str, str] = field(default_factory=dict)

    def publish_session_levels(self, levels: SessionLevels, *, source_session: str, prior_session: str | None = None, atr: float | None = None) -> None:
        self.register_opening_range(levels.opening_range_low, levels.opening_range_high, levels.opening_range_available_at, source_session=source_session, atr=atr)
        if levels.prior_day_low is not None and levels.prior_day_high is not None:
            self.register_prior_day_extremes(levels.prior_day_low, levels.prior_day_high, levels.prior_day_available_at, source_session=prior_session or "prior", atr=atr)

    def publish_session_extrema(self, aggregates: Iterable[Aggregate], *, session_date: str, atr: float | None = None) -> tuple[tuple[LocationRecord, LocationRecord | None], ...]:
        current = sorted((item for item in aggregates if item.session_date == session_date and item.timeframe == "5m" and item.source_bar_count == 5 and item.available_at >= item.end_time), key=lambda item: item.available_at)
        if not current:
            return ()
        published: list[tuple[LocationRecord, LocationRecord | None]] = []
        running_high: float | None = None
        running_low: float | None = None
        for aggregate in current:
            next_high = aggregate.high if running_high is None else max(running_high, aggregate.high)
            next_low = aggregate.low if running_low is None else min(running_low, aggregate.low)
            for alias, price, prior in (("SESSION_HIGH", next_high, running_high), ("SESSION_LOW", next_low, running_low)):
                if prior is not None and price == prior:
                    continue
                source_id = nifty_label(aggregate.time)
                if any(
                    item.alias == alias
                    and item.source_aggregate_id == source_id
                    and item.price == price
                    for item in self.records.values()
                ):
                    continue
                previous = self.resolve_as_of(alias, aggregate.available_at)
                published.append((self._register_level(alias, price, aggregate.available_at, session_date, frozen_atr=atr, source_aggregate_id=source_id), previous))
            running_high = aggregate.high if running_high is None else max(running_high, aggregate.high)
            running_low = aggregate.low if running_low is None else min(running_low, aggregate.low)
        return tuple(published)

    def publish_session_extreme(self, alias: str, price: float, aggregate: Aggregate, *, atr: float) -> LocationRecord | None:
        """Publish one cumulative session extreme at its causal availability."""
        previous = self.resolve_as_of(alias, aggregate.available_at)
        if previous is not None and previous.price == price:
            return None
        return self._register_level(
            alias, price, aggregate.available_at, aggregate.session_date,
            frozen_atr=atr, source_aggregate_id=nifty_label(aggregate.time),
        )

    def _register_level(self, alias: str, price: float, available_at: datetime, source_session: str, *, frozen_atr: float | None, source_aggregate_id: str | None = None) -> LocationRecord:
        if frozen_atr is None:
            raise ValueError("location bands require a frozen ATR")
        previous = self.resolve_as_of(alias, available_at)
        generation = max((item.generation for item in self.records.values() if item.alias == alias), default=0) + 1
        record = LocationRecord(
            f"{alias}:{source_session}:{generation}", alias, float(price), source_session,
            available_at, available_at, self.stream_id,
            LocationBand(float(price) - BAND_ATR_FRACTION * frozen_atr, float(price) + BAND_ATR_FRACTION * frozen_atr),
            generation=generation, frozen_atr=frozen_atr,
            parent_extreme_generation=previous.generation if previous else None,
            source_aggregate_id=source_aggregate_id,
        )
        self.register(record)
        return record

    def register_opening_range(self, or_low: float, or_high: float, available_at: datetime, *, source_session: str = "current", atr: float | None = None) -> tuple[LocationRecord, LocationRecord]:
        return (self._register_level("OR_LOW", or_low, available_at, source_session, frozen_atr=atr), self._register_level("OR_HIGH", or_high, available_at, source_session, frozen_atr=atr))

    def register_prior_day_extremes(self, pd_low: float, pd_high: float, available_at: datetime, *, source_session: str = "prior", atr: float | None = None) -> tuple[LocationRecord, LocationRecord]:
        return (self._register_level("PD_LOW", pd_low, available_at, source_session, frozen_atr=atr), self._register_level("PD_HIGH", pd_high, available_at, source_session, frozen_atr=atr))

    def register(self, record: LocationRecord) -> None:
        if record.stream_id != self.stream_id:
            raise ValueError("location stream identity does not match registry")
        self.records[record.location_id] = record
        self.aliases[record.alias] = record.location_id

    def publish_vwap(self, *, value: float, available_at: datetime, source_session: str, source_aggregate_id: str, source_observation_id: str, timeframe: str, atr: float) -> LocationRecord:
        if timeframe not in {"1m", "5m"}:
            raise ValueError("VWAP records require a 1m or 5m timeframe")
        previous = max((item for item in self.records.values() if item.alias == "VWAP" and item.timeframe == timeframe), key=lambda item: item.generation, default=None)
        generation = previous.generation + 1 if previous else 1
        record = LocationRecord(
            f"VWAP:{source_session}:{timeframe}:{generation}", "VWAP", float(value), source_session,
            available_at, available_at, self.stream_id,
            LocationBand(value - BAND_ATR_FRACTION * atr, value + BAND_ATR_FRACTION * atr),
            generation=generation, frozen_atr=atr, source_aggregate_id=source_aggregate_id,
            source_observation_id=source_observation_id, timeframe=timeframe,
            source_method="vwap_typical_price_volume_v1",
        )
        self.register(record)
        return record

    def resolve(self, alias_or_id: str) -> LocationRecord | None:
        return self.records.get(self.aliases.get(alias_or_id, alias_or_id))

    def resolve_as_of(self, alias: str, available_at: datetime, *, timeframe: str | None = None) -> LocationRecord | None:
        clock = coerce_timestamp(available_at, field="location lookup")
        return max((item for item in self.records.values() if item.alias == alias and item.available_at <= clock and (timeframe is None or item.timeframe == timeframe)), key=lambda item: (item.available_at, item.generation), default=None)

    def available_as_of(self, available_at: datetime) -> Iterable[LocationRecord]:
        clock = coerce_timestamp(available_at, field="location lookup")
        latest: dict[tuple[str, str], LocationRecord] = {}
        for item in self.records.values():
            item_available = item.available_at
            if item_available > clock:
                continue
            key = (item.alias, item.timeframe)
            if key not in latest or (item_available, item.generation) > (latest[key].available_at, latest[key].generation):
                latest[key] = item
        return tuple(item for item in latest.values() if item.status is LocationStatus.ACTIVE)

    def update_status(self, location_id: str, status: str) -> LocationRecord:
        record = self.records[location_id].with_status(status)
        self.records[location_id] = record
        return record


class MarketState(StrEnum):

    LATE_AUCTION = EXHAUSTION_CANDIDATE
    REJECTION = REJECTION_CANDIDATE
    FAILED_CONTINUATION = EXHAUSTION_RISK


@dataclass(frozen=True, slots=True)
class MarketStateRules:
    positive_clv_min: float = 0.5
    rejection_clv_max: float = -0.3
    rejection_volume_multiple: float = 1.5


MARKET_STATE_RULES = MarketStateRules()


@dataclass
class ContinuousState:
    registry: LocationRegistry
    prior_values: list[Any] = field(default_factory=list)
    last_completed_bar: Aggregate | None = None
    last_close: float | None = None
    open_gaps: list[dict[str, Any]] = field(default_factory=list)

    def publish_prior_value(self, prior_value: Any) -> None:
        if prior_value is None or prior_value.reference is None:
            return
        key = prior_value.reference.session_date
        if any(
            item.reference is not None
            and item.reference.session_date == key
            for item in self.prior_values
        ):
            return
        self.prior_values.append(prior_value)

    def prior_value_as_of(self, available_at: datetime) -> Any:
        clock = coerce_timestamp(available_at, field="prior value lookup")
        return max(
            (
                item for item in self.prior_values
                if item.available_at is not None and item.available_at <= clock
            ),
            key=lambda item: (item.available_at, item.reference.session_date),
            default=None,
        )


@dataclass
class MarketContextStore:
    versions: dict[str, list[MarketContext]] = field(default_factory=dict)
    ordinary_context: ContextVersionRef | None = None
    interaction_contexts: dict[str, ContextVersionRef] = field(default_factory=dict)

    def publish(self, context: MarketContext) -> ContextVersionRef:
        versions = self.versions.setdefault(context.context_id, [])
        if versions and versions[-1] == context:
            return ContextVersionRef(context.context_id, context.version)
        if any(item.version == context.version for item in versions):
            raise ValueError("context versions must advance monotonically")
        versions.append(context)
        return ContextVersionRef(context.context_id, context.version)

    def resolve(self, reference: ContextVersionRef) -> MarketContext | None:
        return next((item for item in self.versions.get(reference.context_id, ()) if item.version == reference.version), None)

    def latest_as_of(self, stream_id: str, available_at: datetime) -> MarketContext | None:
        clock = coerce_timestamp(available_at, field="context lookup")
        return max((item for item in self.versions.get(stream_id, ()) if item.available_at <= clock), key=lambda item: item.available_at, default=None)


@dataclass
class OrderflowState:
    rows: list[dict[str, Any]] = field(default_factory=list)
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]] = field(default_factory=dict)
    atr_by_session: Mapping[str, float | None] = field(default_factory=dict)
    prior_session_rows: tuple[tuple[Mapping[str, Any], ...], ...] = ()
    aggregates: dict[str, list[Aggregate]] = field(default_factory=dict)
    market_contexts: MarketContextStore = field(default_factory=MarketContextStore)
    location_registry: LocationRegistry | None = None
    regime: Any = None
    execution_constraints: "ExecutionConstraints" = field(default_factory=lambda: ExecutionConstraints())
    execution_engine: Any = None
    opportunities: dict[str, SetupOpportunity] = field(default_factory=dict)
    execution_proposals: list["CanonicalExecutionProposal"] = field(default_factory=list)
    parity_streams: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    parity_observed: set[str] = field(default_factory=set, repr=False)
    parity_layers: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # Chronological state retained by the canonical state-machine boundary.
    current_session_date: str | None = None
    continuous: Any = None
    opening_gap_policy: Any = None
    previous5_available_at: Any = None
    session: Any = None
    opening_context: dict[str, Any] = field(default_factory=dict)
    last_confirmation_context_time: datetime | None = None
    interactions: dict[str, Any] = field(default_factory=dict)
    regime_gate: Any = None
    regime_gate_blocked_lifecycles: set[str] = field(default_factory=set)
    session_extreme_gate_blocked_lifecycles: set[str] = field(default_factory=set)
    events: list[dict[str, Any]] = field(default_factory=list)
    pending_bar_events: list[dict[str, Any]] = field(default_factory=list, repr=False)
    execution_diagnostics: list[dict[str, Any]] = field(default_factory=list)
    input_diagnostics: list[dict[str, Any]] = field(default_factory=list)
    neutral_event_ordinals: dict[tuple[str, ...], int] = field(default_factory=dict, repr=False)
    transitions: list[dict[str, Any]] = field(default_factory=list)
    decision_context_evidence: list[dict[str, Any]] = field(default_factory=list)
    opportunity_identities: dict[str, str] = field(default_factory=dict)
    published_resolved_responses: set[tuple[str, str]] = field(default_factory=set)
    opportunity_outcomes: dict[tuple[str, str], list[Any]] = field(default_factory=dict)
    or_seed_candles: set[datetime] = field(default_factory=set)
    setup_number: int = 0
    child_observation: ContextObservation | None = None
    market_state: str | None = BALANCE
    neutral_keys_by_causal_id: dict[tuple[str, str], str] = field(default_factory=dict, repr=False)

    def record_opportunity_outcome(
        self, interaction_id: str, template_id: str, outcome: OpportunityOutcome,
    ) -> None:
        self.opportunity_outcomes.setdefault((interaction_id, template_id), []).append(outcome)

    def record_resolved_response(
        self, interaction_id: str, template_id: str,
    ) -> bool:
        key = (interaction_id, template_id)
        if key in self.published_resolved_responses:
            return False
        self.published_resolved_responses.add(key)
        return True

    def emit(
        self,
        event: Mapping[str, Any],
        bar_events: list[Any],
        lifecycle: SetupOpportunity | None = None,
        *,
        record_lifecycle: bool = True,
    ) -> None:
        event_data = dict(event)
        if event_data.get("kind") in _INTERACTION_RESPONSE_KINDS:
            event_data.setdefault("parity_stream", "interaction_response")
        session_date = str(
            event_data.get("session_date")
            or (lifecycle.session_date if lifecycle is not None else "")
            or self.current_session_date
            or ""
        )
        event_data.setdefault("session_date", session_date)
        if lifecycle is not None and record_lifecycle:
            event_data = lifecycle.record(event_data)
        elif lifecycle is not None:
            event_data.setdefault("lifecycle_id", lifecycle.lifecycle_id)
            event_data.setdefault("direction", lifecycle.direction.value)
            event_data.setdefault("lifecycle_stage", lifecycle.stage.value)
        observed_at = event_data.get("observed_at", event_data.get("time"))
        available_at = event_data.get("available_at", observed_at)
        event_data.setdefault("observed_at", observed_at)
        event_data.setdefault("available_at", available_at)
        if isinstance(observed_at, (str, datetime)):
            event_data.setdefault("causal_observed_at", _parity_safe(observed_at))
        if isinstance(available_at, (str, datetime)):
            event_data.setdefault("causal_available_at", _parity_safe(available_at))
        event_data.setdefault("event_id", f"replay:{len(self.events)}")
        event_data.setdefault("sequence", len(self.events))
        event_data.setdefault("policy_version", "orderflow-v1")
        event_data.setdefault("provenance", "causal_replay")
        if event_data.get("kind") in _EXECUTION_DIAGNOSTIC_KINDS:
            self.execution_diagnostics.append({
                "sequence": len(self.execution_diagnostics),
                **dict(event_data),
            })
        self.events.append(dict(event_data))
        bar_events.append(dict(event_data))
        stream = event_data.get("parity_stream")
        if stream is None and event_data.get("kind") == "lifecycle_formed":
            stream = "opportunity_lifecycle"
        if stream is not None:
            parent = (
                event_data.get("causal_parent_id")
                or event_data.get("interaction_id")
                or (lifecycle.lifecycle_id if lifecycle is not None else None)
            )
            identity, values = (
                _lifecycle_parity_values(event_data, lifecycle)
                if stream == "opportunity_lifecycle" else
                (str(event_data.get("neutral_event_key", event_data.get("event_id", ""))), _parity_safe(event_data))
            )
            if stream == "interaction_response" and isinstance(event_data.get("values"), Mapping):
                values.pop("values", None)
                values.update(_parity_safe(event_data["values"]))
                if not values.get("session_date"):
                    values["session_date"] = self.current_session_date or str(
                        _parity_safe(observed_at)
                    )[:10]
            self.record_parity_fact(str(stream), {
                "kind": str(event_data.get("kind", "")),
                "occurred_at": _parity_safe(observed_at),
                "observed_at": _parity_safe(observed_at),
                "available_at": _parity_safe(available_at),
                "sequence": len(self.parity_streams.get(str(stream), [])),
                "identity": identity,
                "causal_parent_id": parent,
                "values": values,
                "unavailable": [],
            })
        layer_kinds = {
            "filled": (("execution_settlement", "fill"), ("portfolio_ledger", "fill")),
            "position_closed": (("execution_settlement", "settlement"), ("portfolio_ledger", "exit_settlement")),
        }
        decision_stage = {
            "interaction_created": "interaction",
            "response_confirmed": "interaction",
            "lifecycle_formed": "candidate",
            "admission_decided": "admission",
            "trigger_observation": "trigger",
            "exit_signal": "exit",
            "exit_decision": "exit",
        }.get(str(event_data.get("kind")))
        if decision_stage is not None and isinstance(observed_at, str) and isinstance(available_at, str):
            self.record_parity_layer_fact("decision", {
                "kind": "decision",
                "stage": decision_stage,
                "outcome": str(event_data.get("outcome") or event_data.get("status") or "observed"),
                "occurred_at": observed_at,
                "observed_at": observed_at,
                "available_at": available_at,
                "sequence": len(self.parity_layers.get("decision", [])),
                "identity": str(event_data.get("neutral_event_key", event_data["event_id"])),
                "causal_parent_id": event_data.get("causal_parent_id"),
                "values": dict(event_data),
                "unavailable": [],
            })
        if str(event_data.get("kind")) in layer_kinds and isinstance(observed_at, str) and isinstance(available_at, str):
            for layer, kind in layer_kinds[str(event_data["kind"])]:
                self.record_parity_layer_fact(layer, {
                    "kind": kind,
                    "occurred_at": observed_at,
                    "observed_at": observed_at,
                    "available_at": available_at,
                    "sequence": len(self.parity_layers.get(layer, [])),
                    "identity": str(event_data.get("neutral_event_key", event_data["event_id"])),
                    "causal_parent_id": event_data.get("causal_parent_id"),
                    "values": dict(event_data),
                    "unavailable": [],
                })

    def emit_market_event(
        self,
        event: dict[str, Any],
        market_state: str,
        bar_events: list[Any],
        lifecycle: SetupOpportunity | None = None,
        *,
        record_lifecycle: bool = True,
    ) -> None:
        previous_state = self.market_state
        self.emit(
            {**event, "market_state": market_state},
            bar_events,
            lifecycle,
            record_lifecycle=record_lifecycle,
        )
        self.market_state = market_state
        if previous_state != market_state:
            emitted = self.events[-1] if self.events else event
            self.input_diagnostics.append({
                "kind": "market_state_transition",
                "market_state_before": previous_state,
                "market_state_after": market_state,
                "event_kind": emitted.get("kind"),
                "event_id": emitted.get("event_id"),
                "sequence": emitted.get("sequence"),
                "lifecycle_id": emitted.get("lifecycle_id"),
                "observed_at": emitted.get("observed_at", emitted.get("time")),
                "available_at": emitted.get("available_at", emitted.get("observed_at", emitted.get("time"))),
                "causal_parent_id": emitted.get("causal_parent_id"),
                "reason": emitted.get("reason"),
                "owner": emitted.get("owner"),
                "evidence_timeframe": emitted.get("evidence_timeframe"),
            })

    def __post_init__(self) -> None:
        if self.execution_engine is None:
            raise ValueError("OrderflowState requires an injected execution engine")
        if self.opening_gap_policy is None:
            from .opening_gap_policy import OpeningGapPolicy
            self.opening_gap_policy = OpeningGapPolicy()
        if self.continuous is None and self.location_registry is not None:
            self.continuous = ContinuousState(registry=self.location_registry)

    def register_opportunity(self, opportunity: SetupOpportunity) -> bool:
        if opportunity.causal_identity is not None:
            existing = self.opportunity_identities.get(opportunity.causal_identity)
            if existing is not None:
                return False
        if self.execution_engine.execution_group_claim(opportunity.execution_group_id) is not None:
            return False
        if opportunity.lifecycle_id in self.opportunities:
            return False
        self.execution_engine.register(opportunity.lifecycle_id)
        self.opportunities[opportunity.lifecycle_id] = opportunity
        if opportunity.causal_identity is not None:
            self.opportunity_identities.setdefault(
                opportunity.causal_identity, opportunity.lifecycle_id,
            )
        return True

    def first_lifecycle_for_identity(self, causal_identity: str) -> str | None:
        return self.opportunity_identities.get(causal_identity)

    def unfilled_execution_group_siblings(
        self, selected: SetupOpportunity,
    ) -> tuple[SetupOpportunity, ...]:
        return tuple(
            item for item in self.opportunities.values()
            if item.lifecycle_id != selected.lifecycle_id
            and item.active
            and not self.position_is_open(item)
            and selected.execution_group_id is not None
            and item.execution_group_id == selected.execution_group_id
        )

    def conflicting_unfilled_opportunities(
        self, selected: SetupOpportunity,
    ) -> tuple[SetupOpportunity, ...]:
        return tuple(
            item for item in self.opportunities.values()
            if item.lifecycle_id != selected.lifecycle_id
            and item.active
            and not self.position_is_open(item)
            and getattr(item, "conflict_key", None)
            == getattr(selected, "conflict_key", None)
            and item.direction is not selected.direction
        )

    def record_decision_context(self, evidence: dict[str, Any]) -> None:
        self.decision_context_evidence.append(dict(evidence))

    def record_event(self, event: dict[str, Any], *, record_lifecycle: bool = True) -> None:
        payload = dict(event)
        if payload.get("kind") in _INTERACTION_RESPONSE_KINDS:
            payload.setdefault("parity_stream", "interaction_response")
        lifecycle_id = payload.get("lifecycle_id")
        lifecycle = (
            self.opportunities.get(str(lifecycle_id))
            if lifecycle_id is not None else None
        )
        if lifecycle is not None and record_lifecycle:
            payload = lifecycle.record(payload)
        elif lifecycle is not None:
            payload.setdefault("lifecycle_id", lifecycle.lifecycle_id)
            payload.setdefault("direction", lifecycle.direction.value)
            payload.setdefault("lifecycle_stage", lifecycle.stage.value)
        observed_at = payload.get("observed_at", payload.get("time"))
        available_at = payload.get("available_at", observed_at)
        payload.setdefault("session_date", str(
            payload.get("session_date")
            or (lifecycle.session_date if lifecycle is not None else "")
            or self.current_session_date
            or ""
        ))
        payload.setdefault("observed_at", observed_at)
        payload.setdefault("available_at", available_at)
        if isinstance(observed_at, (str, datetime)):
            payload.setdefault("causal_observed_at", _parity_safe(observed_at))
        if isinstance(available_at, (str, datetime)):
            payload.setdefault("causal_available_at", _parity_safe(available_at))
        payload.setdefault("event_id", f"replay:{len(self.events)}")
        payload.setdefault("sequence", len(self.events))
        payload.setdefault("policy_version", "orderflow-v1")
        payload.setdefault("provenance", "causal_replay")
        causal_ids = tuple(
            (name, str(payload[name]))
            for name in (
                "interaction_id", "lifecycle_id", "candidate_id", "setup_id",
                "opportunity_id", "intent_id", "reservation_id", "order_id",
                "position_id", "fill_id",
            )
            if payload.get(name) is not None
        )
        event_basis = {
            "kind": str(payload.get("kind", "")),
            "session_date": payload.get("session_date"),
            "observed_at": _parity_safe(observed_at),
            "available_at": _parity_safe(available_at),
            "causal_ids": causal_ids,
            "causal_parent_id": payload.get("causal_parent_id") or payload.get("parent_id"),
            "level": payload.get("level"),
            "reason": payload.get("reason") or payload.get("reason_code"),
        }
        ordinal_key = tuple(
            str(event_basis[key]) for key in (
                "kind", "observed_at", "available_at", "level", "reason",
            ) + ("causal_parent_id",)
        ) + tuple(value for pair in causal_ids for value in pair)
        ordinal = self.neutral_event_ordinals.get(ordinal_key, 0)
        self.neutral_event_ordinals[ordinal_key] = ordinal + 1
        neutral_event_key = "niftyzoning:derived:" + hashlib.sha256(
            json.dumps(event_basis, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        if ordinal:
            neutral_event_key = f"{neutral_event_key}:occurrence-{ordinal}"
        payload.setdefault("neutral_event_key", neutral_event_key)
        parity_stream = payload.get("parity_stream")
        if parity_stream is None and payload.get("kind") in _EXECUTION_TRANSITION_KINDS:
            parity_stream = "execution_transitions"
        if parity_stream is None and payload.get("kind") in OPPORTUNITY_LIFECYCLE_KINDS:
            parity_stream = "opportunity_lifecycle"
        if parity_stream is not None:
            identity, values = (
                _lifecycle_parity_values(payload, lifecycle)
                if parity_stream == "opportunity_lifecycle" else
                (
                    str(payload.get("neutral_event_key", payload.get("event_id", ""))),
                    _parity_safe(payload),
                )
            )
            self.record_parity_fact(str(parity_stream), {
                "kind": str(payload.get("kind", "")),
                "occurred_at": _parity_safe(observed_at),
                "observed_at": _parity_safe(observed_at),
                "available_at": _parity_safe(available_at),
                "sequence": len(self.parity_streams.get(str(parity_stream), [])),
                "identity": identity,
                "causal_parent_id": lifecycle_id,
                "values": values,
                "unavailable": [],
            })
        self.events.append(payload)
        if payload.get("kind") in _EXECUTION_DIAGNOSTIC_KINDS:
            self.execution_diagnostics.append({
                "sequence": len(self.execution_diagnostics),
                **payload,
            })

    def record_transition(self, transition: dict[str, Any]) -> None:
        self.transitions.append(dict(transition))

    def position_is_open(self, opportunity: SetupOpportunity | None = None) -> bool:
        if opportunity is None:
            return bool(self.execution_engine.open_positions())
        state = self.execution_engine.state_for(opportunity.lifecycle_id)
        return state.position is not None and state.position.status.value == "open"

    def record_parity_fact(self, stream: str, fact: Mapping[str, Any]) -> None:
        allowed = set(PHASE_STREAMS) | {
            "execution_fills", "execution_transitions", "ledger_mutations",
        }
        if stream not in allowed:
            raise ValueError(f"unknown parity stream: {stream}")
        self.parity_observed.add(stream)
        self.parity_streams.setdefault(stream, []).append(_parity_safe(dict(fact)))

    def record_parity_layer_fact(self, layer: str, fact: Mapping[str, Any]) -> None:
        """Publish one producer-owned final-layer fact."""
        if layer not in {"run_scope", "decision", "execution_settlement", "portfolio_ledger"}:
            raise ValueError(f"unknown parity layer: {layer}")
        self.parity_layers.setdefault(layer, []).append(_parity_safe(dict(fact)))

    def parity_snapshot(self) -> dict[str, list[dict[str, Any]]]:
        return {
            stream: [dict(fact) for fact in facts]
            for stream, facts in self.parity_streams.items()
            if stream in self.parity_observed
        }

    def publish_context(self, context: MarketContext) -> ContextVersionRef:
        return self.market_contexts.publish(context)

    def context(self, reference: ContextVersionRef) -> MarketContext | None:
        return self.market_contexts.resolve(reference)

    @staticmethod
    def snapshot(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        if hasattr(value, "to_dict"):
            return value.to_dict()
        return {
            item.name: _parity_safe(getattr(value, item.name))
            for item in fields(value)
        }


@dataclass(frozen=True, slots=True)
class ExecutionConstraints:
    max_open_positions: int = 1
    max_session_entries: int | None = None
    max_direction_entries: int | None = None
    post_trade_cooldown_minutes: int = 0
    fill_permission_margin: float | None = 0.05
    stop_atr_buffer: float = 0.25
    failed_reversal_lockout: bool = False


@dataclass(frozen=True, slots=True)
class CanonicalExecutionProposal:
    lifecycle_id: str
    intent_id: str
    observation: TriggerObservation
    setup_available_at: datetime | str
    direction: Direction
    structural_rank: int


# Opportunity-layer contracts are re-exported here for the canonical state
# machine import surface; their implementation remains outside this owner.
