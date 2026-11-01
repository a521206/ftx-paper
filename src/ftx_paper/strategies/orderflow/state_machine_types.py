"""Canonical context/location state contracts required by Layers 1 and 2."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Iterable
from datetime import datetime

from .hierarchy import ContextVersionRef, MarketContext
from .location_geometry import BAND_ATR_FRACTION, LocationBand
from .orderflow_common import Aggregate
from .timestamps import coerce_timestamp, nifty_label
from .lifecycle import EvidenceRef, LifecycleEvent, LifecycleStage, OpportunityStatus, SetupOpportunity  # noqa: F401
from .opportunity import DirectionalResponse, OpportunityOutcome, OpportunityOutcomeStatus  # noqa: F401
from .templates import Direction
from .hierarchy import TriggerObservation


@dataclass(frozen=True, slots=True)
class SessionLevels:
    opening_range_low: float
    opening_range_high: float
    prior_day_low: float | None
    prior_day_high: float | None
    opening_range_available_at: datetime
    prior_day_available_at: datetime


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


@dataclass
class MarketContextStore:
    versions: dict[str, list[MarketContext]] = field(default_factory=dict)
    ordinary_context: ContextVersionRef | None = None

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
class ReplayContext:
    rows: list[dict[str, Any]] = field(default_factory=list)
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
    # Chronological state retained by the canonical state-machine boundary.
    current_session_date: str | None = None
    continuous: Any = None
    session: Any = None
    opening_context: dict[str, Any] = field(default_factory=dict)
    interactions: dict[str, Any] = field(default_factory=dict)
    session_extreme_gate_blocked_lifecycles: set[str] = field(default_factory=set)
    events: list[dict[str, Any]] = field(default_factory=list)
    transitions: list[dict[str, Any]] = field(default_factory=list)
    decision_context_evidence: list[dict[str, Any]] = field(default_factory=list)
    opportunity_identities: dict[str, str] = field(default_factory=dict)
    published_resolved_responses: set[tuple[str, str]] = field(default_factory=set)

    def __post_init__(self) -> None:
        if self.execution_engine is None:
            raise ValueError("ReplayContext requires an injected execution engine")

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

    def record_event(self, event: dict[str, Any]) -> None:
        payload = dict(event)
        self.events.append(payload)
        lifecycle_id = payload.get("lifecycle_id")
        if lifecycle_id is None:
            return
        lifecycle = self.opportunities.get(str(lifecycle_id))
        if lifecycle is not None:
            lifecycle.record(payload)

    def record_transition(self, transition: dict[str, Any]) -> None:
        self.transitions.append(dict(transition))

    def position_is_open(self, opportunity: SetupOpportunity | None = None) -> bool:
        if opportunity is None:
            return bool(self.execution_engine.open_positions())
        state = self.execution_engine.state_for(opportunity.lifecycle_id)
        return state.position is not None and state.position.status.value == "open"

    def record_parity_fact(self, stream: str, fact: dict[str, Any]) -> None:
        if stream not in {
            "intent_admission", "trigger_exit", "execution_fills",
            "execution_transitions", "ledger_mutations",
        }:
            raise ValueError(f"unknown parity stream: {stream}")
        self.parity_observed.add(stream)
        self.parity_streams.setdefault(stream, []).append(dict(fact))

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
