"""Opportunity and setup lifecycle facts, independent of execution."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from .hierarchy import ContextVersionRef
from .orderflow_common import Aggregate, canonical_timestamp
from .templates import Direction


class OpportunityStatus(StrEnum):
    FORMED = "FORMED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"
    SUPERSEDED = "SUPERSEDED"


class LifecycleStage(StrEnum):
    OBSERVING_LOCATION = "OBSERVING_LOCATION"
    RESPONSE_CONFIRMED = "RESPONSE_CONFIRMED"
    STRUCTURE_CONFIRMED = "STRUCTURE_CONFIRMED"
    WAITING_FOR_CONFIRMATION = "WAITING_FOR_CONFIRMATION"
    WAITING_FOR_RETEST = "WAITING_FOR_RETEST"
    RETEST_CONFIRMED = "RETEST_CONFIRMED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"
    SUPERSEDED = "SUPERSEDED"


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    evidence_id: str
    timeframe: str
    observed_at: str
    available_at: str
    source_fingerprint: str
    source_bar_count: int

    @classmethod
    def from_aggregate(cls, aggregate: Aggregate) -> "EvidenceRef":
        return cls(aggregate.time, aggregate.timeframe, aggregate.end_time, aggregate.available_at, aggregate.source_fingerprint, aggregate.source_bar_count)


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    event_id: str
    lifecycle_id: str
    sequence: int
    kind: str
    observed_at: str
    available_at: str
    effective_at: str | None
    payload: Mapping[str, Any]


@dataclass
class SetupOpportunity:
    lifecycle_id: str
    direction: Direction
    session_date: str
    instrument: str
    strategy: str
    formation_at: str
    formation_available_at: str
    candidate_id: str
    parent_context: ContextVersionRef | None
    location: str | None
    reference_level: float | None
    context_candle: Aggregate | None
    breakout_level: float
    failure_boundary: float
    context_candle_lineage: EvidenceRef | None = None
    origin_evidence_id: str | None = None
    response_evidence_ids: tuple[str, ...] = ()
    response_evidence: tuple[Aggregate, ...] = ()
    response_evidence_lineage: tuple[EvidenceRef, ...] = ()
    policy_version: str | None = None
    reference_generation: int = 0
    causal_identity: str | None = None
    location_interaction_id: str | None = None
    opening_interaction_kind: str | None = None
    opening_interaction_evidence: list[Mapping[str, Any]] = field(default_factory=list)
    opening_policy_version: str | None = None
    template_id: str | None = None
    status: OpportunityStatus = OpportunityStatus.FORMED
    response_episode_id: str | None = None
    execution_group_id: str | None = None
    clock: str = ""
    decision_context_required: bool = True
    timeout_limit: int = 6
    timeout_count: int = 0
    timeout_evidence_timeframe: str = "5m"
    timeout_counted_source_ids: list[str] = field(default_factory=list)
    timeout_counted_snapshot_ids: list[str] = field(default_factory=list)
    _events: tuple[LifecycleEvent, ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        self.direction = Direction(self.direction)
        self.formation_at = canonical_timestamp(self.formation_at)
        self.formation_available_at = canonical_timestamp(self.formation_available_at)
        if self.formation_available_at < self.formation_at or self.timeout_limit <= 0:
            raise ValueError("invalid causal opportunity timestamps or timeout")
        self.status = OpportunityStatus(self.status)

    @property
    def active(self) -> bool:
        return self.status is OpportunityStatus.FORMED

    @property
    def candidate_invalidated(self) -> bool:
        return not self.active

    @property
    def stage(self) -> LifecycleStage:
        if self.status is OpportunityStatus.INVALIDATED:
            return LifecycleStage.INVALIDATED
        if self.status is OpportunityStatus.EXPIRED:
            return LifecycleStage.EXPIRED
        if self.status is OpportunityStatus.SUPERSEDED:
            return LifecycleStage.SUPERSEDED
        return LifecycleStage.RESPONSE_CONFIRMED if self.response_evidence_ids else LifecycleStage.OBSERVING_LOCATION

    @property
    def events(self) -> tuple[LifecycleEvent, ...]:
        return deepcopy(self._events)

    def record(self, event: Mapping[str, Any]) -> dict[str, Any]:
        payload = deepcopy(dict(event))
        observed = canonical_timestamp(str(payload.get("observed_at", payload.get("time", self.clock))))
        available = canonical_timestamp(str(payload.get("available_at", observed)))
        if available < observed or available < self.formation_available_at:
            raise ValueError("lifecycle event is not causally available")
        if not self.active and payload.get("kind") not in {"setup_invalidated", "setup_expired"}:
            raise ValueError("cannot append evidence to an inactive opportunity")
        sequence = len(self._events)
        payload.update(lifecycle_id=self.lifecycle_id, direction=self.direction.value, observed_at=observed, available_at=available)
        payload.setdefault("stage", self.stage.value)
        item = LifecycleEvent(f"{self.lifecycle_id}:{sequence}", self.lifecycle_id, sequence, str(payload["kind"]), observed, available, payload.get("effective_at"), payload)
        self._events += (item,)
        return {**payload, "event_id": item.event_id, "lifecycle_sequence": sequence}

    def invalidate(self, *, at: str, reason: str = "", expired: bool = False) -> None:
        at = canonical_timestamp(at)
        if not self.active or at < self.formation_available_at:
            raise ValueError("invalidation requires an available active lifecycle")
        self.status = OpportunityStatus.EXPIRED if expired else OpportunityStatus.INVALIDATED
        self.record({"kind": "setup_expired" if expired else "setup_invalidated", "reason": reason, "observed_at": at, "available_at": at})

    def supersede(self, *, at: str, reason: str = "superseded") -> None:
        if not self.active:
            return
        at = canonical_timestamp(at)
        if at < self.formation_available_at:
            raise ValueError("supersession cannot precede formation")
        self.status = OpportunityStatus.SUPERSEDED
        self._events += (LifecycleEvent(f"{self.lifecycle_id}:{len(self._events)}", self.lifecycle_id, len(self._events), "setup_superseded", at, at, None, {"kind": "setup_superseded", "reason": reason}),)

    def to_dict(self) -> dict[str, Any]:
        return {"lifecycle_id": self.lifecycle_id, "candidate_id": self.candidate_id, "direction": self.direction.value, "status": self.status.value, "stage": self.stage.value, "causal_identity": self.causal_identity, "response_evidence_ids": list(self.response_evidence_ids), "response_evidence_lineage": [asdict(item) for item in self.response_evidence_lineage], "parent_context": asdict(self.parent_context) if self.parent_context else None, "events": [asdict(item) for item in self._events]}


__all__ = ["EvidenceRef", "LifecycleEvent", "LifecycleStage", "OpportunityStatus", "SetupOpportunity"]
