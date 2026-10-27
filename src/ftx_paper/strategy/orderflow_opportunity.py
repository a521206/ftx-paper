"""Diagnostic opportunity/setup lifecycle contracts for orderflow."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from collections.abc import Iterable
import hashlib


class OpportunityStatus(StrEnum):
    ACTIVE = "ACTIVE"
    CONFIRMED = "CONFIRMED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"
    SUPERSEDED = "SUPERSEDED"


@dataclass(frozen=True, slots=True)
class StructuralSetup:
    setup_id: str
    direction: str
    entry_lower: float
    entry_upper: float
    failure_boundary: float
    formed_at: datetime
    expires_at: datetime
    evidence_ids: tuple[str, ...]
    context_version: int | None = None
    confirmation_ids: tuple[str, ...] = ()
    regime_version: int | None = None

    def __post_init__(self) -> None:
        if self.direction not in {"long", "short"}:
            raise ValueError("setup direction must be long or short")
        if self.entry_lower > self.entry_upper or self.expires_at <= self.formed_at:
            raise ValueError("invalid setup structure or expiry")


@dataclass(frozen=True, slots=True)
class OpportunityTransition:
    status: OpportunityStatus
    at: datetime
    reason: str
    evidence_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ExecutionGroupClaim:
    execution_group_id: str
    lifecycle_id: str
    accepted: bool
    reason: str
    evidence_ids: tuple[str, ...] = ()


class ExecutionGroupRegistry:
    """Single-owner claims prevent duplicate candidates while retaining denials."""

    def __init__(self) -> None:
        self._claims: dict[str, ExecutionGroupClaim] = {}
        self._history: list[ExecutionGroupClaim] = []

    def claim(
        self, execution_group_id: str, lifecycle_id: str, *,
        evidence_ids: tuple[str, ...] = (),
    ) -> ExecutionGroupClaim:
        existing = self._claims.get(execution_group_id)
        if existing is not None:
            denied = ExecutionGroupClaim(
                execution_group_id, lifecycle_id, False,
                "execution_group_already_claimed", evidence_ids,
            )
            self._history.append(denied)
            return denied
        accepted = ExecutionGroupClaim(
            execution_group_id, lifecycle_id, True, "claimed", evidence_ids,
        )
        self._claims[execution_group_id] = accepted
        self._history.append(accepted)
        return accepted

    @property
    def history(self) -> tuple[ExecutionGroupClaim, ...]:
        return tuple(self._history)


@dataclass(frozen=True, slots=True)
class SetupOpportunity:
    lifecycle_id: str
    response_episode_id: str
    execution_group_id: str
    setup: StructuralSetup
    status: OpportunityStatus
    regime_version: int | None
    evidence_timeframe: str
    timeout_count: int = 0
    timeout_evidence_ids: tuple[str, ...] = ()
    transitions: tuple["OpportunityTransition", ...] = ()

    @property
    def decision_id(self) -> str:
        return f"opportunity:{self.lifecycle_id}"

    def source_is_current(
        self, *, interaction_id: str, context_version: int | None,
    ) -> bool:
        return (
            self.response_episode_id == f"response-{interaction_id}"
            and self.setup.context_version == context_version
            and self.status is OpportunityStatus.ACTIVE
        )

    def wait_for_retest(self, *, at: datetime, evidence_ids: tuple[str, ...] = ()) -> "SetupOpportunity":
        return self.transition(
            OpportunityStatus.ACTIVE, at=at, reason="retest_waiting", evidence_ids=evidence_ids,
        )

    def supersede(self, *, at: datetime, replacement_id: str, evidence_ids: tuple[str, ...] = ()) -> "SetupOpportunity":
        return self.transition(
            OpportunityStatus.SUPERSEDED, at=at,
            reason=f"superseded_by:{replacement_id}", evidence_ids=evidence_ids,
        )

    def invalidate(self, *, at: datetime, reason: str, evidence_ids: tuple[str, ...] = ()) -> "SetupOpportunity":
        return self.transition(OpportunityStatus.INVALIDATED, at=at, reason=reason, evidence_ids=evidence_ids)

    def source_is_valid(self, *, interaction_id: str, context_version: int | None, at: datetime) -> bool:
        return self.source_is_current(interaction_id=interaction_id, context_version=context_version) and at < self.setup.expires_at

    @classmethod
    def from_response(
        cls, *, interaction: object, response: object, expires_at: datetime,
        entry_width: float, failure_distance: float, regime_version: int | None = None,
    ) -> "SetupOpportunity":
        response_status = getattr(response, "status", None)
        if response_status is None or getattr(response_status, "value", response_status) != "CONFIRMED":
            raise ValueError("only confirmed responses can form opportunities")
        direction = getattr(response, "direction", None)
        formed_at = getattr(response, "confirmation_available_at", None)
        if direction not in {"long", "short"}:
            raise ValueError("confirmed response requires a direction")
        if not isinstance(formed_at, datetime):
            raise ValueError("confirmed response requires confirmation availability")
        level = float(getattr(interaction, "reference_level"))
        if entry_width <= 0 or failure_distance <= 0:
            raise ValueError("entry and failure distances must be positive")
        lower, upper = level - entry_width, level + entry_width
        failure = level - failure_distance if direction == "long" else level + failure_distance
        return cls.create(
            interaction_id=str(getattr(interaction, "interaction_id")),
            direction=direction, formed_at=formed_at,
            expires_at=expires_at, entry_lower=lower, entry_upper=upper,
            failure_boundary=failure,
            evidence_ids=tuple(getattr(response, "evidence_ids")),
            evidence_timeframe=str(getattr(interaction, "timeframe")),
            regime_version=regime_version,
        )

    @classmethod
    def create(
        cls, *, interaction_id: str, direction: str, formed_at: datetime,
        expires_at: datetime, entry_lower: float, entry_upper: float,
        failure_boundary: float, evidence_ids: tuple[str, ...],
        evidence_timeframe: str = "5m", regime_version: int | None = None,
    ) -> "SetupOpportunity":
        seed = (
            f"{interaction_id}|{formed_at.isoformat()}|{direction}|{evidence_ids}|"
            f"{evidence_timeframe}|{regime_version}"
        )
        digest = hashlib.sha256(seed.encode()).hexdigest()[:24]
        setup = StructuralSetup(
            f"setup-{digest}", direction, entry_lower, entry_upper,
            failure_boundary, formed_at, expires_at, evidence_ids,
            context_version=regime_version,
        )
        return cls(
            f"lifecycle-{digest}", f"response-{interaction_id}", f"execution-{digest}",
            setup, OpportunityStatus.ACTIVE, regime_version, evidence_timeframe,
            transitions=(OpportunityTransition(
                OpportunityStatus.ACTIVE, formed_at, "formed", evidence_ids,
            ),),
        )

    def advance_timeout(self, *, timeframe: str, at: datetime, evidence_id: str | None = None) -> "SetupOpportunity":
        if self.status is not OpportunityStatus.ACTIVE or timeframe != self.evidence_timeframe:
            return self
        token = evidence_id or at.isoformat()
        if token in self.timeout_evidence_ids:
            return self
        if at >= self.setup.expires_at:
            return replace(
                self, status=OpportunityStatus.EXPIRED, timeout_count=self.timeout_count + 1,
                timeout_evidence_ids=(*self.timeout_evidence_ids, token),
                transitions=(*self.transitions, OpportunityTransition(
                    OpportunityStatus.EXPIRED, at, "evidence_timeout", (token,),
                )),
            )
        return replace(self, timeout_count=self.timeout_count + 1,
                       timeout_evidence_ids=(*self.timeout_evidence_ids, token))

    def transition(
        self, status: OpportunityStatus, *, at: datetime, reason: str,
        evidence_ids: tuple[str, ...] = (),
    ) -> "SetupOpportunity":
        if self.status is not OpportunityStatus.ACTIVE:
            return self
        if status is OpportunityStatus.CONFIRMED and not evidence_ids:
            raise ValueError("confirmation transition requires evidence")
        if not reason.strip():
            raise ValueError("opportunity transition reason is required")
        return replace(
            self, status=status,
            transitions=(*self.transitions, OpportunityTransition(status, at, reason, evidence_ids)),
        )


def advance_timeouts_before_formation(
    opportunities: Iterable[SetupOpportunity],
    *,
    timeframe: str,
    at: datetime,
    evidence_id: str | None = None,
) -> tuple[SetupOpportunity, ...]:
    """Advance timeout evidence before callers inspect same-bar structure."""
    return tuple(item.advance_timeout(timeframe=timeframe, at=at, evidence_id=evidence_id) for item in opportunities)


__all__ = [
    "ExecutionGroupClaim", "ExecutionGroupRegistry", "OpportunityStatus",
    "OpportunityTransition", "SetupOpportunity", "StructuralSetup", "advance_timeouts_before_formation",
]
