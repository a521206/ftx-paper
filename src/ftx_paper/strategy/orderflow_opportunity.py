"""Diagnostic opportunity/setup lifecycle contracts for orderflow."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from collections.abc import Iterable, Mapping
import hashlib

from .orderflow_context import ContextVersionRef


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

    def snapshot(self) -> dict[str, object]:
        def encode(item: ExecutionGroupClaim) -> dict[str, object]:
            return {
                "execution_group_id": item.execution_group_id,
                "lifecycle_id": item.lifecycle_id,
                "accepted": item.accepted,
                "reason": item.reason,
                "evidence_ids": list(item.evidence_ids),
            }
        return {
            "claims": [encode(item) for item in self._claims.values()],
            "history": [encode(item) for item in self._history],
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "ExecutionGroupRegistry":
        registry = cls()
        claims = snapshot.get("claims", ())
        history = snapshot.get("history", ())
        if not isinstance(claims, (list, tuple)) or not isinstance(history, (list, tuple)):
            raise ValueError("invalid execution group snapshot")

        def decode(raw: object) -> ExecutionGroupClaim:
            if not isinstance(raw, Mapping):
                raise ValueError("execution group claim must be an object")
            evidence = raw.get("evidence_ids", ())
            if not isinstance(evidence, (list, tuple)):
                raise ValueError("execution group evidence must be a sequence")
            accepted = raw.get("accepted")
            if not isinstance(accepted, bool):
                raise ValueError("execution group accepted flag must be boolean")
            return ExecutionGroupClaim(
                str(raw["execution_group_id"]), str(raw["lifecycle_id"]),
                accepted, str(raw["reason"]), tuple(str(item) for item in evidence),
            )

        seen_history: set[tuple[str, str, bool, str, tuple[str, ...]]] = set()
        for item in history:
            claim = decode(item)
            identity = (claim.execution_group_id, claim.lifecycle_id, claim.accepted, claim.reason, claim.evidence_ids)
            if identity in seen_history:
                raise ValueError("duplicate execution group history claim")
            seen_history.add(identity)
            registry._history.append(claim)
        seen_groups: set[str] = set()
        for item in claims:
            claim = decode(item)
            if claim.execution_group_id in seen_groups:
                raise ValueError("duplicate execution group claim")
            seen_groups.add(claim.execution_group_id)
            registry._claims[claim.execution_group_id] = claim
        return registry


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
    template_id: str = ""
    policy_version: str = ""
    context_relation: str | None = None
    source_interaction_id: str | None = None
    formation_context: ContextVersionRef | None = None
    decision_context: ContextVersionRef | None = None

    @property
    def decision_id(self) -> str:
        return f"opportunity:{self.lifecycle_id}"

    def source_is_current(
        self, *, interaction_id: str, context_version: int | None,
    ) -> bool:
        return (
            self.source_interaction_id == interaction_id
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
            template_id=str(getattr(response, "template_id", "")),
            policy_version=str(getattr(response, "policy_version", "")),
            conflict_group_id=getattr(response, "execution_conflict_group", None),
        )

    @classmethod
    def create(
        cls, *, interaction_id: str, direction: str, formed_at: datetime,
        expires_at: datetime, entry_lower: float, entry_upper: float,
        failure_boundary: float, evidence_ids: tuple[str, ...],
         evidence_timeframe: str = "5m", regime_version: int | None = None,
         template_id: str = "", policy_version: str = "", conflict_group_id: str | None = None,
         context_relation: str | None = None,
         formation_context: ContextVersionRef | None = None,
    ) -> "SetupOpportunity":
        seed = (
            f"{interaction_id}|{formed_at.isoformat()}|{direction}|{evidence_ids}|"
            f"{evidence_timeframe}|{regime_version}|{template_id}|{policy_version}"
        )
        digest = hashlib.sha256(seed.encode()).hexdigest()[:24]
        setup = StructuralSetup(
            f"setup-{digest}", direction, entry_lower, entry_upper,
            failure_boundary, formed_at, expires_at, evidence_ids,
            context_version=regime_version,
        )
        return cls(
            f"lifecycle-{digest}", f"response-{interaction_id}:{template_id or 'default'}",
            conflict_group_id or f"execution:{digest}",
            setup, OpportunityStatus.ACTIVE, regime_version, evidence_timeframe,
            transitions=(OpportunityTransition(
                OpportunityStatus.ACTIVE, formed_at, "formed", evidence_ids,
            ),), template_id=template_id, policy_version=policy_version,
            context_relation=context_relation, source_interaction_id=interaction_id,
            formation_context=formation_context, decision_context=formation_context,
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
            previous = self.transitions[-1] if self.transitions else None
            if previous is not None and (
                previous.status is status
                and previous.reason == reason
                and previous.evidence_ids == evidence_ids
            ):
                return self
            raise ValueError("conflicting transition after terminal opportunity state")
        if status is OpportunityStatus.CONFIRMED and not evidence_ids:
            raise ValueError("confirmation transition requires evidence")
        if not reason.strip():
            raise ValueError("opportunity transition reason is required")
        return replace(
            self, status=status,
            transitions=(*self.transitions, OpportunityTransition(status, at, reason, evidence_ids)),
        )

    def snapshot(self) -> dict[str, object]:
        return {
            "lifecycle_id": self.lifecycle_id,
            "response_episode_id": self.response_episode_id,
            "execution_group_id": self.execution_group_id,
            "setup": {
                "setup_id": self.setup.setup_id,
                "direction": self.setup.direction,
                "entry_lower": self.setup.entry_lower,
                "entry_upper": self.setup.entry_upper,
                "failure_boundary": self.setup.failure_boundary,
                "formed_at": self.setup.formed_at.isoformat(),
                "expires_at": self.setup.expires_at.isoformat(),
                "evidence_ids": list(self.setup.evidence_ids),
                "context_version": self.setup.context_version,
                "confirmation_ids": list(self.setup.confirmation_ids),
                "regime_version": self.setup.regime_version,
            },
            "status": self.status.value,
            "regime_version": self.regime_version,
            "evidence_timeframe": self.evidence_timeframe,
            "timeout_count": self.timeout_count,
            "timeout_evidence_ids": list(self.timeout_evidence_ids),
            "transitions": [
                {
                    "status": item.status.value,
                    "at": item.at.isoformat(),
                    "reason": item.reason,
                    "evidence_ids": list(item.evidence_ids),
                }
                for item in self.transitions
            ],
            "template_id": self.template_id,
            "policy_version": self.policy_version,
            "context_relation": self.context_relation,
            "source_interaction_id": self.source_interaction_id,
            "formation_context": (
                {"context_id": self.formation_context.context_id, "version": self.formation_context.version}
                if self.formation_context is not None else None
            ),
            "decision_context": (
                {"context_id": self.decision_context.context_id, "version": self.decision_context.version}
                if self.decision_context is not None else None
            ),
        }

    @classmethod
    def from_snapshot(cls, raw: Mapping[str, object]) -> "SetupOpportunity":
        setup_raw = raw.get("setup")
        transitions_raw = raw.get("transitions", ())
        if not isinstance(setup_raw, Mapping) or not isinstance(transitions_raw, (list, tuple)):
            raise ValueError("invalid opportunity snapshot")

        def strings(value: object, name: str) -> tuple[str, ...]:
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"invalid opportunity {name}")
            return tuple(str(item) for item in value)

        setup = StructuralSetup(
            str(setup_raw["setup_id"]), str(setup_raw["direction"]),
            float(setup_raw["entry_lower"]), float(setup_raw["entry_upper"]),
            float(setup_raw["failure_boundary"]),
            datetime.fromisoformat(str(setup_raw["formed_at"])),
            datetime.fromisoformat(str(setup_raw["expires_at"])),
            strings(setup_raw.get("evidence_ids", ()), "evidence_ids"),
            int(setup_raw["context_version"]) if setup_raw.get("context_version") is not None else None,
            strings(setup_raw.get("confirmation_ids", ()), "confirmation_ids"),
            int(setup_raw["regime_version"]) if setup_raw.get("regime_version") is not None else None,
        )
        transitions_list: list[OpportunityTransition] = []
        for item in transitions_raw:
            if not isinstance(item, Mapping):
                raise ValueError("opportunity transition must be an object")
            transitions_list.append(OpportunityTransition(
                OpportunityStatus(str(item["status"])),
                datetime.fromisoformat(str(item["at"])), str(item["reason"]),
                strings(item.get("evidence_ids", ()), "transition evidence_ids"),
            ))
        transitions = tuple(transitions_list)
        regime_version = raw.get("regime_version")
        timeout_count = raw.get("timeout_count", 0)
        if regime_version is not None and not isinstance(regime_version, int):
            raise ValueError("invalid opportunity regime version")
        if not isinstance(timeout_count, int):
            raise ValueError("invalid opportunity timeout count")
        if not transitions:
            raise ValueError("opportunity snapshot requires an initial transition")
        if len(set(transitions)) != len(transitions):
            raise ValueError("opportunity snapshot contains duplicate transitions")
        if transitions[0].status is not OpportunityStatus.ACTIVE or transitions[0].at != setup.formed_at:
            raise ValueError("opportunity snapshot has an invalid initial transition")
        if transitions[0].reason != "formed":
            raise ValueError("opportunity snapshot initial transition must be formed")
        if any(current.at < previous.at for previous, current in zip(transitions, transitions[1:])):
            raise ValueError("opportunity snapshot transitions must be chronological")
        terminal_index = next(
            (index for index, item in enumerate(transitions) if item.status is not OpportunityStatus.ACTIVE),
            None,
        )
        if terminal_index is not None and any(
            item.status is OpportunityStatus.ACTIVE for item in transitions[terminal_index + 1:]
        ):
            raise ValueError("opportunity snapshot reactivates a terminal lifecycle")
        expected_status = transitions[-1].status
        if OpportunityStatus(str(raw["status"])) is not expected_status:
            raise ValueError("opportunity snapshot status does not match transition history")
        timeout_evidence = strings(raw.get("timeout_evidence_ids", ()), "timeout_evidence_ids")
        if len(set(timeout_evidence)) != len(timeout_evidence):
            raise ValueError("opportunity snapshot contains duplicate timeout evidence")
        if timeout_count < len(timeout_evidence):
            raise ValueError("opportunity timeout count is below timeout evidence count")
        def context_ref(value: object) -> ContextVersionRef | None:
            if value is None:
                return None
            if not isinstance(value, Mapping):
                raise ValueError("invalid opportunity context reference")
            return ContextVersionRef(str(value["context_id"]), int(value["version"]))

        return cls(
            str(raw["lifecycle_id"]), str(raw["response_episode_id"]),
            str(raw["execution_group_id"]), setup,
            OpportunityStatus(str(raw["status"])),
            regime_version,
            str(raw["evidence_timeframe"]), timeout_count,
            timeout_evidence, transitions,
            str(raw.get("template_id", "")), str(raw.get("policy_version", "")),
            str(raw["context_relation"]) if raw.get("context_relation") is not None else None,
            str(raw["source_interaction_id"]) if raw.get("source_interaction_id") is not None else None,
            context_ref(raw.get("formation_context")), context_ref(raw.get("decision_context")),
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
