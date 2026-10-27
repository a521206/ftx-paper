"""Template-driven opportunity formation and causal lifecycle contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Sequence

from .response import ResponseCandle, ResponseOutcomeStatus, build_response_spec, evaluate_response_window
from .templates import Direction, ResponseType, TradeTemplate


class OpportunityOutcomeStatus(StrEnum):
    PENDING = "PENDING"
    RESOLVED = "RESOLVED"
    CONTRADICTED = "CONTRADICTED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    evidence_id: str
    observed_at: str
    available_at: str


@dataclass(frozen=True, slots=True)
class DirectionalResponse:
    direction: Direction
    anchor: str
    reference_level: float
    structure_boundary: float
    invalidation_boundary: float
    entry_boundary: float | None
    evidence_ids: tuple[str, ...]
    observed_at: str
    available_at: str


@dataclass(frozen=True, slots=True)
class OpportunityOutcome:
    status: OpportunityOutcomeStatus
    reason: str
    observed_at: str
    available_at: str
    evidence: tuple[Any, ...] = ()
    response: DirectionalResponse | None = None
    evidence_lineage: tuple[EvidenceRef, ...] = ()
    policy_version: str | None = None
    template: TradeTemplate | None = None
    as_of: str | None = None


@dataclass(frozen=True, slots=True)
class OpportunityContext:
    lifecycle_id: str
    session_date: str
    instrument: str
    strategy: str
    candidate_id: str
    context_candle: Any
    clock: str
    regime_version: str
    policy_version: str
    reference_generation: int = 0
    causal_identity: str | None = None
    location_interaction_id: str | None = None
    opening_interaction_kind: str | None = None
    opening_policy_version: str | None = None
    failure_boundary: float | None = None
    response_episode_id: str | None = None
    parent_context: Any | None = None


def _value(item: Any, name: str) -> Any:
    return item.get(name) if isinstance(item, dict) else getattr(item, name)


def _lineage(item: Any) -> EvidenceRef:
    return EvidenceRef(str(_value(item, "observation_id") if hasattr(item, "observation_id") else _value(item, "time")), str(_value(item, "observed_at") if hasattr(item, "observed_at") else _value(item, "time")), str(_value(item, "available_at")))


def opportunity_identity(*, session_date: str, instrument: str, direction: Direction, interaction_id: str, reference_generation: int, response_evidence_ids: tuple[str, ...], formation_at: str, formation_available_at: str, template_id: str = "") -> str:
    fields = (session_date, instrument, direction.value, interaction_id, str(reference_generation), *([template_id] if template_id else []), *response_evidence_ids, formation_at, formation_available_at)
    return "|".join(fields)


def evaluate_response(template: TradeTemplate, interaction: Any, bars: Sequence[Any], *, as_of: str, response_evidence: Sequence[Any] | None = None) -> OpportunityOutcome:
    available = tuple(response_evidence) if response_evidence is not None else tuple(
        bar for bar in bars
        if _value(bar, "timeframe") == template.response_timeframe
        and _value(bar, "available_at") <= as_of
    )
    if not available:
        return OpportunityOutcome(OpportunityOutcomeStatus.PENDING, "missing_completed_evidence", str(_value(interaction, "created_at")), str(_value(interaction, "available_at")), policy_version=template.policy_version, template=template, as_of=as_of)
    spec = build_response_spec(template, band_lower=float(_value(interaction, "band_lower")), band_upper=float(_value(interaction, "band_upper")))
    evaluation = evaluate_response_window(
        available,
        to_candle=lambda item: ResponseCandle(float(_value(item, "features")["open"] if hasattr(item, "features") else _value(item, "open")), float(_value(item, "features")["high"] if hasattr(item, "features") else _value(item, "high")), float(_value(item, "features")["low"] if hasattr(item, "features") else _value(item, "low")), float(_value(item, "features")["close"] if hasattr(item, "features") else _value(item, "close"))),
        spec_for=lambda _item: spec,
        armed=False,
        pre_contact_side=getattr(interaction, "pre_contact_side", None),
    )
    evidence = tuple(evaluation.evidence)
    lineage = tuple(_lineage(item) for item in evidence)
    observed = str(_value(evidence[-1], "observed_at")) if evidence else str(_value(interaction, "created_at"))
    available_at = str(_value(evidence[-1], "available_at")) if evidence else str(_value(interaction, "available_at"))
    if evaluation.status is ResponseOutcomeStatus.INVALIDATED:
        return OpportunityOutcome(OpportunityOutcomeStatus.CONTRADICTED, "reference_crossed_after_contact", observed, available_at, evidence, evidence_lineage=lineage, policy_version=template.policy_version, template=template, as_of=as_of)
    if evaluation.status is not ResponseOutcomeStatus.CONFIRMED:
        status = OpportunityOutcomeStatus.EXPIRED if spec.expired(session_date=as_of[:10], as_of=as_of) else OpportunityOutcomeStatus.PENDING
        return OpportunityOutcome(status, "response_window_expired" if status is OpportunityOutcomeStatus.EXPIRED else "response_window_open", observed, available_at, evidence, evidence_lineage=lineage, policy_version=template.policy_version, template=template, as_of=as_of)
    candle = evaluation.confirmation_candle
    if candle is None:
        raise RuntimeError("confirmed response is missing its candle")
    fields = candle.features if hasattr(candle, "features") else candle
    direction = evaluation.direction or template.direction
    response = DirectionalResponse(direction, str(_value(interaction, "location_alias")), float(_value(interaction, "reference_level")), float(fields["high"] if direction is Direction.LONG else fields["low"]), float(_value(interaction, "band_lower") if direction is Direction.LONG else _value(interaction, "band_upper")), float(_value(interaction, "band_upper") if direction is Direction.LONG and template.response_type is ResponseType.REJECTION else _value(interaction, "band_lower") if direction is Direction.SHORT and template.response_type is ResponseType.REJECTION else fields["close"]), (str(_value(candle, "observation_id")),), str(_value(candle, "observed_at")), str(_value(candle, "available_at")))
    return OpportunityOutcome(OpportunityOutcomeStatus.RESOLVED, f"causal_{template.response_type.value}", response.observed_at, response.available_at, evidence, response, lineage, template.policy_version, template, as_of)


def identify_opportunity(template: TradeTemplate, interaction: Any, bars: Sequence[Any], *, as_of: str, response_evidence: Sequence[Any] | None = None) -> tuple[OpportunityOutcome, str | None]:
    outcome = evaluate_response(template, interaction, bars, as_of=as_of, response_evidence=response_evidence)
    response = outcome.response
    if response is None or outcome.status is not OpportunityOutcomeStatus.RESOLVED:
        return outcome, None
    return outcome, opportunity_identity(session_date=str(_value(interaction, "session_date")), instrument=str(_value(interaction, "instrument")), direction=response.direction, interaction_id=str(_value(interaction, "interaction_id")), reference_generation=int(_value(interaction, "reference_generation")), template_id=template.template_id, response_evidence_ids=response.evidence_ids, formation_at=response.observed_at, formation_available_at=response.available_at)


__all__ = ["DirectionalResponse", "EvidenceRef", "OpportunityContext", "OpportunityOutcome", "OpportunityOutcomeStatus", "evaluate_response", "identify_opportunity", "opportunity_identity"]
