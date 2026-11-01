"""Template-driven opportunity formation and causal lifecycle contracts."""

from __future__ import annotations
# pyright: reportArgumentType=false, reportOperatorIssue=false, reportAssignmentType=false

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Sequence

from .hierarchy import StructuralSetup
from .interactions import candle_identity
from .lifecycle import EvidenceRef as LifecycleEvidenceRef, SetupOpportunity
from .timestamps import coerce_timestamp, nifty_label, parse_nifty_label, serialize_utc
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
    observed_at: datetime
    available_at: datetime


@dataclass(frozen=True, slots=True)
class DirectionalResponse:
    direction: Direction
    anchor: str
    reference_level: float
    structure_boundary: float
    invalidation_boundary: float
    entry_boundary: float | None
    evidence_ids: tuple[str, ...]
    observed_at: datetime
    available_at: datetime
    band: Any | None = None
    evidence: tuple[Any, ...] = ()
    evidence_lineage: tuple[EvidenceRef, ...] = ()


@dataclass(frozen=True, slots=True)
class OpportunityOutcome:
    status: OpportunityOutcomeStatus
    reason: str
    observed_at: datetime
    available_at: datetime
    evidence: tuple[Any, ...] = ()
    response: DirectionalResponse | None = None
    evidence_lineage: tuple[EvidenceRef, ...] = ()
    policy_version: str | None = None
    template: TradeTemplate | None = None
    as_of: datetime | None = None


@dataclass(frozen=True, slots=True)
class OpportunityContext:
    lifecycle_id: str
    session_date: str
    instrument: str
    strategy: str
    candidate_id: str
    context_candle: Any
    clock: datetime
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
    return EvidenceRef(str(_value(item, "observation_id") if hasattr(item, "observation_id") else _value(item, "time")), _value(item, "observed_at") if hasattr(item, "observed_at") else _value(item, "time"), _value(item, "available_at"))


def opportunity_identity(*, session_date: str, instrument: str, direction: Direction, interaction_id: str, reference_generation: int, response_evidence_ids: tuple[str, ...], formation_at: datetime | str, formation_available_at: datetime | str, template_id: str = "") -> str:
    evidence_ids = tuple(
        nifty_label(value) if isinstance(value, datetime) else str(value)
        for value in response_evidence_ids
    )
    fields = (session_date, instrument, direction.value, interaction_id, str(reference_generation), *([template_id] if template_id else []), *evidence_ids, nifty_label(coerce_timestamp(formation_at)), nifty_label(coerce_timestamp(formation_available_at)))
    return "|".join(fields)


def evaluate_response(template: TradeTemplate, interaction: Any, bars: Sequence[Any], *, as_of: str, response_evidence: Sequence[Any] | None = None) -> OpportunityOutcome:
    as_of = coerce_timestamp(as_of)
    interaction_available = coerce_timestamp(_value(interaction, "available_at"))
    if as_of < interaction_available:
        raise ValueError("evaluation as-of cannot precede interaction availability")
    interaction_observed = _value(interaction, "interaction_started_at")
    if template.carry_sessions < 0:
        raise ValueError("template response timing is invalid")
    if getattr(interaction, "sessions_carried", 0) > template.carry_sessions:
        latest = max(
            (
                coerce_timestamp(_value(bar, "available_at"))
                for bar in bars
                if coerce_timestamp(_value(bar, "available_at")) <= as_of
            ),
            default=interaction_available,
        )
        return OpportunityOutcome(OpportunityOutcomeStatus.EXPIRED, "response_window_expired", latest, latest, template=template, as_of=as_of)
    creation = _value(interaction, "creation_candle")
    source = response_evidence if response_evidence is not None else bars
    available = tuple(sorted(
        (
            bar for bar in source
            if _value(bar, "timeframe") == template.response_timeframe
            and _value(bar, "symbol") == _value(interaction, "instrument")
            and coerce_timestamp(_value(bar, "available_at")) >= coerce_timestamp(_value(creation, "available_at"))
            and coerce_timestamp(_value(bar, "available_at")) >= coerce_timestamp(_value(bar, "end_time"))
            and coerce_timestamp(_value(bar, "available_at")) <= as_of
            and coerce_timestamp(_value(bar, "available_at")) <= parse_nifty_label(
                f"{_value(bar, 'session_date')}T{template.expiry_at}"
            )
        ),
        key=lambda item: (_value(item, "time"), _value(item, "available_at")),
    ))
    terminal_id = getattr(interaction, "terminal_candle_id", None)
    if terminal_id is not None:
        terminal = next((bar for bar in available if candle_identity(bar) == terminal_id or _value(bar, "time") == terminal_id), None)
        if terminal is not None:
            available = tuple(bar for bar in available if _value(bar, "time") <= _value(terminal, "time"))
        else:
            available = tuple(bar for bar in available if candle_identity(bar) <= terminal_id)
    contact_index = next((index for index, bar in enumerate(available)
                          if (_value(bar, "time") == _value(creation, "time")
                              or _value(bar, "end_time") == _value(creation, "time"))
                          and _value(bar, "session_date") == _value(interaction, "session_date")), None)
    if contact_index is None:
        return OpportunityOutcome(OpportunityOutcomeStatus.PENDING, "contact_not_available", interaction_observed, interaction_available, template=template, as_of=as_of)
    available = available[contact_index:]
    sessions = tuple(dict.fromkeys(_value(bar, "session_date") for bar in available))
    available = tuple(bar for bar in available if sessions.index(_value(bar, "session_date")) <= template.carry_sessions)
    available = tuple(sorted(available, key=lambda item: (_value(item, "time"), _value(item, "available_at"))))
    if not available:
        return OpportunityOutcome(OpportunityOutcomeStatus.PENDING, "missing_completed_5m", interaction_observed, interaction_available, template=template, as_of=as_of)
    spec = build_response_spec(template, band=_value(interaction, "band"))
    evaluation = evaluate_response_window(
        available,
        to_candle=lambda item: ResponseCandle(float(_value(item, "features")["open"] if hasattr(item, "features") else _value(item, "open")), float(_value(item, "features")["high"] if hasattr(item, "features") else _value(item, "high")), float(_value(item, "features")["low"] if hasattr(item, "features") else _value(item, "low")), float(_value(item, "features")["close"] if hasattr(item, "features") else _value(item, "close"))),
        spec_for=lambda _item: spec,
        armed=False,
        pre_contact_side=getattr(interaction, "pre_contact_side", None),
    )
    evidence = tuple(evaluation.evidence)
    lineage = tuple(_lineage(item) for item in evidence)
    observed = _value(evidence[-1], "end_time") if evidence else _value(interaction, "interaction_started_at")
    available_at = _value(evidence[-1], "available_at") if evidence else _value(interaction, "available_at")
    if evaluation.status is ResponseOutcomeStatus.INVALIDATED:
        return OpportunityOutcome(OpportunityOutcomeStatus.CONTRADICTED, "reference_crossed_after_contact", observed, available_at, evidence, evidence_lineage=lineage, policy_version=template.policy_version, template=template, as_of=as_of)
    if evaluation.status is not ResponseOutcomeStatus.CONFIRMED:
        expired = len(sessions) > template.carry_sessions + 1 or spec.expired(session_date=nifty_label(as_of)[:10], as_of=as_of)
        status = OpportunityOutcomeStatus.EXPIRED if expired else OpportunityOutcomeStatus.PENDING
        reason = "response_window_expired" if expired else (
            "inconclusive_contact" if any(
                _value(bar, "close") == float(_value(interaction, "reference_level"))
                or _value(bar, "open") == _value(bar, "close") for bar in evidence
            ) else "response_window_open"
        )
        return OpportunityOutcome(status, reason, observed, available_at, evidence, evidence_lineage=lineage, policy_version=template.policy_version, template=template, as_of=as_of)
    candle = evaluation.confirmation_candle
    if candle is None:
        raise RuntimeError("confirmed response is missing its candle")
    fields = candle.features if hasattr(candle, "features") else {"high": _value(candle, "high"), "low": _value(candle, "low"), "close": _value(candle, "close")}
    direction = evaluation.direction or template.direction
    band = _value(interaction, "band")
    response = DirectionalResponse(direction, str(_value(interaction, "location")), float(_value(interaction, "reference_level")), float(fields["high"] if direction is Direction.LONG else fields["low"]), float(band.low if direction is Direction.LONG else band.high), float(band.high if direction is Direction.LONG else band.low) if template.response_type is ResponseType.REJECTION else None, (str(_value(interaction, "interaction_id")), candle_identity(candle)), _value(candle, "end_time"), _value(candle, "available_at"), band=band, evidence=evidence, evidence_lineage=lineage)
    return OpportunityOutcome(OpportunityOutcomeStatus.RESOLVED, f"causal_{template.response_type.value}", response.observed_at, response.available_at, evidence, response, lineage, template.policy_version, template, as_of)


def identify_opportunity(template: TradeTemplate, interaction: Any, bars: Sequence[Any], *, as_of: str, response_evidence: Sequence[Any] | None = None) -> tuple[OpportunityOutcome, str | None]:
    outcome = evaluate_response(template, interaction, bars, as_of=as_of, response_evidence=response_evidence)
    response = outcome.response
    if response is None or outcome.status is not OpportunityOutcomeStatus.RESOLVED:
        return outcome, None
    return outcome, opportunity_identity(session_date=str(_value(interaction, "session_date")), instrument=str(_value(interaction, "instrument")), direction=response.direction, interaction_id=str(_value(interaction, "interaction_id")), reference_generation=int(_value(interaction, "reference_generation")), template_id=template.template_id, response_evidence_ids=response.evidence_ids, formation_at=response.observed_at, formation_available_at=response.available_at)


def form_opportunity(response: DirectionalResponse, context: OpportunityContext, template: TradeTemplate, *, formation_available_at: str | None = None, decision_context_required: bool = True) -> tuple[SetupOpportunity, StructuralSetup]:
    if template.direction is not response.direction:
        raise ValueError("trade template direction does not match response direction")
    available = coerce_timestamp(formation_available_at or response.available_at)
    if available < response.available_at:
        raise ValueError("formation availability cannot precede response availability")
    failure = context.failure_boundary if context.failure_boundary is not None else response.invalidation_boundary
    entry = response.entry_boundary or response.structure_boundary
    response_episode = context.response_episode_id or f"response-episode:{context.session_date}:{context.instrument}:{template.template_id}:{context.location_interaction_id or context.lifecycle_id}"
    group = f"execution-group:{context.session_date}:{context.instrument}:{template.execution_conflict_group}:{serialize_utc(available)}" if template.execution_conflict_group else response_episode
    lineage = tuple(LifecycleEvidenceRef(ref.evidence_id, "5m", ref.observed_at, ref.available_at, "", 0) for ref in response.evidence_lineage)
    opportunity = SetupOpportunity(context.lifecycle_id, response.direction, context.session_date, context.instrument, context.strategy, response.observed_at, available, context.candidate_id, context.parent_context, response.anchor, response.reference_level, context.context_candle, response.structure_boundary, failure, policy_version=context.policy_version, reference_generation=context.reference_generation, causal_identity=context.causal_identity, location_interaction_id=context.location_interaction_id, opening_interaction_kind=context.opening_interaction_kind, opening_policy_version=context.opening_policy_version, template_id=template.template_id, response_episode_id=response_episode, execution_group_id=group, clock=context.clock, decision_context_required=decision_context_required, origin_evidence_id=response.evidence_ids[0] if response.evidence_ids else None, response_evidence_ids=response.evidence_ids, response_evidence_lineage=lineage)
    setup = StructuralSetup(context.candidate_id, f"{context.candidate_id}.template-{template.version}", context.regime_version, response.direction.value, "5m_acceptance_breakout" if template.response_type is ResponseType.ACCEPTANCE else "5m_recovery_breakout", entry, entry, context.context_candle.low if response.direction is Direction.LONG else context.context_candle.high, context.context_candle.low if response.direction is Direction.LONG else context.context_candle.high, failure, parse_nifty_label(f"{context.session_date}T{template.expiry_at}"), required_confirmation=template.required_confirmation, parent_context_id=context.parent_context.context_id if context.parent_context else None, parent_context_version=context.parent_context.version if context.parent_context else None, context_reference_level=response.reference_level, lifecycle_id=context.lifecycle_id, template=template)
    return opportunity, setup


__all__ = ["DirectionalResponse", "EvidenceRef", "OpportunityContext", "OpportunityOutcome", "OpportunityOutcomeStatus", "evaluate_response", "form_opportunity", "identify_opportunity", "opportunity_identity"]
