"""Causal response-window evaluation independent from interaction lifecycle."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, time, timezone
from enum import StrEnum

from .orderflow_contract import OrderflowObservation
from .orderflow_interaction import LocationInteraction
from .response import (
    ResponseCandle,
    ResponseEvaluation as GeometryResponseEvaluation,
    ResponseOutcomeStatus as GeometryResponseOutcomeStatus,
    ResponseSpec,
    build_response_spec,
    evaluate_response_window as evaluate_geometry_response_window,
)
from .templates import Direction, ResponseType, TRADE_TEMPLATES, TradeTemplate
from ftx_paper.config import IST


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _prior_day_role(interaction: object) -> str | None:
    alias = str(getattr(interaction, "location_alias", ""))
    if alias == "PD_LOW":
        return getattr(interaction, "pd_role", None) or "SUPPORT"
    if alias == "PD_HIGH":
        return getattr(interaction, "pd_role", None) or "RESISTANCE"
    return None


def _effective_template(template: TradeTemplate, interaction: object) -> TradeTemplate:
    role = _prior_day_role(interaction)
    if role is None:
        return template
    direction = Direction.LONG if (
        template.response_type is ResponseType.REJECTION
    ) == (role == "SUPPORT") else Direction.SHORT
    return replace(template, direction=direction)


def _after_prior_day_transition(
    candidates: Sequence[object], interaction: object,
) -> tuple[object, ...]:
    transition_at = getattr(interaction, "pd_role_transition_available_at", None)
    if _prior_day_role(interaction) is None or transition_at is None:
        return tuple(candidates)
    return tuple(
        item for item in candidates
        if getattr(item, "available_at", None) >= transition_at
    )


def _template_expiry(created_at: datetime, expiry_at: str) -> datetime:
    expiry = time.fromisoformat(expiry_at)
    # Session timestamps are normally IST-aware.  UTC fixtures represent the
    # already-normalized session clock and must not be shifted a second time.
    if created_at.utcoffset() in {timezone.utc.utcoffset(created_at), IST.utcoffset(created_at)}:
        return created_at.replace(
            hour=expiry.hour, minute=expiry.minute, second=expiry.second, microsecond=0,
        )
    local_created = created_at.astimezone(IST)
    return local_created.replace(
        hour=expiry.hour, minute=expiry.minute, second=expiry.second, microsecond=0,
    ).astimezone(created_at.tzinfo)


class ResponseOutcomeStatus(StrEnum):
    UNAVAILABLE = "UNAVAILABLE"
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class ResponseEvaluation:
    interaction_id: str
    status: ResponseOutcomeStatus
    direction: str | None
    evidence_ids: tuple[str, ...]
    confirmation_observation_id: str | None = None
    age: int = 0
    policy_version: str = "response_v1"
    reason: str | None = None
    confirmation_available_at: datetime | None = None
    expected_available_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ResponseHypothesis:
    policy_id: str
    policy_version: str
    max_candles: int = 3
    invalidation_distance: float = 1.0
    require_pre_event_context: bool = False


def select_response_hypotheses(
    location_alias: str,
    policies: dict[str, tuple[ResponseHypothesis, ...]],
    *,
    default: tuple[ResponseHypothesis, ...] = (),
) -> tuple[ResponseHypothesis, ...]:
    """Select every policy explicitly applicable to a location."""
    return tuple(policies.get(location_alias, default))


def evaluate_response_window(
    interaction: LocationInteraction,
    observations: tuple[OrderflowObservation, ...],
    *,
    max_candles: int = 3,
    evaluation_cutoff: datetime | None = None,
    invalidation_lower: float | None = None,
    invalidation_upper: float | None = None,
) -> ResponseEvaluation:
    """Evaluate ordered completed response candles using frozen location data."""
    if max_candles < 1:
        raise ValueError("response window must be positive")
    direction: str | None = None
    evidence: list[str] = []
    status = ResponseOutcomeStatus.PENDING
    confirmation: str | None = None
    age = 0
    previous_observed: datetime | None = None
    for observation in observations:
        if observation.timeframe != interaction.timeframe or observation.observed_at <= interaction.created_at:
            raise ValueError("response evidence does not match interaction timing")
        if age >= max_candles:
            break
        if evaluation_cutoff is not None and observation.available_at > evaluation_cutoff:
            return ResponseEvaluation(
                interaction.interaction_id, ResponseOutcomeStatus.UNAVAILABLE,
                direction, tuple(evidence), age=age,
                reason="response_evidence_unavailable_at_cutoff",
                expected_available_at=observation.available_at,
            )
        if previous_observed is not None and observation.observed_at <= previous_observed:
            raise ValueError("response evidence must be strictly chronological")
        previous_observed = observation.observed_at
        age += 1
        evidence.extend(observation.evidence_ids or (observation.observation_id,))
        close = observation.features.get("close")
        previous = observation.features.get("previous_close")
        open_price = observation.features.get("open")
        close_value = _number(close)
        previous_value = _number(previous)
        open_value = _number(open_price)
        if close_value is None or previous_value is None or open_value is None:
            return ResponseEvaluation(
                interaction.interaction_id, ResponseOutcomeStatus.UNAVAILABLE,
                direction, tuple(evidence), age=age,
                reason="response_ohlc_evidence_unavailable",
                expected_available_at=observation.available_at,
            )
        if direction is None:
            direction = "long" if previous_value <= interaction.reference_level else "short"
        lower = (
            interaction.band_lower
            if interaction.band_lower is not None else
            invalidation_lower if invalidation_lower is not None else
            interaction.reference_level - 1
        )
        upper = (
            interaction.band_upper
            if interaction.band_upper is not None else
            invalidation_upper if invalidation_upper is not None else
            interaction.reference_level + 1
        )
        low_raw = observation.features.get("low")
        high_raw = observation.features.get("high")
        low_value = _number(low_raw)
        high_value = _number(high_raw)
        touched = low_value if low_value is not None else close_value
        touched_band = (
            high_value is None or low_value is None
            or touched <= upper and high_value >= lower
        )
        if direction == "long" and close_value < lower and touched_band:
            status = ResponseOutcomeStatus.INVALIDATED
            break
        if direction == "short" and close_value > upper and touched_band:
            status = ResponseOutcomeStatus.INVALIDATED
            break
        if direction == "long" and close_value >= interaction.reference_level and close_value > open_value:
            status = ResponseOutcomeStatus.CONFIRMED
            confirmation = observation.observation_id
            break
        if direction == "short" and close_value <= interaction.reference_level and close_value < open_value:
            status = ResponseOutcomeStatus.CONFIRMED
            confirmation = observation.observation_id
            break
    if status is ResponseOutcomeStatus.PENDING and age >= max_candles:
        status = ResponseOutcomeStatus.EXPIRED
    return ResponseEvaluation(
        interaction.interaction_id, status, direction, tuple(evidence), confirmation, age,
        reason=("confirmed" if status is ResponseOutcomeStatus.CONFIRMED else status.value.lower()),
        confirmation_available_at=(
            next((item.available_at for item in observations if item.observation_id == confirmation), None)
            if confirmation is not None else None
        ),
    )


def evaluate_response_hypotheses(
    interaction: LocationInteraction,
    observations: tuple[OrderflowObservation, ...],
    hypotheses: tuple[ResponseHypothesis, ...],
    *,
    evaluation_cutoff: datetime | None = None,
) -> tuple[ResponseEvaluation, ...]:
    """Evaluate every applicable policy independently on one causal prefix."""
    evaluations: list[ResponseEvaluation] = []
    for hypothesis in hypotheses:
        if hypothesis.require_pre_event_context and any(
            item.features.get("event_bar") is True for item in observations
        ):
            evaluations.append(ResponseEvaluation(
                interaction.interaction_id, ResponseOutcomeStatus.UNAVAILABLE, None, (),
                policy_version=hypothesis.policy_version,
                reason="pre_event_context_required",
            ))
            continue
        evaluation = evaluate_response_window(
            interaction, observations, max_candles=hypothesis.max_candles,
            evaluation_cutoff=evaluation_cutoff,
        )
        evaluations.append(ResponseEvaluation(
            evaluation.interaction_id, evaluation.status, evaluation.direction,
            evaluation.evidence_ids, evaluation.confirmation_observation_id,
            evaluation.age, hypothesis.policy_version, evaluation.reason,
            evaluation.confirmation_available_at, evaluation.expected_available_at,
        ))
    return tuple(evaluations)


def evaluate_policy_response(
    interaction: object,
    observations: Sequence[object],
    *,
    max_candles: int,
    cutoff: object,
) -> GeometryResponseEvaluation[object] | None:
    """Adapt orderflow observations to the reusable response geometry contract."""
    alias = str(getattr(interaction, "location_alias", "")).upper()
    template = TRADE_TEMPLATES.get(("5m_recovery_breakout", alias))
    if template is None or not isinstance(cutoff, datetime):
        return None
    template = _effective_template(template, interaction)
    created_at = getattr(interaction, "created_at", None)
    terminal_at = getattr(interaction, "closed_at", None)
    expiry = _template_expiry(created_at, template.expiry_at) if isinstance(created_at, datetime) else None
    candidates = _after_prior_day_transition(tuple(
        item for item in observations
        if getattr(item, "timeframe", None) == template.response_timeframe
        and isinstance(getattr(item, "observed_at", None), datetime)
        and isinstance(getattr(interaction, "created_at", None), datetime)
        and getattr(item, "observed_at") > getattr(interaction, "created_at")
        and isinstance(getattr(item, "available_at", None), datetime)
        and getattr(item, "available_at") <= cutoff
        and (expiry is None or getattr(item, "available_at") <= expiry)
        and (not isinstance(terminal_at, datetime) or getattr(item, "observed_at") <= terminal_at)
    ), interaction)[:max_candles]
    if not candidates:
        return GeometryResponseEvaluation(None, False, (), None, template.direction)
    lower, upper = getattr(interaction, "band_lower", None), getattr(interaction, "band_upper", None)
    if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
        return None

    def evaluate(spec: ResponseSpec, *, armed: bool = False, pre_contact_side: str | None = None) -> GeometryResponseEvaluation[object]:
        return evaluate_geometry_response_window(
            candidates,
            to_candle=lambda item: ResponseCandle(*(
                float(getattr(item, "features")[name]) for name in ("open", "high", "low", "close")
            )),
            spec_for=lambda _item: spec,
            armed=armed,
            pre_contact_side=pre_contact_side,
        )

    spec = build_response_spec(template, band_lower=float(lower), band_upper=float(upper))
    result = evaluate(spec, pre_contact_side=getattr(interaction, "pre_contact_side", None))
    if result.status is not GeometryResponseOutcomeStatus.CONFIRMED:
        first_features = getattr(candidates[0], "features", {})
        previous_close = first_features.get("previous_close") if isinstance(first_features, Mapping) else None
        reference = getattr(interaction, "reference_level", None)
        if isinstance(previous_close, (int, float)) and isinstance(reference, (int, float)) and previous_close != reference:
            template = replace(template, direction=Direction.LONG if previous_close < reference else Direction.SHORT)
            result = evaluate(build_response_spec(template, band_lower=float(lower), band_upper=float(upper)))
    return result


def evaluate_policy_responses(
    interaction: object,
    observations: Sequence[object],
    *,
    max_candles: int,
    cutoff: object,
    initial_armed: Mapping[str, bool] | None = None,
) -> tuple[tuple[TradeTemplate, GeometryResponseEvaluation[object]], ...]:
    """Evaluate orderflow acceptance and rejection hypotheses independently."""
    alias = str(getattr(interaction, "location_alias", "")).upper()
    if not isinstance(cutoff, datetime):
        return ()
    templates = tuple(sorted(
        (template for (family, location), template in TRADE_TEMPLATES.items()
         if location == alias and family in {"5m_recovery_breakout", "5m_acceptance_breakout"}),
         key=lambda item: item.response_type.value != "acceptance",
    ))
    results: list[tuple[TradeTemplate, GeometryResponseEvaluation[object]]] = []
    for template in templates:
        template_for_evaluation = _effective_template(template, interaction)
        created_at = getattr(interaction, "created_at", None)
        terminal_at = getattr(interaction, "closed_at", None)
        expiry = _template_expiry(created_at, template_for_evaluation.expiry_at) if isinstance(created_at, datetime) else None
        candidates = _after_prior_day_transition(tuple(
            item for item in observations
            if getattr(item, "timeframe", None) == template_for_evaluation.response_timeframe
            and isinstance(getattr(item, "observed_at", None), datetime)
            and isinstance(created_at, datetime)
            # The contact candle is response-window candle one.  NiftyZoning
            # includes the creation candle once its completed evidence is
            # available; excluding it shifts every response window by one bar.
            and getattr(item, "observed_at") >= created_at
            and isinstance(getattr(item, "available_at", None), datetime)
            and getattr(item, "available_at") <= cutoff
            and (expiry is None or getattr(item, "available_at") <= expiry)
            and (not isinstance(terminal_at, datetime) or getattr(item, "observed_at") <= terminal_at)
        ), interaction)[:max_candles]
        lower, upper = getattr(interaction, "band_lower", None), getattr(interaction, "band_upper", None)
        if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
            continue
        spec = build_response_spec(template_for_evaluation, band_lower=float(lower), band_upper=float(upper))
        evaluation = evaluate_geometry_response_window(
            candidates,
            to_candle=lambda item: ResponseCandle(*(
                float(getattr(item, "features")[name]) for name in ("open", "high", "low", "close")
            )),
            spec_for=lambda _item: spec,
            armed=bool(initial_armed.get(template.template_id, False)) if initial_armed else False,
            pre_contact_side=getattr(interaction, "pre_contact_side", None),
        )
        results.append((template, evaluation))
    return tuple(results)


__all__ = [
    "ResponseEvaluation", "ResponseHypothesis", "ResponseOutcomeStatus",
    "evaluate_policy_response", "evaluate_policy_responses", "evaluate_response_hypotheses",
    "evaluate_response_window", "select_response_hypotheses",
]
