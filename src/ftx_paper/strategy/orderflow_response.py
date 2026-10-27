"""Causal response-window evaluation independent from interaction lifecycle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from .orderflow_contract import OrderflowObservation
from .orderflow_interaction import LocationInteraction


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


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
        if direction == "long" and close_value < lower:
            status = ResponseOutcomeStatus.INVALIDATED
            break
        if direction == "short" and close_value > upper:
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


__all__ = [
    "ResponseEvaluation", "ResponseHypothesis", "ResponseOutcomeStatus",
    "evaluate_response_hypotheses", "evaluate_response_window", "select_response_hypotheses",
]
