"""Causal response-window evaluation independent from interaction lifecycle."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, time, timezone

from .response import (
    ResponseCandle,
    ResponseEvaluation as GeometryResponseEvaluation,
    ResponseOutcomeStatus as GeometryResponseOutcomeStatus,
    build_response_spec,
    evaluate_response_window as evaluate_geometry_response_window,
)
from .templates import Direction, ResponseType, TRADE_TEMPLATES, TradeTemplate
from ftx_paper.config import IST


@dataclass(frozen=True, slots=True)
class PolicyResponseEvaluation:
    """One response hypothesis plus explicit same-interaction arbitration."""

    template: TradeTemplate
    evaluation: GeometryResponseEvaluation[object]
    selected: bool = False
    arbitration_reason: str | None = None


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


def _causal_candidates(
    interaction: object, observations: Sequence[object], template: TradeTemplate,
    cutoff: datetime, *, expiry: datetime | None,
) -> tuple[object, ...]:
    """Select only completed, same-instrument/session response evidence."""
    created_at = getattr(interaction, "created_at", None)
    terminal_at = getattr(interaction, "closed_at", None)
    expected_instrument = getattr(interaction, "instrument", None)
    expected_session = getattr(interaction, "session_id", None)
    selected: list[object] = []
    for item in observations:
        observed_at = getattr(item, "observed_at", None)
        available_at = getattr(item, "available_at", None)
        if (
            getattr(item, "timeframe", None) != template.response_timeframe
            or not isinstance(observed_at, datetime)
            or not isinstance(available_at, datetime)
            or not isinstance(created_at, datetime)
            or observed_at < created_at
            or available_at > cutoff
            or (expiry is not None and available_at > expiry)
            or (isinstance(terminal_at, datetime) and observed_at > terminal_at)
            or (expected_instrument is not None and getattr(item, "instrument", None) != expected_instrument)
            or (expected_session is not None and getattr(getattr(item, "context", None), "session_id", None) != expected_session)
        ):
            continue
        interval_end = getattr(item, "interval_end", None)
        if isinstance(interval_end, datetime) and available_at < interval_end:
            continue
        selected.append(item)
    return tuple(sorted(
        selected,
        key=lambda item: (
            getattr(item, "observed_at"), getattr(item, "available_at"),
            getattr(item, "observation_id", ""),
        ),
    ))


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


def evaluate_policy_responses(
    interaction: object,
    observations: Sequence[object],
    *,
    max_candles: int,
    cutoff: object,
    initial_armed: Mapping[str, bool] | None = None,
) -> tuple[PolicyResponseEvaluation, ...]:
    """Evaluate all arms on shared evidence, then select execution authority."""
    alias = str(getattr(interaction, "location_alias", "")).upper()
    if not isinstance(cutoff, datetime):
        return ()
    templates = tuple(sorted(
        (template for (family, location), template in TRADE_TEMPLATES.items()
         if location == alias and family in {"5m_recovery_breakout", "5m_acceptance_breakout"}),
         key=lambda item: item.response_type.value != "acceptance",
    ))
    results: list[PolicyResponseEvaluation] = []
    shared_candidates: tuple[object, ...] | None = None
    for template in templates:
        template_for_evaluation = _effective_template(template, interaction)
        created_at = getattr(interaction, "created_at", None)
        expiry = _template_expiry(created_at, template_for_evaluation.expiry_at) if isinstance(created_at, datetime) else None
        if shared_candidates is None:
            shared_candidates = _after_prior_day_transition(
                _causal_candidates(
                    interaction, observations, template_for_evaluation, cutoff, expiry=expiry,
                ), interaction,
            )[:max_candles]
        candidates = shared_candidates
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
        results.append(PolicyResponseEvaluation(template, evaluation))
    resolved = [
        item for item in results
        if item.evaluation.status is GeometryResponseOutcomeStatus.CONFIRMED
        and item.evaluation.confirmation_available_at is not None
    ]
    if not resolved:
        return tuple(results)
    latest_available_at = max(
        item.evaluation.confirmation_available_at for item in resolved
        if item.evaluation.confirmation_available_at is not None
    )
    latest = [
        item for item in resolved
        if item.evaluation.confirmation_available_at == latest_available_at
    ]
    winner = min(
        latest,
        key=lambda item: (
            0 if item.template.response_type is ResponseType.REJECTION else 1,
            item.evaluation.direction.value if item.evaluation.direction is not None else "",
            item.template.template_id,
        ),
    )
    has_prior = any(
        item.evaluation.confirmation_available_at is not None
        and item.evaluation.confirmation_available_at < latest_available_at
        for item in resolved
    )
    has_rejection = any(item.template.response_type is ResponseType.REJECTION for item in latest)
    return tuple(
        replace(
            item,
            selected=item is winner,
            arbitration_reason=(
                "later_causal_response_superseded_prior" if item is winner and has_prior
                else "same_time_rejection_precedence" if item is winner and has_rejection
                else "single_latest_causal_response" if item is winner
                else "same_interaction_execution_arbitration"
            ),
        ) if item.evaluation.status is GeometryResponseOutcomeStatus.CONFIRMED else item
        for item in results
    )


__all__ = [
    "PolicyResponseEvaluation", "evaluate_policy_responses",
]
