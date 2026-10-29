"""Causal response-window evaluation independent from interaction lifecycle."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, time, timezone

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
    candidates = _after_prior_day_transition(tuple(sorted(
        (item for item in observations
        if getattr(item, "timeframe", None) == template.response_timeframe
        and isinstance(getattr(item, "observed_at", None), datetime)
        and isinstance(getattr(interaction, "created_at", None), datetime)
        and getattr(item, "observed_at") > getattr(interaction, "created_at")
        and isinstance(getattr(item, "available_at", None), datetime)
        and getattr(item, "available_at") <= cutoff
        and (expiry is None or getattr(item, "available_at") <= expiry)
        and (not isinstance(terminal_at, datetime) or getattr(item, "observed_at") <= terminal_at)),
        key=lambda item: (getattr(item, "observed_at"), getattr(item, "available_at"), getattr(item, "observation_id", "")),
    )), interaction)[:max_candles]
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
            result = evaluate(
                build_response_spec(template, band_lower=float(lower), band_upper=float(upper)),
                pre_contact_side=getattr(interaction, "pre_contact_side", None),
            )
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
        candidates = _after_prior_day_transition(tuple(sorted(
            (item for item in observations
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
                and (not isinstance(terminal_at, datetime) or getattr(item, "observed_at") <= terminal_at)),
            key=lambda item: (getattr(item, "observed_at"), getattr(item, "available_at"), getattr(item, "observation_id", "")),
        )), interaction)[:max_candles]
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
    "evaluate_policy_response", "evaluate_policy_responses",
]
