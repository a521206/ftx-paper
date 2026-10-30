"""Canonical orderflow exit-plan conversion and evaluation helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ftx_paper.contracts import ExitPlanRule, MarketBar, OrderSide, exit_plan_from_payload

from .response import ResponseCandle, ResponseSpec, ResponseOutcomeStatus, evaluate_response_window
from .templates import Direction, ResponseType, TradeExit


def exit_plan_from_template(
    exits: Sequence[TradeExit],
    *,
    levels: dict[str, float] | None = None,
    bands: dict[str, tuple[float, float]] | None = None,
) -> tuple[ExitPlanRule, ...]:
    """Convert declarative template exits into frozen execution metadata."""
    resolved_levels = levels or {}
    resolved_bands = bands or {}
    return tuple(
        ExitPlanRule(
            rule_id=rule.rule_id,
            location=rule.location.value,
            mode=rule.mode.value,
            required=rule.required,
            trigger=rule.trigger,
            score_direction=rule.score_direction,
            score_margin=rule.score_margin,
            response_direction=(rule.response.direction.value if rule.response else None),
            response_type=(rule.response.response_type.value if rule.response else None),
            evidence=rule.execution.evidence,
            order=rule.execution.order,
            fill=rule.execution.fill,
            gap_policy=rule.execution.gap_policy,
            level=resolved_levels.get(rule.location.value),
            band_lower=(resolved_bands.get(rule.location.value) or (None, None))[0],
            band_upper=(resolved_bands.get(rule.location.value) or (None, None))[1],
        )
        for rule in exits
    )


def exit_plan_payload(rules: Sequence[ExitPlanRule]) -> tuple[dict[str, object], ...]:
    return tuple({
        "rule_id": rule.rule_id, "location": rule.location, "mode": rule.mode,
        "required": rule.required, "trigger": rule.trigger,
        "score_direction": rule.score_direction, "score_margin": rule.score_margin,
        "response_direction": rule.response_direction, "response_type": rule.response_type,
        "evidence": rule.evidence, "order": rule.order, "fill": rule.fill,
        "gap_policy": rule.gap_policy, "level": rule.level,
        "band_lower": rule.band_lower, "band_upper": rule.band_upper,
        "interaction_id": rule.interaction_id,
    } for rule in rules)


@dataclass(frozen=True, slots=True)
class ExitPlanEvaluation:
    rule: ExitPlanRule
    triggered: bool
    reason: str


def evaluate_exit_plan_rule(
    rule: ExitPlanRule,
    *,
    position_side: OrderSide,
    bars: Sequence[MarketBar],
    entry_bar_index: int,
    decision_scores: tuple[float | None, float | None] | None = None,
) -> ExitPlanEvaluation:
    """Evaluate one frozen rule using only bars after the entry bar."""
    if rule.level is None:
        return ExitPlanEvaluation(rule, False, "exit_level_unavailable")
    candidates = tuple(bars[entry_bar_index + 1:])
    if not candidates:
        return ExitPlanEvaluation(rule, False, "pending")
    target = rule.level
    if rule.evidence == "completed_close" or rule.trigger == "completed_close":
        touched = any(
            bar.close >= target if position_side is OrderSide.BUY else bar.close <= target
            for bar in candidates
        )
    elif position_side is OrderSide.BUY:
        touched = any(bar.high >= target for bar in candidates)
    else:
        touched = any(bar.low <= target for bar in candidates)
    if not touched:
        return ExitPlanEvaluation(rule, False, "pending")
    if rule.mode == "fixed_target" or rule.response_direction is None:
        return ExitPlanEvaluation(rule, True, "exit_triggered")
    lower = rule.band_lower if rule.band_lower is not None else target
    upper = rule.band_upper if rule.band_upper is not None else target
    spec = ResponseSpec(
        Direction(rule.response_direction),
        ResponseType(rule.response_type or "rejection"),
        lower,
        upper,
    )
    response = evaluate_response_window(
        candidates,
        to_candle=lambda bar: ResponseCandle(bar.open, bar.high, bar.low, bar.close),
        spec_for=lambda _bar: spec,
        armed=False,
    )
    if response.status is ResponseOutcomeStatus.CONFIRMED:
        if rule.score_direction is not None and rule.score_margin is not None:
            if decision_scores is None or any(value is None for value in decision_scores):
                return ExitPlanEvaluation(rule, False, "exit_context_scores_unavailable")
            own, opposing = (
                decision_scores if rule.score_direction == "long" else decision_scores[::-1]
            )
            assert own is not None and opposing is not None
            if own - opposing < rule.score_margin:
                return ExitPlanEvaluation(rule, False, "exit_score_mismatch")
        return ExitPlanEvaluation(rule, True, "exit_triggered")
    return ExitPlanEvaluation(rule, False, "exit_response_pending")


__all__ = [
    "ExitPlanEvaluation", "evaluate_exit_plan_rule", "exit_plan_from_payload",
    "exit_plan_from_template", "exit_plan_payload",
]
