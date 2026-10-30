"""Canonical entry fill and initial-risk calculations."""

from __future__ import annotations

from dataclasses import dataclass

from ftx_paper.contracts import ExitPlanRule


MAX_INITIAL_RISK_BP = 20.0


@dataclass(frozen=True, slots=True)
class EntryFillDecision:
    fill_price: float
    hard_stop: float | None
    hard_stop_source: str
    quantity: float
    risk_metrics: dict[str, float]


@dataclass(frozen=True, slots=True)
class ExitPlanDecision:
    accepted: tuple[ExitPlanRule, ...]
    rejected: tuple[ExitPlanRule, ...]
    required_rejections: tuple[ExitPlanRule, ...]


def entry_fill_decision(*, direction: str, trigger_price: float, requested_fill_price: float,
                        bar_open: float, failure_boundary: float) -> EntryFillDecision:
    fill_price = max(requested_fill_price, bar_open) if direction == "long" else min(requested_fill_price, bar_open)
    hard_stop, source = protective_stop_price(direction, fill_price, failure_boundary)
    quantity, metrics = risk_adjustment(trigger_price=trigger_price, fill_price=fill_price, hard_stop=hard_stop)
    return EntryFillDecision(fill_price, hard_stop, source, quantity, metrics)


def protective_stop_price(direction: str, entry_price: float, failure_boundary: float) -> tuple[float | None, str]:
    if not ((failure_boundary < entry_price if direction == "long" else failure_boundary > entry_price)
            and abs(entry_price - failure_boundary) > 0.0):
        return None, "trail_distance_fallback"
    return float(failure_boundary), "failure_boundary"


def risk_adjustment(*, trigger_price: float, fill_price: float,
                    hard_stop: float | None) -> tuple[float, dict[str, float]]:
    if hard_stop is None or trigger_price <= 0 or fill_price <= 0:
        return 0.0, {}
    trigger_risk_bp = abs(hard_stop - trigger_price) / trigger_price * 10000.0
    actual_risk_bp = abs(hard_stop - fill_price) / fill_price * 10000.0
    if actual_risk_bp == 0.0:
        return 0.0, {"trigger_risk_bp": trigger_risk_bp, "actual_risk_bp": actual_risk_bp,
                      "risk_budget_bp": MAX_INITIAL_RISK_BP, "risk_adjusted_quantity": 0.0,
                      "effective_risk_bp": 0.0}
    quantity = min(1.0, MAX_INITIAL_RISK_BP / actual_risk_bp)
    return quantity, {"trigger_risk_bp": trigger_risk_bp, "actual_risk_bp": actual_risk_bp,
                      "risk_expansion": actual_risk_bp / trigger_risk_bp if trigger_risk_bp else float("inf"),
                      "risk_budget_bp": MAX_INITIAL_RISK_BP, "risk_adjusted_quantity": quantity,
                      "effective_risk_bp": actual_risk_bp * quantity}


def validate_exit_plan(
    rules: tuple[ExitPlanRule, ...], *, direction: str, fill_price: float,
) -> ExitPlanDecision:
    """Apply canonical direction checks after the actual entry fill."""
    rejected = tuple(
        rule for rule in rules
        if rule.mode != "conditional" and rule.level is not None and (
            (direction == "long" and rule.level <= fill_price)
            or (direction == "short" and rule.level >= fill_price)
        )
    )
    accepted = tuple(rule for rule in rules if rule not in rejected)
    return ExitPlanDecision(
        accepted=accepted,
        rejected=rejected,
        required_rejections=tuple(rule for rule in rejected if rule.required),
    )


__all__ = [
    "EntryFillDecision", "ExitPlanDecision", "entry_fill_decision",
    "protective_stop_price", "risk_adjustment", "validate_exit_plan",
]
