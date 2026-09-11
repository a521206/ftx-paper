from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RiskConfig:
    risk_fraction: float = 0.01
    max_drawdown_fraction: float = 0.10
    lot_size: int = 1
    max_quantity: int = 100


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    quantity: int
    reason: str
    risk_amount: float = 0.0
    score_multiplier: float = 1.0


class RiskSizer:
    """Pure capital, drawdown, and stop-distance guard for order sizing."""

    def __init__(self, config: RiskConfig = RiskConfig()) -> None:
        if not 0 < config.risk_fraction <= 1 or not 0 < config.max_drawdown_fraction <= 1:
            raise ValueError("risk fractions must be between zero and one")
        if config.lot_size < 1 or config.max_quantity < config.lot_size:
            raise ValueError("invalid quantity limits")
        self.config = config

    def size(
        self, *, capital: float, equity: float, peak_equity: float,
        entry: float, stop: float, score: int | None = None,
    ) -> RiskDecision:
        if capital <= 0 or equity <= 0 or peak_equity <= 0:
            return RiskDecision(False, 0, "non_positive_capital")
        if equity < peak_equity * (1 - self.config.max_drawdown_fraction):
            return RiskDecision(False, 0, "drawdown_limit")
        distance = abs(entry - stop)
        if distance <= 0:
            return RiskDecision(False, 0, "invalid_stop_distance")
        risk_amount = capital * self.config.risk_fraction
        raw_quantity = int(risk_amount // distance)
        multiplier = 1.0 if score is None else (1.5 if score >= 8 else 1.0 if score >= 5 else 0.5)
        risk_ceiling = min(raw_quantity, self.config.max_quantity)
        quantity = min(int(round(risk_ceiling * multiplier)), risk_ceiling)
        quantity -= quantity % self.config.lot_size
        if quantity < self.config.lot_size:
            return RiskDecision(False, 0, "insufficient_risk_budget", risk_amount, multiplier)
        return RiskDecision(True, quantity, "approved", risk_amount, multiplier)
