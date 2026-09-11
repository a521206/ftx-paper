from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence

from ftx_paper.contracts import MarketBar, SyntheticPremiumPair
from .adaptive_stop import adaptive_stop_bp


@dataclass(frozen=True, slots=True)
class RiskConfig:
    risk_fraction: float = 0.02
    max_drawdown_fraction: float = 0.15
    lot_size: int = 1
    max_quantity: int = 10
    margin_per_lot: float = 175_000.0
    margin_utilization_cap: float = 0.80
    low_vix_threshold: float = 13.0
    low_vix_multiplier: float = 0.5


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    quantity: int
    reason: str
    risk_amount: float = 0.0
    score_multiplier: float = 1.0
    stop_bp: float | None = None
    risk_budget: float = 0.0
    vehicle: str = "futures"


class RiskSizer:
    """Pure capital, drawdown, and stop-distance guard for order sizing."""

    def __init__(self, config: RiskConfig = RiskConfig()) -> None:
        if not 0 < config.risk_fraction <= 1 or not 0 < config.max_drawdown_fraction <= 1:
            raise ValueError("risk fractions must be between zero and one")
        if config.lot_size < 1 or config.max_quantity < config.lot_size:
            raise ValueError("invalid quantity limits")
        if config.margin_per_lot < 0 or not 0 < config.margin_utilization_cap <= 1:
            raise ValueError("invalid margin configuration")
        self.config = config

    def size(
        self, *, capital: float, equity: float, peak_equity: float,
        entry: float, stop: float | None = None, score: int | None = None,
        bars_before: Sequence[MarketBar] | None = None, vix: float | None = None,
        is_expiry_day: bool = False, vehicle: str = "futures",
        synthetic_premiums: SyntheticPremiumPair | None = None,
    ) -> RiskDecision:
        if capital <= 0 or equity <= 0 or peak_equity <= 0:
            return RiskDecision(False, 0, "non_positive_capital")
        if equity < peak_equity * (1 - self.config.max_drawdown_fraction):
            return RiskDecision(False, 0, "drawdown_limit")
        normalized_vehicle = str(vehicle).lower()
        if normalized_vehicle not in {"futures", "synthetic"}:
            return RiskDecision(False, 0, "unsupported_vehicle", vehicle=normalized_vehicle)
        if normalized_vehicle == "synthetic" and synthetic_premiums is None:
            return RiskDecision(False, 0, "missing_synthetic_premium", vehicle=normalized_vehicle)
        stop_bp = None
        if bars_before is not None and vix is not None:
            stop_bp = adaptive_stop_bp(bars_before, vix, is_expiry_day=is_expiry_day)
            distance = abs(entry) * stop_bp / 10000.0
        elif stop is not None:
            distance = abs(entry - stop)
        else:
            return RiskDecision(False, 0, "invalid_stop_distance", vehicle=normalized_vehicle)
        if distance <= 0:
            return RiskDecision(False, 0, "invalid_stop_distance", vehicle=normalized_vehicle)
        risk_budget = min(float(capital), float(equity)) * self.config.risk_fraction
        raw_quantity = int(risk_budget // distance)
        score_multiplier = 1.0 if score is None else (1.5 if score >= 8 else 1.0 if score >= 5 else 0.5)
        vix_multiplier = self.config.low_vix_multiplier if vix is not None and vix < self.config.low_vix_threshold else 1.0
        multiplier = score_multiplier * vix_multiplier
        available = min(float(capital), float(equity))
        margin_lots = int(available * self.config.margin_utilization_cap // self.config.margin_per_lot) if self.config.margin_per_lot else self.config.max_quantity
        risk_ceiling = min(raw_quantity, self.config.max_quantity, margin_lots)
        quantity = min(int(round(risk_ceiling * multiplier)), risk_ceiling)
        quantity -= quantity % self.config.lot_size
        if quantity < self.config.lot_size:
            return RiskDecision(False, 0, "insufficient_risk_budget", risk_budget, multiplier, stop_bp, risk_budget, normalized_vehicle)
        return RiskDecision(True, quantity, "approved", risk_budget, multiplier, stop_bp, risk_budget, normalized_vehicle)
