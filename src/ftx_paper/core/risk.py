from __future__ import annotations

from dataclasses import dataclass, replace
from collections.abc import Mapping, Sequence

from ftx_paper.contracts import MarketBar, SyntheticPremiumPair
from .adaptive_stop import adaptive_stop_bp
from ftx_paper.config import NIFTY_LOT_SIZE


@dataclass(frozen=True, slots=True)
class VehicleRiskLimits:
    max_quantity: int = 10
    margin_per_lot: float = 175_000.0

    def __post_init__(self) -> None:
        if self.max_quantity < 1 or self.margin_per_lot < 0:
            raise ValueError("invalid vehicle risk limits")


@dataclass(frozen=True, slots=True)
class RiskConfig:
    risk_fraction: float = 0.02
    max_drawdown_fraction: float = 0.15
    lot_size: int = 1  # sizing quantity is expressed in whole lots
    contract_lot_size: int = NIFTY_LOT_SIZE
    max_quantity: int = 10
    margin_per_lot: float = 175_000.0
    # The canonical FTX capital bridge reserves up to 80% of available
    margin_utilization_cap: float = 0.80
    low_vix_threshold: float = 13.0
    # Canonical capital sizing applies the score multiplier and policy
    # stability, but does not apply an additional VIX multiplier.
    low_vix_multiplier: float = 1.0
    drawdown_tiers: tuple[tuple[float, float], ...] = (
        (-100_000.0, 0.50),
        (-200_000.0, 0.25),
    )


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
    available_capital: float = 0.0
    open_margin_used: float = 0.0
    raw_quantity: int = 0
    margin_lots: int = 0
    risk_ceiling: int = 0
    drawdown_multiplier: float = 1.0


class RiskEngine:
    """Pure capital, drawdown, and stop-distance guard for order sizing."""

    def __init__(
        self,
        config: RiskConfig = RiskConfig(),
        vehicle_limits: Mapping[str, VehicleRiskLimits] | None = None,
        context=None,
    ) -> None:
        if context is not None:
            profile = context.profile
            config = replace(
                config,
                risk_fraction=profile.risk_per_trade,
                max_quantity=profile.max_lots,
                max_drawdown_fraction=profile.max_drawdown_fraction,
            )
            profile_limits = {
                name: VehicleRiskLimits(limit.max_lots, limit.margin_per_lot)
                for name, limit in profile.vehicle_limits
            }
            vehicle_limits = {**(vehicle_limits or {}), **profile_limits}
        if not 0 < config.risk_fraction <= 1 or not 0 < config.max_drawdown_fraction <= 1:
            raise ValueError("risk fractions must be between zero and one")
        if config.lot_size < 1 or config.max_quantity < config.lot_size:
            raise ValueError("invalid quantity limits")
        if config.margin_per_lot < 0 or not 0 < config.margin_utilization_cap <= 1:
            raise ValueError("invalid margin configuration")
        if config.contract_lot_size < 1:
            raise ValueError("invalid contract lot size")
        self.config = config
        self.context = context
        default_limits = VehicleRiskLimits(config.max_quantity, config.margin_per_lot)
        self.vehicle_limits = {
            "futures": default_limits,
            "synthetic": default_limits,
            **{str(name).lower(): limits for name, limits in (vehicle_limits or {}).items()},
        }

    def size(
        self, *, capital: float, equity: float, peak_equity: float,
        entry: float, stop: float | None = None, score: int | None = None,
        bars_before: Sequence[MarketBar] | None = None, vix: float | None = None,
        is_expiry_day: bool = False, vehicle: str = "futures",
        synthetic_premiums: SyntheticPremiumPair | None = None,
        open_margin_used: float = 0.0,
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
        limits = self.vehicle_limits[normalized_vehicle]
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
        # Canonical PortfolioState exposes capital less reserved margin. Equity
        # is used for drawdown checks, not as a second available-capital cap.
        # PortfolioState is the equity and reservation owner. Use the same
        # available-capital definition after realized PnL changes equity.
        available = max(0.0, float(equity) - float(open_margin_used))
        if available <= 0:
            return RiskDecision(False, 0, "capital_exhausted", vehicle=normalized_vehicle)
        risk_budget = available * self.config.risk_fraction
        raw_quantity = int(risk_budget // (distance * self.config.contract_lot_size))
        score_multiplier = 1.0 if score is None else (1.5 if score >= 8 else 1.0 if score >= 5 else 0.5)
        vix_multiplier = self.config.low_vix_multiplier if vix is not None and vix < self.config.low_vix_threshold else 1.0
        # Legacy callers retain the historical score/drawdown scaling. When a
        # runtime context is present, those are downstream SizingPipeline
        # stages and RiskEngine returns only the permission ceiling.
        multiplier = 1.0 if self.context is not None else score_multiplier * vix_multiplier
        margin_lots = int(available * self.config.margin_utilization_cap // limits.margin_per_lot) if limits.margin_per_lot else limits.max_quantity
        risk_ceiling = min(raw_quantity, limits.max_quantity, margin_lots)
        cumulative_pnl = float(equity) - float(capital)
        peak_pnl = float(peak_equity) - float(capital)
        drawdown = cumulative_pnl - peak_pnl
        drawdown_multiplier = 1.0
        # More negative thresholds are more severe and must win first.
        for threshold, scale in sorted(self.config.drawdown_tiers):
            if drawdown <= threshold:
                drawdown_multiplier = scale
                break
        if self.context is None:
            multiplier *= drawdown_multiplier
        else:
            drawdown_multiplier = 1.0
        # Match the canonical allocator's nearest-lot score scaling before
        # policy stability is applied.  Stability is a separate downstream
        # stage; its integer normalization remains downward-only.
        quantity = min(int(round(risk_ceiling * multiplier)), risk_ceiling)
        quantity -= quantity % self.config.lot_size
        if quantity < self.config.lot_size:
            return RiskDecision(
                False, 0, "insufficient_risk_budget", risk_budget, multiplier, stop_bp,
                risk_budget, normalized_vehicle, available, float(open_margin_used),
                raw_quantity, margin_lots, risk_ceiling, drawdown_multiplier,
            )
        return RiskDecision(
            True, quantity, "approved", risk_budget, multiplier, stop_bp, risk_budget,
            normalized_vehicle, available, float(open_margin_used), raw_quantity,
            margin_lots, risk_ceiling, drawdown_multiplier,
        )


RiskAssessment = RiskDecision

__all__ = [
    "RiskAssessment", "RiskConfig", "RiskDecision", "RiskEngine",
    "VehicleRiskLimits",
]
