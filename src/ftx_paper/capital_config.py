"""Explicit capital configuration for the standalone FTX paper runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
@dataclass(frozen=True, slots=True)
class DrawdownPolicy:
    """Paper-owned position scaling policy, independent of canonical FTX."""

    tiers: tuple[tuple[float, float], ...] = (
        (-100_000.0, 0.50),
        (-200_000.0, 0.25),
    )

    def __post_init__(self) -> None:
        normalized = tuple(sorted((float(level), float(scale)) for level, scale in self.tiers))
        if any(scale <= 0 or scale > 1 for _, scale in normalized):
            raise ValueError("drawdown scales must be greater than zero and at most one")
        object.__setattr__(self, "tiers", normalized)


@dataclass(frozen=True, slots=True)
class VehicleLimits:
    """Immutable Paper vehicle sizing limits."""

    max_lots: int = 3
    margin_per_lot: float = 175_000.0

    def __post_init__(self) -> None:
        if self.max_lots <= 0 or self.margin_per_lot < 0:
            raise ValueError("invalid vehicle limits")


@dataclass(frozen=True, slots=True)
class ResearchCapitalProfile:
    """Complete Paper research profile.

    This intentionally has the same public name as the canonical profile but
    is a separate implementation and type under the ``ftx_paper`` namespace.
    """

    name: str = "paper-research"
    schema_version: int = 1
    strategy_version: str = ""
    initial_capital: float = 2_500_000.0
    max_daily_loss: float = 0.05
    max_drawdown_fraction: float = 0.15
    risk_per_trade: float = 0.01
    max_lots: int = 3
    max_net_directional_lots: float = 8.0
    drawdown_policy: DrawdownPolicy = field(default_factory=DrawdownPolicy)
    selected_sessions: tuple[str, ...] = ("morning", "afternoon")
    enabled_vehicles: tuple[str, ...] = ("futures", "synthetic")
    vehicle_limits: tuple[tuple[str, VehicleLimits], ...] = (
        ("futures", VehicleLimits()),
        ("synthetic", VehicleLimits()),
    )
    stability_policy: tuple[tuple[str, float], ...] = ()
    candidate_id: str = "baseline"

    def __post_init__(self) -> None:
        if self.schema_version < 1 or not self.name or self.initial_capital <= 0:
            raise ValueError("invalid Paper capital profile identity or capital")
        if not 0 < self.max_daily_loss <= 1 or not 0 < self.max_drawdown_fraction <= 1 or not 0 < self.risk_per_trade <= 1:
            raise ValueError("Paper risk fractions must be between zero and one")
        if self.max_lots <= 0 or self.max_net_directional_lots <= 0:
            raise ValueError("Paper capital limits must be positive")
        sessions = tuple(dict.fromkeys(str(value).lower() for value in self.selected_sessions))
        vehicles = tuple(dict.fromkeys(str(value).lower() for value in self.enabled_vehicles))
        if not sessions or any(value not in {"morning", "afternoon"} for value in sessions):
            raise ValueError("selected_sessions must contain morning and/or afternoon")
        if not vehicles or "futures" not in vehicles or any(value not in {"futures", "synthetic"} for value in vehicles):
            raise ValueError("enabled_vehicles must include futures")
        limits = tuple(sorted((str(name).lower(), value) for name, value in self.vehicle_limits))
        if {name for name, _ in limits} != set(vehicles):
            raise ValueError("vehicle_limits must match enabled_vehicles")
        stability = tuple(sorted((str(key), float(value)) for key, value in self.stability_policy))
        if any(value <= 0 or value > 1 for _, value in stability):
            raise ValueError("stability values must be greater than zero and at most one")
        if not self.candidate_id:
            raise ValueError("candidate_id must be non-empty")
        object.__setattr__(self, "selected_sessions", sessions)
        object.__setattr__(self, "enabled_vehicles", vehicles)
        object.__setattr__(self, "vehicle_limits", limits)
        object.__setattr__(self, "stability_policy", stability)

    def vehicle_limit(self, vehicle: str) -> VehicleLimits:
        values = dict(self.vehicle_limits)
        return values[str(vehicle).lower()]

    def stability_for(self, key: str) -> float:
        return dict(self.stability_policy).get(str(key), 1.0)

RESEARCH_CAPITAL_PROFILE = ResearchCapitalProfile(
    name="ftx-paper-research",
    strategy_version="ftx-paper-v1",
    stability_policy=(
        ("morning:session_low+or_low", 1.0),
        ("morning:vwap_zone+session_low+or_low", 0.5),
        ("morning:vwap_zone+or_low", 0.5),
        ("morning:session_high+or_high", 0.8),
        ("afternoon:session_high+or_high", 0.8),
        ("afternoon:or_low", 0.8),
        ("afternoon:vwap_zone+prior_day_high", 1.0),
    ),
)

__all__ = [
    "DrawdownPolicy", "ResearchCapitalProfile", "RESEARCH_CAPITAL_PROFILE",
    "VehicleLimits",
]
