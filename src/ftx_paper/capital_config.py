"""Explicit capital configuration for the standalone FTX paper runtime."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
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

    @classmethod
    def from_dict(cls, values: Mapping[str, object]) -> "ResearchCapitalProfile":
        """Restore a normalized profile from a strategy snapshot."""
        def text(name: str, default: str | None = None) -> str:
            value = values.get(name, default)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
            return value

        def number(name: str, *, default: float | None = None) -> float:
            value = values.get(name, default)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be numeric")
            return float(value)

        def integer(name: str, *, default: int | None = None) -> int:
            value = values.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer")
            return value

        raw_drawdown = values.get("drawdown_policy", {})
        if not isinstance(raw_drawdown, Mapping):
            raise ValueError("drawdown_policy must be an object")
        raw_tiers = raw_drawdown.get("tiers", ())
        if not isinstance(raw_tiers, Sequence) or isinstance(raw_tiers, str):
            raise ValueError("drawdown_policy.tiers must be an array")
        tiers = []
        for item in raw_tiers:
            if not isinstance(item, Sequence) or isinstance(item, str) or len(item) != 2:
                raise ValueError("drawdown policy tiers must contain [threshold, scale]")
            if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in item):
                raise ValueError("drawdown policy tier values must be numeric")
            tiers.append((float(item[0]), float(item[1])))
        raw_limits = values.get("vehicle_limits", {})
        if not isinstance(raw_limits, Mapping):
            raise ValueError("vehicle_limits must be an object")
        limits = []
        for name, limit in raw_limits.items():
            if not isinstance(name, str) or not isinstance(limit, Mapping):
                raise ValueError("vehicle_limits entries must be named objects")
            max_lots = limit.get("max_lots")
            margin = limit.get("margin_per_lot")
            if isinstance(max_lots, bool) or not isinstance(max_lots, int):
                raise ValueError(f"vehicle limit {name} max_lots must be an integer")
            if isinstance(margin, bool) or not isinstance(margin, (int, float)):
                raise ValueError(f"vehicle limit {name} margin_per_lot must be numeric")
            limits.append((name, VehicleLimits(max_lots, float(margin))))
        raw_stability = values.get("stability_policy", {})
        if not isinstance(raw_stability, Mapping):
            raise ValueError("stability_policy must be an object")
        if any(not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, (int, float))
               for key, value in raw_stability.items()):
            raise ValueError("stability_policy must map strings to numeric values")
        raw_sessions = values.get("selected_sessions", ())
        raw_vehicles = values.get("enabled_vehicles", ())
        if (isinstance(raw_sessions, str) or not isinstance(raw_sessions, Sequence)
                or any(not isinstance(item, str) for item in raw_sessions)):
            raise ValueError("selected_sessions must be an array of strings")
        if (isinstance(raw_vehicles, str) or not isinstance(raw_vehicles, Sequence)
                or any(not isinstance(item, str) for item in raw_vehicles)):
            raise ValueError("enabled_vehicles must be an array of strings")
        raw_strategy_version = values.get("strategy_version", "")
        if not isinstance(raw_strategy_version, str):
            raise ValueError("strategy_version must be a string")
        return cls(
            name=text("name", "paper-research"),
            schema_version=integer("schema_version", default=1),
            strategy_version=raw_strategy_version,
            initial_capital=number("initial_capital"),
            max_daily_loss=number("max_daily_loss"),
            max_drawdown_fraction=number("max_drawdown_fraction", default=0.15),
            risk_per_trade=number("risk_per_trade"),
            max_lots=integer("max_lots"),
            max_net_directional_lots=number("max_net_directional_lots"),
            drawdown_policy=DrawdownPolicy(tuple(tiers)),
            selected_sessions=tuple(raw_sessions),
            enabled_vehicles=tuple(raw_vehicles),
            vehicle_limits=tuple(limits),
            stability_policy=tuple((str(key), float(value)) for key, value in raw_stability.items()),
            candidate_id=text("candidate_id", "baseline"),
        )

    def vehicle_limit(self, vehicle: str) -> VehicleLimits:
        values = dict(self.vehicle_limits)
        return values[str(vehicle).lower()]

    def stability_for(self, key: str) -> float:
        return dict(self.stability_policy).get(str(key), 1.0)

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "schema_version": self.schema_version,
            "strategy_version": self.strategy_version,
            "candidate_id": self.candidate_id,
            "initial_capital": self.initial_capital,
            "max_daily_loss": self.max_daily_loss,
            "max_drawdown_fraction": self.max_drawdown_fraction,
            "risk_per_trade": self.risk_per_trade,
            "max_lots": self.max_lots,
            "max_net_directional_lots": self.max_net_directional_lots,
            "drawdown_policy": {"tiers": [list(item) for item in self.drawdown_policy.tiers]},
            "selected_sessions": list(self.selected_sessions),
            "enabled_vehicles": list(self.enabled_vehicles),
            "vehicle_limits": {name: {"max_lots": limit.max_lots, "margin_per_lot": limit.margin_per_lot} for name, limit in self.vehicle_limits},
            "stability_policy": dict(self.stability_policy),
        }


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
