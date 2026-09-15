"""Immutable Paper capital runtime context."""

from __future__ import annotations

from dataclasses import dataclass

from ftx_paper.capital_config import ResearchCapitalProfile


@dataclass(frozen=True, slots=True)
class CapitalRuntimeContext:
    """Normalized immutable policy supplied to Paper capital components."""

    profile: ResearchCapitalProfile
    environment: str = "replay"

    @property
    def risk_per_trade(self) -> float:
        return self.profile.risk_per_trade

    @property
    def max_lots(self) -> int:
        return self.profile.max_lots

    @property
    def max_net_directional_lots(self) -> float:
        return self.profile.max_net_directional_lots

    @property
    def selected_sessions(self) -> tuple[str, ...]:
        return self.profile.selected_sessions

    @property
    def enabled_vehicles(self) -> tuple[str, ...]:
        return self.profile.enabled_vehicles

    def to_dict(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "profile": self.profile.to_dict(),
        }


__all__ = ["CapitalRuntimeContext"]
