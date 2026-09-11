"""Versioned, runtime-independent production strategy configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
import json
from hashlib import sha256
from typing import TYPE_CHECKING

from ftx_paper.contracts import OrderSide

if TYPE_CHECKING:
    from ftx_paper.core.location_engine import TransitionPattern

STRATEGY_NAME = "ftx-paper-production"
STRATEGY_VERSION = "0.2.0-live-composition"

MORNING_ENTRY_MINUTES = (60, 120)
AFTERNOON_ENTRY_MINUTES = (255, 300)
COOLDOWN_MINUTES = 30


class Session(StrEnum):
    MORNING = "morning"
    AFTERNOON = "afternoon"
    OUTSIDE = "outside"


@dataclass(frozen=True, slots=True)
class CellPolicyConfig:
    """Fixed production assignment for one canonical composite cell."""

    cell: str
    direction: OrderSide
    transition_patterns: tuple[TransitionPattern, ...] = ()


MORNING_CELL_POLICIES = (
    CellPolicyConfig("session_low+or_low", OrderSide.BUY),
    CellPolicyConfig("vwap_zone+session_low+or_low", OrderSide.BUY),
    CellPolicyConfig("session_high+or_high", OrderSide.SELL),
    CellPolicyConfig("vwap_zone+or_low", OrderSide.SELL),
)
AFTERNOON_CELL_POLICIES = (
    CellPolicyConfig("session_high+or_high", OrderSide.BUY),
    CellPolicyConfig("or_low", OrderSide.BUY),
    CellPolicyConfig("vwap_zone+prior_day_high", OrderSide.SELL),
)


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Only values needed by the live strategy; no repository settings."""

    name: str = STRATEGY_NAME
    version: str = STRATEGY_VERSION
    morning_entry_minutes: tuple[int, int] = MORNING_ENTRY_MINUTES
    afternoon_entry_minutes: tuple[int, int] = AFTERNOON_ENTRY_MINUTES
    cooldown_minutes: int = COOLDOWN_MINUTES

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def policy_manifest(self) -> tuple[dict[str, object], ...]:
        """Return the stable, JSON-safe policy payload used for provenance."""
        return tuple(
            {
                "session": session,
                "cell": item.cell,
                "direction": item.direction.value.lower(),
            }
            for session, policies in (
                ("morning", MORNING_CELL_POLICIES),
                ("afternoon", AFTERNOON_CELL_POLICIES),
            )
            for item in policies
        )

    @property
    def config_hash(self) -> str:
        payload = json.dumps(
            {"config": self.as_dict(), "policy_manifest": self.policy_manifest()},
            sort_keys=True,
            separators=(",", ":"),
        )
        return sha256(payload.encode("utf-8")).hexdigest()


DEFAULT_CONFIG = StrategyConfig()

__all__ = [
    "AFTERNOON_ENTRY_MINUTES",
    "COOLDOWN_MINUTES",
    "MORNING_ENTRY_MINUTES",
    "STRATEGY_NAME",
    "STRATEGY_VERSION",
    "DEFAULT_CONFIG",
    "StrategyConfig",
    "CellPolicyConfig",
    "MORNING_CELL_POLICIES",
    "AFTERNOON_CELL_POLICIES",
    "Session",
]
