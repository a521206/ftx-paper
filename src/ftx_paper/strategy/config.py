"""Versioned, runtime-independent production strategy configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
import json
from hashlib import sha256
from typing import TYPE_CHECKING

from ftx_paper.contracts import OrderSide
from ftx_paper.config import NIFTY_LOT_SIZE

if TYPE_CHECKING:
    from ftx_paper.core.location_engine import TransitionPattern

STRATEGY_NAME = "ftx-paper-research"
STRATEGY_VERSION = "0.3.0-pipeline-research-aligned"

MORNING_ENTRY_MINUTES = (0, 150)
AFTERNOON_ENTRY_MINUTES = (255, 300)
ENTRY_COOLDOWN_BARS = 15
POST_EXIT_COOLDOWN_BARS = 30
TRAIL_ACTIVATE_BP = 20.0
TRAIL_DISTANCE_BP = 20.0
LOT_SIZE = NIFTY_LOT_SIZE
class Session(StrEnum):
    MORNING = "morning"
    AFTERNOON = "afternoon"
    OUTSIDE = "outside"


class ExitMode(StrEnum):
    SIGNAL = "signal"
    TRAIL = "trail"


@dataclass(frozen=True, slots=True)
class CellPolicyConfig:
    """Fixed production assignment for one canonical composite cell."""

    cell: str
    direction: OrderSide
    exit_mode: ExitMode = ExitMode.SIGNAL
    stability: float = 1.0
    transition_patterns: tuple[TransitionPattern, ...] = ()
    hypothesis: str = "trend_continuation"

    def __post_init__(self) -> None:
        if not 0 < self.stability <= 1:
            raise ValueError("stability must be greater than zero and at most one")


MORNING_CELL_POLICIES = (
    CellPolicyConfig("new_low", OrderSide.BUY, ExitMode.SIGNAL, hypothesis="mean_reversion"),
    CellPolicyConfig("vwap_zone+new_high+prior_day_high", OrderSide.BUY, ExitMode.TRAIL),
    CellPolicyConfig("vwap_zone+or_high+prior_day_low", OrderSide.BUY, ExitMode.TRAIL),
    CellPolicyConfig("vwap_zone+or_low+prior_day_high", OrderSide.SELL, ExitMode.TRAIL),
    CellPolicyConfig("vwap_zone+session_high+or_high+prior_day_high", OrderSide.SELL, ExitMode.TRAIL),
)
AFTERNOON_CELL_POLICIES = (
    CellPolicyConfig("vwap_zone+prior_day_high", OrderSide.SELL, ExitMode.TRAIL, hypothesis="mean_reversion"),
    CellPolicyConfig("vwap_zone+or_low+prior_day_high", OrderSide.SELL, ExitMode.TRAIL),
    CellPolicyConfig("vwap_zone+prior_day_low", OrderSide.BUY, ExitMode.TRAIL),
)


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Only values needed by the live strategy; no repository settings."""

    name: str = STRATEGY_NAME
    version: str = STRATEGY_VERSION
    morning_entry_minutes: tuple[int, int] = MORNING_ENTRY_MINUTES
    afternoon_entry_minutes: tuple[int, int] = AFTERNOON_ENTRY_MINUTES
    entry_cooldown_bars: int = ENTRY_COOLDOWN_BARS
    post_exit_cooldown_bars: int = POST_EXIT_COOLDOWN_BARS

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def policy_manifest(self) -> tuple[dict[str, object], ...]:
        """Return the stable, JSON-safe policy payload used for provenance."""
        return tuple(
            {
                "session": session,
                "cell": item.cell,
                "direction": item.direction.value.lower(),
                "exit_mode": item.exit_mode.value,
                "stability": item.stability,
                "hypothesis": item.hypothesis,
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
    "LOT_SIZE",
    "ENTRY_COOLDOWN_BARS",
    "POST_EXIT_COOLDOWN_BARS",
    "TRAIL_ACTIVATE_BP",
    "TRAIL_DISTANCE_BP",
    "MORNING_ENTRY_MINUTES",
    "STRATEGY_NAME",
    "STRATEGY_VERSION",
    "DEFAULT_CONFIG",
    "StrategyConfig",
    "CellPolicyConfig",
    "MORNING_CELL_POLICIES",
    "AFTERNOON_CELL_POLICIES",
    "Session",
    "ExitMode",
]
