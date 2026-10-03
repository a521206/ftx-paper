"""Versioned, runtime-independent configuration for the live strategy."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
import json
from hashlib import sha256
from typing import TYPE_CHECKING

from ftx_paper.contracts import OrderSide
from ftx_paper.core.location_engine import Cell, Location

if TYPE_CHECKING:
    from ftx_paper.core.location_engine import TransitionPattern

STRATEGY_NAME = "ftx-paper-research"
STRATEGY_VERSION = "0.3.0-pipeline-research-aligned"
MORNING_ENTRY_MINUTES = (0, 150)
AFTERNOON_ENTRY_MINUTES = (255, 300)
ENTRY_COOLDOWN_BARS = 0
POST_EXIT_COOLDOWN_BARS = 2
TRAIL_ACTIVATE_BP = 20.0
TRAIL_DISTANCE_BP = 20.0


class Session(StrEnum):
    MORNING = "morning"
    AFTERNOON = "afternoon"
    OUTSIDE = "outside"


class ExitMode(StrEnum):
    SIGNAL = "signal"
    TRAIL = "trail"


class TradeHypothesis(StrEnum):
    MEAN_REVERSION = "mean_reversion"
    TREND_CONTINUATION = "trend_continuation"


@dataclass(frozen=True, slots=True)
class CellPolicyConfig:
    """Fixed production assignment for one canonical composite cell."""

    cell: Cell
    direction: OrderSide
    exit_mode: ExitMode = ExitMode.SIGNAL
    stability: float = 1.0
    transition_patterns: tuple[TransitionPattern, ...] = ()
    hypothesis: TradeHypothesis = TradeHypothesis.TREND_CONTINUATION

    def __post_init__(self) -> None:
        if not isinstance(self.cell, Cell):
            raise TypeError("cell must be a Cell")
        if not 0 < self.stability <= 1:
            raise ValueError("stability must be greater than zero and at most one")

    def as_manifest(self, session: Session) -> dict[str, object]:
        return {
            "session": session,
            "cell": self.cell.name,
            "direction": self.direction.value.lower(),
            "exit_mode": self.exit_mode.value,
            "stability": self.stability,
            "hypothesis": self.hypothesis.value,
        }


MORNING_CELL_POLICIES = (
    CellPolicyConfig(Cell(Location.NEW_LOW), OrderSide.BUY, ExitMode.SIGNAL, 0.5, hypothesis=TradeHypothesis.MEAN_REVERSION),
    CellPolicyConfig(Cell(Location.VWAP_ZONE, Location.NEW_HIGH, Location.PRIOR_DAY_HIGH), OrderSide.BUY, ExitMode.TRAIL),
    CellPolicyConfig(Cell(Location.VWAP_ZONE, Location.OR_HIGH, Location.PRIOR_DAY_LOW), OrderSide.BUY, ExitMode.TRAIL),
    CellPolicyConfig(Cell(Location.VWAP_ZONE, Location.OR_LOW, Location.PRIOR_DAY_HIGH), OrderSide.SELL, ExitMode.TRAIL),
    CellPolicyConfig(Cell(Location.PRIOR_DAY_HIGH), OrderSide.SELL, ExitMode.TRAIL, hypothesis=TradeHypothesis.MEAN_REVERSION),
    CellPolicyConfig(Cell(Location.PRIOR_DAY_LOW), OrderSide.BUY, ExitMode.TRAIL, hypothesis=TradeHypothesis.MEAN_REVERSION),
)
AFTERNOON_CELL_POLICIES = (
    CellPolicyConfig(Cell(Location.VWAP_ZONE, Location.OR_LOW, Location.PRIOR_DAY_HIGH), OrderSide.SELL, ExitMode.TRAIL),
    CellPolicyConfig(Cell(Location.VWAP_ZONE, Location.PRIOR_DAY_HIGH), OrderSide.SELL, ExitMode.TRAIL, hypothesis=TradeHypothesis.MEAN_REVERSION),
    CellPolicyConfig(Cell(Location.VWAP_ZONE, Location.PRIOR_DAY_LOW), OrderSide.BUY, ExitMode.TRAIL, 0.5, hypothesis=TradeHypothesis.MEAN_REVERSION),
    CellPolicyConfig(Cell(Location.OR_LOW, Location.PRIOR_DAY_LOW), OrderSide.BUY, ExitMode.TRAIL, 0.5, hypothesis=TradeHypothesis.MEAN_REVERSION),
)
SESSION_POLICIES = (
    (Session.MORNING, MORNING_CELL_POLICIES),
    (Session.AFTERNOON, AFTERNOON_CELL_POLICIES),
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
        return tuple(
            item.as_manifest(session)
            for session, policies in SESSION_POLICIES
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
    "SESSION_POLICIES",
    "Session",
    "ExitMode",
    "TradeHypothesis",
]
