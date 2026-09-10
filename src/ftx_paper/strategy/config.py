"""Versioned, runtime-independent production strategy configuration."""

from dataclasses import asdict, dataclass
import json
from hashlib import sha256

STRATEGY_NAME = "ftx-paper-production"
STRATEGY_VERSION = "0.2.0-live-composition"

MORNING_ENTRY_MINUTES = (60, 120)
AFTERNOON_ENTRY_MINUTES = (255, 300)
COOLDOWN_MINUTES = 30


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

    @property
    def config_hash(self) -> str:
        payload = json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))
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
]
