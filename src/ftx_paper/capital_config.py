"""Explicit capital configuration for the standalone FTX paper runtime."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CAPITAL_CONFIG_PATH = PACKAGE_ROOT / "config" / "ftx.json"


@dataclass(frozen=True, slots=True)
class FtxCapitalConfig:
    """Capital inputs owned by an application run, not by strategy engines."""

    initial_capital: float
    max_daily_loss: float = 0.05
    max_net_directional_lots: float = 8.0
    risk_per_trade: float = 0.01
    max_lots: int = 3

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if not 0 <= self.max_daily_loss <= 1:
            raise ValueError("max_daily_loss must be between zero and one")
        if self.max_net_directional_lots <= 0:
            raise ValueError("max_net_directional_lots must be positive")
        if not 0 < self.risk_per_trade <= 1:
            raise ValueError("risk_per_trade must be between zero and one")
        if self.max_lots <= 0:
            raise ValueError("max_lots must be positive")

    @classmethod
    def from_file(cls, path: str | Path = DEFAULT_CAPITAL_CONFIG_PATH) -> "FtxCapitalConfig":
        config_path = Path(path)
        try:
            values = json.loads(config_path.read_text(encoding="utf-8"))
            if not isinstance(values, Mapping):
                raise TypeError("top-level config must be an object")
            capital = values["capital"]
            if not isinstance(capital, Mapping):
                raise TypeError("capital config must be an object")
            return cls(
                initial_capital=float(capital["initial_capital"]),
                max_daily_loss=float(capital.get("max_daily_loss", 0.05)),
                max_net_directional_lots=float(capital.get("max_net_directional_lots", 8.0)),
                risk_per_trade=float(capital.get("risk_per_trade", 0.01)),
                max_lots=int(capital.get("max_lots", 3)),
            )
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid FTX paper capital config: {config_path}") from exc


RESEARCH_CAPITAL_CONFIG = FtxCapitalConfig(initial_capital=2_500_000.0)

__all__ = ["DEFAULT_CAPITAL_CONFIG_PATH", "FtxCapitalConfig", "RESEARCH_CAPITAL_CONFIG"]
