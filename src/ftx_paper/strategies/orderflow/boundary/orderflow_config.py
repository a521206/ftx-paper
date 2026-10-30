"""Minimal configuration boundary reserved for the canonical orderflow port."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json


ORDERFLOW_NAME = "ftx-paper-orderflow"
ORDERFLOW_VERSION = "scaffold"


@dataclass(frozen=True, slots=True)
class OrderflowConfig:
    """Configuration placeholder for the canonical orderflow port."""

    enabled_vehicles: tuple[str, ...] = ("futures",)

    @property
    def config_hash(self) -> str:
        payload = json.dumps({"enabled_vehicles": self.enabled_vehicles}, sort_keys=True)
        return sha256(payload.encode("utf-8")).hexdigest()


__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig"]
