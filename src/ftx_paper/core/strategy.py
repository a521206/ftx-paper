from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from ftx_paper.contracts import MarketBar, OrderIntent

from .bundles import DecisionBundle
from .live_decision import LiveDecision


@dataclass(frozen=True, slots=True)
class StrategyMetadata:
    """Stable identity recorded with every live decision."""

    name: str
    version: str
    config_hash: str


class Strategy(Protocol):
    """Pure production boundary for normalized completed market bars.

    Implementations may keep only deterministic strategy state.  Persistence,
    broker execution, and clock access belong to the runtime layer.
    """

    @property
    def metadata(self) -> StrategyMetadata: ...

    def on_bar(self, bar: MarketBar) -> tuple[OrderIntent, ...]: ...

    def on_bundle(self, bundle: DecisionBundle) -> tuple[LiveDecision, ...]: ...

    def snapshot(self) -> Mapping[str, object]: ...

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "Strategy": ...
