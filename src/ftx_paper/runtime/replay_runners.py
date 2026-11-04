"""Generic replay execution contracts.

The worker owns queueing and persistence. Runners own strategy execution and
return the shared, API-safe replay result envelope.
"""

from __future__ import annotations

from collections.abc import Mapping
from threading import Event
from typing import Any, Protocol


class StrategyReplayRunner(Protocol):
    """Contract implemented by one strategy's isolated replay executor."""

    strategy_id: str

    def run(self, request: Mapping[str, Any], cancel: Event) -> dict[str, Any]:
        """Execute one replay and return a serialized ``ReplayResult``."""
        ...


class ResearchReplayRunner:
    """Run generic Paper execution and settlement for research strategies."""

    strategy_id = "ftx-paper-research"

    def __init__(self, worker: Any) -> None:
        self._worker = worker

    def run(self, request: Mapping[str, Any], cancel: Event) -> dict[str, Any]:
        return self._worker._execute_research(dict(request), cancel)


__all__ = ["ResearchReplayRunner", "StrategyReplayRunner"]
