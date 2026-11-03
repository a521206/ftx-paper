"""Strategy-specific replay execution contracts.

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


class OrderflowReplayRunner:
    """Run orderflow through its causal state-machine runtime."""

    strategy_id = "ftx-paper-orderflow"

    def __init__(self, worker: Any) -> None:
        self._worker = worker

    def run(self, request: Mapping[str, Any], cancel: Event) -> dict[str, Any]:
        session_date = request.get("session_date")
        if not isinstance(session_date, str) or not session_date[:10]:
            raise ValueError("replay requires one session_date")
        raw_vehicles = request.get("vehicles", ("futures",))
        if not isinstance(raw_vehicles, (list, tuple)) or any(
            not isinstance(item, str) for item in raw_vehicles
        ):
            raise ValueError("replay vehicles must be an array containing 'futures'")
        vehicles = tuple(dict.fromkeys(item.lower() for item in raw_vehicles))
        if not vehicles or any(item not in {"futures", "synthetic"} for item in vehicles):
            raise ValueError(f"unsupported replay vehicles: {vehicles}")
        if "futures" not in vehicles:
            raise ValueError("synthetic replay is reporting-only; futures must be enabled")
        include_parity_layers = request.get("include_parity_layers", False)
        if not isinstance(include_parity_layers, bool):
            raise ValueError("include_parity_layers must be a boolean")
        return self._worker._execute_orderflow_impl(
            dict(request), cancel, session_date=session_date[:10],
            vehicles=vehicles, include_parity_layers=include_parity_layers,
        )


class ResearchReplayRunner:
    """Run generic Paper execution and settlement for research strategies."""

    strategy_id = "ftx-paper-research"

    def __init__(self, worker: Any) -> None:
        self._worker = worker

    def run(self, request: Mapping[str, Any], cancel: Event) -> dict[str, Any]:
        return self._worker._execute_research(dict(request), cancel)


__all__ = ["OrderflowReplayRunner", "ResearchReplayRunner", "StrategyReplayRunner"]
