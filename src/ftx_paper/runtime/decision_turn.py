"""Shared causal strategy-turn boundary for live and replay runtimes.

The market source, persistence, and execution adapter differ by environment.
Strategy evaluation must not: both paths must present the same completed bundle
and account snapshot to the same engine entry point.
"""

from __future__ import annotations

from dataclasses import dataclass
from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.market.bundles import DecisionBundle

from .engine import EngineResult, MultiStrategyEngine, PaperEngine


@dataclass(frozen=True, slots=True)
class DecisionTurn:
    """One causally bounded strategy evaluation."""

    bundle: DecisionBundle
    source: str
    context: DecisionContext | None = None


def evaluate_turn(
    engine: PaperEngine | MultiStrategyEngine,
    turn: DecisionTurn,
) -> EngineResult:
    """Evaluate a live or replay turn through one engine boundary.

    Replay may intentionally omit an account context for strategies that only
    implement ``on_bundle``. The fallback is retained for older strategy
    implementations, but both runtimes use this same dispatch behavior.
    """
    if turn.context is None:
        return engine.on_bundle(turn.bundle)
    try:
        return engine.on_bundle(turn.bundle, context=turn.context)
    except TypeError as exc:
        if "context" not in str(exc):
            raise
        return engine.on_bundle(turn.bundle)


__all__ = ["DecisionTurn", "evaluate_turn"]
