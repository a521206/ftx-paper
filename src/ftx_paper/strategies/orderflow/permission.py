"""Completed-5m permission publication for the orderflow strategy."""

from __future__ import annotations

from typing import Any

from .hierarchy import Permission
from .timestamps import nifty_label


class PermissionOwner:
    """Publish the canonical ordinary context and permission facts."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def publish(
        self,
        latest: Any,
        long_score: float | None,
        short_score: float | None,
        bar_events: list[dict[str, Any]],
    ) -> None:
        context = self.engine.context
        context.previous5_available_at = latest.available_at
        self.engine._context_publisher.ordinary_context(
            latest, (long_score, short_score),
        )
        current = self.engine._opening_gap.current(latest.session_date)
        ordinary_reference = context.market_contexts.ordinary_context
        context.regime = context.opening_gap_policy.regime_for(
            state=str(current.get("state")) if current is not None else None,
            direction=str(current.get("direction")) if current is not None else None,
            version="5m-permission-allowed",
            context="5m_directional_context",
            restrictions=("completed_5m_only",),
        )
        context.emit(
            {
                "kind": "permission",
                "level": "5m",
                "time": nifty_label(latest.time),
                "decision_at": nifty_label(latest.time),
                "observed_at": latest.end_time,
                "aggregate_time": latest.time,
                "available_at": latest.available_at,
                "version": context.regime.version,
                "status": Permission.ALLOWED.value,
                "owner": "5m",
                "evidence_timeframe": "5m",
                "context_id": (
                    ordinary_reference.context_id
                    if ordinary_reference is not None else None
                ),
                "market_state": context.market_state,
                "directional": context.regime.long != context.regime.short,
                "long_permission": context.regime.long.value,
                "short_permission": context.regime.short.value,
                "opening_gap_state": context.regime.opening_gap_state,
                "long_score": long_score,
                "short_score": short_score,
                "reference_level": None,
                "opening_context": dict(context.opening_context),
            },
            bar_events,
        )
        for life in context.opportunities.values():
            if life.active:
                context.emit(
                    {
                        "kind": "context_observed",
                        "observed_at": latest.end_time,
                        "available_at": latest.available_at,
                        "context": context.snapshot(
                            context.market_contexts.resolve(ordinary_reference)
                            if ordinary_reference is not None else None
                        ),
                        "long_score": long_score,
                        "short_score": short_score,
                    },
                    bar_events,
                )


__all__ = ["PermissionOwner"]
