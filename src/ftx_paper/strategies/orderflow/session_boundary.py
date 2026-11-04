"""Session-boundary lifecycle policy for the orderflow strategy."""

from __future__ import annotations

from typing import Any

from .interactions import InteractionCloseReason, InteractionStatus
from .session_policy import classify_opening_gap


class SessionBoundaryOwner:
    """Advance opening context and session-scoped interaction lifecycle."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def advance(
        self,
        previous_session: str | None,
        session_date: str,
        timestamp: str,
        opening: Any,
        bar_events: list[dict[str, Any]],
    ) -> None:
        context = self.engine.context
        context.current_session_date = session_date
        continuous = context.continuous
        context.opening_context = classify_opening_gap(
            opening=opening,
            registry=self.engine.location_registry,
            prior_value=(
                continuous.prior_value_as_of(opening.available_at)
                if continuous is not None else None
            ),
        )
        context.emit(
            {
                "kind": "session_boundary",
                "previous_session": previous_session,
                "session_date": session_date,
                "observed_at": timestamp,
                "available_at": timestamp,
            },
            bar_events,
        )
        detector = self.engine.observation_owner.detector
        for interaction in tuple(detector.interactions.values()):
            if interaction.status is not InteractionStatus.ACTIVE:
                continue
            templates = tuple(
                item for item in self.engine.opportunity_owner.host.templates.values()
                if interaction.location in item.locations
            )
            updated = detector.advance_session(
                interaction.interaction_id,
                available_at=timestamp,
                carry_sessions=max(
                    (item.carry_sessions for item in templates), default=-1,
                ),
            )
            if updated.status is InteractionStatus.ENDED:
                self.engine.opportunity_owner.sync_interaction_snapshot(updated)
                context.emit(
                    {
                        "kind": "interaction_ended",
                        "interaction_id": updated.interaction_id,
                        "reason": InteractionCloseReason.SESSION_END.value,
                        "observed_at": timestamp,
                        "available_at": timestamp,
                    },
                    bar_events,
                )
        self.engine._opening_gap.start({
            **context.opening_context,
            "session_date": session_date,
        })


__all__ = ["SessionBoundaryOwner"]
