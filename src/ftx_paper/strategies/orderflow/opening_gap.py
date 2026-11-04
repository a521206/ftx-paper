"""Stateful opening-gap advancement for the orderflow transition."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .orderflow_common import Aggregate


class OpeningGapOwner:
    """Advance opening-gap state and publish its causal permission facts."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    @property
    def context(self) -> Any:
        return self.engine.context

    def start(self, opening: Mapping[str, Any]) -> None:
        continuous = self.context.continuous
        if continuous is None or opening.get("direction") not in {"up", "down"}:
            return
        continuous.open_gaps.append({
            **opening,
            "state": opening.get("state", opening.get("classification")),
            "completed_bars": 0,
        })

    def current(self, session_date: str) -> Mapping[str, Any] | None:
        continuous = self.context.continuous
        if continuous is None:
            return None
        return next(
            (
                gap for gap in reversed(continuous.open_gaps)
                if gap.get("session_date") == session_date
            ),
            None,
        )

    def advance(
        self,
        session_date: str,
        available1: Sequence[Aggregate],
        timestamp: str,
        bar_events: list[dict[str, Any]],
    ) -> None:
        continuous = self.context.continuous
        if continuous is None:
            return
        current = [item for item in available1 if item.session_date == session_date]
        if not current:
            return
        latest = current[-1]
        for gap in reversed(continuous.open_gaps):
            if gap.get("session_date", session_date) != session_date or gap.get("state") in {
                "GAP_DOWN_RECLAIMED", "GAP_UP_RECLAIMED", "RETURN_TO_VALUE", "EXPIRED",
            }:
                continue
            if gap.get("last_evaluated") == latest.available_at:
                continue
            gap["last_evaluated"] = latest.available_at
            gap["completed_bars"] = int(gap.get("completed_bars", 0)) + 1
            reference = float(gap["reference_level"])
            prior_value = continuous.prior_value_as_of(latest.available_at)
            prior_reference = getattr(prior_value, "reference", None)
            state = self.context.opening_gap_policy.transition(
                direction=str(gap["direction"]),
                completed_bars=int(gap["completed_bars"]),
                close=latest.close,
                reference_level=reference,
                prior_value_low=(
                    prior_reference.value_area_low
                    if prior_reference is not None else None
                ),
                prior_value_high=(
                    prior_reference.value_area_high
                    if prior_reference is not None else None
                ),
            )
            if state is None or state == gap.get("state"):
                continue
            previous_state = str(gap.get("state", "GAP_CANDIDATE"))
            gap["state"] = state
            reference_id = getattr(prior_reference, "reference_id", None)
            reference_scope = getattr(prior_reference, "scope", None)
            reference_identity = (
                {"value_reference_id": reference_id, "value_scope": reference_scope}
                if reference_id is not None else {}
            )
            self.context.emit({
                "kind": "opening_gap_transition",
                "session_date": session_date,
                "from_state": previous_state,
                "state": state,
                "observed_at": timestamp,
                "available_at": latest.available_at,
                "reference_level": reference,
                **reference_identity,
            }, bar_events)
            self.publish_regime(
                state=state,
                direction=str(gap["direction"]),
                available_at=latest.available_at,
                bar_events=bar_events,
            )
            break

    def publish_regime(
        self,
        *,
        state: str,
        direction: str,
        available_at: Any,
        bar_events: list[dict[str, Any]],
    ) -> None:
        context = self.context
        context.regime = context.opening_gap_policy.regime_for(
            state=state,
            direction=direction,
            version=f"opening-gap-permission-{available_at}",
            context="opening_gap",
            restrictions=("causal_opening_gap",),
        )
        context.emit({
            "kind": "opening_gap_permission",
            "state": state,
            "direction": direction,
            "available_at": available_at,
            "version": context.regime.version,
            "long": context.regime.long.value,
            "short": context.regime.short.value,
        }, bar_events)
