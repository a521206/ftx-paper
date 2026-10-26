"""Canonical causal trigger observations for orderflow policies."""

from __future__ import annotations

from dataclasses import dataclass

from .orderflow_contract import OrderflowObservation


@dataclass(frozen=True, slots=True)
class TriggerObservation:
    crossed: bool
    eligible: bool
    trigger_price: float | None
    source_observation_id: str
    observed_at: str
    available_at: str
    reason: str | None = None
    ineligible_reason: str | None = None


def observe_trigger(
    observation: OrderflowObservation,
    *,
    boundary: float,
    execution_mode: str,
    trigger_type: str = "diagnostic_touch",
) -> TriggerObservation:
    value = observation.features.get("close")
    close = float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None
    previous_value = observation.features.get("previous_close")
    previous = (
        float(previous_value)
        if isinstance(previous_value, (int, float)) and not isinstance(previous_value, bool)
        else None
    )
    crossed = close is not None and previous is not None and (
        (previous <= boundary < close) or (previous >= boundary > close)
    )
    armed_value = observation.features.get("trigger_armed", observation.features.get("armed"))
    armed = armed_value is True
    displacement = observation.features.get("displacement")
    if not isinstance(displacement, (int, float)) or isinstance(displacement, bool):
        displacement = (close - previous) if close is not None and previous is not None else None
    directional_displacement = (
        isinstance(displacement, (int, float)) and not isinstance(displacement, bool)
        and displacement != 0 and previous is not None and close is not None
        and ((displacement > 0 and close > previous and close > boundary)
             or (displacement < 0 and close < previous and close < boundary))
    )
    touched = close is not None and close == boundary
    if execution_mode == "diagnostic":
        return TriggerObservation(
            crossed=crossed,
            eligible=False,
            trigger_price=close,
            source_observation_id=observation.observation_id,
            observed_at=observation.observed_at.isoformat(),
            available_at=observation.available_at.isoformat(),
            reason=("boundary_crossed" if crossed else "boundary_touched" if touched else "no_causal_crossing"),
            ineligible_reason="diagnostic_only",
        )
    if close is None:
        return TriggerObservation(
            crossed=False,
            eligible=False,
            trigger_price=None,
            source_observation_id=observation.observation_id,
            observed_at=observation.observed_at.isoformat(),
            available_at=observation.available_at.isoformat(),
            ineligible_reason="missing_trigger_price",
        )
    eligible = crossed and armed and directional_displacement
    ineligible_reason = None
    if not crossed:
        ineligible_reason = "touch_not_crossing" if touched else "no_causal_crossing"
    elif not armed:
        ineligible_reason = "not_armed"
    elif not directional_displacement:
        ineligible_reason = "insufficient_displacement"
    return TriggerObservation(
        crossed=crossed,
        eligible=eligible,
        trigger_price=close,
        source_observation_id=observation.observation_id,
        observed_at=observation.observed_at.isoformat(),
        available_at=observation.available_at.isoformat(),
        reason=("boundary_crossed" if crossed else "boundary_touched" if touched else f"unsupported_trigger:{trigger_type}"),
        ineligible_reason=ineligible_reason,
    )


__all__ = ["TriggerObservation", "observe_trigger"]
