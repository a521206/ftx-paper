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
    return TriggerObservation(
        crossed=crossed,
        eligible=crossed,
        trigger_price=close,
        source_observation_id=observation.observation_id,
        observed_at=observation.observed_at.isoformat(),
        available_at=observation.available_at.isoformat(),
        reason=("boundary_crossed" if crossed else "boundary_touched" if touched else f"unsupported_trigger:{trigger_type}"),
        ineligible_reason=None if crossed else "touch_not_crossing" if touched else "no_causal_crossing",
    )


__all__ = ["TriggerObservation", "observe_trigger"]
