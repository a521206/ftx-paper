"""Canonical causal trigger observations for orderflow policies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .orderflow_contract import OrderflowObservation
from .templates import TriggerType


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
    effective_at: str | None = None
    expires_at: str | None = None
    setup_version: str | None = None
    permission_version: str | None = None
    trigger_type: str | None = None
    boundary: float | None = None
    armed: bool | None = None
    displacement: float | None = None


def observe_trigger(
    observation: OrderflowObservation,
    *,
    boundary: float,
    execution_mode: str,
    trigger_type: str = TriggerType.BREAKOUT.value,
    effective_at: str | None = None,
    expires_at: str | None = None,
    setup_version: str | None = None,
    permission_version: str | None = None,
    expected_setup_version: str | None = None,
    expected_permission_version: str | None = None,
    next_bar_open_at: str | None = None,
    armed: bool | None = None,
) -> TriggerObservation:
    trigger_type = (
        trigger_type.value if isinstance(trigger_type, TriggerType) else str(trigger_type)
    )
    if trigger_type not in {item.value for item in TriggerType} | {"retest"}:
        return TriggerObservation(
            False, False, None, observation.observation_id,
            observation.observed_at.isoformat(), observation.available_at.isoformat(),
            reason=f"unsupported_trigger:{trigger_type}",
            ineligible_reason=f"unsupported_trigger:{trigger_type}",
            effective_at=effective_at, expires_at=expires_at,
            setup_version=setup_version, permission_version=permission_version,
            trigger_type=trigger_type, boundary=boundary, armed=armed, displacement=None,
        )
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
    trigger_armed = armed if armed is not None else armed_value is True
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
    retest_touch = observation.features.get("retest_touch") is True
    retest_confirmation = observation.features.get("retest_confirmation") is True
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
            effective_at=effective_at, expires_at=expires_at,
            setup_version=setup_version, permission_version=permission_version,
            trigger_type=trigger_type, boundary=boundary, armed=trigger_armed,
            displacement=float(displacement) if isinstance(displacement, (int, float)) else None,
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
            effective_at=effective_at, expires_at=expires_at,
            setup_version=setup_version, permission_version=permission_version,
            trigger_type=trigger_type, boundary=boundary, armed=trigger_armed,
            displacement=float(displacement) if isinstance(displacement, (int, float)) else None,
        )
    beyond_boundary = (
        close is not None
        and ((close > boundary and previous is not None and previous <= boundary)
             or (close < boundary and previous is not None and previous >= boundary))
    )
    eligible = crossed and trigger_armed and directional_displacement
    available_dt = observation.available_at
    def _parse(value: str | None) -> datetime | None:
        if value is None:
            return None
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("trigger timestamps must be timezone-aware")
        return parsed
    effective_dt = _parse(effective_at)
    expires_dt = _parse(expires_at)
    if trigger_type in {TriggerType.PULLBACK.value, "retest"}:
        eligible = eligible and retest_touch and retest_confirmation
    elif trigger_type == TriggerType.RECLAIM.value:
        eligible = crossed and trigger_armed
    elif trigger_type == TriggerType.IMMEDIATE.value:
        eligible = beyond_boundary and trigger_armed
    ineligible_reason = None
    if not crossed:
        ineligible_reason = "touch_not_crossing" if touched else "no_causal_crossing"
    elif not trigger_armed:
        ineligible_reason = "not_armed"
    elif trigger_type == TriggerType.BREAKOUT.value and not directional_displacement:
        ineligible_reason = "insufficient_displacement"
    elif trigger_type in {TriggerType.PULLBACK.value, "retest"} and not retest_touch:
        ineligible_reason = "retest_touch_required"
        eligible = False
    elif trigger_type in {TriggerType.PULLBACK.value, "retest"} and not retest_confirmation:
        ineligible_reason = "retest_confirmation_required"
        eligible = False
    elif effective_dt is not None and available_dt < effective_dt:
        ineligible_reason = "before_effective_time"
        eligible = False
    elif expires_dt is not None and available_dt > expires_dt:
        ineligible_reason = "expired_setup"
        eligible = False
    elif expected_setup_version is not None and setup_version != expected_setup_version:
        ineligible_reason = "setup_version_mismatch"
        eligible = False
    elif expected_permission_version is not None and permission_version != expected_permission_version:
        ineligible_reason = "permission_version_mismatch"
        eligible = False
    elif next_bar_open_at is not None and (
        (next_bar_dt := _parse(next_bar_open_at)) is not None and available_dt < next_bar_dt
    ):
        ineligible_reason = "next_bar_open_unavailable"
        eligible = False
    return TriggerObservation(
        crossed=crossed,
        eligible=eligible,
        trigger_price=close,
        source_observation_id=observation.observation_id,
        observed_at=observation.observed_at.isoformat(),
        available_at=observation.available_at.isoformat(),
        reason=("boundary_crossed" if crossed else "boundary_touched" if touched else "no_causal_crossing"),
        ineligible_reason=ineligible_reason,
        effective_at=effective_at, expires_at=expires_at,
        setup_version=setup_version, permission_version=permission_version,
        trigger_type=trigger_type, boundary=boundary, armed=trigger_armed,
        displacement=float(displacement) if isinstance(displacement, (int, float)) else None,
    )


__all__ = ["TriggerObservation", "observe_trigger"]
