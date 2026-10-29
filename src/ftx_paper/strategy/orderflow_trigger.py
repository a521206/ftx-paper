"""Canonical causal trigger observations for orderflow policies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from .orderflow_contract import OrderflowObservation
from .templates import Direction, TriggerType


class EntryPolicy(StrEnum):
    """Canonical evidence policy for an entry boundary."""

    INTRABAR_CROSS = "intrabar_cross"
    CLOSE_CONFIRMATION = "close_confirmation"


@dataclass(frozen=True, slots=True)
class TriggerObservation:
    """A causal observation that may, but does not necessarily, trigger an intent."""

    intent_id: str
    observed_at: str
    available_at: str
    crossed: bool
    eligible: bool
    price: float | None
    reason: str
    ineligible_reason: str | None = None
    retest_touched: bool = False
    retest_confirmed: bool = False
    retest_touch_observed_at: str | None = None
    retest_touch_available_at: str | None = None
    source_observation_id: str | None = None
    trigger_type: str | None = None
    boundary: float | None = None
    setup_version: str | None = None
    permission_version: str | None = None

    def __post_init__(self) -> None:
        if self.available_at < self.observed_at:
            raise ValueError("trigger evidence must be causally available")

def _number(features: Any, name: str) -> float | None:
    value = features.get(name)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _parse(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("trigger timestamps must be timezone-aware")
    return parsed


def _geometry(
    *,
    direction: Direction,
    trigger_type: TriggerType,
    entry_policy: EntryPolicy,
    boundary: float,
    high: float,
    low: float,
    close: float,
    retest_touched: bool,
) -> tuple[bool, bool, float | None, bool, bool, str]:
    if trigger_type is TriggerType.IMMEDIATE:
        return True, True, boundary, False, False, "entry boundary crossed"

    if trigger_type is TriggerType.PULLBACK:
        touched = low <= boundary if direction is Direction.LONG else high >= boundary
        crossed = (
            low <= boundary and close > boundary
            if direction is Direction.LONG
            else high >= boundary and close < boundary
        ) if retest_touched else False
        recorded = touched and not retest_touched
        confirmed = crossed and retest_touched
        if confirmed:
            return True, touched, boundary, recorded, True, "retest rejection confirmed"
        if recorded:
            return False, True, None, True, False, "retest boundary touched; awaiting rejection"
        return False, touched, None, False, False, "entry boundary not crossed"

    if direction is Direction.LONG:
        crossed = high >= boundary if entry_policy is EntryPolicy.INTRABAR_CROSS else close >= boundary
    else:
        crossed = low <= boundary if entry_policy is EntryPolicy.INTRABAR_CROSS else close <= boundary
    return crossed, crossed, boundary if crossed else None, False, False, (
        "entry boundary crossed" if crossed else "entry boundary not crossed"
    )


def observe_trigger(
    observation: OrderflowObservation,
    *,
    intent_id: str,
    direction: Direction | str,
    boundary: float,
    entry_policy: EntryPolicy | str = EntryPolicy.INTRABAR_CROSS,
    trigger_type: TriggerType | str = TriggerType.BREAKOUT,
    armed: bool = True,
    setup_valid: bool = True,
    permission_allowed: bool | None = None,
    setup_available_at: str | None = None,
    effective_at: str | None = None,
    expires_at: str | None = None,
    setup_id: str | None = None,
    expected_setup_id: str | None = None,
    setup_version: str | None = None,
    expected_setup_version: str | None = None,
    permission_version: str | None = None,
    expected_permission_version: str | None = None,
    context_conflicted: bool = False,
    retest_touched: bool = False,
) -> TriggerObservation:
    """Evaluate one completed candle using the canonical trigger contract."""
    direction = Direction(direction)
    entry_policy = EntryPolicy(entry_policy)
    trigger_type = TriggerType(trigger_type)
    observed_at = observation.observed_at.isoformat()
    available_at = observation.available_at.isoformat()
    high, low, close = (_number(observation.features, name) for name in ("high", "low", "close"))

    def result(
        *, crossed: bool, eligible: bool, price: float | None, reason: str,
        ineligible_reason: str | None = None, retest_touched: bool = False,
        retest_confirmed: bool = False, retest_touch_observed_at: str | None = None,
        retest_touch_available_at: str | None = None,
    ) -> TriggerObservation:
        return TriggerObservation(
            intent_id=intent_id, observed_at=observed_at, available_at=available_at,
            source_observation_id=observation.observation_id, trigger_type=trigger_type.value,
            boundary=boundary, setup_version=setup_version, permission_version=permission_version,
            crossed=crossed, eligible=eligible, price=price, reason=reason,
            ineligible_reason=ineligible_reason, retest_touched=retest_touched,
            retest_confirmed=retest_confirmed,
            retest_touch_observed_at=retest_touch_observed_at,
            retest_touch_available_at=retest_touch_available_at,
        )

    if high is None or low is None or close is None:
        return result(
            crossed=False, eligible=False, price=None, reason="entry boundary not crossed",
            ineligible_reason="missing_trigger_price",
        )

    crossed, touched, price, touch_recorded, retest_confirmed, reason = _geometry(
        direction=direction, trigger_type=trigger_type, entry_policy=entry_policy,
        boundary=boundary, high=high, low=low, close=close,
        retest_touched=retest_touched,
    )
    ineligible_reason: str | None = None
    observed_dt = observation.observed_at
    setup_available_dt = _parse(setup_available_at)
    effective_dt = _parse(effective_at)
    expires_dt = _parse(expires_at)
    if setup_available_dt is not None and observed_dt < setup_available_dt:
        ineligible_reason = "before_setup_available_at"
    elif not armed:
        ineligible_reason = "intent_not_yet_armed"
    elif effective_dt is not None and observed_dt < effective_dt:
        ineligible_reason = "before_effective_at"
    elif expires_dt is not None and observed_dt >= expires_dt:
        ineligible_reason = "intent_expired"
    elif permission_allowed is False:
        ineligible_reason = "parent_permission_blocked"
    elif not setup_valid:
        ineligible_reason = "setup_no_longer_valid"
    elif context_conflicted:
        ineligible_reason = "context_relation_conflicted"
    elif expected_setup_id is not None and setup_id != expected_setup_id:
        ineligible_reason = "setup_no_longer_valid"
    elif expected_setup_version is not None and setup_version != expected_setup_version:
        ineligible_reason = "setup_no_longer_valid"
    elif expected_permission_version is not None and permission_version != expected_permission_version:
        ineligible_reason = "parent_permission_blocked"
    elif not crossed:
        ineligible_reason = "entry boundary touched; awaiting rejection" if touched else "entry boundary not crossed"
    elif trigger_type is TriggerType.PULLBACK and not retest_confirmed:
        ineligible_reason = "retest_confirmation_required"

    return result(
        crossed=crossed,
        eligible=crossed and ineligible_reason is None,
        price=price,
        reason=reason,
        ineligible_reason=ineligible_reason,
        retest_touched=touch_recorded or retest_touched,
        retest_confirmed=retest_confirmed,
        retest_touch_observed_at=observed_at if touch_recorded else None,
        retest_touch_available_at=available_at if touch_recorded else None,
    )


__all__ = ["EntryPolicy", "TriggerObservation", "observe_trigger"]
