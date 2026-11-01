"""Causal context and directional permission contracts for orderflow."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from types import MappingProxyType
from typing import Any
from datetime import datetime

from .timestamps import coerce_timestamp



class Permission(StrEnum):
    ALLOWED = "allowed"
    CONDITIONAL = "conditional"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class ContextVersionRef:
    context_id: str
    version: str


class ContextRelation(StrEnum):
    CONFIRMED = "confirmed"
    CONFLICTED = "conflicted"
    NEUTRAL = "neutral"


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("context mappings must use string keys")
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool, bytes)):
        return value
    raise TypeError(f"unsupported mutable context value: {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class MarketContext:
    context_id: str
    version: str
    timestamp: datetime
    available_at: datetime
    location: str | None
    market_state: str
    long_score: float | None = None
    short_score: float | None = None
    evidence: tuple[Mapping[str, Any], ...] = ()
    source_timeframe: str = "5m"
    parent_context_id: str | None = None
    definition_version: str = "1"
    source_provenance: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", coerce_timestamp(self.timestamp, field="context.timestamp"))
        object.__setattr__(self, "available_at", coerce_timestamp(self.available_at, field="context.available_at"))
        if self.available_at < self.timestamp:
            raise ValueError("context availability cannot precede observation")
        object.__setattr__(self, "evidence", _freeze(self.evidence))
        object.__setattr__(self, "source_provenance", _freeze(self.source_provenance))


@dataclass(frozen=True, slots=True)
class RegimePermission:
    version: str
    context: str
    long: Permission
    short: Permission
    restrictions: tuple[str, ...] = ()
    opening_gap_state: str | None = None
    opening_gap_direction: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "long", Permission(self.long))
        object.__setattr__(self, "short", Permission(self.short))

    def permits(self, direction: str) -> bool:
        if direction not in {"long", "short"}:
            raise ValueError(f"unsupported direction: {direction}")
        return (self.long if direction == "long" else self.short) is not Permission.BLOCKED


@dataclass(frozen=True, slots=True)
class StructuralSetup:
    """Immutable setup facts resolved from one opportunity."""

    setup_id: str
    version: str
    regime_version: str
    direction: str
    kind: str
    entry_zone_low: float
    entry_zone_high: float
    structural_reference: float
    sweep_extreme: float | None
    failure_boundary: float
    expires_at: datetime
    active: bool = True
    required_confirmation: tuple[str, ...] = ()
    parent_context_id: str | None = None
    parent_context_version: str | None = None
    context_relation: ContextRelation = ContextRelation.CONFIRMED
    context_reference_level: float | None = None
    lifecycle_id: str | None = None
    template: Any | None = None
    exit_plan: tuple[ResolvedExitRule, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "context_relation", ContextRelation(self.context_relation))
        object.__setattr__(self, "expires_at", coerce_timestamp(self.expires_at, field="setup.expires_at"))

    def invalidated_by(self, *, close: float) -> bool:
        return close < self.failure_boundary if self.direction == "long" else close > self.failure_boundary


class IntentStatus(StrEnum):
    OBSERVING = "observing"
    CONFIRMATION_FOUND = "confirmation_found"
    ARMED = "armed"
    TRIGGER_OBSERVED = "trigger_observed"
    TRIGGERED = "triggered"
    FILLED = "filled"
    CANCELLED = "cancelled"


class PositionStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class TriggerObservation:
    intent_id: str
    observed_at: datetime | str
    available_at: datetime | str
    crossed: bool
    eligible: bool
    price: float | None
    reason: str
    ineligible_reason: str | None = None
    retest_touched: bool = False
    retest_confirmed: bool = False

    def __post_init__(self) -> None:
        observed = coerce_timestamp(self.observed_at, field="trigger.observed_at")
        available = coerce_timestamp(self.available_at, field="trigger.available_at")
        if available < observed:
            raise ValueError("trigger evidence must be causally available")


@dataclass(frozen=True, slots=True)
class ResolvedExitRule:
    rule_id: str
    location: str
    mode: str
    level: float | None
    source_context: ContextVersionRef | None
    interaction_id: str | None = None
    score_direction: str | None = None
    score_margin: float | None = None
    required: bool = False
    trigger_policy: str = "touch"

    def __post_init__(self) -> None:
        if self.mode not in {"fixed_target", "conditional"}:
            raise ValueError("unsupported resolved exit mode")
        if self.score_direction not in {None, "long", "short"}:
            raise ValueError("exit score direction must be long or short")
        if self.score_margin is not None and self.score_margin < 0:
            raise ValueError("exit score margin must be non-negative")
        if self.trigger_policy not in {"touch", "close_confirmation"}:
            raise ValueError("unsupported exit trigger policy")

    def validation_reason(self, *, direction: str, fill_price: float) -> str | None:
        if self.level is None:
            return "required_exit_level_unavailable" if self.required else "optional_exit_level_unavailable"
        if direction == "long" and self.level <= fill_price:
            return "exit_level_not_above_long_fill"
        if direction == "short" and self.level >= fill_price:
            return "exit_level_not_below_short_fill"
        return None


@dataclass(frozen=True, slots=True)
class ExitIntent:
    intent_id: str
    position_id: str
    rule_id: str
    location: str
    status: IntentStatus
    created_at: datetime | str
    expiry: datetime | str
    interaction_id: str | None = None
    formation_context: ContextVersionRef | None = None
    decision_context: ContextVersionRef | None = None
    cancel_reason: str | None = None
    triggered_at: datetime | str | None = None


def cancel_exit_intent(*, intent: ExitIntent, reason: str) -> ExitIntent:
    if intent.status in {IntentStatus.TRIGGERED, IntentStatus.CANCELLED}:
        return intent
    return replace(intent, status=IntentStatus.CANCELLED, cancel_reason=reason)


def trigger_exit_intent(*, intent: ExitIntent, at: datetime | str, decision_context: ContextVersionRef | None = None) -> ExitIntent:
    if intent.status is IntentStatus.TRIGGERED:
        return intent
    if intent.status is not IntentStatus.CONFIRMATION_FOUND:
        raise ValueError("only a confirmed exit intent can trigger")
    if coerce_timestamp(at, field="exit.triggered_at") >= coerce_timestamp(intent.expiry, field="exit.expiry"):
        raise ValueError("exit intent has expired")
    return replace(intent, status=IntentStatus.TRIGGERED, triggered_at=at, decision_context=decision_context or intent.decision_context)


@dataclass(frozen=True, slots=True)
class PositionManagement:
    position_id: str
    intent_id: str
    setup_id: str
    regime_version: str
    direction: str
    status: PositionStatus
    failure_boundary: float
    initial_risk_points: float
    protective_stop: float | None = None
    target: float | None = None
    closed_at: datetime | str | None = None
    exit_reason: str | None = None
    fill_price: float | None = None
    quantity: float = 1.0
    lifecycle_id: str | None = None
    exit_plan: tuple[ResolvedExitRule, ...] = ()
    exit_intent: ExitIntent | None = None
    partial_exit_taken: bool = False
    post_3r_activated: bool = False


def open_position(*, intent: TradeIntent, position_id: str, fill_price: float | None = None, quantity: float = 1.0, protective_stop: float | None = None, exit_plan: tuple[ResolvedExitRule, ...] = ()) -> PositionManagement:
    if intent.status not in {IntentStatus.TRIGGERED, IntentStatus.FILLED}:
        raise ValueError("only a triggered intent can open a position")
    if quantity <= 0:
        raise ValueError("position quantity must be positive")
    entry = intent.trigger_price if fill_price is None else fill_price
    stop = intent.failure_boundary if protective_stop is None else protective_stop
    risk = abs(entry - stop)
    if risk <= 0 or (intent.direction == "long" and stop >= entry) or (intent.direction == "short" and stop <= entry):
        raise ValueError("position entry stop must be protective")
    return PositionManagement(position_id, intent.intent_id, intent.setup_id, intent.regime_version, intent.direction, PositionStatus.OPEN, intent.failure_boundary, risk, protective_stop, fill_price=entry, quantity=quantity, lifecycle_id=intent.lifecycle_id, exit_plan=exit_plan)


def close_position(*, position: PositionManagement, at: datetime | str, reason: str) -> PositionManagement:
    if position.status is PositionStatus.CLOSED:
        return position
    return replace(position, status=PositionStatus.CLOSED, closed_at=at, exit_reason=reason)


class EntryPolicy(StrEnum):
    INTRABAR_CROSS = "intrabar_cross"
    CLOSE_CONFIRMATION = "close_confirmation"


@dataclass(frozen=True, slots=True)
class TradeIntent:
    intent_id: str
    setup_id: str
    regime_version: str
    direction: str
    status: IntentStatus
    created_at: datetime | str
    trigger_price: float
    failure_boundary: float
    expiry: datetime | str
    confirmation: tuple[str, ...] = ()
    cancel_reason: str | None = None
    triggered_at: datetime | str | None = None
    setup_version: str | None = None
    entry_policy: EntryPolicy = EntryPolicy.INTRABAR_CROSS
    required_confirmation: tuple[str, ...] = ()
    armed_at: datetime | str | None = None
    effective_at: datetime | str | None = None
    filled_at: datetime | str | None = None
    lifecycle_id: str | None = None
    formation_context: ContextVersionRef | None = None
    decision_context: ContextVersionRef | None = None


def arm_intent(*, intent: TradeIntent, regime: RegimePermission, setup: StructuralSetup,
               at: datetime | str, confirmations: tuple[str, ...] = (),
               effective_at: datetime | str | None = None) -> TradeIntent:
    if intent.status is not IntentStatus.CONFIRMATION_FOUND:
        raise ValueError("only a confirmed intent can arm")
    if not setup.active:
        raise ValueError("cannot arm an inactive setup")
    if setup.regime_version != regime.version:
        raise ValueError("setup regime version does not match current permission")
    if intent.setup_id != setup.setup_id:
        raise ValueError("intent setup ID does not match setup")
    if intent.direction != setup.direction:
        raise ValueError("intent direction does not match setup")
    if intent.setup_version is not None and intent.setup_version != setup.version:
        raise ValueError("intent setup version does not match setup")
    armed_at = coerce_timestamp(at, field="intent.armed_at")
    if armed_at >= coerce_timestamp(setup.expires_at, field="setup.expires_at"):
        raise ValueError("setup has expired")
    available_confirmations = set(confirmations) | set(intent.confirmation)
    missing = set(setup.required_confirmation) - available_confirmations
    if missing:
        raise ValueError(f"required confirmation missing: {sorted(missing)}")
    if not regime.permits(setup.direction):
        raise ValueError("parent permission blocks this direction")
    effective = armed_at if effective_at is None else coerce_timestamp(effective_at, field="intent.effective_at")
    if effective < armed_at:
        raise ValueError("effective time cannot precede arming time")
    return replace(intent, status=IntentStatus.ARMED, created_at=at,
                   armed_at=at, effective_at=effective)


def observe_trigger(*, intent: TradeIntent, high: float, low: float, close: float,
                    at: datetime | str, available_at: datetime | str,
                    regime: RegimePermission | None = None,
                    setup: StructuralSetup | None = None,
                    setup_available_at: datetime | str | None = None,
                    setup_valid: bool = True,
                    context_relation: ContextRelation | None = None,
                    template: Any | None = None,
                    retest_touched: bool = False) -> TriggerObservation:
    if template is None:
        crossed = (
            high >= intent.trigger_price if intent.direction == "long" else low <= intent.trigger_price
        ) if intent.entry_policy is EntryPolicy.INTRABAR_CROSS else (
            close >= intent.trigger_price if intent.direction == "long" else close <= intent.trigger_price
        )
    elif template.trigger_type.value == "immediate":
        crossed = True
    else:
        boundary = (
            setup.entry_zone_high if setup is not None and intent.direction == "long"
            else setup.entry_zone_low if setup is not None else intent.trigger_price
        )
        price = (
            close if intent.entry_policy is not EntryPolicy.INTRABAR_CROSS
            else low if template.trigger_type.value == "pullback" and intent.direction == "long"
            else high if template.trigger_type.value == "pullback"
            else high if intent.direction == "long" else low
        )
        if template.trigger_type.value == "pullback" and retest_touched:
            crossed = low <= boundary and close > boundary if intent.direction == "long" else high >= boundary and close < boundary
        elif template.trigger_type.value == "pullback":
            crossed = False
        elif template.trigger_type.value in {"breakout", "reclaim"}:
            crossed = price >= boundary if intent.direction == "long" else price <= boundary
        else:
            crossed = False
    observed = coerce_timestamp(at, field="trigger.observed_at")
    ineligible_reason: str | None = None
    if setup_available_at is not None and observed < coerce_timestamp(setup_available_at, field="setup.available_at"):
        ineligible_reason = "before_setup_available_at"
    elif intent.status is not IntentStatus.ARMED:
        ineligible_reason = "intent_not_yet_armed"
    elif intent.effective_at is not None and observed < coerce_timestamp(intent.effective_at, field="intent.effective_at"):
        ineligible_reason = "before_effective_at"
    elif observed >= coerce_timestamp(intent.expiry, field="intent.expiry"):
        ineligible_reason = "intent_expired"
    elif regime is not None and not regime.permits(intent.direction):
        ineligible_reason = "parent_permission_blocked"
    elif not setup_valid:
        ineligible_reason = "setup_no_longer_valid"
    elif context_relation is ContextRelation.CONFLICTED:
        ineligible_reason = "context_relation_conflicted"
    elif setup is not None and (
        not setup.active or observed >= coerce_timestamp(setup.expires_at, field="setup.expires_at")
        or setup.setup_id != intent.setup_id
        or (intent.setup_version is not None and setup.version != intent.setup_version)
        or setup.regime_version != intent.regime_version
        or setup.invalidated_by(close=close)
    ):
        ineligible_reason = "setup_no_longer_valid"
    touch_recorded = bool(
        template is not None and template.trigger_type.value == "pullback"
        and not retest_touched and not ineligible_reason and (
            low <= (setup.entry_zone_high if setup is not None else intent.trigger_price)
            if intent.direction == "long" else high >= (setup.entry_zone_low if setup is not None else intent.trigger_price)
        )
    )
    retest_confirmed = bool(template is not None and template.trigger_type.value == "pullback" and retest_touched and crossed and not ineligible_reason)
    return TriggerObservation(
        intent.intent_id, at, available_at, crossed, crossed and ineligible_reason is None,
        intent.trigger_price if crossed else None,
        "retest rejection confirmed" if retest_confirmed else "retest boundary touched; awaiting rejection" if touch_recorded else "entry boundary crossed" if crossed else "entry boundary not crossed",
        ineligible_reason, retest_touched=touch_recorded, retest_confirmed=retest_confirmed,
    )


def record_trigger(*, intent: TradeIntent, observation: TriggerObservation) -> TradeIntent:
    if intent.status is not IntentStatus.ARMED:
        raise ValueError("only an armed intent can record a trigger")
    if observation.intent_id != intent.intent_id or not observation.crossed or not observation.eligible:
        raise ValueError("trigger observation does not trigger this intent")
    return replace(intent, status=IntentStatus.TRIGGERED,
                   triggered_at=observation.available_at,
                   trigger_price=observation.price or intent.trigger_price)


def fill_intent(*, intent: TradeIntent, at: datetime | str) -> TradeIntent:
    if intent.status is not IntentStatus.TRIGGERED:
        raise ValueError("only a triggered intent can fill")
    if coerce_timestamp(at, field="intent.filled_at") < coerce_timestamp(intent.triggered_at or at, field="intent.triggered_at"):
        raise ValueError("fill time cannot precede trigger time")
    return replace(intent, status=IntentStatus.FILLED, filled_at=at)


def cancel_intent(*, intent: TradeIntent, reason: str) -> TradeIntent:
    if intent.status in {IntentStatus.FILLED, IntentStatus.CANCELLED}:
        return intent
    return replace(intent, status=IntentStatus.CANCELLED, cancel_reason=reason)
