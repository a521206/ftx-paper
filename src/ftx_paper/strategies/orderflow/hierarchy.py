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


def arm_intent(*, intent: TradeIntent, at: datetime | str) -> TradeIntent:
    if intent.status not in {IntentStatus.CONFIRMATION_FOUND, IntentStatus.OBSERVING}:
        return intent
    return replace(intent, status=IntentStatus.ARMED, armed_at=at, effective_at=at)


def record_trigger(*, intent: TradeIntent, at: datetime | str, price: float | None = None) -> TradeIntent:
    if intent.status not in {IntentStatus.ARMED, IntentStatus.TRIGGER_OBSERVED}:
        raise ValueError("only an armed intent can trigger")
    return replace(intent, status=IntentStatus.TRIGGERED, triggered_at=at)


def fill_intent(*, intent: TradeIntent, at: datetime | str) -> TradeIntent:
    if intent.status is not IntentStatus.TRIGGERED:
        raise ValueError("only a triggered intent can fill")
    return replace(intent, status=IntentStatus.FILLED, filled_at=at)


def cancel_intent(*, intent: TradeIntent, reason: str) -> TradeIntent:
    if intent.status in {IntentStatus.FILLED, IntentStatus.CANCELLED}:
        return intent
    return replace(intent, status=IntentStatus.CANCELLED, cancel_reason=reason)
