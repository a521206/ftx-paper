"""Causal context and directional permission contracts for orderflow."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any


class Permission(StrEnum):
    ALLOWED = "allowed"
    CONDITIONAL = "conditional"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class ContextVersionRef:
    context_id: str
    version: str


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
    timestamp: str
    available_at: str
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
