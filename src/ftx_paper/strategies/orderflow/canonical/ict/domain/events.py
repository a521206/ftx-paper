"""Observed events, kept separate from established state and hypotheses."""

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping

from ftx_paper.strategies.orderflow.canonical.ict.domain.time import require_order


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported mutable event payload value: {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class ObservedEvent:
    event_id: str
    kind: str
    observed_at: datetime
    available_at: datetime
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.event_id or not self.kind:
            raise ValueError("observed events require identity and kind")
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="event")
        object.__setattr__(self, "payload", _freeze(self.payload))
