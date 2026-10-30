"""Causal value-area references shared by replay producers and consumers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


@dataclass(frozen=True, slots=True)
class ValueAreaReference:
    """Immutable value-area geometry published by a replay producer."""

    reference_id: str
    scope: str
    poc: float
    vah: float
    val: float
    source_sessions: tuple[str, ...]
    available_at: str
    fingerprint: str

    def __post_init__(self) -> None:
        if not self.reference_id:
            raise ValueError("value area reference requires reference_id")
        if not self.scope:
            raise ValueError("value area reference requires scope")
        if not self.source_sessions:
            raise ValueError("value area reference requires source_sessions")
        if any(not session for session in self.source_sessions):
            raise ValueError("value area reference source_sessions cannot be empty")
        if self.vah < self.val:
            raise ValueError("value area reference value area is inverted")
        _parse_timestamp(self.available_at)
        if not self.fingerprint:
            raise ValueError("value area reference requires fingerprint")


@dataclass
class ValueAreaReferenceStream:
    """Causal reference history queried by consumers as of a decision time."""

    references: list[ValueAreaReference] = field(default_factory=list)

    def publish(self, reference: ValueAreaReference) -> None:
        existing = next(
            (item for item in self.references if item.reference_id == reference.reference_id),
            None,
        )
        if existing is not None:
            if existing != reference:
                raise ValueError("value reference id cannot be republished with new values")
            return
        if self.references and _parse_timestamp(self.references[-1].available_at) > _parse_timestamp(
            reference.available_at,
        ):
            raise ValueError("value references must be published in causal order")
        self.references.append(reference)

    def latest_as_of(self, scope: str, available_at: str) -> ValueAreaReference | None:
        return max(
            (
                item for item in self.references
                if item.scope == scope
                and _parse_timestamp(item.available_at) <= _parse_timestamp(available_at)
            ),
            key=lambda item: (_parse_timestamp(item.available_at), item.reference_id),
            default=None,
        )
