"""Detached causal replay records for diagnostic orderflow evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from collections.abc import Mapping
from copy import deepcopy
from enum import StrEnum
import hashlib
import json


class CaptureMode(StrEnum):
    SUMMARY = "summary"
    DIAGNOSTIC = "diagnostic"
    FULL = "full"


@dataclass(frozen=True, slots=True)
class ReplayRecord:
    sequence: int
    timestamp: datetime
    available_at: datetime
    before_state: Mapping[str, object]
    after_state: Mapping[str, object]
    observations: tuple[str, ...] = ()
    decisions: tuple[str, ...] = ()
    aggregates: tuple[Mapping[str, object], ...] = ()
    predicates: tuple[Mapping[str, object], ...] = ()
    events: tuple[Mapping[str, object], ...] = ()
    transitions: tuple[Mapping[str, object], ...] = ()
    unavailable: tuple[Mapping[str, object], ...] = ()
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported replay record schema version")
        if self.sequence < 1 or self.available_at < self.timestamp:
            raise ValueError("invalid replay record timing")
        object.__setattr__(self, "before_state", MappingProxyType(deepcopy(dict(self.before_state))))
        object.__setattr__(self, "after_state", MappingProxyType(deepcopy(dict(self.after_state))))
        for name in ("aggregates", "predicates", "events", "transitions", "unavailable"):
            object.__setattr__(self, name, tuple(deepcopy(item) for item in getattr(self, name)))

    @property
    def record_id(self) -> str:
        payload = json.dumps(
            (self.sequence, self.timestamp.isoformat(), self.available_at.isoformat(), self.observations,
             self.decisions, self.events, self.transitions),
            sort_keys=True, separators=(",", ":"), default=str,
        )
        return f"replay:{hashlib.sha256(payload.encode()).hexdigest()[:24]}"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version, "record_id": self.record_id,
            "sequence": self.sequence, "timestamp": self.timestamp.isoformat(),
            "available_at": self.available_at.isoformat(),
            "before_state": deepcopy(dict(self.before_state)),
            "after_state": deepcopy(dict(self.after_state)),
            "observations": list(self.observations), "decisions": list(self.decisions),
            "aggregates": deepcopy(list(self.aggregates)), "predicates": deepcopy(list(self.predicates)),
            "events": deepcopy(list(self.events)), "transitions": deepcopy(list(self.transitions)),
            "unavailable": deepcopy(list(self.unavailable)),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ReplayRecord":
        """Read one versioned detached artifact and reject unknown schemas."""
        if payload.get("schema_version") != 1:
            raise ValueError("unsupported replay record schema version")
        required = ("sequence", "timestamp", "available_at", "before_state", "after_state")
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValueError("replay record is missing required fields: " + ", ".join(missing))
        try:
            timestamp = datetime.fromisoformat(str(payload["timestamp"]).replace("Z", "+00:00"))
            available_at = datetime.fromisoformat(str(payload["available_at"]).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("replay record timestamps must be ISO timestamps") from exc
        if timestamp.tzinfo is None or available_at.tzinfo is None:
            raise ValueError("replay record timestamps must be timezone-aware")

        def _tuple(name: str) -> tuple:
            value = payload.get(name, ())
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"replay record {name} must be an array")
            return tuple(deepcopy(value))

        before = payload["before_state"]
        after = payload["after_state"]
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            raise ValueError("replay record states must be objects")
        sequence = payload["sequence"]
        if not isinstance(sequence, int) or isinstance(sequence, bool):
            raise ValueError("replay record sequence must be an integer")
        return cls(
            sequence, timestamp, available_at, before, after,
            _tuple("observations"), _tuple("decisions"), _tuple("aggregates"),
            _tuple("predicates"), _tuple("events"), _tuple("transitions"),
            _tuple("unavailable"), 1,
        )


class ReplayRecorder:
    """Append-only detached capture; records are never exposed mutably."""

    def __init__(self, capture_mode: CaptureMode | str = CaptureMode.FULL) -> None:
        self.capture_mode = CaptureMode(capture_mode)
        self._records: list[ReplayRecord] = []

    def record(self, item: ReplayRecord) -> None:
        if not self._records and item.sequence != 1:
            raise ValueError("replay record sequence must start at one")
        if self._records and item.sequence != self._records[-1].sequence + 1:
            raise ValueError("replay record sequence must increase by one")
        if self._records and item.timestamp <= self._records[-1].timestamp:
            raise ValueError("replay record timestamps must increase")
        if self.capture_mode is CaptureMode.SUMMARY:
            item = ReplayRecord(
                item.sequence, item.timestamp, item.available_at,
                item.before_state, item.after_state, (), item.decisions,
                item.aggregates, item.predicates, item.events, item.transitions, item.unavailable,
            )
        self._records.append(item)

    @property
    def records(self) -> tuple[ReplayRecord, ...]:
        return tuple(self._records)


__all__ = ["CaptureMode", "ReplayRecord", "ReplayRecorder"]
