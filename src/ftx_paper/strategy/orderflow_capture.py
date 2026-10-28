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
from typing import cast


class CaptureMode(StrEnum):
    SUMMARY = "summary"
    DIAGNOSTIC = "diagnostic"
    FULL = "full"

REPLAY_RECORD_SCHEMA_VERSION = 2


def validate_replay_scope(scope: Mapping[str, object]) -> dict[str, object]:
    """Validate the frozen replay contract before facts are captured."""
    normalized = deepcopy(dict(scope))
    required = {"source_contract", "input_identity", "requested_sessions", "effective_sessions",
                "vehicles", "strategy_configuration", "counts"}
    missing = sorted(required - normalized.keys())
    if missing:
        raise ValueError("replay scope is missing required fields: " + ", ".join(missing))
    timezone_name = normalized.get("timezone")
    if not isinstance(timezone_name, str) or not timezone_name:
        raise ValueError("replay scope requires a timezone")
    cutoff = normalized.get("decision_clock", {"cutoff": "exclusive"})
    if not isinstance(cutoff, Mapping) or cutoff.get("cutoff", "exclusive") not in {"inclusive", "exclusive"}:
        raise ValueError("replay scope decision cutoff must be inclusive or exclusive")
    counts = normalized.get("counts", {})
    if not isinstance(counts, Mapping):
        raise ValueError("replay scope counts must be an object")
    if any(not isinstance(value, int) or value < 0 for value in counts.values()):
        raise ValueError("replay scope counts must be non-negative integers")
    count_names = {"selected_source_rows", "filtered_source_rows", "completed_source_rows",
                   "gap_rows", "processed_decision_bars"}
    if not count_names.issubset(counts):
        raise ValueError("replay scope counts are incomplete")
    return normalized


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
    # Detached causal checkpoints are replay evidence, not a runtime event bus.
    checkpoints: tuple[Mapping[str, object], ...] = ()
    scope: Mapping[str, object] = MappingProxyType({})

    def __post_init__(self) -> None:
        if self.schema_version not in {1, REPLAY_RECORD_SCHEMA_VERSION}:
            raise ValueError("unsupported replay record schema version")
        if self.sequence < 1 or self.available_at < self.timestamp:
            raise ValueError("invalid replay record timing")
        object.__setattr__(self, "before_state", MappingProxyType(deepcopy(dict(self.before_state))))
        object.__setattr__(self, "after_state", MappingProxyType(deepcopy(dict(self.after_state))))
        for name in ("aggregates", "predicates", "events", "transitions", "unavailable", "checkpoints"):
            object.__setattr__(self, name, tuple(deepcopy(item) for item in getattr(self, name)))
        object.__setattr__(self, "scope", MappingProxyType(validate_replay_scope(self.scope) if self.scope else {}))

    @property
    def record_id(self) -> str:
        payload = json.dumps(
            (self.sequence, self.timestamp.isoformat(), self.available_at.isoformat(), self.observations,
             self.decisions, self.aggregates, self.predicates, self.events,
             self.transitions, self.unavailable, self.checkpoints, self.scope),
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
            "checkpoints": deepcopy(list(self.checkpoints)),
            "scope": deepcopy(dict(self.scope)),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ReplayRecord":
        """Read one versioned detached artifact and reject unknown schemas."""
        schema_version = payload.get("schema_version")
        if schema_version not in {1, REPLAY_RECORD_SCHEMA_VERSION}:
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
            _tuple("unavailable"), REPLAY_RECORD_SCHEMA_VERSION, _tuple("checkpoints"),
            cast(Mapping[str, object], payload.get("scope", {}))
            if isinstance(payload.get("scope", {}), Mapping) else {},
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
                scope=item.scope,
            )
        self._records.append(item)

    @property
    def records(self) -> tuple[ReplayRecord, ...]:
        return tuple(self._records)


__all__ = ["CaptureMode", "REPLAY_RECORD_SCHEMA_VERSION", "ReplayRecord", "ReplayRecorder", "validate_replay_scope"]
