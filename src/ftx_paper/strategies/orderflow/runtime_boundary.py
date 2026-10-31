"""Phase 2 runtime boundary for the orderflow port.

This module deliberately contains no orderflow policy.  It owns only the
host-neutral transition envelope that later canonical layers will consume.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any, TypeAlias, cast

from .timestamps import require_utc, parse_utc, serialize_utc


JSONScalar: TypeAlias = None | bool | int | float | str
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
FrozenJSONValue: TypeAlias = JSONScalar | tuple["FrozenJSONValue", ...] | Mapping[str, "FrozenJSONValue"]


_utc = require_utc


def _timestamp(value: datetime) -> str:
    return serialize_utc(value)


def _freeze(value: JSONValue) -> FrozenJSONValue:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("boundary values must contain finite JSON numbers")
        return value
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    raise TypeError("boundary values must be JSON-compatible")


def _thaw(value: FrozenJSONValue) -> JSONValue:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _freeze_mapping(values: Mapping[str, JSONValue]) -> Mapping[str, FrozenJSONValue]:
    frozen = _freeze(dict(values))
    if not isinstance(frozen, Mapping):
        raise TypeError("boundary values must be a JSON object")
    return frozen


@dataclass(frozen=True, slots=True)
class CompletedObservation:
    """One completed, causally available observation at the host boundary."""

    identity: str
    observed_at: datetime
    available_at: datetime
    values: Mapping[str, FrozenJSONValue] = field(default_factory=dict)
    lineage: tuple[str, ...] = ()
    unavailable: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.identity:
            raise ValueError("observation identity is required")
        observed = _utc(self.observed_at)
        available = _utc(self.available_at)
        if available < observed:
            raise ValueError("observation availability cannot precede observation")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "values", _freeze_mapping(cast(Mapping[str, JSONValue], self.values)))
        object.__setattr__(self, "lineage", tuple(self.lineage))
        object.__setattr__(self, "unavailable", tuple(self.unavailable))


@dataclass(frozen=True, slots=True)
class BoundaryEvent:
    """Paper boundary event envelope; values are never inferred by Paper."""

    kind: str
    occurred_at: datetime
    observed_at: datetime
    available_at: datetime
    sequence: int
    identity: str
    causal_parent_id: str | None
    values: Mapping[str, FrozenJSONValue]
    unavailable: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        observed = _utc(self.observed_at)
        available = _utc(self.available_at)
        occurred = _utc(self.occurred_at)
        if available < observed:
            raise ValueError("event availability cannot precede observation")
        if self.sequence < 0 or not self.kind or not self.identity:
            raise ValueError("event kind, identity, and non-negative sequence are required")
        object.__setattr__(self, "occurred_at", occurred)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "values", _freeze_mapping(cast(Mapping[str, JSONValue], self.values)))
        object.__setattr__(self, "unavailable", tuple(self.unavailable))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "occurred_at": _timestamp(self.occurred_at),
            "observed_at": _timestamp(self.observed_at),
            "available_at": _timestamp(self.available_at),
            "sequence": self.sequence,
            "identity": self.identity,
            "causal_parent_id": self.causal_parent_id,
            "values": _thaw(self.values),
            "unavailable": list(self.unavailable),
        }


@dataclass(frozen=True, slots=True)
class BoundaryState:
    """Serializable state container required for bounded recovery."""

    schema_version: int = 1
    input_prefix: tuple[str, ...] = ()
    events: tuple[BoundaryEvent, ...] = ()
    last_available_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported canonical state snapshot schema")
        if len(self.input_prefix) != len(self.events):
            raise ValueError("canonical state prefix and events must have equal length")
        if len(set(self.input_prefix)) != len(self.input_prefix):
            raise ValueError("canonical state input_prefix must be unique")
        if tuple(event.sequence for event in self.events) != tuple(range(len(self.events))):
            raise ValueError("canonical state event sequences must be contiguous")
        if tuple(event.identity for event in self.events) != self.input_prefix:
            raise ValueError("canonical state identities do not match input_prefix")
        expected_last = self.events[-1].available_at if self.events else None
        if self.last_available_at != expected_last:
            raise ValueError("canonical state last_available_at does not match events")

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "input_prefix": list(self.input_prefix),
            "events": [event.to_dict() for event in self.events],
            "last_available_at": (
                _timestamp(self.last_available_at) if self.last_available_at else None
            ),
        }

    @property
    def snapshot_sha256(self) -> str:
        payload = json.dumps(self.snapshot(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, Any]) -> "BoundaryState":
        if type(snapshot.get("schema_version")) is not int or snapshot["schema_version"] != 1:
            raise ValueError("unsupported canonical state snapshot schema")
        # Phase 2 recovery restores the boundary cursor and event prefix.  Later
        # policy layers extend this shape without changing host ownership.
        raw_events = snapshot.get("events", ())
        raw_prefix = snapshot.get("input_prefix", ())
        if not isinstance(raw_prefix, list) or not all(isinstance(item, str) for item in raw_prefix):
            raise ValueError("canonical snapshot input_prefix must be a list of strings")
        if not isinstance(raw_events, list) or len(raw_prefix) != len(raw_events):
            raise ValueError("canonical snapshot events must be a list")
        events = tuple(_event_from_dict(item) for item in raw_events)
        last = snapshot.get("last_available_at")
        if last is not None and not isinstance(last, str):
            raise ValueError("canonical snapshot last_available_at must be a string or null")
        parsed_last = parse_utc(last) if last else None
        return cls(1, tuple(raw_prefix), events, parsed_last)


@dataclass(frozen=True, slots=True)
class RuntimeBoundaryArtifact:
    """Versioned Phase 2 artifact for replay, incremental, and recovery runs."""

    run_id: str
    input_prefix: Mapping[str, Any]
    replay: Mapping[str, Any]
    incremental: Mapping[str, Any]
    recovery: Mapping[str, Any]
    coverage: Mapping[str, Any]
    artifact_type: str = "orderflow_runtime_boundary"
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "schema_version": self.schema_version,
            "contract": {"id": "orderflow_runtime_boundary", "version": 1},
            "run_id": self.run_id,
            "input_prefix": _thaw(self.input_prefix),
            "replay": _thaw(self.replay),
            "incremental": _thaw(self.incremental),
            "recovery": _thaw(self.recovery),
            "coverage": _thaw(self.coverage),
        }


def _event_from_dict(value: Any) -> BoundaryEvent:
    if not isinstance(value, Mapping):
        raise ValueError("canonical snapshot contains an invalid event")
    required = ("kind", "occurred_at", "observed_at", "available_at", "sequence", "identity")
    if any(key not in value for key in required):
        raise ValueError("canonical snapshot event is missing required fields")
    if not all(isinstance(value[key], str) for key in required if key != "sequence"):
        raise ValueError("canonical snapshot event string fields are invalid")
    if type(value["sequence"]) is not int or value["sequence"] < 0:
        raise ValueError("canonical snapshot event sequence must be a non-negative integer")
    raw_values = value.get("values", {})
    if not isinstance(raw_values, dict):
        raise ValueError("canonical snapshot event values must be an object")
    raw_unavailable = value.get("unavailable", [])
    if not isinstance(raw_unavailable, list) or not all(isinstance(item, str) for item in raw_unavailable):
        raise ValueError("canonical snapshot event unavailable must be a list of strings")
    return BoundaryEvent(
        kind=value["kind"],
        occurred_at=parse_utc(value["occurred_at"]),
        observed_at=parse_utc(value["observed_at"]),
        available_at=parse_utc(value["available_at"]),
        sequence=value["sequence"], identity=value["identity"],
        causal_parent_id=value.get("causal_parent_id"),
        values=cast(Mapping[str, FrozenJSONValue], raw_values),
        unavailable=tuple(raw_unavailable),
    )


def transition_one(state: BoundaryState, observation: CompletedObservation) -> BoundaryState:
    """Apply exactly one completed observation, idempotently and in order."""
    if observation.identity in state.input_prefix:
        prior = next(event for event in state.events if event.identity == observation.identity)
        expected_parent = observation.lineage[-1] if observation.lineage else None
        if (
            prior.observed_at != observation.observed_at
            or prior.available_at != observation.available_at
            or prior.causal_parent_id != expected_parent
            or dict(prior.values) != dict(observation.values)
            or prior.unavailable != observation.unavailable
        ):
            raise ValueError("duplicate observation identity has conflicting evidence")
        return state
    if state.last_available_at is not None and observation.available_at < state.last_available_at:
        raise ValueError("observations must be submitted in available timestamp order")
    event = BoundaryEvent(
        kind="source_observation", occurred_at=observation.observed_at,
        observed_at=observation.observed_at, available_at=observation.available_at,
        sequence=len(state.events), identity=observation.identity,
        causal_parent_id=observation.lineage[-1] if observation.lineage else None,
        values=dict(observation.values), unavailable=observation.unavailable,
    )
    return BoundaryState(
        input_prefix=(*state.input_prefix, observation.identity),
        events=(*state.events, event), last_available_at=observation.available_at,
    )


def _execution(state: BoundaryState) -> dict[str, Any]:
    event_payload = [event.to_dict() for event in state.events]
    event_json = json.dumps(event_payload, sort_keys=True, separators=(",", ":"))
    return {
        "state_snapshot_sha256": state.snapshot_sha256,
        "event_prefix_sha256": hashlib.sha256(event_json.encode("utf-8")).hexdigest(),
        "last_available_at": (
            _timestamp(state.last_available_at) if state.last_available_at else None
        ),
        "status": "MATCHED",
    }


def build_runtime_boundary_artifact(
    observations: Iterable[CompletedObservation], *, run_id: str = "paper-runtime-boundary",
) -> dict[str, Any]:
    """Run all Phase 2 input modes through the one-observation boundary."""
    items = tuple(observations)
    replay = ReplayInputAdapter()
    replay.process(items)
    incremental = LiveInputAdapter()
    for item in items:
        incremental.on_completed_observation(item)
    split = len(items) // 2
    recovery_prefix = ReplayInputAdapter()
    recovery_prefix.process(items[:split])
    recovery = LiveInputAdapter(BoundaryState.from_snapshot(recovery_prefix.state.snapshot()))
    for item in items[split:]:
        recovery.on_completed_observation(item)
    executions = {
        "replay": _execution(replay.state),
        "incremental": _execution(incremental.state),
        "recovery": _execution(recovery.state),
    }
    reference = executions["replay"]
    for execution in executions.values():
        execution["status"] = "MATCHED" if execution == reference else "MISMATCHED"
    ordered_ids = [item.identity for item in items]
    input_prefix = {
        "identity": hashlib.sha256(
            json.dumps(ordered_ids, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "observation_count": len(items),
        "last_available_at": executions["replay"]["last_available_at"],
    }
    return RuntimeBoundaryArtifact(
        run_id=run_id,
        input_prefix=input_prefix,
        replay=executions["replay"],
        incremental=executions["incremental"],
        recovery=executions["recovery"],
        coverage={"complete": True, "observation_count": len(items)},
    ).to_dict()


class LiveInputAdapter:
    """Incremental adapter: one completed observation per call."""

    def __init__(self, state: BoundaryState | None = None) -> None:
        self.state = state or BoundaryState()

    def on_completed_observation(self, observation: CompletedObservation) -> BoundaryState:
        self.state = transition_one(self.state, observation)
        return self.state


class ReplayInputAdapter:
    """Bounded stored-input adapter using the same transition boundary."""

    def __init__(self, state: BoundaryState | None = None) -> None:
        self.state = state or BoundaryState()

    def process(self, observations: Iterable[CompletedObservation]) -> BoundaryState:
        items = tuple(observations)
        for previous, current in zip(items, items[1:], strict=False):
            if current.available_at < previous.available_at:
                raise ValueError("replay input must be timestamp ordered")
        for observation in items:
            self.state = transition_one(self.state, observation)
        return self.state


__all__ = [
    "BoundaryEvent", "BoundaryState", "CompletedObservation",
    "RuntimeBoundaryArtifact", "build_runtime_boundary_artifact",
    "LiveInputAdapter", "ReplayInputAdapter", "transition_one",
]
