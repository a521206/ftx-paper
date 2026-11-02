"""Versioned Paper host contracts for canonical orderflow input and output."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import Any

from .runtime_boundary import (
    BoundaryEvent,
    BoundaryState,
    CompletedObservation,
    LiveInputAdapter,
    ReplayInputAdapter,
)
from .timestamps import require_utc, serialize_utc


HOST_ADAPTER_CONTRACT_ID = "orderflow_paper_host_adapter"
HOST_ADAPTER_CONTRACT_VERSION = 1


def _validate_json_object(value: Mapping[str, Any], field_name: str) -> None:
    try:
        json.dumps(value, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a JSON object") from exc


@dataclass(frozen=True, slots=True)
class MarketEvidence:
    """Explicit host evidence; adapters do not infer missing market facts."""

    source_id: str
    observed_at: datetime
    available_at: datetime
    values: Mapping[str, Any] = field(default_factory=dict)
    lineage: tuple[str, ...] = ()
    unavailable: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_id:
            raise ValueError("source_id is required")
        observed = require_utc(self.observed_at, field="observed_at")
        available = require_utc(self.available_at, field="available_at")
        if available < observed:
            raise ValueError("available_at cannot precede observed_at")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "lineage", tuple(self.lineage))
        object.__setattr__(self, "unavailable", tuple(self.unavailable))

    def to_observation(self) -> CompletedObservation:
        return CompletedObservation(
            identity=self.source_id,
            observed_at=self.observed_at,
            available_at=self.available_at,
            values=self.values,
            lineage=self.lineage,
            unavailable=self.unavailable,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "observed_at": serialize_utc(self.observed_at),
            "available_at": serialize_utc(self.available_at),
            "values": dict(self.values),
            "lineage": list(self.lineage),
            "unavailable": list(self.unavailable),
        }


class CanonicalInputAdapter:
    """Shared conversion from explicit Paper evidence to canonical input."""

    @staticmethod
    def observation(evidence: MarketEvidence) -> CompletedObservation:
        return evidence.to_observation()


class LiveHostInputAdapter:
    """Incremental live adapter; it only submits supplied completed evidence."""

    def __init__(self, state: BoundaryState | None = None) -> None:
        self.boundary = LiveInputAdapter(state)

    def publish(self, evidence: MarketEvidence) -> BoundaryState:
        return self.boundary.on_completed_observation(
            CanonicalInputAdapter.observation(evidence)
        )


class ReplayHostInputAdapter:
    """Bounded replay adapter using the same input conversion and boundary."""

    def __init__(self, state: BoundaryState | None = None) -> None:
        self.boundary = ReplayInputAdapter(state)

    def process(self, evidence: Iterable[MarketEvidence]) -> BoundaryState:
        return self.boundary.process(
            CanonicalInputAdapter.observation(item) for item in evidence
        )


@dataclass(frozen=True, slots=True)
class HostOutputEnvelope:
    """Versioned projection of canonical facts for Paper runtime persistence."""

    events: tuple[BoundaryEvent, ...] = ()
    proposals: tuple[Mapping[str, Any], ...] = ()
    snapshot: Mapping[str, Any] = field(default_factory=dict)
    artifact: Mapping[str, Any] = field(default_factory=dict)
    contract_id: str = HOST_ADAPTER_CONTRACT_ID
    contract_version: int = HOST_ADAPTER_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if self.contract_id != HOST_ADAPTER_CONTRACT_ID:
            raise ValueError("unsupported host adapter contract")
        if self.contract_version != HOST_ADAPTER_CONTRACT_VERSION:
            raise ValueError("unsupported host adapter contract version")
        object.__setattr__(self, "events", tuple(self.events))
        object.__setattr__(self, "proposals", tuple(self.proposals))
        for proposal in self.proposals:
            _validate_json_object(proposal, "proposal")
        _validate_json_object(self.snapshot, "snapshot")
        _validate_json_object(self.artifact, "artifact")

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": {
                "id": self.contract_id,
                "version": self.contract_version,
            },
            "events": [event.to_dict() for event in self.events],
            "proposals": [dict(proposal) for proposal in self.proposals],
            "snapshot": dict(self.snapshot),
            "artifact": dict(self.artifact),
        }


class CanonicalOutputAdapter:
    """Project canonical facts into the versioned Paper output envelope."""

    @staticmethod
    def envelope(
        *,
        events: Iterable[BoundaryEvent] = (),
        proposals: Iterable[Mapping[str, Any]] = (),
        snapshot: Mapping[str, Any] | None = None,
        artifact: Mapping[str, Any] | None = None,
    ) -> HostOutputEnvelope:
        return HostOutputEnvelope(
            events=tuple(events),
            proposals=tuple(proposals),
            snapshot={} if snapshot is None else snapshot,
            artifact={} if artifact is None else artifact,
        )


__all__ = [
    "HOST_ADAPTER_CONTRACT_ID",
    "HOST_ADAPTER_CONTRACT_VERSION",
    "CanonicalInputAdapter",
    "CanonicalOutputAdapter",
    "HostOutputEnvelope",
    "LiveHostInputAdapter",
    "MarketEvidence",
    "ReplayHostInputAdapter",
]
