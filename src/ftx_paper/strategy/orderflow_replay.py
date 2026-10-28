"""Causal per-bar replay boundary for the diagnostic orderflow layers."""

from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass, field
from enum import StrEnum
from collections.abc import Mapping, Sequence

from .orderflow_capture import CaptureMode, ReplayRecord, ReplayRecorder
from .orderflow_observation import MarketObservation


class ReplayPhase(StrEnum):
    REFERENCE_PUBLICATION = "reference_publication"
    EXISTING_MANAGEMENT = "existing_management"
    PERMISSION_REFRESH = "permission_refresh"
    INTERACTIONS = "interactions"
    TIMEOUT = "timeout"
    FORMATION = "formation"
    ADMISSION = "admission"
    TRIGGER = "trigger"
    ARBITRATION = "arbitration"
    RECORDING = "recording"


CANONICAL_PHASE_ORDER = tuple(ReplayPhase)

_EXECUTION_PHASE_TO_REPLAY_PHASE = {
    "reference_publication": ReplayPhase.REFERENCE_PUBLICATION,
    "existing_position_management": ReplayPhase.EXISTING_MANAGEMENT,
    "permission_and_gap": ReplayPhase.PERMISSION_REFRESH,
    "interaction_advancement": ReplayPhase.INTERACTIONS,
    "opportunity_timeout": ReplayPhase.TIMEOUT,
    "opportunity_formation": ReplayPhase.FORMATION,
    "proposal_arbitration": ReplayPhase.ARBITRATION,
    "execution_settlement": ReplayPhase.TRIGGER,
    "event_recording": ReplayPhase.RECORDING,
}


@dataclass(frozen=True, slots=True)
class ReplayBarResult:
    sequence: int
    timestamp: datetime
    available_observation_ids: tuple[str, ...]
    phases: tuple[ReplayPhase, ...]
    admission_suspended: bool = False


@dataclass(slots=True)
class OrderflowRuntimeState:
    """Single owner for mutable causal replay lifecycle state."""

    context: object | None = None
    replay_records: list[ReplayRecord] = field(default_factory=list)
    phase_history: list[tuple[ReplayPhase, ...]] = field(default_factory=list)


class ReplayMachine:
    """Own one causal observation and capture boundary.

    Execution remains owned by the runtime execution coordinator. The machine
    records the phase results that coordinator actually produced.
    """

    def __init__(
        self, observation: MarketObservation,
        *, capture_mode: CaptureMode | str = CaptureMode.FULL,
    ) -> None:
        self.observation = observation
        self.recorder = ReplayRecorder(capture_mode)
        self.state = OrderflowRuntimeState(context=observation.context)
        self._last_timestamp: datetime | None = None
        self._admission_suspended = False

    def process_bar(
        self,
        timestamp: datetime,
        *,
        available_at: datetime | None = None,
        before_state: Mapping[str, object],
        after_state: Mapping[str, object],
        decisions: tuple[str, ...] = (),
        aggregates: tuple[Mapping[str, object], ...] = (),
        predicates: tuple[Mapping[str, object], ...] = (),
        events: tuple[Mapping[str, object], ...] = (),
        transitions: tuple[Mapping[str, object], ...] = (),
        unavailable: tuple[Mapping[str, object], ...] = (),
        checkpoints: tuple[Mapping[str, object], ...] = (),
        scope: Mapping[str, object] | None = None,
        phase_results: Sequence[Mapping[str, object]] = (),
    ) -> ReplayBarResult:
        if self._last_timestamp is not None and timestamp <= self._last_timestamp:
            raise ValueError("replay bars must be strictly chronological")
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("replay bar timestamps must be timezone-aware")
        if available_at is not None and (available_at.tzinfo is None or available_at.utcoffset() is None):
            raise ValueError("replay availability timestamps must be timezone-aware")
        if available_at is not None and available_at < timestamp:
            raise ValueError("replay availability cannot precede the decision timestamp")
        context = self.observation.available(timestamp)
        if any(gap.start <= timestamp for gap in self.observation.gaps):
            self._admission_suspended = True
        recovery = tuple(
            item for item in context.observations_for("5m")
            if item.available_at <= timestamp
            and item.context is not None
            and item.context.replay_provenance.get("gap_status") == "none"
            and any(gap.end <= item.available_at for gap in self.observation.gaps)
        )
        if recovery:
            self._admission_suspended = False
        admission_suspended = self._admission_suspended
        # Suspension gates admission only; completed evidence remains visible
        # for diagnostics and recovery reasoning.
        available_ids = tuple(item.observation_id for item in context.observations)
        phases = (
            tuple(
                _EXECUTION_PHASE_TO_REPLAY_PHASE[str(item["phase"])]
                for item in phase_results
                if str(item.get("phase")) in _EXECUTION_PHASE_TO_REPLAY_PHASE
            )
            or CANONICAL_PHASE_ORDER
        )
        self.state.phase_history.append(phases)
        sequence = len(self.recorder.records) + 1
        self.recorder.record(ReplayRecord(
            sequence, timestamp, available_at or timestamp, before_state, after_state,
            available_ids, decisions, aggregates, predicates, events, transitions, unavailable,
            checkpoints=(*checkpoints, *phase_results), scope=scope or {},
        ))
        self.state.replay_records = list(self.recorder.records)
        self._last_timestamp = timestamp
        return ReplayBarResult(sequence, timestamp, available_ids, phases, admission_suspended)


__all__ = [
    "CANONICAL_PHASE_ORDER", "OrderflowRuntimeState", "ReplayBarResult",
    "ReplayMachine", "ReplayPhase",
]
