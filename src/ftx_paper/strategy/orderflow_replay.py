"""Causal per-bar replay boundary for the diagnostic orderflow layers."""

from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass, field
from enum import StrEnum
from collections.abc import Mapping, Sequence

from .orderflow_capture import CaptureMode, ReplayRecord, ReplayRecorder, detached_asdict
from .orderflow_observation import MarketObservation


class ReplayPhase(StrEnum):
    REFERENCE_PUBLICATION = "reference_publication"
    EXISTING_MANAGEMENT = "existing_management"
    PERMISSION_REFRESH = "permission_refresh"
    INTERACTIONS = "interactions"
    TIMEOUT = "timeout"
    FORMATION = "formation"
    ARBITRATION = "arbitration"
    TRIGGER = "trigger"
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

    def process_bar(
        self,
        timestamp: datetime,
        *,
        available_at: datetime,
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
        if available_at.tzinfo is None or available_at.utcoffset() is None:
            raise ValueError("replay availability timestamps must be timezone-aware")
        if available_at < timestamp:
            raise ValueError("replay availability cannot precede the decision timestamp")
        context = self.observation.available(timestamp)
        available_ids = tuple(item.observation_id for item in context.observations)
        if self.recorder.capture_mode is CaptureMode.SUMMARY:
            captured_aggregates = ()
            captured_unavailable = ()
        else:
            captured_aggregates = aggregates or tuple(
                detached_asdict(item) for item in context.observations
            )
            captured_unavailable = unavailable or tuple(
                dict(issue) for issue in context.source_issues
            )
        phases = tuple(
            _EXECUTION_PHASE_TO_REPLAY_PHASE[str(item.get("phase"))]
            for item in phase_results
            if str(item.get("phase")) in _EXECUTION_PHASE_TO_REPLAY_PHASE
        )
        if phases != CANONICAL_PHASE_ORDER:
            raise ValueError(
                "replay phases must exactly match canonical causal order"
            )
        self.state.phase_history.append(phases)
        sequence = len(self.recorder.records) + 1
        self.recorder.record(ReplayRecord(
            sequence, timestamp, available_at, before_state, after_state,
            available_ids, decisions, captured_aggregates, predicates, events, transitions,
            captured_unavailable,
            checkpoints=(*checkpoints, *phase_results), scope=scope or {},
        ))
        self.state.replay_records = list(self.recorder.records)
        self._last_timestamp = timestamp
        return ReplayBarResult(sequence, timestamp, available_ids, phases)


__all__ = [
    "CANONICAL_PHASE_ORDER", "OrderflowRuntimeState", "ReplayBarResult",
    "ReplayMachine", "ReplayPhase",
]
