"""Paper replay clock boundary for canonical evidence publications."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable


EvidenceAtClock = Callable[[str], dict[str, Any]]


@dataclass(frozen=True, slots=True)
class ReplayState:
    """Immutable state exposed after one bounded replay prefix."""

    sequence: int
    latest_5m: dict[str, Any] | None
    evidence: dict[str, Any]
    ordinary_context_ref: dict[str, str] | None = None
    context_publications: tuple[dict[str, Any], ...] = ()


class ReplayMachine:
    """Advance replay clocks without deriving strategy or context policy."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        evidence_events: Iterable[tuple[str, dict[str, Any], dict[str, Any] | None]],
        evidence_at_clock: EvidenceAtClock,
        session_date: str,
    ) -> None:
        if not rows:
            raise ValueError("replay session has no rows")
        self._rows = rows
        self._events = tuple(evidence_events)
        self._evidence_at_clock = evidence_at_clock
        self._session_date = session_date
        self._state = ReplayState(
            sequence=-1,
            latest_5m=None,
            evidence={
                "atr": None,
                "locations": [],
                "vwap_measurements": [],
                "prior_value": None,
                "contexts": [],
                "unavailable": ["evidence_not_yet_available"],
                "context_stream_id": None,
                "location_stream_id": None,
            },
        )

    def run(self) -> tuple[list[dict[str, Any]], ReplayState]:
        records: list[dict[str, Any]] = []
        event_index = 0
        current_5m = self._state.latest_5m
        ordinary_context_ref = self._state.ordinary_context_ref
        for index, row in enumerate(self._rows):
            clock = f"{self._session_date}T{str(row['time']).split('T')[-1][:5]}"
            while event_index < len(self._events) and self._events[event_index][0] <= clock:
                _, _, current_5m = self._events[event_index]
                event_index += 1
            evidence = dict(self._evidence_at_clock(clock))
            contexts = [item for item in evidence.get("contexts", ()) if isinstance(item, dict)]
            if contexts:
                candidate = contexts[-1]
                if isinstance(candidate.get("context_id"), str) and isinstance(candidate.get("version"), str):
                    ordinary_context_ref = {
                        "context_id": candidate["context_id"],
                        "version": candidate["version"],
                    }
            records.append(self._record(index, row, evidence, current_5m, ordinary_context_ref))
            self._state = ReplayState(index, current_5m, evidence, ordinary_context_ref, ())
        return records, self._state

    def _record(
        self,
        index: int,
        row: dict[str, Any],
        evidence: dict[str, Any],
        latest_5m: dict[str, Any] | None,
        ordinary_context_ref: dict[str, str] | None,
    ) -> dict[str, Any]:
        timestamp = f"{self._session_date}T{str(row['time']).split('T')[-1][:5]}"
        next_time = (
            f"{self._session_date}T{str(self._rows[index + 1]['time']).split('T')[-1][:5]}"
            if index + 1 < len(self._rows) else timestamp
        )
        return {
            "sequence": index,
            "timestamp": timestamp,
            "available_at": next_time,
            "primary_state_before": "UNAVAILABLE",
            "primary_state_after": "UNAVAILABLE",
            "market_state_before": "UNAVAILABLE",
            "market_state_after": "UNAVAILABLE",
            "predicates": [],
            "events": [],
            "hypotheses": [],
            "structural_windows": [],
            "bar": row,
            "latest_5m": latest_5m,
            "permission": None,
            "setup": None,
            "intent": None,
            "position": None,
            "location_observations": [],
            "setup_opportunities": [],
            "ordinary_context_ref": ordinary_context_ref,
            "capture_scope": "phase3_5_evidence_only",
        }


__all__ = ["ReplayMachine", "ReplayState"]
