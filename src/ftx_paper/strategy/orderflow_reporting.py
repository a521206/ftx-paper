"""Reports derived only from detached causal replay records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone

from .orderflow_capture import ReplayRecord

def detached_replay_records(
    events: tuple[Mapping[str, object], ...], *, scope: Mapping[str, object],
) -> tuple[ReplayRecord, ...]:
    """Capture the already-published replay facts without recomputation."""
    grouped: dict[datetime, list[Mapping[str, object]]] = {}
    for event in events:
        raw = event.get("occurred_at") or event.get("timestamp") or event.get("decision_at") or event.get("fill_timestamp")
        if raw is None:
            continue
        timestamp = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError("detached replay events require timezone-aware timestamps")
        timestamp = timestamp.astimezone(timezone.utc)
        grouped.setdefault(timestamp, []).append(dict(event))
    records: list[ReplayRecord] = []
    for sequence, timestamp in enumerate(sorted(grouped), start=1):
        facts = tuple(sorted(
            grouped[timestamp],
            key=lambda event: (
                str(event.get("decision_id", "")),
                str(event.get("event_type", "")).upper(),
                repr(sorted(event.items())),
            ),
        ))
        checkpoints = tuple(
            {
                "phase": (
                    "execution_settlement"
                    if str(event.get("event_type", "")).upper() in {"FILL", "EXIT_FILL", "SETTLEMENT", "RESERVATION"}
                    else "trigger_observation"
                    if str(event.get("event_type", "")).upper() in {"TRIGGER", "TRIGGERDECISION"}
                    else "opportunity_transition"
                    if str(event.get("event_type", "")).upper() in {"CANDIDATEDECISION", "OPPORTUNITY"}
                    else "interaction_transition"
                ),
                "status": event.get("outcome") or event.get("status") or "observed",
                "reason": event.get("reason") or event.get("reason_code"),
                "parent_identity": event.get("causal_parent_id") or event.get("decision_id"),
                "observed_at": timestamp.isoformat(),
                "available_at": timestamp.isoformat(),
            }
            for event in facts
        )
        records.append(ReplayRecord(
            sequence, timestamp, timestamp, {}, {},
            events=facts, checkpoints=checkpoints, scope=scope,
        ))
    return tuple(records)


__all__ = ["detached_replay_records"]
