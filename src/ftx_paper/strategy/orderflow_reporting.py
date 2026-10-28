"""Reports derived only from detached causal replay records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone

from .orderflow_capture import ReplayRecord

CAUSAL_CHECKPOINT_PHASES = (
    "source_row", "completed_observation", "context_publication", "location_publication",
    "interaction_transition", "response_hypothesis_transition", "opportunity_transition",
    "admission_predicate", "trigger_observation", "execution_settlement",
)


def causal_actions(records: tuple[ReplayRecord, ...]) -> tuple[Mapping[str, object], ...]:
    """Project recorded decisions without consulting future or live state."""
    return tuple(
        {
            "sequence": record.sequence,
            "timestamp": record.timestamp.isoformat(),
            "available_at": record.available_at.isoformat(),
            "observations": record.observations,
            "decisions": record.decisions,
            "aggregates": record.aggregates,
            "predicates": record.predicates,
            "events": record.events,
            "transitions": record.transitions,
            "unavailable": record.unavailable,
            "checkpoints": record.checkpoints,
            "scope": record.scope,
            "before_state": record.before_state,
            "after_state": record.after_state,
        }
        for record in records
    )


def build_session_summary(records: tuple[ReplayRecord, ...]) -> Mapping[str, object]:
    """Build a bounded diagnostic summary from recorded facts."""
    ordered_records = tuple(sorted(records, key=lambda item: (item.sequence, item.timestamp, item.record_id)))
    return {
        "schema_version": 1,
        "record_count": len(ordered_records),
        "first_timestamp": ordered_records[0].timestamp.isoformat() if ordered_records else None,
        "last_timestamp": ordered_records[-1].timestamp.isoformat() if ordered_records else None,
        "decision_count": sum(len(record.decisions) for record in ordered_records),
        "observation_count": sum(len(record.observations) for record in ordered_records),
        "checkpoint_count": sum(len(record.checkpoints) for record in ordered_records),
        "actions": causal_actions(ordered_records),
    }


def build_multi_session_report(
    sessions: Mapping[str, tuple[ReplayRecord, ...]],
) -> Mapping[str, object]:
    """Aggregate detached session facts in deterministic date/key order."""
    ordered = sorted(sessions.items(), key=lambda item: item[0])
    return {
        "schema_version": 1,
        "sessions": tuple(
            {"session_date": session_date, **dict(build_session_summary(records))}
            for session_date, records in ordered
        ),
    }


def first_divergent_phase(
    left: tuple[ReplayRecord, ...], right: tuple[ReplayRecord, ...],
) -> str | None:
    """Return the first causal checkpoint phase that differs, without trades."""
    left_facts = [fact for record in left for fact in record.checkpoints]
    right_facts = [fact for record in right for fact in record.checkpoints]
    for index in range(max(len(left_facts), len(right_facts))):
        lhs = left_facts[index] if index < len(left_facts) else None
        rhs = right_facts[index] if index < len(right_facts) else None
        if lhs != rhs:
            return str((lhs or rhs or {}).get("phase", "unknown"))
    return None


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


__all__ = ["CAUSAL_CHECKPOINT_PHASES", "build_multi_session_report", "build_session_summary",
           "causal_actions", "detached_replay_records", "first_divergent_phase"]
