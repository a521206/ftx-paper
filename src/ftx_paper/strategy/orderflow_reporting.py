"""Reports derived only from detached causal replay records."""

from __future__ import annotations

from collections.abc import Mapping

from .orderflow_capture import ReplayRecord


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
            "before_state": record.before_state,
            "after_state": record.after_state,
        }
        for record in records
    )


def build_session_summary(records: tuple[ReplayRecord, ...]) -> Mapping[str, object]:
    """Build a bounded diagnostic summary from recorded facts."""
    return {
        "schema_version": 1,
        "record_count": len(records),
        "first_timestamp": records[0].timestamp.isoformat() if records else None,
        "last_timestamp": records[-1].timestamp.isoformat() if records else None,
        "decision_count": sum(len(record.decisions) for record in records),
        "observation_count": sum(len(record.observations) for record in records),
        "actions": causal_actions(records),
    }


def build_multi_session_report(
    sessions: Mapping[str, tuple[ReplayRecord, ...]],
) -> Mapping[str, object]:
    """Aggregate detached session facts in deterministic date/key order."""
    ordered = sorted(sessions.items(), key=lambda item: item[0])
    return {
        "schema_version": 1,
        "sessions": tuple(
            {"session_date": session_date, **build_session_summary(records)}
            for session_date, records in ordered
        ),
    }


__all__ = ["build_multi_session_report", "build_session_summary", "causal_actions"]
