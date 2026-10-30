"""Reports derived only from detached causal replay records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from ftx_paper.contracts.replay import detach_replay_value

from .orderflow_capture import ReplayRecord


CAUSAL_CHECKPOINT_PHASES = (
    "reference_publication",
    "existing_position_management",
    "permission_and_gap",
    "interaction_advancement",
    "opportunity_timeout",
    "opportunity_formation",
    "proposal_arbitration",
    "trigger_observation",
    "execution_settlement",
    "event_recording",
)


def causal_actions(records: tuple[ReplayRecord, ...]) -> tuple[Mapping[str, object], ...]:
    """Project detached replay records without consulting live or future state."""
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
    """Build a deterministic diagnostic summary from detached facts."""
    ordered = tuple(sorted(records, key=lambda item: (item.sequence, item.timestamp, item.record_id)))
    return {
        "schema_version": 1,
        "record_count": len(ordered),
        "first_timestamp": ordered[0].timestamp.isoformat() if ordered else None,
        "last_timestamp": ordered[-1].timestamp.isoformat() if ordered else None,
        "decision_count": sum(len(record.decisions) for record in ordered),
        "observation_count": sum(len(record.observations) for record in ordered),
        "checkpoint_count": sum(len(record.checkpoints) for record in ordered),
        "actions": causal_actions(ordered),
    }


def build_multi_session_report(
    sessions: Mapping[str, tuple[ReplayRecord, ...]],
) -> Mapping[str, object]:
    """Aggregate detached session facts in deterministic session-date order."""
    return {
        "schema_version": 1,
        "sessions": tuple(
            {"session_date": session_date, **dict(build_session_summary(records))}
            for session_date, records in sorted(sessions.items(), key=lambda item: item[0])
        ),
    }


def first_divergent_phase(
    left: tuple[ReplayRecord, ...], right: tuple[ReplayRecord, ...],
) -> str | None:
    """Return the first differing recorded checkpoint phase."""
    left_facts = [fact for record in left for fact in record.checkpoints]
    right_facts = [fact for record in right for fact in record.checkpoints]
    for index in range(max(len(left_facts), len(right_facts))):
        lhs = left_facts[index] if index < len(left_facts) else None
        rhs = right_facts[index] if index < len(right_facts) else None
        if lhs != rhs:
            return str((lhs or rhs or {}).get("phase", "unknown"))
    return None


def build_replay_summary(
    events: list[dict[str, Any]], trades: list[dict[str, Any]], *, lot_size: float,
) -> dict[str, Any]:
    """Build the Paper replay metrics from already-recorded events and trades."""
    def reason_identities(reason: str, *, identity_fields: tuple[str, ...]) -> set[str]:
        return {
            str(next((event.get(field) for field in identity_fields if event.get(field) is not None), index))
            for index, event in enumerate(events)
            if event.get("reason") == reason
        }

    futures = [trade for trade in trades if trade.get("vehicle") == "futures"]
    gross_pnl_rs = sum(float(trade.get("gross_pnl_rs") or 0.0) for trade in futures)
    risk_reward_values: list[float] = []
    missing_initial_risk = 0
    for trade in futures:
        initial_risk_rs = float(trade.get("initial_risk_rs") or 0.0)
        if initial_risk_rs:
            risk_reward_values.append(float(trade.get("gross_pnl_rs") or 0.0) / initial_risk_rs)
        else:
            missing_initial_risk += 1
    setup_ids = reason_identities(
        "setup_candidate_formed", identity_fields=("opportunity_id", "setup_id", "decision_id"),
    )
    all_interaction_ids = reason_identities(
        "location_interaction_observed", identity_fields=("interaction_id",),
    )
    interaction_events = [
        event for event in events if event.get("reason") == "location_interaction_observed"
    ]
    canonical_locations = {"OR_HIGH", "OR_LOW", "SESSION_HIGH", "SESSION_LOW"}
    structural_interaction_ids = {
        str(event.get("interaction_id") or index)
        for index, event in enumerate(interaction_events)
        if str(event.get("location") or "").upper() in canonical_locations
    }
    interaction_ids_by_timeframe: dict[str, set[str]] = {}
    interaction_ids_by_location: dict[str, set[str]] = {}
    for event in interaction_events:
        timeframe = str(event.get("timeframe") or "unknown")
        location = str(event.get("location") or "unknown").upper()
        interaction_id = str(event.get("interaction_id") or "")
        interaction_ids_by_timeframe.setdefault(timeframe, set()).add(interaction_id)
        interaction_ids_by_location.setdefault(location, set()).add(interaction_id)
    interaction_by_timeframe = {
        timeframe: len(ids) for timeframe, ids in interaction_ids_by_timeframe.items()
    }
    interaction_by_location = {
        location: len(ids) for location, ids in interaction_ids_by_location.items()
    }
    vwap_interaction_ids = interaction_ids_by_location.get("VWAP", set())
    return {
        "setup_candidates": len(setup_ids),
        "location_interactions": len(structural_interaction_ids),
        "total_location_interactions": len(all_interaction_ids),
        "vwap_interactions": len(vwap_interaction_ids),
        "entries_filled": len(futures),
        "gross_reward_points": gross_pnl_rs / lot_size,
        "achieved_risk_reward": round(sum(risk_reward_values), 5) if risk_reward_values else None,
        "metric_lineage": {
            "setup_candidate_event_count": sum(event.get("reason") == "setup_candidate_formed" for event in events),
            "setup_candidate_unique_count": len(setup_ids),
            "location_interaction_event_count": sum(event.get("reason") == "location_interaction_observed" for event in events),
            "location_interaction_unique_count": len(all_interaction_ids),
            "canonical_location_interaction_unique_count": len(structural_interaction_ids),
            "location_interaction_by_timeframe": dict(sorted(interaction_by_timeframe.items())),
            "location_interaction_by_location": dict(sorted(interaction_by_location.items())),
            "futures_trade_count": len(futures),
            "futures_trades_missing_initial_risk": missing_initial_risk,
            "risk_reward_component_count": len(risk_reward_values),
        },
    }


def compare_paired_replay(
    diagnostic: Mapping[str, Any], executable: Mapping[str, Any], *, execution_event_types: frozenset[str],
) -> dict[str, Any]:
    """Compare two detached replay projections without comparing object identity."""
    def decisions(result: Mapping[str, Any]) -> dict[str, tuple[Any, ...]]:
        values: dict[str, tuple[Any, ...]] = {}
        for event in result.get("events", ()):
            if not isinstance(event, Mapping) or not event.get("decision_id"):
                continue
            decision_id = str(event["decision_id"])
            values[decision_id] = tuple(event.get(field) for field in (
                "decision_stage", "outcome", "reason", "response_state", "response_evidence_ids",
                "source_observation_ids", "observed_at", "available_at", "decision_at",
            ))
        return values

    diagnostic_decisions = decisions(diagnostic)
    executable_decisions = decisions(executable)
    mismatches = [
        {"decision_id": decision_id, "diagnostic": diagnostic_decisions.get(decision_id),
         "executable": executable_decisions.get(decision_id)}
        for decision_id in sorted(set(diagnostic_decisions) | set(executable_decisions))
        if diagnostic_decisions.get(decision_id) != executable_decisions.get(decision_id)
    ]

    def lifecycle(result: Mapping[str, Any]) -> tuple[tuple[Any, ...], ...]:
        values: list[tuple[Any, ...]] = []
        for event in result.get("events", ()):
            if not isinstance(event, Mapping):
                continue
            event_type = str(event.get("event_type", "")).upper()
            if event_type not in execution_event_types and event_type not in {"EXIT", "CLOSE"}:
                continue
            values.append(tuple(event.get(field) for field in (
                "event_type", "decision_id", "client_order_id", "entry_decision_id", "outcome",
                "reason", "status", "quantity", "price", "fill_timestamp", "timestamp",
                "available_at", "expected_available_at", "reservation_amount", "gross_pnl", "net_pnl", "costs",
            )))
        return tuple(values)

    def projection(result: Mapping[str, Any], name: str) -> tuple[Any, ...]:
        values = result.get(name, ())
        if not isinstance(values, (list, tuple)):
            return (values,)
        return tuple(detach_replay_value(item) for item in values)

    diagnostic_mutations = bool(diagnostic.get("authoritative_trades") or diagnostic.get("authoritative_ledger"))
    diagnostic_lifecycle, executable_lifecycle = lifecycle(diagnostic), lifecycle(executable)
    projection_mismatches = []
    compare_projections = "execution_scope" in diagnostic and "execution_scope" in executable
    for name in ("shadow_trades", "shadow_ledger", "authoritative_trades", "authoritative_ledger"):
        left, right = projection(diagnostic, name), projection(executable, name)
        if (compare_projections or name.startswith("authoritative_")) and left != right:
            projection_mismatches.append({"projection": name, "diagnostic": left, "executable": right})
    diagnostic_projections = {
        name: projection(diagnostic, name)
        for name in ("shadow_trades", "shadow_ledger", "authoritative_trades", "authoritative_ledger")
        if projection(diagnostic, name)
    }
    result: dict[str, Any] = {
        "matched": not mismatches and not diagnostic_mutations and not diagnostic_lifecycle and not diagnostic_projections,
        "mismatches": mismatches,
        "diagnostic_mutations": diagnostic_mutations,
    }
    if diagnostic_lifecycle != executable_lifecycle:
        result["executable_lifecycle"] = executable_lifecycle
    if projection_mismatches:
        result["executable_projections"] = {
            name: projection(executable, name)
            for name in ("shadow_trades", "shadow_ledger", "authoritative_trades", "authoritative_ledger")
            if projection(executable, name)
        }
    if diagnostic_lifecycle or diagnostic_projections:
        result["unexpected_diagnostic_execution"] = {
            "lifecycle": diagnostic_lifecycle, "projections": diagnostic_projections,
        }
    return result


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


__all__ = [
    "CAUSAL_CHECKPOINT_PHASES", "build_multi_session_report", "build_replay_summary",
    "build_session_summary", "causal_actions", "compare_paired_replay",
    "detached_replay_records", "first_divergent_phase",
]
