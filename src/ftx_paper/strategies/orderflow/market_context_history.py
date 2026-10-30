"""Artifact-only projection of causal orderflow replay history.

This module consumes a serialized full replay artifact and never re-evaluates a
decision.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any

PROJECTOR_VERSION = "1"


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("artifact mappings must use string keys")
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (str, int, float, bool, bytes)) or value is None:
        return value
    raise TypeError(f"unsupported mutable artifact value: {type(value).__name__}")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("artifact mappings must use string keys")
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_jsonable(item) for item in value)
    return value


def _fingerprint(value: Any) -> str:
    payload = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _timestamp(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an ISO timestamp string")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO timestamp string") from exc


def _timestamp_after(later: Any, earlier: Any, *, later_field: str, earlier_field: str) -> bool:
    try:
        return _timestamp(later, field=later_field) > _timestamp(earlier, field=earlier_field)
    except TypeError as exc:
        raise ValueError("artifact timestamps must use matching timezone formats") from exc


@dataclass(frozen=True, slots=True)
class HistoryProvenance:
    artifact_type: str | None
    schema_version: int | str | None
    date: str | None
    source_fingerprint: str
    artifact_fingerprint: str
    policy_fingerprint: str | None
    capture_mode: str
    timestamp_convention: str | None
    projector_version: str = PROJECTOR_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "schema_version": self.schema_version,
            "date": self.date,
            "source_fingerprint": self.source_fingerprint,
            "artifact_fingerprint": self.artifact_fingerprint,
            "policy_fingerprint": self.policy_fingerprint,
            "capture_mode": self.capture_mode,
            "timestamp_convention": self.timestamp_convention,
            "projector_version": self.projector_version,
        }


@dataclass(frozen=True, slots=True)
class TimelineRow:
    replay_sequence: int
    source_record_index: int
    source_record_ref: str
    candle_id: str
    timeframe: str
    observed_at: str
    available_at: str
    primary_state_before: str | None
    primary_state_after: str | None
    market_state_before: str | None
    market_state_after: str | None
    permission: Mapping[str, Any] | None
    permission_before: Any
    permission_after: Any
    ordinary_context_ref: Mapping[str, Any] | None
    decision_input_context_refs: Mapping[str, Any] | None
    location_observations: tuple[Mapping[str, Any], ...]
    event_ids: tuple[str, ...]
    events: tuple[Mapping[str, Any], ...]
    source_record: Mapping[str, Any]
    run_id: str | None = None
    session_date: str | None = None
    instrument: str | None = None
    candle_time: str | None = None
    candle_end_time: str | None = None
    interaction_state_as_of: Mapping[str, Any] = MappingProxyType({})

    def to_dict(self) -> dict[str, Any]:
        return {
            "replay_sequence": self.replay_sequence,
            "source_record_index": self.source_record_index,
            "source_record_ref": self.source_record_ref,
            "candle_id": self.candle_id,
            "timeframe": self.timeframe,
            "observed_at": self.observed_at,
            "available_at": self.available_at,
            "primary_state_before": self.primary_state_before,
            "primary_state_after": self.primary_state_after,
            "market_state_before": self.market_state_before,
            "market_state_after": self.market_state_after,
            "permission": _jsonable(self.permission),
            "permission_before": _jsonable(self.permission_before),
            "permission_after": _jsonable(self.permission_after),
            "ordinary_context_ref": _jsonable(self.ordinary_context_ref),
            "decision_input_context_refs": _jsonable(self.decision_input_context_refs),
            "location_observations": _jsonable(self.location_observations),
            "event_ids": list(self.event_ids),
            "events": _jsonable(self.events),
            "source_record": _jsonable(self.source_record),
            "run_id": self.run_id,
            "session_date": self.session_date,
            "instrument": self.instrument,
            "candle_time": self.candle_time,
            "candle_end_time": self.candle_end_time,
            "interaction_state_as_of": _jsonable(self.interaction_state_as_of),
        }


@dataclass(frozen=True, slots=True)
class IntervalView:
    field: str
    value: Any
    start_at: str
    end_at: str | None
    source_sequences: tuple[int, ...]
    gap_after: bool = False
    source_start_record_ref: str | None = None
    source_end_record_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "value": _jsonable(self.value),
            "start_at": self.start_at,
            "end_at": self.end_at,
            "source_sequences": list(self.source_sequences),
            "gap_after": self.gap_after,
            "source_start_record_ref": self.source_start_record_ref,
            "source_end_record_ref": self.source_end_record_ref,
        }


@dataclass(frozen=True, slots=True)
class HistoryProjection:
    provenance: HistoryProvenance
    timeline: tuple[TimelineRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "provenance": self.provenance.to_dict(),
            "timeline": [row.to_dict() for row in self.timeline],
        }


@dataclass(frozen=True, slots=True)
class IntervalSpecification:
    field: str
    split_fields: tuple[str, ...] = ()
    location_id: str | None = None


def _records(artifact: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    records = artifact.get("replay_records")
    if records is None:
        raise ValueError("full replay artifact does not contain replay_records")
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise TypeError("replay_records must be a sequence of mappings")
    result = tuple(record for record in records if isinstance(record, Mapping))
    if len(result) != len(records):
        raise TypeError("every replay record must be a mapping")
    return result


def project_history(replay_artifact: Mapping[str, Any]) -> HistoryProjection:
    """Project one serialized full replay artifact without mutating it."""
    if not isinstance(replay_artifact, Mapping):
        raise TypeError("replay_artifact must be a mapping")
    replay_mode = replay_artifact.get("replay_mode")
    capture_mode = replay_artifact.get("capture_mode")
    if replay_mode is None and capture_mode is None:
        raise ValueError("history projection requires explicit full capture metadata")
    if replay_mode is not None and replay_mode != "full":
        raise ValueError("history projection requires a full replay artifact")
    if capture_mode is not None and capture_mode != "full":
        raise ValueError("history projection requires a full replay artifact")
    records = _records(replay_artifact)
    rows: list[TimelineRow] = []
    interaction_state: dict[str, dict[str, Any]] = {}
    interactions_by_id: dict[str, str] = {}
    source_identity = replay_artifact.get("source_identity")
    source_identity_mapping = source_identity if isinstance(source_identity, Mapping) else {}
    source = replay_artifact.get("source")
    source_mapping = source if isinstance(source, Mapping) else {}
    run_id = replay_artifact.get("run_id") or source_identity_mapping.get("run_id")
    session_date = replay_artifact.get("date") or source_identity_mapping.get("session_date")
    instrument = (
        replay_artifact.get("instrument")
        or source_identity_mapping.get("instrument")
        or source_mapping.get("instrument")
        or source_identity_mapping.get("symbol")
    )
    if isinstance(instrument, Sequence) and not isinstance(instrument, (str, bytes)):
        instrument = instrument[0] if instrument else None
    for index, record in enumerate(records):
        if record.get("evaluation", True) is False or record.get("is_evaluation", True) is False:
            raise ValueError(f"replay record {index} is not an authoritative evaluation record")
        if record.get("sequence") is None:
            raise ValueError(f"replay record {index} has no replay sequence")
        if record.get("timestamp") is None or record.get("available_at") is None:
            raise ValueError(f"replay record {index} must declare timestamp and available_at")
        sequence = int(record["sequence"])
        bar = record.get("bar")
        if not isinstance(bar, Mapping):
            raise TypeError(f"replay record {sequence} bar must be a mapping")
        bar_mapping = bar
        candle_id = bar_mapping.get("id") or bar_mapping.get("time")
        if candle_id is None or not str(candle_id):
            raise ValueError(f"replay record {sequence} has no authoritative candle identity")
        if source_mapping.get("evaluation_timeframe") is None:
            raise ValueError("replay artifact must declare evaluation_timeframe")
        timeframe = str(source_mapping["evaluation_timeframe"])
        events_raw = record.get("events", ())
        if not isinstance(events_raw, Sequence) or isinstance(events_raw, (str, bytes)):
            raise TypeError(f"replay record {sequence} events must be a sequence")
        if any(not isinstance(event, Mapping) for event in events_raw):
            raise TypeError(f"replay record {sequence} events must contain mappings")
        events = tuple(_freeze(event) for event in events_raw)
        _interaction_updates(events_raw, interaction_state, interactions_by_id)
        event_ids = tuple(str(event["event_id"]) for event in events if event.get("event_id") is not None)
        if len(event_ids) != len(set(event_ids)):
            raise ValueError(f"replay record {sequence} contains duplicate event IDs")
        context = record.get("ordinary_context_ref")
        same_record_visibility = source_mapping.get("same_record_context_visibility") == "allowed"
        if isinstance(context, Mapping) and context.get("available_at") is not None:
            cutoff = str(record["timestamp"]) if same_record_visibility else str(record["available_at"])
            if _timestamp_after(context["available_at"], cutoff, later_field="context availability", earlier_field="record cutoff"):
                context = None
        permission = record.get("permission")
        permission_before = _freeze(permission.get("before")) if isinstance(permission, Mapping) and "before" in permission else None
        permission_after = _freeze(permission.get("after")) if isinstance(permission, Mapping) and "after" in permission else None
        decision_input_context_refs = record.get("decision_input_context_refs")
        if decision_input_context_refs is not None and not isinstance(decision_input_context_refs, Mapping):
            raise TypeError(f"replay record {sequence} decision input context references must be a mapping")
        location_raw = record.get("location_observations", ())
        if isinstance(location_raw, Mapping):
            location_raw = (location_raw,)
        if not isinstance(location_raw, Sequence) or isinstance(location_raw, (str, bytes)):
            raise TypeError(f"replay record {sequence} location observations must be a sequence")
        if any(not isinstance(item, Mapping) for item in location_raw):
            raise TypeError(f"replay record {sequence} location observations must contain mappings")
        for item in location_raw:
            if item.get("available_at") is None:
                raise ValueError(f"replay record {sequence} location observation has no availability")
            if item.get("observed_at") is None:
                raise ValueError(f"replay record {sequence} location observation has no observation time")
            if _timestamp_after(item["observed_at"], item["available_at"], later_field="location observation", earlier_field="location availability"):
                raise ValueError(f"replay record {sequence} location observation is available before it was observed")
            if _timestamp_after(item["available_at"], record["available_at"], later_field="location availability", earlier_field="record availability"):
                raise ValueError(f"replay record {sequence} contains a future location observation")
            required = {
                "location", "source_aggregate_id", "reference_generation", "band_low", "band_high",
                "open", "high", "low", "close", "close_relation", "observed_at", "available_at",
                "timeframe", "observation_id",
            }
            if item.get("location") == "VWAP":
                required.update({"source_observation_id", "reference_source_aggregate_id", "reference_source_timeframe"})
            missing = sorted(required - set(item))
            if missing:
                raise ValueError(f"replay record {sequence} location observation is missing {', '.join(missing)}")
            timeframe_value = item.get("timeframe")
            if timeframe_value not in {"1m", "5m"}:
                raise ValueError(f"replay record {sequence} has invalid location timeframe")
            child_ids = item.get("child_observation_ids", ())
            if timeframe_value == "5m":
                if not isinstance(child_ids, Sequence) or isinstance(child_ids, (str, bytes)):
                    raise TypeError(f"replay record {sequence} 5m observation children must be a sequence")
                if len(child_ids) != 5:
                    raise ValueError(f"replay record {sequence} 5m observation requires five children")
        observation_keys = tuple((item.get("timeframe"), item["location"], item["source_aggregate_id"], item["reference_generation"]) for item in location_raw)
        if len(observation_keys) != len(set(observation_keys)):
            raise ValueError(f"replay record {sequence} contains duplicate location observations")
        rows.append(TimelineRow(
            replay_sequence=sequence,
            source_record_index=index,
            source_record_ref=f"{replay_artifact.get('date', 'unknown')}:{timeframe}:{candle_id}",
            candle_id=str(candle_id), timeframe=timeframe, observed_at=str(record["timestamp"]),
            available_at=str(record["available_at"]),
            primary_state_before=record.get("primary_state_before"),
            primary_state_after=record.get("primary_state_after"),
            market_state_before=record.get("market_state_before"),
            market_state_after=record.get("market_state_after"),
            permission=_freeze(permission) if isinstance(permission, Mapping) else None,
            permission_before=permission_before, permission_after=permission_after,
            ordinary_context_ref=_freeze(context) if isinstance(context, Mapping) else None,
            decision_input_context_refs=_freeze(decision_input_context_refs) if isinstance(decision_input_context_refs, Mapping) else None,
            location_observations=tuple(_freeze(item) for item in location_raw),
            event_ids=event_ids, events=events, source_record=_freeze(record),
            run_id=str(run_id) if run_id is not None else None,
            session_date=str(session_date) if session_date is not None else None,
            instrument=str(instrument) if instrument is not None else None,
            candle_time=str(bar_mapping.get("time")) if bar_mapping.get("time") is not None else None,
            candle_end_time=str(bar_mapping.get("end_time")) if bar_mapping.get("end_time") is not None else None,
            interaction_state_as_of=_freeze(interaction_state),
        ))
    _validate_observation_graph(rows)
    sequences = tuple(row.replay_sequence for row in rows)
    if sequences != tuple(sorted(sequences)):
        raise ValueError("replay records must be ordered by replay sequence")
    if len(set(sequences)) != len(sequences):
        raise ValueError("replay records must have unique replay sequences")
    candle_keys = tuple((row.timeframe, row.candle_id) for row in rows)
    if len(set(candle_keys)) != len(candle_keys):
        raise ValueError("replay records must map one-to-one to authoritative candles")
    for row in rows:
        observed_at = _timestamp(row.observed_at, field="observation timestamp")
        available_at = _timestamp(row.available_at, field="availability timestamp")
        try:
            causally_available = available_at >= observed_at
        except TypeError as exc:
            raise ValueError("artifact timestamps must use matching timezone formats") from exc
        if not causally_available:
            raise ValueError("replay record is available before it was observed")
    source = replay_artifact.get("source")
    source_mapping = source if isinstance(source, Mapping) else {}
    source_identity = replay_artifact.get("source_identity")
    source_identity_mapping = source_identity if isinstance(source_identity, Mapping) else {}
    policy = replay_artifact.get("policy") or replay_artifact.get("config") or replay_artifact.get("method")
    required = {
        "artifact_type": replay_artifact.get("artifact_type"),
        "schema_version": replay_artifact.get("schema_version"),
        "timestamp_convention": source_mapping.get("timestamp_convention"),
        "source_fingerprint": source_identity_mapping.get("source_fingerprint") or source_mapping.get("source_fingerprint"),
        "policy": policy,
    }
    missing = tuple(name for name, value in required.items() if value is None)
    if missing:
        raise ValueError(f"replay artifact is missing required provenance: {', '.join(missing)}")
    artifact_fingerprint = _fingerprint(replay_artifact)
    return HistoryProjection(
        provenance=HistoryProvenance(
            artifact_type=str(replay_artifact.get("artifact_type")) if replay_artifact.get("artifact_type") is not None else None,
            schema_version=replay_artifact.get("schema_version"),
            date=str(replay_artifact.get("date")) if replay_artifact.get("date") is not None else None,
            source_fingerprint=str(replay_artifact.get("source_identity_fingerprint") or source_identity_mapping.get("source_fingerprint") or source_mapping.get("source_fingerprint") or _fingerprint(source_mapping)),
            artifact_fingerprint=artifact_fingerprint,
            policy_fingerprint=_fingerprint(policy) if policy is not None else None,
            capture_mode=str(capture_mode or replay_mode),
            timestamp_convention=str(source_mapping["timestamp_convention"]) if source_mapping.get("timestamp_convention") is not None else None,
        ),
        timeline=tuple(rows),
    )


def _validate_observation_graph(rows: Sequence[TimelineRow]) -> None:
    """Validate serialized 1m/5m observation references without replaying."""
    observations: dict[str, Mapping[str, Any]] = {}
    parents: list[Mapping[str, Any]] = []
    for row in rows:
        for observation in row.location_observations:
            observation_id = observation.get("observation_id")
            timeframe = observation.get("timeframe")
            if not observation_id or timeframe is None:
                continue
            prior = observations.get(str(observation_id))
            detached = _jsonable(observation)
            if prior is not None and prior != detached:
                raise ValueError(f"observation ID {observation_id!r} has conflicting payloads")
            observations[str(observation_id)] = detached
            if timeframe == "5m":
                parents.append(detached)
    for parent in parents:
        child_ids = parent.get("child_observation_ids", ())
        children: list[Mapping[str, Any]] = []
        for child_id in child_ids:
            child = observations.get(str(child_id))
            if child is None:
                raise ValueError(f"5m observation references missing child {child_id!r}")
            if child.get("timeframe") != "1m":
                raise ValueError("5m observation child must be a 1m observation")
            if child.get("location") != parent.get("location") or child.get("location_id") != parent.get("location_id") or child.get("reference_generation") != parent.get("reference_generation"):
                raise ValueError("5m observation child has mismatched location generation")
            if _timestamp_after(child["available_at"], parent["available_at"], later_field="child availability", earlier_field="parent availability"):
                raise ValueError("5m observation child is not causally available")
            children.append(child)
        if len(children) != 5 or len({str(item["observation_id"]) for item in children}) != 5:
            raise ValueError("5m observation must resolve five distinct children")
        parent_time = _timestamp(parent["source_aggregate_id"], field="parent aggregate")
        expected = tuple((parent_time + timedelta(minutes=index)).strftime("%Y-%m-%dT%H:%M") for index in range(5))
        if tuple(str(item["source_aggregate_id"]) for item in children) != expected:
            raise ValueError("5m observation children do not exactly cover the parent interval")


def project_saved_session(session_artifact: Mapping[str, Any]) -> HistoryProjection:
    """Project the full replay payload embedded in a saved session result."""
    if not isinstance(session_artifact, Mapping):
        raise TypeError("session_artifact must be a mapping")
    replay = session_artifact.get("replay")
    if not isinstance(replay, Mapping):
        raise ValueError("saved session artifact does not contain a full replay payload")
    return project_history(replay)


def _value(row: TimelineRow, field: str, location_id: str | None = None) -> Any:
    if field in {"observed_at", "available_at", "replay_sequence"}:
        return getattr(row, field)
    if field in {"primary_state_before", "primary_state_after", "market_state_before", "market_state_after"}:
        return getattr(row, field)
    if field == "ordinary_context_ref":
        return row.ordinary_context_ref
    if field == "permission":
        return row.permission
    if field == "location_observations":
        return row.location_observations
    if field in {"close_relation", "band_contact", "location", "location_id", "reference_generation"}:
        observations = row.location_observations
        if location_id is not None:
            observations = tuple(item for item in observations if item.get("location") == location_id or item.get("location_id") == location_id)
            if not observations:
                return None
        if len(observations) != 1:
            raise ValueError(f"{field} requires exactly one selected location per row")
        return observations[0].get(field)
    raise ValueError(f"unsupported timeline field: {field}")


def _has_location(row: TimelineRow, location_id: str | None) -> bool:
    if location_id is None:
        return True
    return any(item.get("location") == location_id or item.get("location_id") == location_id for item in row.location_observations)


def _is_available(value: Any) -> bool:
    return value is not None and value != () and value != {}


def _interaction_updates(events: Sequence[Mapping[str, Any]], state: dict[str, dict[str, Any]], interactions_by_id: dict[str, str]) -> None:
    """Apply emitted interaction facts under a location/timeframe/generation key."""
    for event in events:
        interaction_id = event.get("interaction_id")
        interaction_key = str(interaction_id) if interaction_id is not None else None
        prior_key = interactions_by_id.get(interaction_key) if interaction_key is not None else None
        location = event.get("location")
        if location is None and prior_key is not None:
            location = prior_key.split(":", 1)[0]
        if interaction_id is None and location is None:
            continue
        location_name = str(location or event.get("location_id") or "unknown")
        timeframe = event.get("timeframe")
        if timeframe is None and prior_key is not None:
            prior_parts = prior_key.split(":")
            if len(prior_parts) == 3:
                timeframe = prior_parts[1]
        if timeframe is None and interaction_key is not None:
            parts = interaction_key.split(":")
            if len(parts) >= 5 and parts[-2] in {"1m", "5m"}:
                timeframe = parts[-2]
        generation = event.get("reference_generation", "unknown")
        if generation == "unknown" and prior_key is not None:
            prior_parts = prior_key.split(":")
            if len(prior_parts) == 3:
                generation = prior_parts[2]
        key = f"{location_name}:{timeframe or 'unknown'}:{generation}"
        current = dict(state.get(key, {}))
        if interaction_id is not None:
            current["active_interaction_id"] = str(interaction_id)
            interactions_by_id[str(interaction_id)] = key
        current["location"] = location_name
        current["timeframe"] = timeframe or "unknown"
        current["reference_generation"] = generation
        if event.get("kind") == "interaction_created":
            current["active_interaction_status"] = "ACTIVE"
        elif event.get("status") is not None:
            current["active_interaction_status"] = event["status"]
        if event.get("response_status") is not None:
            current["last_response_status"] = event["response_status"]
        if event.get("response_reason") is not None:
            current["last_response_reason"] = event["response_reason"]
        if event.get("kind") in {"interaction_ended", "interaction_terminal", "episode_terminal"}:
            if interaction_key is not None:
                current["last_interaction_id"] = interaction_key
            if current.get("active_interaction_id") == interaction_key:
                current["active_interaction_id"] = None
                current["active_interaction_status"] = "ENDED"
            if event.get("response_status") is not None or event.get("kind") in {"response_confirmed", "response_invalidated"}:
                current["last_response_at"] = event.get("observed_at") or event.get("time")
        state[key] = current


def _expected_step(timeframe: str) -> int | None:
    match = re.fullmatch(r"(\d+)([mhd])", timeframe.lower())
    if match is None:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    return amount * {"m": 60, "h": 3600, "d": 86400}[unit]


def coalesce_intervals(timeline: Sequence[TimelineRow], specification: IntervalSpecification) -> tuple[IntervalView, ...]:
    """Build half-open intervals from timeline rows only."""
    if not timeline:
        return ()
    sequences = tuple(row.replay_sequence for row in timeline)
    if sequences != tuple(sorted(sequences)) or len(set(sequences)) != len(sequences):
        raise ValueError("timeline rows must be ordered by unique replay sequence")
    observed = tuple(_timestamp(row.observed_at, field="observation timestamp") for row in timeline)
    if observed != tuple(sorted(observed)):
        raise ValueError("timeline rows must be ordered by observation timestamp")
    for previous, current in zip(timeline, timeline[1:]):
        if previous.observed_at == current.observed_at and _value(previous, specification.field, specification.location_id) != _value(current, specification.field, specification.location_id):
            raise ValueError("equal-timestamp rows with different values are ambiguous")
    intervals: list[IntervalView] = []
    start = timeline[0]
    current_value = _value(start, specification.field, specification.location_id)
    sequences = [start.replay_sequence]
    for row, next_row in zip(timeline, timeline[1:]):
        current_time = _timestamp(row.observed_at, field="observation timestamp")
        next_time = _timestamp(next_row.observed_at, field="observation timestamp")
        expected_step = _expected_step(row.timeframe)
        grid_gap = expected_step is not None and (next_time - current_time).total_seconds() > expected_step
        split = any(_value(row, field, specification.location_id) != _value(next_row, field, specification.location_id) for field in specification.split_fields)
        changed = _value(row, specification.field, specification.location_id) != _value(next_row, specification.field, specification.location_id)
        gap = _timestamp_after(row.available_at, next_row.observed_at, later_field="availability timestamp", earlier_field="observation timestamp") or grid_gap or (specification.location_id is not None and (not _has_location(row, specification.location_id) or not _has_location(next_row, specification.location_id)))
        if split or changed or gap:
            end_at = next_row.observed_at
            if _is_available(current_value):
                intervals.append(IntervalView(specification.field, current_value, start.observed_at, end_at, tuple(sequences), gap_after=gap, source_start_record_ref=start.source_record_ref, source_end_record_ref=row.source_record_ref))
            start = next_row
            current_value = _value(start, specification.field, specification.location_id)
            sequences = [start.replay_sequence]
        else:
            sequences.append(next_row.replay_sequence)
    if _is_available(current_value):
        intervals.append(IntervalView(specification.field, current_value, start.observed_at, None, tuple(sequences), source_start_record_ref=start.source_record_ref, source_end_record_ref=timeline[-1].source_record_ref))
    return tuple(intervals)
