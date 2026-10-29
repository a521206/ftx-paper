from datetime import datetime, timedelta, timezone

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy import (
    CaptureMode,
    LocationBand,
    LocationRecord,
    LocationRegistry,
    ReplayMachine,
    SourceIdentity,
    observe_market,
    prepare_replay_input,
)
from ftx_paper.contracts import build_decision_view, validate_decision_view


UTC = timezone.utc


def _bar(minute: int, *, close: float = 100.0) -> MarketBar:
    return MarketBar(
        Instrument("NIFTY26JAN", "NSE", "FUTURES", expiry="2026-01-29"),
        datetime(2026, 1, 1, 3, 45, tzinfo=UTC) + timedelta(minutes=minute),
        99.0, 101.0, 98.0, close, 10.0,
    )


def test_source_identity_is_typed_and_scope_safe() -> None:
    identity = SourceIdentity("NIFTY26JAN", "NSE", "FUTURES", "2026-01-29")
    other = SourceIdentity("NIFTY26JAN", "BSE", "FUTURES", "2026-01-29")
    assert identity.key() != other.key()
    replay = prepare_replay_input([_bar(0)], "2026-01-01")
    assert replay.metadata.identity == identity


def test_post_gap_complete_parent_is_recovery_evidence() -> None:
    rows = [_bar(index) for index in range(11) if index != 2]
    replay = prepare_replay_input(rows, "2026-01-01", session_partition="09:15-09:26")
    observation = observe_market(replay)
    parents = observation.context.observations_for("5m")
    assert parents
    assert any(
        item.context is not None
        and item.context.replay_provenance["gap_status"] == "none"
        for item in parents
    )


def test_capture_modes_keep_phase_two_outputs_identical() -> None:
    observation = observe_market(prepare_replay_input(
        [_bar(index) for index in range(5)], "2026-01-01",
        session_partition="09:15-09:20",
    ))
    timestamp = datetime(2026, 1, 1, 3, 50, tzinfo=UTC)
    results = []
    records = []
    for mode in CaptureMode:
        machine = ReplayMachine(observation, capture_mode=mode)
        results.append(machine.process_bar(
            timestamp, before_state={}, after_state={}, decisions=("d",),
            aggregates=({"aggregate": "a"},),
            unavailable=({"unavailable": "u"},),
        ))
        records.append(machine.recorder.records[0])

    assert {result.available_observation_ids for result in results} == {results[0].available_observation_ids}
    assert {record.decisions for record in records} == {records[0].decisions}
    assert all(record.aggregates == records[0].aggregates for record in records)
    assert all(record.unavailable == records[0].unavailable for record in records)
    assert observation.gaps == observe_market(prepare_replay_input(
        [_bar(index) for index in range(5)], "2026-01-01",
        session_partition="09:15-09:20",
    )).gaps
    assert tuple(observation.context.observations) == tuple(
        observe_market(prepare_replay_input(
            [_bar(index) for index in range(5)], "2026-01-01",
            session_partition="09:15-09:20",
        )).context.observations
    )


def test_incomplete_parent_is_diagnosed_without_publishing_children_or_parent() -> None:
    rows = [_bar(index) for index in range(5)]
    rows[2] = MarketBar(rows[2].instrument, rows[2].timestamp, 99.0, 101.0, 98.0, 100.0, None)
    observation = observe_market(prepare_replay_input(
        rows, "2026-01-01", session_partition="09:15-09:20",
    ))

    assert len(observation.context.observations_for("1m")) == 4
    assert observation.context.observations_for("5m") == ()
    assert observation.issues[0].reason == "incomplete_session_aligned_parent"
    assert any(issue.reason == "incomplete_source_row" for issue in observation.issues)
    assert observation.issues[0].observed_child_times == (
        datetime(2026, 1, 1, 3, 45, tzinfo=UTC),
        datetime(2026, 1, 1, 3, 46, tzinfo=UTC),
        datetime(2026, 1, 1, 3, 48, tzinfo=UTC),
        datetime(2026, 1, 1, 3, 49, tzinfo=UTC),
    )


def test_location_boundary_carry_is_once_and_detached() -> None:
    registry = LocationRegistry()
    start = datetime(2026, 1, 1, 9, 15, tzinfo=UTC)
    registry.publish(LocationRecord("or-1", "opening_range", 100.0, LocationBand(99.0, 101.0), 1, start, "agg-1"))
    carried = registry.next_generation(at=start + timedelta(days=1), carry=True)
    assert len(carried) == 1
    assert registry.next_generation(at=start + timedelta(days=1), carry=True) == ()
    assert carried[0].to_dict()["source_aggregate_id"] == "agg-1"


def test_unavailable_decision_preserves_reason_and_availability_boundary() -> None:
    view = build_decision_view({
        "decision_id": "d-unavailable", "decision_at": "2026-01-01T10:00:00+00:00",
        "outcome": "unavailable", "reason": "future_evidence",
        "available_at": "2026-01-01T10:05:00+00:00",
    }, event_type="CANDIDATEDECISION")
    validated = validate_decision_view(view)
    assert validated["policy"]["outcome"] == "unavailable"
    assert validated["policy"]["reason_code"] == "future_evidence"
    assert validated["evidence"]["provenance"]["available_at"] == "2026-01-01T10:05:00+00:00"
