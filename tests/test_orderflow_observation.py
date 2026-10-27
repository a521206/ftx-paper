from datetime import datetime, timedelta, timezone

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy import observe_market, prepare_replay_input


def _rows(count: int = 6) -> list[MarketBar]:
    instrument = Instrument("NIFTY26JAN", "NSE", "FUTURES", expiry="2026-01-29")
    start = datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)
    return [MarketBar(instrument, start + timedelta(minutes=index), 99, 102, 98, 100 + index, 10) for index in range(count)]


def test_observe_market_publishes_only_complete_parent_aggregates() -> None:
    replay = prepare_replay_input(_rows(6), "2026-01-01", session_partition="09:15-09:21")
    observation = observe_market(replay)

    assert len(observation.context.observations_for("1m")) == 6
    assert len(observation.context.observations_for("5m")) == 1
    assert observation.context.observations_for("5m")[0].available_at == datetime(2026, 1, 1, 3, 50, tzinfo=timezone.utc)
    assert observation.gaps == ()


def test_observe_market_reports_source_gap_and_does_not_fabricate_parent() -> None:
    rows = _rows(6)
    rows.pop(2)
    replay = prepare_replay_input(rows, "2026-01-01", session_partition="09:15-09:21")
    observation = observe_market(replay)

    assert len(observation.gaps) == 1
    assert observation.gaps[0].start == datetime(2026, 1, 1, 3, 47, tzinfo=timezone.utc)
    assert observation.context.observations_for("5m") == ()


def test_market_observation_as_of_filters_future_aggregates() -> None:
    observation = observe_market(prepare_replay_input(_rows(6), "2026-01-01", session_partition="09:15-09:21"))

    as_of = observation.available(datetime(2026, 1, 1, 3, 48, tzinfo=timezone.utc))

    assert len(as_of.observations_for("1m")) == 3
    assert as_of.observations_for("5m") == ()


def test_observe_market_reports_session_edge_gaps() -> None:
    rows = _rows(1)
    observation = observe_market(prepare_replay_input(rows, "2026-01-01", session_partition="09:15-09:20"))

    assert observation.gaps[0].start.isoformat() == "2026-01-01T03:46:00+00:00"
    assert observation.gaps[-1].end.isoformat() == "2026-01-01T03:50:00+00:00"


def test_five_minute_window_does_not_shift_to_delayed_first_row() -> None:
    rows = _rows(6)[1:]
    replay = prepare_replay_input(rows, "2026-01-01", session_partition="09:15-09:21")
    observation = observe_market(replay)

    assert observation.context.observations_for("5m") == ()
    assert observation.gaps[0].start == datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc)


def test_leading_boundary_gap_is_propagated_to_first_minute_features() -> None:
    rows = _rows(6)[1:]
    observation = observe_market(
        prepare_replay_input(rows, "2026-01-01", session_partition="09:15-09:21")
    )

    first = observation.context.observations_for("1m")[0]
    assert first.context is not None
    assert first.context.replay_provenance["gap_status"] == "boundary_gap"
    assert first.features["vwap"].reason == "boundary_gap"
    assert observation.context.observations_for("1m")[1].context is not None
    assert observation.context.observations_for("1m")[1].context.replay_provenance["gap_status"] == "source_gap"
    assert observation.context.observations_for("1m")[1].features["vwap"].reason == "source_gap"
    assert observation.context.observations_for("5m") == ()


def test_parent_lineage_exactly_covers_children_and_selected_source() -> None:
    replay = prepare_replay_input(_rows(5), "2026-01-01", session_partition="09:15-09:20")
    observation = observe_market(replay)
    parent = observation.context.observations_for("5m")[0]
    children = observation.context.observations_for("1m")

    assert parent.context is not None
    assert parent.source_observation_id == parent.observation_id
    assert parent.context.child_observation_ids == tuple(item.observation_id for item in children)
    assert parent.context.child_intervals == tuple(
        (item.interval_start, item.interval_end) for item in children
    )
    assert parent.context.replay_provenance["source_fingerprint"] == replay.metadata.source_fingerprint
    assert parent.interval_start == children[0].interval_start
    assert parent.interval_end == children[-1].interval_end


def test_out_of_session_rows_do_not_create_gaps_or_published_evidence() -> None:
    rows = [
        MarketBar(
            _rows(1)[0].instrument,
            datetime(2026, 1, 1, 3, 44, tzinfo=timezone.utc),
            99, 102, 98, 100, 10,
        ),
        _rows(1)[0],
        MarketBar(
            _rows(1)[0].instrument,
            datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
            99, 102, 98, 100, 10,
        ),
    ]
    observation = observe_market(
        prepare_replay_input(rows, "2026-01-01", session_partition="09:15-09:16")
    )

    assert len(observation.context.observations_for("1m")) == 1
    assert observation.context.observations_for("1m")[0].observed_at == _rows(1)[0].timestamp
    assert observation.gaps == ()


def test_empty_aligned_windows_are_retained_as_incomplete_parent_issues() -> None:
    observation = observe_market(
        prepare_replay_input(_rows(1), "2026-01-01", session_partition="09:15-09:25")
    )

    assert any(
        issue.timeframe == "5m"
        and issue.interval_start == datetime(2026, 1, 1, 3, 50, tzinfo=timezone.utc)
        and issue.reason == "incomplete_session_aligned_parent"
        for issue in observation.issues
    )
