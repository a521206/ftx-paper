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
