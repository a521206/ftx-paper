from ftx_paper.infrastructure.sqlite.daily_profile_repository import SqliteDailyProfileRepository


def test_daily_profile_repository_round_trips_and_updates(tmp_path) -> None:
    repository = SqliteDailyProfileRepository(tmp_path / "runtime.sqlite3")
    payload = {
        "snapshot_id": "vp:daily:2026-09-29:abc",
        "instrument": "NIFTY",
        "timeframe_minutes": 1,
        "session_date": "2026-09-29",
        "poc": 100.0,
        "vah": 101.0,
        "val": 99.0,
        "vwap": 100.25,
        "window_first_bar_ts": "2026-09-29T09:15:00+05:30",
        "window_last_bar_ts": "2026-09-29T15:29:00+05:30",
        "bar_count": 375,
        "total_volume": 1234.0,
        "gap_count": 0,
        "quality": "complete",
        "observed_at": "2026-09-29T15:29:00+05:30",
        "available_at": "2026-09-29T15:30:00+05:30",
        "config_fingerprint": "config",
        "profile_version": "value_profile.v1",
        "aggregate_source_fingerprint": "source",
    }

    repository.save(payload)
    assert repository.load(
        instrument="NIFTY",
        timeframe_minutes=1,
        config_fingerprint="config",
        session_date="2026-09-29",
    ) == payload

    updated = {**payload, "poc": 100.5}
    repository.save(updated)
    assert repository.load_all(
        instrument="NIFTY",
        timeframe_minutes=1,
        config_fingerprint="config",
    ) == [updated]
