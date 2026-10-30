from datetime import datetime, timezone

from ftx_paper.strategies.orderflow.prior_session_value import PriorSessionValueStore, build_prior_session_profile


def _rows(date: str, complete: bool = True) -> list[dict[str, object]]:
    times = [f"{minute // 60:02d}:{minute % 60:02d}" for minute in range(9 * 60 + 15, 15 * 60 + 30)]
    rows = [{"session_date": date, "time": time, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1, "source_fingerprint": time} for time in times]
    if complete:
        rows.append({"session_date": date, "time": "10:00", "high": 101.0, "low": 101.0, "close": 101.0, "volume": 10, "source_fingerprint": "poc"})
    return rows


def test_profile_levels_lineage_and_availability_are_deterministic() -> None:
    profile = build_prior_session_profile(_rows("2026-09-29"), session_date="2026-09-29")
    assert profile is not None and profile.complete
    assert (profile.value_area_low, profile.point_of_control, profile.value_area_high) == (100.0, 100.0, 100.0)
    assert profile.available_at == datetime(2026, 9, 29, 15, 30, tzinfo=timezone.utc)
    assert profile.lineage == build_prior_session_profile(_rows("2026-09-29"), session_date="2026-09-29").lineage


def test_incomplete_profile_is_not_published_and_store_respects_session_open() -> None:
    incomplete = build_prior_session_profile(_rows("2026-09-29", False)[:-1], session_date="2026-09-29")
    assert incomplete is not None and incomplete.reference() is None
    profile = build_prior_session_profile(_rows("2026-09-29"), session_date="2026-09-29")
    store = PriorSessionValueStore([profile])
    reference = store.prior_session_value("2026-09-30")
    assert reference is not None
    assert reference.available_at == datetime(2026, 9, 30, 9, 15, tzinfo=timezone.utc)
    assert store.prior_session_value("2026-09-29") is None


def test_profile_ignores_bars_after_session_end() -> None:
    rows = _rows("2026-09-29")
    rows.append({
        "session_date": "2026-09-29", "time": "15:30",
        "high": 200.0, "low": 200.0, "close": 200.0,
        "volume": 1000, "source_fingerprint": "after-hours",
    })

    profile = build_prior_session_profile(rows, session_date="2026-09-29")

    assert profile is not None and profile.complete
    assert (profile.value_area_low, profile.point_of_control, profile.value_area_high) == (100.0, 100.0, 100.0)
