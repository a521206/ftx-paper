from __future__ import annotations

from ftx_paper.strategies.orderflow.features import daily_atr


def _rows(session_date: str, offset: float) -> list[dict[str, object]]:
    return [
        {"time": "09:15", "open": offset, "high": offset + 2, "low": offset - 1, "close": offset + 1},
        {"time": "11:00", "open": offset + 1, "high": offset + 3, "low": offset, "close": offset + 2},
        {"time": "13:00", "open": offset + 2, "high": offset + 4, "low": offset + 1, "close": offset + 3},
        {"time": "14:59", "open": offset + 3, "high": offset + 5, "low": offset + 2, "close": offset + 4},
    ]


def test_daily_atr_uses_wilder_smoothing_after_fourteen_completed_sessions() -> None:
    rows = {f"2026-09-{index:02d}": _rows(f"2026-09-{index:02d}", float(index)) for index in range(1, 16)}

    assert daily_atr("2026-09-15", rows) == 6.0
    assert daily_atr("2026-09-14", rows) is None


def test_daily_atr_applies_wilder_update_to_later_sessions() -> None:
    rows = {f"2026-09-{index:02d}": _rows(f"2026-09-{index:02d}", float(index)) for index in range(1, 17)}
    rows["2026-09-15"] = [
        {"time": "09:15", "open": 15.0, "high": 35.0, "low": 5.0, "close": 25.0},
        {"time": "11:00", "open": 25.0, "high": 35.0, "low": 5.0, "close": 25.0},
        {"time": "13:00", "open": 25.0, "high": 35.0, "low": 5.0, "close": 25.0},
        {"time": "14:59", "open": 25.0, "high": 35.0, "low": 5.0, "close": 25.0},
    ]

    assert daily_atr("2026-09-16", rows) == 7.714285714285714


def test_daily_atr_uses_full_session_ohlc_not_zone_boundaries() -> None:
    rows = {f"2026-09-{index:02d}": _rows(f"2026-09-{index:02d}", float(index)) for index in range(1, 16)}
    rows["2026-09-14"].append(
        {"time": "15:20", "open": 18.0, "high": 30.0, "low": 0.0, "close": 20.0},
    )

    assert daily_atr("2026-09-15", rows) == 7.714285714285714
