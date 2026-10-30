from __future__ import annotations

from ftx_paper.strategies.orderflow.features import daily_atr, derive_session_levels
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate


def _rows(session_date: str, offset: float) -> list[dict[str, object]]:
    return [
        {"time": "09:15", "open": offset, "high": offset + 2, "low": offset - 1, "close": offset + 1},
        {"time": "11:00", "open": offset + 1, "high": offset + 3, "low": offset, "close": offset + 2},
        {"time": "13:00", "open": offset + 2, "high": offset + 4, "low": offset + 1, "close": offset + 3},
        {"time": "14:59", "open": offset + 3, "high": offset + 5, "low": offset + 2, "close": offset + 4},
    ]


def test_daily_atr_uses_fourteen_completed_prior_sessions() -> None:
    rows = {f"2026-09-{index:02d}": _rows(f"2026-09-{index:02d}", float(index)) for index in range(1, 16)}

    assert daily_atr("2026-09-15", rows) == 6.0
    assert daily_atr("2026-09-14", rows) is None


def test_derive_session_levels_matches_canonical_availability() -> None:
    aggregate = Aggregate(
        "2026-09-15T09:15", "2026-09-15T09:19", "2026-09-15T09:20", "5m", 5,
        100.0, 105.0, 99.0, 104.0, 50, "NIFTY", "", "CONTINUOUS", "2026-09-15", "source",
    )

    levels = derive_session_levels(
        [aggregate], [{"low": 95.0, "high": 110.0}], "2026-09-15",
    )

    assert levels.opening_range_available_at == "2026-09-15T09:20"
    assert levels.prior_day_available_at == "2026-09-15T09:15"
    assert (levels.prior_day_low, levels.prior_day_high) == (95.0, 110.0)
