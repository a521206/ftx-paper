from datetime import date, timedelta

from ftx_paper.strategies.orderflow.features import daily_atr_cache


def test_daily_atr_cache_publishes_prior_session_values() -> None:
    rows_by_date = {}
    start = date(2026, 1, 1)
    for offset in range(18):
        session_date = (start + timedelta(days=offset)).isoformat()
        rows_by_date[session_date] = ({
            "time": f"{session_date}T09:15",
            "high": 100 + offset,
            "low": 90 + offset,
            "close": 95 + offset,
        },)

    cached = daily_atr_cache(rows_by_date)

    assert cached["2026-01-01"] is None
    assert cached["2026-01-15"] == 10.0
