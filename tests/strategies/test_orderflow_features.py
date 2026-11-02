from datetime import date, timedelta

from ftx_paper.strategies.orderflow.features import daily_atr, daily_atr_cache
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry
from ftx_paper.strategies.orderflow.transition import OrderflowTransitionOwner


def test_daily_atr_cache_matches_point_lookup() -> None:
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

    assert cached == {
        session_date: daily_atr(session_date, rows_by_date)
        for session_date in rows_by_date
    }


def test_transition_uses_injected_atr_materialization() -> None:
    transition = OrderflowTransitionOwner(
        LocationRegistry("test"),
        rows_by_date={},
        atr_by_session={"2026-01-01": 12.5},
    )

    assert transition.context.atr_by_session == {"2026-01-01": 12.5}
