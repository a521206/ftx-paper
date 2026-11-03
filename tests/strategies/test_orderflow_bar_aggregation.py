from datetime import datetime, timedelta, timezone

from ftx_paper.strategies.orderflow.canonical.ict.bars import SourceBar, build_completed_bars
from ftx_paper.strategies.orderflow.orderflow_common import build_aggregate_result


def test_missing_in_session_minute_is_ignored() -> None:
    session_zone = timezone(timedelta(hours=5, minutes=30))
    first = datetime(2026, 10, 12, 9, 15, tzinfo=session_zone)
    bars = [
        SourceBar(
            instrument="NIFTY26NOVFUT",
            timestamp=timestamp.astimezone(timezone.utc),
            available_at=(timestamp + timedelta(minutes=1)).astimezone(timezone.utc),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1.0,
            source_id=f"bar-{timestamp:%H%M}",
            session_date="2026-10-12",
        )
        for timestamp in (first, first + timedelta(minutes=2))
    ]

    completed = build_completed_bars(bars, timeframe_minutes=1)

    assert [bar.timestamp for bar in completed] == [
        first.astimezone(timezone.utc),
        (first + timedelta(minutes=2)).astimezone(timezone.utc),
    ]


def test_aggregate_defaults_to_continuous_contract() -> None:
    result = build_aggregate_result(
        [{
            "time": "2026-10-12T09:15",
            "session_date": "2026-10-12",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 10,
        }],
        1,
    )

    assert result.aggregates[0].contract_type == "CONTINUOUS"
