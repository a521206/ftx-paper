from datetime import datetime
from types import SimpleNamespace

from ftx_paper.strategies.orderflow.orderflow_common import candle_reference


def _candle(start: str, end: str) -> SimpleNamespace:
    return SimpleNamespace(
        session_date="2026-09-29",
        timeframe="5m",
        time=datetime.fromisoformat(start),
        end_time=datetime.fromisoformat(end),
    )


def test_candle_reference_uses_utc_z_timestamps() -> None:
    reference = candle_reference(_candle("2026-09-29T03:45:00+00:00", "2026-09-29T03:50:00+00:00"))
    assert reference == {
        "session_date": "2026-09-29",
        "timeframe": "5m",
        "start_at": "2026-09-29T03:45:00.000000Z",
        "end_at": "2026-09-29T03:50:00.000000Z",
    }
