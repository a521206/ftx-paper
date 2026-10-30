from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.vwap_location import causal_vwap_prefix


def _aggregate(time: str, *, high: float = 103.0, low: float = 101.0, close: float = 102.0, available_at: str | None = None, timeframe: str = "1m", session_date: str = "2026-09-29") -> Aggregate:
    return Aggregate(time, time, available_at or time, timeframe, 1, 100.0, high, low, close, 10, "NIFTY", "", "FUT", session_date, "source")


def test_vwap_uses_only_completed_causal_prefix() -> None:
    values = causal_vwap_prefix([_aggregate("2026-09-29T09:15"), _aggregate("2026-09-29T09:16", high=203.0, low=201.0, close=202.0)], session_date="2026-09-29", available_at="2026-09-29T09:16")
    assert [item.value for item in values] == [102.0, 152.0]


def test_vwap_resets_session_and_rejects_mixed_timeframes() -> None:
    assert len(causal_vwap_prefix([_aggregate("2026-09-29T09:15"), _aggregate("2026-09-30T09:15", session_date="2026-09-30", high=202.0, low=198.0, close=200.0)], session_date="2026-09-30")) == 1
    try:
        causal_vwap_prefix([_aggregate("2026-09-29T09:15"), _aggregate("2026-09-29T09:20", timeframe="5m")], session_date="2026-09-29")
    except ValueError as exc:
        assert "one source timeframe" in str(exc)
    else:
        raise AssertionError("mixed VWAP timeframes must fail")
