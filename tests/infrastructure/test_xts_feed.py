from datetime import datetime, timezone

from ftx_paper.broker.xts.feed import XTSFeed
from ftx_paper.market import LiveMarketNormalizer


def test_xts_feed_converts_cumulative_volume_and_carries_open_interest() -> None:
    emitted = []
    normalizer = LiveMarketNormalizer(
        [{"instrument_token": 1, "exchange": "NFO", "symbol": "NIFTYFUT", "instrument_type": "FUT"}],
        expiry_normalizer=lambda value: str(value) if value is not None else None,
    )
    feed = XTSFeed(object(), [(2, 1)], normalizer, emitted.append)
    feed._running = True

    timestamp = int(datetime(2026, 11, 4, 6, 47, tzinfo=timezone.utc).timestamp() * 1000)
    feed._on_message({"instrument_token": 1, "last_price": 100, "volume_traded": 100, "oi": 10, "timestamp": timestamp})
    feed._on_message({"instrument_token": 1, "last_price": 101, "volume_traded": 120, "timestamp": timestamp + 20_000})
    feed._on_message({"instrument_token": 1, "last_price": 101.5, "volume_traded": 130, "timestamp": timestamp + 40_000})
    feed._on_message({"instrument_token": 1, "last_price": 102, "volume_traded": 150, "timestamp": timestamp + 60_000})

    assert len(emitted) == 1
    assert emitted[0].volume == 30
    assert emitted[0].open_interest == 10
