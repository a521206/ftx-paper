from datetime import datetime, timezone

from ftx_paper.contracts.market import normalize_exchange_timestamp


def test_normalize_exchange_timestamp_accepts_zerodha_epoch_seconds():
    expected = datetime(2026, 9, 28, 5, 27, 41, tzinfo=timezone.utc)

    assert normalize_exchange_timestamp(1790573261) == expected
    assert normalize_exchange_timestamp("1790573261") == expected


def test_normalize_exchange_timestamp_preserves_iso8601_support():
    value = "2026-09-28T10:57:52+05:30"

    assert normalize_exchange_timestamp(value) == datetime(2026, 9, 28, 5, 27, 52, tzinfo=timezone.utc)
