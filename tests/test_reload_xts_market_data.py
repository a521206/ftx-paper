from collections import Counter

import pytest

from scripts.reload_xts_market_data import _validate_counts


def test_validate_counts_allows_sparse_futures_data() -> None:
    assert _validate_counts(
        Counter({"NIFTY 50": 375, "INDIA VIX": 375, "NIFTY26NOVFUT": 369}),
        ("NIFTY 50", "INDIA VIX", "NIFTY26NOVFUT"),
    ) == "complete"


def test_validate_counts_classifies_partial_session() -> None:
    assert _validate_counts(
        Counter({"NIFTY 50": 120, "INDIA VIX": 120, "NIFTY26NOVFUT": 118}),
        ("NIFTY 50", "INDIA VIX", "NIFTY26NOVFUT"),
    ) == "partial"


def test_validate_counts_classifies_holiday_or_unavailable_session() -> None:
    assert _validate_counts(
        Counter(), ("NIFTY 50", "INDIA VIX", "NIFTY26NOVFUT")
    ) == "holiday_or_unavailable"


def test_validate_counts_rejects_missing_anchor() -> None:
    with pytest.raises(RuntimeError, match="inconsistent spot/index"):
        _validate_counts(
            Counter({"NIFTY 50": 0, "INDIA VIX": 375, "NIFTY26NOVFUT": 369}),
            ("NIFTY 50", "INDIA VIX", "NIFTY26NOVFUT"),
        )
