"""Cross-boundary score parity tests.

This file is an intentional exception to the one-test-file-per-source-module
rule: it crosses the module boundary between independently runnable
``ftx_paper`` and canonical ``src.ftx`` implementations.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.core import scoring


def _score_fixture() -> tuple[list[MarketBar], MarketBar, dict[str, object]]:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    start = datetime(2026, 1, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))
    closes = [101.0, 100.0, 102.0, 99.0, 100.0, 98.0, 97.0, 96.0, 98.0, 97.0, 96.0, 95.0, 97.0, 96.0, 100.0]
    volumes = [100.0, 120.0, 500.0, 80.0, 110.0, 90.0, 1000.0, 700.0, 600.0, 100.0, 90.0, 80.0, 70.0, 60.0, 50.0]
    prior = [
        MarketBar(instrument, start + timedelta(minutes=index), close, close + 2, close - 2, close, volume)
        for index, (close, volume) in enumerate(zip(closes, volumes))
    ]
    current = MarketBar(instrument, start + timedelta(minutes=len(prior)), 100, 101, 98, 99, 200)
    bars = (*prior, current)
    day_arr = {
        "sh": np.array([bar.high for bar in bars]),
        "sl": np.array([bar.low for bar in bars]),
        "sc": np.array([bar.close for bar in bars]),
        "sv": np.array([bar.volume for bar in bars]),
        "n": len(bars),
        "time_strs": [bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%H:%M") for bar in bars],
    }
    return prior, current, day_arr


def test_paper_and_canonical_scoring_match_shared_fixture() -> None:
    """Run both scorers and compare selling values, factors, and score tiers."""
    from src.ftx.selling_structure import compute_selling_structure as canonical_selling
    from src.ftx.setup_score import (
        compute_setup_score as canonical_score,
        score_to_setup_type as canonical_score_to_setup_type,
    )

    prior, current, day_arr = _score_fixture()
    canonical_selling_value = canonical_selling(
        day_arr, len(prior), vix_open=16.0, vix_at_event=18.0,
    )
    paper_selling = scoring.compute_selling_structure(
        prior, current, vix_open=16.0, vix_at_event=18.0,
    )
    assert paper_selling == scoring.SellingStructure(
        canonical_selling_value.consec_down, canonical_selling_value.descent_speed_bp,
        canonical_selling_value.vol_climax, canonical_selling_value.vol_drying,
        canonical_selling_value.wick_rejection, canonical_selling_value.delta_divergence,
        canonical_selling_value.vix_trend_pct, canonical_selling_value.climax_score,
        canonical_selling_value.selling_type,
    )

    paper_result = scoring.calculate_setup_score(
        paper_selling, {"minutes_from_open": 120.0}, 18.0, 16.0, 1.5, True,
    )
    canonical_result = canonical_score(
        canonical_selling_value, {"minutes_from_open": 120.0}, 18.0, 16.0, 1.5, True,
    )
    assert paper_result == canonical_result

    for score in range(10):
        assert scoring.score_to_setup_type(score) == canonical_score_to_setup_type(score)[:2]


def test_paper_and_canonical_scoring_match_insufficient_history() -> None:
    from src.ftx.selling_structure import compute_selling_structure as canonical_selling

    prior, current, day_arr = _score_fixture()
    short_prior = prior[:2]
    short_day_arr = {
        key: value[:3] if hasattr(value, "__getitem__") else value
        for key, value in day_arr.items()
    }
    expected = canonical_selling(short_day_arr, 2, vix_open=16.0, vix_at_event=18.0)
    actual = scoring.compute_selling_structure(
        short_prior, current, vix_open=16.0, vix_at_event=18.0,
    )

    assert actual == scoring.SellingStructure(
        expected.consec_down, expected.descent_speed_bp, expected.vol_climax,
        expected.vol_drying, expected.wick_rejection, expected.delta_divergence,
        expected.vix_trend_pct, expected.climax_score, expected.selling_type,
    )
