from datetime import datetime, timezone

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy.admission import admission_result, build_admission_features
from ftx_paper.strategy.scoring import compute_selling_structure


def test_admission_contract_replaces_grinding_score_filter() -> None:
    features: dict[str, bool | None] = {
        "climactic_selling": False, "panic_descent": False,
        "volume_drying": True, "pcr_extreme": False, "vix_spike": False,
        "volume_climax_prior": True, "prior_midpoint_reclaim": False,
    }
    assert admission_result(features, "trend_continuation") == (True, None)
    assert admission_result({**features, "volume_climax_prior": False}, "trend_continuation") == (False, "tr_confirmation_count")


def test_admission_missing_required_predicate_is_generic_rejection() -> None:
    assert admission_result({"panic_descent": False, "pcr_extreme": None, "climactic_selling": False}, "mean_reversion") == (False, "admission_rejected")


def test_live_admission_matches_replay_for_shifted_causal_window() -> None:
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    bars = tuple(MarketBar(instrument, datetime(2026, 1, 1, 10, i, tzinfo=timezone.utc), 100 - i, 101 - i, 99 - i, 100 - i, 100 if i < 11 else 300) for i in range(12))
    replay = compute_selling_structure(bars[:-1], bars[-1], vix_open=15.0, vix_at_event=15.0)
    live = build_admission_features(bars, bars[-1], pcr=1.0, vix_open=15.0, vix_at_event=15.0)
    assert live["climactic_selling"] is (replay.consec_down >= 3)
    assert live["volume_drying"] is replay.vol_drying
    assert live["volume_climax_prior"] is (replay.vol_climax >= 2.0)
    assert live["pcr_extreme"] is False and live["vix_spike"] is False
