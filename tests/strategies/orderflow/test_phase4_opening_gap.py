from ftx_paper.strategies.orderflow.opening_gap_policy import OpeningGapPolicy
from ftx_paper.strategies.orderflow.hierarchy import Permission


def test_opening_gap_classification_and_transitions() -> None:
    policy = OpeningGapPolicy()
    result = policy.classify(opening={"open": 95.0}, prior_low=100.0, prior_high=110.0, prior_value_low=102.0, prior_value_high=108.0, available_at="2026-09-29T09:15")
    assert result["classification"] == "GAP_DOWN_INITIATIVE"
    assert policy.transition(direction="down", completed_bars=1, close=99.0, reference_level=100.0, prior_value_low=102.0, prior_value_high=108.0) == "GAP_DOWN_ACCEPTED"
    assert policy.transition(direction="down", completed_bars=2, close=100.0, reference_level=100.0, prior_value_low=102.0, prior_value_high=108.0) == "GAP_DOWN_RECLAIMED"


def test_gap_regime_blocks_initiative_direction() -> None:
    regime = OpeningGapPolicy().regime_for(state="GAP_UP_ACCEPTED", direction="up", version="v1", context="opening")
    assert regime.long is Permission.ALLOWED
    assert regime.short is Permission.BLOCKED
