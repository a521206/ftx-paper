from ftx_paper.core.cost import futures_cost, synthetic_futures_cost


def test_futures_cost_matches_canonical_round_trip() -> None:
    assert futures_cost(2) == 1000.0


def test_synthetic_cost_charges_stt_on_sold_leg() -> None:
    assert synthetic_futures_cost(100.1, 102.0, 2, is_short=False) == 229.89
