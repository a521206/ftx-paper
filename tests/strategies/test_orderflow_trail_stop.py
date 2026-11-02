from ftx_paper.risk.trail_stop import TrailConfig, TrailTracker


def test_trail_tracker_activates_and_updates_causally_after_entry() -> None:
    tracker = TrailTracker(
        entry_price=100.0,
        direction="long",
        hard_stop_bp=100.0,
        config=TrailConfig(activation_bp=20, distance_bp=20, breakeven_lock=True),
    )

    assert tracker.is_active is False
    assert tracker.current_trail_price is None

    first = tracker.update(100.0, 100.25, 100.05, 100.2, evaluate_exit=False)

    assert first.should_exit is False
    assert tracker.is_active is True
    assert tracker.current_trail_price == 100.05

    tracker.update(100.2, 100.4, 100.2, 100.35, evaluate_exit=False)

    assert tracker.current_trail_price == 100.2
