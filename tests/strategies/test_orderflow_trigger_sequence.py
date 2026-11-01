from types import SimpleNamespace

from ftx_paper.strategies.orderflow.opportunity_coordinator import OpportunityCoordinator
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_evidence import acceptance_trigger_evidence
from ftx_paper.strategies.orderflow.templates import Direction, TRADE_TEMPLATES
from ftx_paper.strategies.orderflow.transition import OrderflowTransitionOwner


def _row(*, open_: float, high: float, low: float, close: float, volume: float = 100.0) -> dict[str, object]:
    return {
        "time": "09:20",
        "session_date": "2026-10-01",
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def test_acceptance_displacement_rejects_directionless_small_bar() -> None:
    evidence = acceptance_trigger_evidence(
        _row(open_=100.0, high=100.4, low=99.8, close=100.1),
        [_row(open_=100.0, high=102.0, low=98.0, close=101.0)] * 20,
        (),
        Direction.LONG,
    )

    assert evidence["qualified"] is False
    assert evidence["detector"]["min_body_fraction"] == 0.3


def test_acceptance_displacement_accepts_directional_expansion() -> None:
    evidence = acceptance_trigger_evidence(
        _row(open_=100.0, high=105.0, low=99.0, close=104.5, volume=300.0),
        [_row(open_=100.0, high=102.0, low=99.0, close=101.0)] * 20,
        (),
        Direction.LONG,
    )

    assert evidence["qualified"] is True


def test_opening_seed_remains_provisional(monkeypatch) -> None:
    candle = Aggregate(
        "2026-10-01T09:15", "2026-10-01T09:19", "2026-10-01T09:20",
        "5m", 5, 100.0, 102.0, 99.0, 101.0, 100,
        "NIFTY", "", "FUT", "2026-10-01", "opening",
    )
    from ftx_paper.strategies.orderflow import opportunity_coordinator as module

    monkeypatch.setattr(module, "build_context_scores", lambda *args, **kwargs: (1.0, 0.0))
    coordinator = OpportunityCoordinator(fill_permission_margin=None)
    from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry

    registry = LocationRegistry("test")
    registry.register_opening_range(99.0, 101.0, candle.available_at, atr=1.0)

    result = coordinator.form_opening_seed(
        candle=candle,
        registry=registry,
        templates=TRADE_TEMPLATES,
        aggregates={"1m": [], "5m": [candle]},
        raw_rows=[_row(open_=100.0, high=102.0, low=99.0, close=101.0)],
        rows_by_date={"2026-10-01": [_row(open_=100.0, high=102.0, low=99.0, close=101.0)]},
    )

    assert result is not None
    assert result[1].lifecycle is not None
    assert result[1].lifecycle.episode_confirmed is False


def test_episode_promotion_is_idempotent_and_invalidates_competitor() -> None:
    events: list[dict[str, object]] = []
    invalidations: list[tuple[object, str]] = []
    life = SimpleNamespace(
        episode_confirmed=False,
        lifecycle_id="winner",
        direction=Direction.LONG,
        conflict_key="same",
    )
    competing = SimpleNamespace(active=True, lifecycle_id="loser")
    owner = SimpleNamespace(
        context=SimpleNamespace(
            conflicting_unfilled_opportunities=lambda selected: (competing,),
            record_event=events.append,
        ),
        execution_owner=SimpleNamespace(
            invalidate_opportunity=lambda candidate, **kwargs: invalidations.append(
                (candidate, kwargs["reason"])
            ),
        ),
    )
    bar_events: list[dict[str, object]] = []

    OrderflowTransitionOwner.promote_episode(
        owner,
        life,
        observed_at="2026-10-01T03:50:00Z",
        available_at="2026-10-01T03:51:00Z",
        bar_events=bar_events,
    )
    OrderflowTransitionOwner.promote_episode(
        owner,
        life,
        observed_at="2026-10-01T03:52:00Z",
        available_at="2026-10-01T03:53:00Z",
        bar_events=bar_events,
    )

    assert life.episode_confirmed is True
    assert invalidations == [(competing, "opposing_episode_confirmed")]
    assert [event["kind"] for event in events] == ["episode_confirmed"]
    assert [event["kind"] for event in bar_events] == ["episode_confirmed"]
