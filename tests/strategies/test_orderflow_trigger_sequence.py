from types import SimpleNamespace

from ftx_paper.strategies.orderflow.opportunity_replay_host import build_opportunity_coordinator
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.interaction_detector import InteractionDetector
from ftx_paper.strategies.orderflow.state_machine_evidence import acceptance_trigger_evidence
from ftx_paper.strategies.orderflow.state_machine_types import ExecutionConstraints, LocationRegistry
from ftx_paper.strategies.orderflow.templates import Direction, TRADE_TEMPLATES
from ftx_paper.strategies.orderflow.coordinator import OrderflowInput
from ftx_paper.strategies.orderflow.transition import OrderflowEngine
from ftx_paper.strategies.orderflow.opportunity_coordinator import OpportunityCoordinator
from ftx_paper.strategies.orderflow.phase4 import ContextPublisher
from ftx_paper.strategies.orderflow.execution import AdmissionStatus, IntentStatus
from ftx_paper.strategies.orderflow.execution_clock import intent_allows_requalification


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


def test_session_extreme_gate_cancellation_allows_requalification() -> None:
    state = SimpleNamespace(
        admission=SimpleNamespace(status=AdmissionStatus.NOT_EVALUATED),
        intent=SimpleNamespace(
            status=IntentStatus.CANCELLED,
            cancel_reason="active_position_session_extreme_gate",
        ),
    )

    assert intent_allows_requalification(state) is True

    state.intent.cancel_reason = "entry_already_owned"
    assert intent_allows_requalification(state) is False


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
        "NIFTY", "", "2026-10-01", "opening",
    )
    from ftx_paper.strategies.orderflow import opportunity_coordinator as module

    monkeypatch.setattr(module, "build_context_scores", lambda *args, **kwargs: (1.0, 0.0))
    registry = LocationRegistry("test")
    registry.register_opening_range(99.0, 101.0, candle.available_at, atr=1.0)
    transition = OrderflowEngine(registry)
    transition.context.execution_constraints = ExecutionConstraints(
        fill_permission_margin=None,
    )
    transition.context.aggregates = {"1m": [], "5m": [candle]}
    transition.context.rows = [
        _row(open_=100.0, high=102.0, low=99.0, close=101.0),
    ]
    transition.context.rows_by_date = {
        "2026-10-01": transition.context.rows,
    }
    coordinator = build_opportunity_coordinator(
        context=transition.context,
        execution=transition.execution_owner,
        location_registry=transition.location_registry,
        detector=InteractionDetector(registry),
        templates=TRADE_TEMPLATES,
    )
    events: list[dict[str, object]] = []
    coordinator.handle_structure(
        "2026-10-01T09:20:00Z",
        "2026-10-01T09:21:00Z",
        candle.to_dict(),
        [candle],
        events,
    )

    assert any(item["kind"] == "or_directional_seed" for item in events)
    assert len(transition.context.opportunities) == 1
    assert next(iter(transition.context.opportunities.values())).episode_confirmed is False


def test_structure_does_not_advance_confirmation_context_time() -> None:
    candle = Aggregate(
        "2026-10-01T09:15", "2026-10-01T09:19", "2026-10-01T09:20",
        "5m", 5, 100.0, 102.0, 99.0, 101.0, 100,
        "NIFTY", "", "2026-10-01", "opening",
    )
    transition = OrderflowEngine(LocationRegistry("test"))
    transition.context.aggregates = {"1m": [], "5m": [candle]}
    transition.context.last_confirmation_context_time = None

    transition.opportunity_owner.handle_structure(
        "2026-10-01T09:20:00Z",
        "2026-10-01T09:21:00Z",
        candle.to_dict(),
        [candle],
        [],
    )

    assert transition.context.last_confirmation_context_time is None


def test_ordinary_context_publication_does_not_advance_confirmation_context_time() -> None:
    candle = Aggregate(
        "2026-10-01T09:15", "2026-10-01T09:19", "2026-10-01T09:20",
        "5m", 5, 100.0, 102.0, 99.0, 101.0, 100,
        "NIFTY", "", "2026-10-01", "ordinary-context",
    )
    transition = OrderflowEngine(LocationRegistry("test"))
    transition.context.aggregates = {"1m": [], "5m": [candle]}
    transition.context.rows = [_row(open_=100.0, high=102.0, low=99.0, close=101.0)]

    ContextPublisher(transition.context).ordinary_context(candle, None)

    assert transition.context.last_confirmation_context_time is None


def test_transition_publishes_current_one_minute_aggregates_to_context() -> None:
    one_minute = Aggregate(
        "2026-10-01T09:20", "2026-10-01T09:20", "2026-10-01T09:21",
        "1m", 1, 100.0, 101.0, 99.0, 100.5, 100,
        "NIFTY", "", "2026-10-01", "one-minute",
    )
    transition = OrderflowEngine(LocationRegistry("test"))

    transition.advance(OrderflowInput(one_minute))

    assert transition.context.aggregates["1m"] == [one_minute]


def test_transition_publishes_ordinary_context_before_structure(monkeypatch) -> None:
    one_minute = Aggregate(
        "2026-10-01T09:20", "2026-10-01T09:20", "2026-10-01T09:21",
        "1m", 1, 100.0, 101.0, 99.0, 100.5, 100,
        "NIFTY", "", "2026-10-01", "one-minute",
    )
    five_minute = Aggregate(
        "2026-10-01T09:15", "2026-10-01T09:19", "2026-10-01T09:20",
        "5m", 5, 100.0, 102.0, 99.0, 101.0, 500,
        "NIFTY", "", "2026-10-01", "five-minute",
    )
    transition = OrderflowEngine(LocationRegistry("test"))
    calls: list[str] = []

    coordinator_type = type(transition.observation_owner)
    original_observe = coordinator_type.observe_interactions

    def observe_interactions(*args, **kwargs):
        calls.append("interactions")
        return original_observe(*args, **kwargs)

    monkeypatch.setattr(coordinator_type, "observe_interactions", observe_interactions)

    def handle_structure(*_args: object) -> None:
        calls.append("structure")

    monkeypatch.setattr(
        transition.opportunity_owner,
        "handle_structure",
        handle_structure,
    )
    monkeypatch.setattr(
        type(transition._context_publisher),
        "ordinary_context",
        lambda *_args, **_kwargs: calls.append("ordinary_context"),
    )
    monkeypatch.setattr(
        type(transition._phase4),
        "source_observations",
        lambda *_args, **_kwargs: calls.append("source_observations"),
    )
    monkeypatch.setattr(
        type(transition._phase4),
        "context_location",
        lambda *_args, **_kwargs: calls.append("context_location"),
    )

    transition.advance(OrderflowInput(one_minute, five_minute=five_minute))

    assert calls.index("ordinary_context") < calls.index("interactions")
    assert calls.index("interactions") < calls.index("source_observations")
    assert calls.index("source_observations") < calls.index("context_location")
    assert calls.index("interactions") < calls.index("structure")


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
        execution=SimpleNamespace(
            invalidate_opportunity=lambda candidate, **kwargs: invalidations.append(
                (candidate, kwargs["reason"])
            ),
        ),
    )
    coordinator = OpportunityCoordinator(owner)
    bar_events: list[dict[str, object]] = []

    coordinator.promote_episode(
        life,
        observed_at="2026-10-01T03:50:00Z",
        available_at="2026-10-01T03:51:00Z",
        bar_events=bar_events,
    )
    coordinator.promote_episode(
        life,
        observed_at="2026-10-01T03:52:00Z",
        available_at="2026-10-01T03:53:00Z",
        bar_events=bar_events,
    )

    assert life.episode_confirmed is True
    assert invalidations == [(competing, "opposing_episode_confirmed")]
    assert [event["kind"] for event in events] == ["episode_confirmed"]
    assert [event["kind"] for event in bar_events] == ["episode_confirmed"]
