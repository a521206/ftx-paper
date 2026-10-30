from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import cast

import pytest

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.market import DecisionBundle
from ftx_paper.contracts import Instrument, MarketBar, OrderIntent, OrderRole, OrderSide
from ftx_paper.runtime.engine import MultiStrategyEngine, PaperEngine
from ftx_paper.runtime.parity_artifacts import build_parity_artifact
from ftx_paper.strategies.orderflow import (
    LocationObservation,
    OrderflowContext,
    OrderflowObservation,
    OrderflowObservationContext,
    OrderflowStrategy,
)
from ftx_paper.strategies.orderflow import OrderflowConfig, OrderflowPolicy, _score_rejection
from ftx_paper.strategies.orderflow.orderflow_codec import (
    bar_from_snapshot,
    bar_snapshot,
    instrument_from_snapshot,
    instrument_snapshot,
    position_from_snapshot,
    position_snapshot,
)
from ftx_paper.strategies.shared.exits import PositionState
from ftx_paper.strategies.shared.policy_contract import ExecutionMode


UTC = timezone.utc
OBSERVED = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("direction", "long_score", "short_score", "expected"),
    [("long", 0.60, 0.54, None), ("long", 0.57, 0.54, "fill_permission_mismatch"),
     ("short", None, 0.54, "setup_scores_unavailable")],
)
def test_score_margin_gate_preserves_unavailable_scores(
    direction: str, long_score: float | None, short_score: float | None,
    expected: str | None,
) -> None:
    assert _score_rejection(
        direction=direction, long_score=long_score, short_score=short_score, margin=0.05,
    ) == expected


def _context(
    *,
    close: float = 101.0,
    observation_id: str = "obs-5m",
    generation: int = 1,
    contact: bool = True,
    response_close: float | None = None,
    gap_status: str = "none",
) -> tuple[DecisionBundle, DecisionContext]:
    available = OBSERVED + timedelta(minutes=5)
    location = LocationObservation(
        location_id=f"session-g{generation}",
        location_alias="session_high",
        reference_generation=generation,
        reference=100.0,
        band_lower=99.5,
        band_upper=100.5,
        open=99.0,
        high=101.0 if contact else 110.0,
        low=99.0 if contact else 109.0,
        close=close,
        band_contact=contact,
        source_aggregate_id=f"parent-{observation_id}",
        observed_at=OBSERVED,
        available_at=available,
        observation_id=f"location-{observation_id}-g{generation}",
        timeframe="5m",
        frozen_atr=1.0,
        child_observation_ids=tuple(f"obs-1m-{index}" for index in range(5)),
    )
    children = tuple(
        OrderflowObservation(
            observation_id=f"obs-1m-{index}",
            source_observation_id=f"bar-1m-{index}",
            feature_version="typical_price_volume_v1",
            timeframe="1m",
            instrument="NIFTYFUT",
            observed_at=OBSERVED + timedelta(minutes=index),
            available_at=OBSERVED + timedelta(minutes=index, seconds=1),
            features={"open": 99.0, "high": 101.0, "low": 99.0,
                      "close": 100.0, "vwap": 100.0, "volume": 5.0},
            interval_start=OBSERVED + timedelta(minutes=index),
            interval_end=OBSERVED + timedelta(minutes=index + 1),
            context=OrderflowObservationContext(
                session_id="2026-01-01",
                completeness="complete",
                availability_status="available",
                replay_provenance={"source": "test"},
                generation=generation,
                frozen_atr=1.0,
                causal_sequence=1,
            ),
        )
        for index in range(5)
    )
    observation = OrderflowObservation(
        observation_id=observation_id,
        source_observation_id=f"bar-{observation_id}",
        feature_version="typical_price_volume_v1",
        timeframe="5m",
        instrument="NIFTYFUT",
        observed_at=OBSERVED,
        available_at=available,
        features={"open": 99.0, "high": 101.0 if contact else 110.0,
                  "low": 99.0 if contact else 109.0,
                  "close": close, "previous_close": 99.0, "vwap": 100.0,
                  "volume": 25.0},
        interval_start=OBSERVED,
        interval_end=available,
        context=OrderflowObservationContext(
            session_id="2026-01-01",
            completeness="complete",
            availability_status="available",
            replay_provenance={"source": "test", "gap_status": gap_status},
            generation=generation,
            frozen_atr=1.0,
            causal_sequence=1,
            child_observation_ids=tuple(item.observation_id for item in children),
            child_intervals=tuple(
                cast(tuple[datetime, datetime], (item.interval_start, item.interval_end))
                for item in children
            ),
        ),
        location_observations=(location,),
    )
    response_observations: tuple[OrderflowObservation, ...] = ()
    if response_close is not None:
        response_time = OBSERVED + timedelta(minutes=5)
        response_available = response_time + timedelta(minutes=5)
        response_observations = (OrderflowObservation(
            observation_id=f"response-{observation_id}",
            source_observation_id=f"response-bar-{observation_id}",
            feature_version="typical_price_volume_v1",
            timeframe="5m",
            instrument="NIFTYFUT",
            observed_at=response_time,
            available_at=response_available,
                features={"open": 100.0, "high": 102.0, "low": 99.0,
                      "close": response_close, "previous_close": 99.0, "volume": 25.0},
            interval_start=response_time,
            interval_end=response_available,
            evidence_ids=(f"response-evidence-{observation_id}",),
            context=OrderflowObservationContext(
                session_id="2026-01-01",
                completeness="complete",
                availability_status="available",
                replay_provenance={"source": "test", "gap_status": "none"},
                generation=generation,
                frozen_atr=1.0,
                causal_sequence=2,
            ),
        ),)
    cutoff = response_observations[-1].available_at if response_observations else available
    orderflow = OrderflowContext(
        "2026-01-01",
        cutoff,
        (observation, *response_observations, *children),
        {"5m": (observation, *response_observations), "1m": children},
    )
    bundle = DecisionBundle(
        "2026-01-01:10:00", "2026-01-01", "10:00", {}, (),
        orderflow_context=orderflow,
    )
    account = DecisionContext(
        as_of=cutoff,
        account_revision=0,
        capital=CapitalSnapshot(1000.0, 1000.0, 0.0, 0.0),
        margin=MarginSnapshot(0.0, 1000.0, 1000.0),
        exposure=ExposureSnapshot(0.0, 0),
    )
    return bundle, account


def test_producer_published_location_observation_starts_diagnostic_interaction() -> None:
    bundle, account = _context()

    events = OrderflowStrategy().evaluate(bundle, account)

    event = next(event for event in events if event.payload.get("decision_stage") == "opportunity")
    assert event.payload["reason"] == "location_interaction_observed"
    assert event.payload["decision_stage"] == "opportunity"
    assert event.payload["location"] == "session_high"
    assert event.payload["reference_generation"] == 1
    assert event.payload["source_observation_ids"] == ["location-obs-5m-g1"]
    assert event.order is None


def test_opening_seed_materializes_an_or_candidate() -> None:
    bundle, account = _context()
    assert bundle.orderflow_context is not None
    observation = bundle.orderflow_context.observations_for("1m")[0]
    policy = OrderflowPolicy()

    events = policy._form_opening_candidate(
        observation,
        {
            "seed_id": "opening-seed",
            "direction": "short",
            "location": "OR_LOW",
            "reference_level": 100.0,
            "band_lower": 99.5,
            "band_upper": 100.5,
        },
        bundle.orderflow_context,
        account,
        {},
        None,
    )

    assert len(events) == 3
    candidate, arbitration, outcome = events
    assert candidate.payload["reason"] == "setup_candidate_formed"
    assert candidate.payload["candidate_provenance"] == "opening_hypothesis"
    assert candidate.payload["location"] == "OR_LOW"
    assert candidate.payload["interaction_id"] is None
    assert candidate.payload["opening_seed_id"] == "opening-seed"
    assert arbitration.payload["reason"] == "hypothesis_arbitrated"
    assert arbitration.payload["arbitration_selected"] is True
    assert outcome.payload["reason"] == "opening_candle_directional_seed"
    assert outcome.payload["status"] == "RESOLVED"


def test_completed_five_minute_extreme_publishes_before_same_clock_location() -> None:
    bundle, account = _context()
    assert bundle.orderflow_context is not None
    parent = bundle.orderflow_context.observations_for("5m")[0]
    location = replace(
        parent.location_observations[0],
        observation_id="location-obs-1m-g1",
        timeframe="1m",
        available_at=parent.available_at,
    )
    children = list(bundle.orderflow_context.observations_for("1m"))
    children[-1] = replace(
        children[-1],
        available_at=parent.available_at,
        location_observations=(location,),
    )
    assert parent.context is not None
    parent = replace(
        parent,
        context=replace(
            parent.context,
            replay_provenance={"source": "test", "producer": "ftx-paper"},
        ),
    )
    context = replace(
        bundle.orderflow_context,
        observations=(*children, parent),
        prefixes={"5m": (parent,), "1m": tuple(children)},
    )

    events = OrderflowStrategy().evaluate(replace(bundle, orderflow_context=context), account)
    reasons = [event.payload.get("reason") for event in events]

    assert reasons.index("session_extreme_registered") < reasons.index("location_interaction_observed")


def test_earlier_one_minute_evidence_advances_before_later_parent(monkeypatch) -> None:
    bundle, account = _context()
    assert bundle.orderflow_context is not None
    parent = bundle.orderflow_context.observations_for("5m")[0]
    children = bundle.orderflow_context.observations_for("1m")
    context = replace(
        bundle.orderflow_context,
        observations=(*children, parent),
        prefixes={"5m": (parent,), "1m": children},
    )
    policy = OrderflowStrategy().policy
    order: list[tuple[str, str]] = []
    original_advance = policy._advance_context

    def record_advance(observation, orderflow, causal_cutoff, already_processed, base):
        order.append(("context", observation.observation_id))
        return original_advance(
            observation, orderflow, causal_cutoff, already_processed, base,
        )

    monkeypatch.setattr(policy, "_advance_context", record_advance)
    policy.evaluate(replace(bundle, orderflow_context=context), account)

    assert order.index(("context", children[0].observation_id)) < order.index(
        ("context", parent.observation_id),
    )


def test_future_five_minute_parent_cannot_register_session_extreme() -> None:
    bundle, account = _context()
    assert bundle.orderflow_context is not None
    cutoff = OBSERVED + timedelta(minutes=1)

    events = OrderflowStrategy().evaluate(
        bundle,
        replace(account, as_of=cutoff),
    )

    assert not any(
        event.payload.get("reason") == "session_extreme_registered"
        for event in events
    )


def test_first_five_minute_or_location_does_not_start_ordinary_interaction() -> None:
    bundle, account = _context()
    assert bundle.orderflow_context is not None
    parent = bundle.orderflow_context.observations_for("5m")[0]
    location = replace(parent.location_observations[0], location_alias="OR_HIGH")
    opening = replace(
        parent,
        features={**parent.features, "close": parent.features["open"]},
        location_observations=(location,),
    )
    context = replace(
        bundle.orderflow_context,
        observations=(opening, *bundle.orderflow_context.observations[1:]),
        prefixes={
            **bundle.orderflow_context.prefixes,
            "5m": (opening, *bundle.orderflow_context.observations_for("5m")[1:]),
        },
    )
    events = OrderflowStrategy().evaluate(replace(bundle, orderflow_context=context), account)

    assert not any(
        event.payload.get("reason") == "location_interaction_observed"
        and event.payload.get("location") == "OR_HIGH"
        for event in events
    )


def test_opening_gap_context_uses_open_availability_not_completed_bar_boundary() -> None:
    bundle, account = _context()
    assert bundle.orderflow_context is not None
    children = list(bundle.orderflow_context.observations_for("1m"))
    children[0] = replace(
        children[0],
        features={"open": 101.0, "previous_close": 100.0, "close": 100.0},
    )
    context = replace(
        bundle.orderflow_context,
        observations=(bundle.orderflow_context.observations[0], *children),
        prefixes={"5m": bundle.orderflow_context.observations_for("5m"), "1m": tuple(children)},
    )

    events = OrderflowStrategy().evaluate(replace(bundle, orderflow_context=context), account)

    context_event = next(
        event for event in events
        if event.payload.get("reason") == "opening_gap_context"
    )
    assert context_event.payload["observed_at"] == OBSERVED.isoformat()
    assert context_event.payload["available_at"] == OBSERVED.isoformat()
    assert context_event.payload["decision_at"] == OBSERVED.isoformat()


def test_public_strategy_snapshot_restores_processed_location_lifecycle() -> None:
    bundle, account = _context()
    strategy = OrderflowStrategy()
    first = strategy.evaluate(bundle, account)

    restored = OrderflowStrategy.from_snapshot(strategy.snapshot())

    assert first[0].payload["interaction_id"] == "NIFTYFUT:2026-01-01:session_high:5m:2026-01-01T10:00:00+00:00"
    assert restored.evaluate(bundle, account) == ()


def test_orderflow_snapshot_codecs_round_trip_instrument_bar_and_position() -> None:
    instrument = Instrument("NIFTYFUT", "NSE", "FUT", "2026-01-29")
    bar = MarketBar(instrument, OBSERVED, 99.0, 101.0, 98.5, 100.0, 25.0, 10.0)
    position = PositionState(
        instrument, 100.0, 98.0, 2, OrderSide.BUY,
        entry_fill_time=OBSERVED, entry_order_id="entry-1", initial_risk_points=2.0,
    )

    assert instrument_from_snapshot(instrument_snapshot(instrument)) == instrument
    assert bar_from_snapshot(bar_snapshot(bar)) == bar
    assert position_from_snapshot(position_snapshot(position)) == position


@pytest.mark.parametrize(
    ("codec", "raw", "message"),
    [
        (bar_from_snapshot, {"instrument": {}, "timestamp": OBSERVED.isoformat()}, "prices"),
        (position_from_snapshot, {"instrument": {"symbol": "NIFTYFUT", "exchange": "NSE", "instrument_type": "FUT"}, "entry_price": 1.0, "stop_price": 0.0}, "quantity"),
        (instrument_from_snapshot, {"symbol": "NIFTYFUT"}, "exchange"),
    ],
)
def test_orderflow_snapshot_codecs_reject_malformed_values(codec, raw, message) -> None:
    with pytest.raises((ValueError, KeyError), match=message):
        codec(raw)


def test_interaction_projection_advancement_records_causal_score_once() -> None:
    bundle, account = _context()
    assert bundle.orderflow_context is not None
    strategy = OrderflowStrategy()
    strategy.evaluate(bundle, account)
    snapshot = strategy.policy.snapshot()
    detectors = cast(dict[str, object], snapshot["location_detectors"])
    five_minute = cast(dict[str, object], detectors["5m"])
    active = cast(list[object], five_minute["active"])
    interaction = cast(dict[str, object], active[0])
    scores = cast(list[object], interaction["score_history"])
    assert len(scores) == 1
    score = cast(dict[str, object], scores[0])
    assert score["long_score"] == pytest.approx(0.07)
    assert score["short_score"] == pytest.approx(0.07)
    assert cast(dict[str, str], interaction["source_cursors"])["5m"] == "obs-5m"


def test_snapshot_restore_rejects_invalid_location_detector_state() -> None:
    strategy = OrderflowStrategy()
    bundle, account = _context()
    strategy.evaluate(bundle, account)
    snapshot = strategy.snapshot()
    payload = snapshot["payload"]
    assert isinstance(payload, dict)
    payload["location_detectors"]["5m"] = []

    with pytest.raises(ValueError, match="detector"):
        OrderflowStrategy.from_snapshot(snapshot)


def test_policy_snapshot_rejects_obsolete_single_detector_state() -> None:
    strategy = OrderflowStrategy()
    bundle, account = _context()
    strategy.evaluate(bundle, account)
    current = dict(strategy.policy.snapshot())
    current["schema_version"] = 2
    detectors = cast(dict[str, object], current["location_detectors"])
    current["location_detector"] = detectors["5m"]
    del current["location_detectors"]

    with pytest.raises(ValueError, match="unsupported orderflow snapshot schema"):
        OrderflowPolicy.from_snapshot(current)


def test_generation_change_keeps_active_interaction_episode() -> None:
    strategy = OrderflowStrategy()
    bundle, account = _context()
    strategy.evaluate(bundle, account)

    replacement, replacement_account = _context(
        observation_id="replacement", generation=2,
    )
    events = strategy.evaluate(replacement, replacement_account)

    observed = [event for event in events if event.payload.get("reason") == "location_interaction_observed"]
    assert observed
    assert any("2026-01-01T10:00:00+00:00" in str(event.payload["interaction_id"]) for event in observed)
    detector = strategy.policy._location_detectors["5m"]
    assert len(detector.active_interactions) == 1
    # Reference generations may change while the canonical interaction episode
    # remains active against its original frozen band.
    assert detector.active_interactions[0].reference_generation == 1
    assert len(detector.closed_interactions) == 0


def test_non_contact_location_observation_does_not_start_interaction() -> None:
    bundle, account = _context(contact=False, observation_id="no-contact")

    assert OrderflowStrategy().evaluate(bundle, account) == ()


def test_orderflow_executable_mode_emits_bounded_futures_entry() -> None:
    bundle, account = _context(response_close=101.0)
    account = replace(account, margin=MarginSnapshot(0.0, 10_000.0, 1_000.0))
    strategy = OrderflowStrategy(OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE))

    events = strategy.evaluate(bundle, account)

    candidate = next(
        event for event in events
        if event.payload.get("decision_stage") == "candidate"
        and event.payload.get("reason") == "setup_score_mismatch"
    )
    assert candidate.order is None
    assert candidate.payload["outcome"] == "rejected"
    assert "candidate_provenance" not in candidate.payload
    assert candidate.payload["decision_at"] == candidate.payload["available_at"]
    assert candidate.payload["observed_at"] <= candidate.payload["available_at"]


def test_executable_orderflow_registers_and_settles_protective_exit() -> None:
    strategy = OrderflowStrategy(OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE))
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent(
        "entry-1", instrument, OrderSide.BUY, 1, stop_price=99.0,
        role=OrderRole.ENTRY, cell="OR_HIGH", exit_mode="signal",
    )
    entry_time = OBSERVED
    strategy.register_entry(entry, fill_price=100.0, entry_fill_time=entry_time)

    assert strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=1), 100.0, 101.0, 98.5, 99.0,
    )) == ()
    actions = strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=2), 100.0, 101.0, 98.5, 99.0,
    ))

    assert len(actions) == 1
    assert actions[0].reason == "hard_stop"
    strategy.settle_exit(actions[0].intent.client_order_id, filled=True)
    assert strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=2), 100.0, 101.0, 99.5, 100.0,
    )) == ()


def test_delayed_exit_is_dispatched_once_until_settlement() -> None:
    strategy = OrderflowStrategy(OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE))
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry = OrderIntent(
        "entry-delayed", instrument, OrderSide.BUY, 1, stop_price=99.0,
        role=OrderRole.ENTRY, cell="OR_HIGH", exit_mode="signal",
    )
    entry_time = OBSERVED
    strategy.register_entry(entry, fill_price=100.0, entry_fill_time=entry_time)

    assert strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=1), 100.0, 101.0, 98.5, 99.0,
    )) == ()
    first_dispatch = strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=2), 100.0, 101.0, 98.5, 99.0,
    ))
    assert len(first_dispatch) == 1
    assert strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=3), 100.0, 101.0, 98.5, 99.0,
    )) == ()


def test_orderflow_snapshot_restores_exit_state_and_pending_dispatch() -> None:
    strategy = OrderflowStrategy(OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE))
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry_time = OBSERVED
    strategy.register_entry(
        OrderIntent("entry-snapshot", instrument, OrderSide.BUY, 1, stop_price=99.0,
                    role=OrderRole.ENTRY, cell="OR_HIGH", exit_mode="signal"),
        fill_price=100.0, entry_fill_time=entry_time,
    )
    assert strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=1), 100.0, 101.0, 98.5, 99.0,
    )) == ()
    restored = OrderflowStrategy.from_snapshot(strategy.snapshot())
    assert set(restored._decision_positions) == {"entry-snapshot"}
    assert len(restored.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=2), 100.0, 101.0, 98.5, 99.0,
    ))) == 1
    assert restored.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=3), 100.0, 101.0, 98.5, 99.0,
    )) == ()


def test_orderflow_pending_exit_has_priority_over_new_exit_evaluation() -> None:
    strategy = OrderflowStrategy(OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE))
    instrument = Instrument("NIFTYFUT", "NFO", "FUTURES")
    entry_time = OBSERVED
    strategy.register_entry(
        OrderIntent("entry-priority", instrument, OrderSide.BUY, 1, stop_price=99.0,
                    role=OrderRole.ENTRY, cell="OR_HIGH", exit_mode="signal"),
        fill_price=100.0, entry_fill_time=entry_time,
    )
    bar = MarketBar(instrument, entry_time + timedelta(minutes=1), 100.0, 101.0, 98.5, 99.0)
    assert strategy.on_closed_bar(bar) == ()
    assert len(strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=2), 100.0, 101.0, 98.5, 99.0,
    ))) == 1
    assert strategy.on_closed_bar(MarketBar(
        instrument, entry_time + timedelta(minutes=3), 100.0, 101.0, 98.5, 99.0,
    )) == ()


def test_diagnostic_and_executable_projections_share_decisions_but_not_orders() -> None:
    bundle, account = _context(response_close=101.0)
    executable = OrderflowStrategy(OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE))
    diagnostic = OrderflowStrategy()

    diagnostic_events = diagnostic.evaluate(bundle, account)
    executable_events = executable.evaluate(bundle, account)

    assert [item.payload for item in diagnostic_events] != []
    assert [item.payload["decision_id"] for item in diagnostic_events] == [
        item.payload["decision_id"] for item in executable_events
    ]
    assert all(item.order is None for item in diagnostic_events)
    assert all(item.order is None for item in executable_events)
    assert "execution_groups" not in diagnostic.policy.snapshot()
    assert "execution_groups" not in executable.policy.snapshot()


def test_executable_orderflow_does_not_duplicate_entry_on_replay() -> None:
    bundle, account = _context(response_close=101.0)
    strategy = OrderflowStrategy(OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE))

    first = strategy.evaluate(bundle, account)
    second = strategy.evaluate(bundle, account)

    assert all(item.order is None for item in first)
    assert second == ()


def test_p9_paired_replay_matches_decisions_while_projecting_execution_only_on_opt_in() -> None:
    bundle, account = _context(response_close=101.0)
    diagnostic = PaperEngine(OrderflowStrategy()).on_bundle(bundle, context=account)
    executable = PaperEngine(
        OrderflowStrategy(OrderflowConfig(execution_mode=ExecutionMode.EXECUTABLE)),
    ).on_bundle(bundle, context=account)

    def behavior(event: dict[str, object]) -> dict[str, object]:
        value = dict(event)
        value.pop("config_hash", None)
        decision_view = value.get("decision_view")
        if isinstance(decision_view, dict):
            decision_view = dict(decision_view)
            decision_view.pop("config_hash", None)
            value["decision_view"] = decision_view
        return value

    assert [behavior(event) for event in diagnostic.events] == [
        behavior(event) for event in executable.events
    ]
    assert diagnostic.orders == ()
    assert executable.orders == ()


def test_gap_closes_active_interaction_without_order() -> None:
    strategy = OrderflowStrategy()
    active, active_context = _context()
    strategy.evaluate(active, active_context)

    gap_time = OBSERVED + timedelta(minutes=5)
    gap_observation = OrderflowObservation(
        observation_id="gap", source_observation_id="gap-source",
        feature_version="typical_price_volume_v1", timeframe="5m",
        instrument="NIFTYFUT", observed_at=gap_time,
        available_at=gap_time + timedelta(minutes=5),
        features={"close": 100.0}, interval_start=gap_time,
        interval_end=gap_time + timedelta(minutes=5),
        context=OrderflowObservationContext(
            session_id="2026-01-01", completeness="complete",
            availability_status="available",
            replay_provenance={"source": "test", "gap_status": "source_gap"},
            generation=1, causal_sequence=2,
        ),
    )
    orderflow = OrderflowContext(
        "2026-01-01", gap_observation.available_at, (gap_observation,),
        {"5m": (gap_observation,), "1m": ()},
    )
    gap = DecisionBundle("gap", "2026-01-01", "10:05", {}, (), orderflow_context=orderflow)
    gap_context = replace(active_context, as_of=gap_observation.available_at)
    events = strategy.evaluate(gap, gap_context)

    closure = next(event for event in events if event.payload.get("reason") == "DATA_GAP")
    assert closure.payload["interaction_id"] == "NIFTYFUT:2026-01-01:session_high:5m:2026-01-01T10:00:00+00:00"
    assert all(event.order is None for event in events)


def test_orderflow_without_available_observation_emits_nothing() -> None:
    bundle, account = _context()
    account = replace(account, as_of=account.as_of - timedelta(seconds=1))

    assert OrderflowStrategy().evaluate(bundle, account) == ()


def test_orderflow_on_bundle_reports_missing_context() -> None:
    bundle = DecisionBundle("bundle", "2026-01-01", "10:00", {}, ())

    events = OrderflowStrategy().on_bundle(bundle)

    assert events[0].payload["reason"] == "orderflow_context_unavailable"


def test_orderflow_diagnostic_is_attributed_in_multi_strategy_engine() -> None:
    bundle, account = _context()
    engine = MultiStrategyEngine({"orderflow-research": OrderflowStrategy()})

    result = engine.on_bundle(bundle, context=account)

    assert result.events[0]["strategy_id"] == "orderflow-research"
    assert result.orders == ()


def test_non_executable_location_decision_is_persisted_in_parity_stream() -> None:
    bundle, context = _context()
    result = PaperEngine(OrderflowStrategy()).on_bundle(bundle, context=context)

    assert result.orders == ()
    assert result.events[0]["decision_stage"] == "opportunity"
    artifact = build_parity_artifact(
        {"strategy": {"name": "ftx-paper-orderflow"}, "bars_seen": 1,
         "metadata": {"input_fingerprint": "fixture"}},
        request={"session_date": "2026-01-01"},
        layer_records={"decision": list(result.events)},
    )
    record = artifact["layers"]["decision"][0]
    assert record["values"]["decision_stage"] == "opportunity"


def test_location_observation_has_public_causal_identity_contract() -> None:
    bundle, _ = _context()
    assert bundle.orderflow_context is not None
    observation = bundle.orderflow_context.observations_for("5m")[0]
    location = observation.location_observations[0]

    assert isinstance(location, LocationObservation)
    assert location.available_at >= location.observed_at
    assert location.source_aggregate_id == f"parent-{observation.observation_id}"


def test_strategy_rejects_mismatched_location_timeframe() -> None:
    bundle, account = _context()
    assert bundle.orderflow_context is not None
    parent = bundle.orderflow_context.observations_for("5m")[0]
    location = replace(parent.location_observations[0], timeframe="1m")
    malformed = replace(parent, location_observations=(location,))
    malformed_context = replace(
        bundle.orderflow_context,
        observations=(malformed, *bundle.orderflow_context.observations[1:]),
        prefixes={
            "5m": (malformed, *bundle.orderflow_context.observations_for("5m")[1:]),
            "1m": bundle.orderflow_context.observations_for("1m"),
        },
    )
    malformed_bundle = replace(bundle, orderflow_context=malformed_context)

    with pytest.raises(ValueError, match="timeframe"):
        OrderflowStrategy().evaluate(malformed_bundle, account)


def test_late_parent_location_facts_do_not_replay_historical_children() -> None:
    from ftx_paper.strategies.orderflow import CausalOrderflowProducer

    producer = CausalOrderflowProducer()
    producer.set_frozen_atr(10.0)
    start = datetime(2026, 1, 1, 3, 45, tzinfo=UTC)
    instrument = Instrument("NIFTYFUT", "NSE", "FUTURES")
    for index in range(5):
        producer.ingest(MarketBar(
            instrument, start + timedelta(minutes=index),
            100.0, 101.0, 99.0, 100.0, 10.0,
        ))
    native = producer.context("2026-01-01", "09:20")
    assert native is not None
    bundle, account = _context()
    policy = OrderflowPolicy()
    events = policy.evaluate(
        replace(bundle, orderflow_context=native),
        replace(account, as_of=start + timedelta(minutes=5)),
    )
    interactions = [event.payload for event in events
                    if event.payload.get("reason") == "location_interaction_observed"]
    one_minute = [event for event in interactions if event.get("timeframe") == "1m"]
    assert one_minute
    assert all(str(event["interaction_id"]).endswith("09:19:00+05:30")
               or str(event["interaction_id"]).endswith("03:49:00+00:00")
               for event in one_minute)
    assert all(datetime.fromisoformat(str(event["available_at"])) == start + timedelta(minutes=5)
               for event in one_minute)
    assert policy.evaluate(
        replace(bundle, orderflow_context=native),
        replace(account, as_of=start + timedelta(minutes=5)),
    ) == ()
