from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime, timezone

from ftx_paper.strategies.orderflow.state_machine import ReplayMachine
from ftx_paper.strategies.orderflow.execution import ExecutionCoordinator, ExecutionEngine
from ftx_paper.strategies.orderflow.hierarchy import (
    ContextVersionRef,
    MarketContext,
    Permission,
    RegimePermission,
    StructuralSetup,
)
from ftx_paper.strategies.orderflow.interactions import (
    InteractionSnapshot,
    InteractionStatus,
)
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry, ReplayContext
from ftx_paper.strategies.orderflow.transition import OrderflowTransitionOwner


def test_replay_records_decision_clock_at_five_minute_boundary() -> None:
    rows = [
        {"time": "09:29", "session_date": "2026-09-29"},
        {"time": "09:30", "session_date": "2026-09-29"},
    ]
    events = (
        (
            "2026-09-29T09:25", {},
            {"time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"},
            {"timeframe": "5m", "time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"},
        ),
        (
            "2026-09-29T09:29", {},
            {"time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"},
            {"timeframe": "1m", "time": "2026-09-29T09:28", "available_at": "2026-09-29T09:29"},
        ),
        (
            "2026-09-29T09:30", {},
            {"time": "2026-09-29T09:20", "available_at": "2026-09-29T09:25"},
            {"timeframe": "1m", "time": "2026-09-29T09:29", "available_at": "2026-09-29T09:30"},
        ),
        (
            "2026-09-29T09:30", {},
            {"time": "2026-09-29T09:25", "available_at": "2026-09-29T09:30"},
            {"timeframe": "5m", "time": "2026-09-29T09:25", "available_at": "2026-09-29T09:30"},
        ),
    )

    records, replay_state = ReplayMachine(
        rows,
        evidence_events=events,
        evidence_at_clock=lambda _clock: {"contexts": []},
        session_date="2026-09-29",
    ).run()

    assert replay_state.sequence == 1

    first_clock = records[0]["decision_clock"]
    assert first_clock["decision_at"] == "2026-09-29T09:29"
    assert first_clock["next_decision_at"] == "2026-09-29T09:30"
    assert first_clock["source_1m_time"] == "09:29"
    assert first_clock["new_5m"] is True
    assert first_clock["previous_5m_available_at"] is None
    assert first_clock["updated_5m_available_at"] == "2026-09-29T09:25"
    assert first_clock["available_1m"] == [{
        "time": "2026-09-29T09:28", "available_at": "2026-09-29T09:29",
    }]

    second_clock = records[1]["decision_clock"]
    assert second_clock["new_5m"] is True
    assert second_clock["previous_5m_available_at"] == "2026-09-29T09:25"
    assert second_clock["updated_5m_available_at"] == "2026-09-29T09:30"
    assert second_clock["available_1m"][-1] == {
        "time": "2026-09-29T09:29", "available_at": "2026-09-29T09:30",
    }
    assert second_clock["available_5m"][-1] == {
        "time": "2026-09-29T09:25", "available_at": "2026-09-29T09:30",
    }


def test_replay_captures_execution_diagnostic_order_without_changing_events() -> None:
    context = ReplayContext(execution_engine=ExecutionEngine())
    context.record_event({
        "kind": "trigger_observation",
        "lifecycle_id": "life-1",
        "observed_at": "2026-09-29T09:21:00Z",
        "available_at": "2026-09-29T09:22:00Z",
        "eligible": True,
    })
    context.record_event({"kind": "location_interaction_observed"})

    assert [item["kind"] for item in context.execution_diagnostics] == [
        "trigger_observation",
    ]
    assert context.execution_diagnostics[0]["sequence"] == 0
    assert len(context.events) == 2


def test_replay_event_identity_is_not_based_on_global_event_ordinal() -> None:
    first = ReplayContext(execution_engine=ExecutionEngine())
    second = ReplayContext(execution_engine=ExecutionEngine())
    event = {
        "kind": "filled",
        "lifecycle_id": "life-1",
        "intent_id": "intent-1",
        "observed_at": "2026-09-29T09:21:00Z",
        "available_at": "2026-09-29T09:21:00Z",
    }
    first.record_event(event)
    second.record_event({"kind": "location_interaction_observed"})
    second.record_event(event)

    assert first.events[0]["neutral_event_key"] == second.events[1]["neutral_event_key"]


def test_replay_event_identity_includes_causal_parent() -> None:
    first = ReplayContext(execution_engine=ExecutionEngine())
    second = ReplayContext(execution_engine=ExecutionEngine())
    event = {
        "kind": "exit_signal",
        "lifecycle_id": "life-1",
        "observed_at": "2026-09-29T09:21:00Z",
        "available_at": "2026-09-29T09:22:00Z",
    }
    first.record_event({**event, "causal_parent_id": "parent-1"})
    second.record_event({**event, "causal_parent_id": "parent-2"})

    assert first.events[0]["neutral_event_key"] != second.events[0]["neutral_event_key"]


def test_inactive_lifecycle_without_position_skips_management() -> None:
    engine = ExecutionEngine()
    engine.register("life-1")
    owner = ExecutionCoordinator(machine=SimpleNamespace(context=None), engine=engine)
    life = SimpleNamespace(active=False, lifecycle_id="life-1")

    owner.handle_management(
        0,
        "2026-09-29T09:21:00Z",
        "2026-09-29T09:22:00Z",
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
        [],
        [],
        life,
    )


def test_transition_snapshot_restores_canonical_context_and_identity_state() -> None:
    owner = OrderflowTransitionOwner(LocationRegistry("orderflow:test"))
    context = MarketContext(
        context_id="ctx-1",
        version="v1",
        timestamp=datetime(2026, 9, 29, 9, 25, tzinfo=timezone.utc),
        available_at=datetime(2026, 9, 29, 9, 30, tzinfo=timezone.utc),
        location=None,
        market_state="BALANCE",
        evidence=({"source": "5m"},),
    )
    owner.context.publish_context(context)
    owner.context.market_contexts.ordinary_context = ContextVersionRef("ctx-1", "v1")
    owner.context.record_event({
        "kind": "context_observed",
        "observed_at": "2026-09-29T09:25:00Z",
        "available_at": "2026-09-29T09:30:00Z",
    })
    owner._published_intents.add("intent-1")

    snapshot = owner.snapshot()
    assert snapshot["schema_version"] == 2
    assert snapshot["canonical_state"]["context_store"]["versions"]["ctx-1"][0]["version"] == "v1"
    assert snapshot["canonical_state"]["published_intents"] == ["intent-1"]

    restored = OrderflowTransitionOwner(LocationRegistry("orderflow:other"))
    restored.restore_snapshot(snapshot)
    reference = restored.context.market_contexts.ordinary_context
    assert reference == ContextVersionRef("ctx-1", "v1")
    assert restored.context.context(reference) == context
    assert restored.context.events[0]["kind"] == "context_observed"
    assert restored.location_registry.stream_id == "orderflow:test"


def test_transition_snapshot_restores_tuple_entry_ownership() -> None:
    owner = OrderflowTransitionOwner(LocationRegistry("orderflow:test"))
    key = ("2026-09-29", "NIFTY", "long", 100.0, "2026-09-29T09:30:00Z")
    owner.engine._entry_owners[key] = "life-1"

    restored = OrderflowTransitionOwner(LocationRegistry("orderflow:other"))
    restored.restore_snapshot(owner.snapshot())

    assert restored.engine._entry_owners == {key: "life-1"}


def test_transition_snapshot_restores_regime_setup_and_empty_opportunities() -> None:
    owner = OrderflowTransitionOwner(LocationRegistry("orderflow:test"))
    owner.context.regime = RegimePermission(
        "regime-1", "opening_gap", Permission.BLOCKED, Permission.ALLOWED,
        restrictions=("causal_opening_gap",),
    )
    owner.context.execution_engine.register("life-1")
    owner.context.execution_engine._states["life-1"].setup = StructuralSetup(
        setup_id="setup-1", version="setup-1.v1", regime_version="regime-1",
        direction="short", kind="breakout", entry_zone_low=99.0,
        entry_zone_high=100.0, structural_reference=99.5, sweep_extreme=101.0,
        failure_boundary=101.5,
        expires_at=datetime(2026, 9, 29, 10, 0, tzinfo=timezone.utc),
        lifecycle_id="life-1",
    )

    snapshot = owner.snapshot()
    restored = OrderflowTransitionOwner(LocationRegistry("orderflow:other"))
    restored.context.opportunities["stale"] = object()  # type: ignore[assignment]
    restored.restore_snapshot(snapshot)

    assert restored.context.regime == owner.context.regime
    assert restored.context.opportunities == {}
    setup = restored.context.execution_engine.setup_for("life-1")
    assert isinstance(setup, StructuralSetup)
    assert setup.setup_id == "setup-1"
    assert setup.expires_at == datetime(2026, 9, 29, 10, 0, tzinfo=timezone.utc)


def test_transition_snapshot_restores_typed_interactions_and_session_state() -> None:
    owner = OrderflowTransitionOwner(LocationRegistry("orderflow:test"))
    candle = Aggregate(
        time=datetime(2026, 9, 29, 3, 45, tzinfo=timezone.utc),
        end_time=datetime(2026, 9, 29, 3, 50, tzinfo=timezone.utc),
        available_at=datetime(2026, 9, 29, 3, 50, tzinfo=timezone.utc),
        timeframe="5m", source_bar_count=5, open=100.0, high=102.0,
        low=99.0, close=101.0, volume=10, symbol="NIFTY", expiry="",
        contract_type="CONTINUOUS", session_date="2026-09-29",
        source_fingerprint="source",
    )
    interaction = InteractionSnapshot(
        interaction_id="interaction-1", instrument="NIFTY",
        session_date="2026-09-29", location="OR_HIGH", timeframe="5m",
        template_version="v1", creation_candle=candle, reference_level=101.0,
        band=LocationBand(100.0, 102.0), pre_contact_side=None,
        interaction_started_at=candle.end_time, available_at=candle.available_at,
        reference_generation=1, sessions_carried=0,
        status=InteractionStatus.ACTIVE, departure_direction=None,
        terminal_close_side=None, terminal_candle_id=None, close_reason=None,
        candles=(), synthetic_snapshots=(),
    )
    owner.context.interactions[interaction.interaction_id] = interaction
    owner.context.session = SimpleNamespace(
        failed_reversal_counts={"OR_HIGH": 1}, failed_reversal_locked=set(),
        failed_reversal_locked_at={},
    )
    snapshot = owner.snapshot()

    restored = OrderflowTransitionOwner(LocationRegistry("orderflow:other"))
    restored.restore_snapshot(snapshot)

    recovered = restored.context.interactions["interaction-1"]
    assert isinstance(recovered, InteractionSnapshot)
    assert recovered.creation_candle == candle
    assert restored.context.session.failed_reversal_counts == {"OR_HIGH": 1}
