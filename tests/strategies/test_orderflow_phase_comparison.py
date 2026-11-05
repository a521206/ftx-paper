from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from ftx_paper.strategies.orderflow.execution import ExecutionEngine, validate_exit_plan
from ftx_paper.strategies.orderflow.interactions import ProjectionSnapshot
from ftx_paper.strategies.orderflow.lifecycle import SetupOpportunity
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.opportunity_coordinator import OpportunityCoordinator
from ftx_paper.strategies.orderflow.opportunity import (
    DirectionalResponse,
    OpportunityContext,
    form_opportunity,
)
from ftx_paper.strategies.orderflow.parity_artifacts import normalize_phase_record
from ftx_paper.strategies.orderflow.phase7_projection import build_phase7_projection
from ftx_paper.strategies.orderflow.state_machine_reporting import build_replay_payload
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry, OrderflowState
from ftx_paper.strategies.orderflow.templates import (
    DEFAULT_TEMPLATES,
    Direction,
    LocationTemplate,
    OpeningInteractionKind,
    TRADE_TEMPLATES,
)


def test_conditional_exit_is_required_at_entry_like_canonical() -> None:
    class ConditionalRule:
        mode = "conditional"
        required = False

        @staticmethod
        def validation_reason(*, direction: str, fill_price: float) -> str:
            del direction, fill_price
            return "optional_exit_level_unavailable"

    rule = ConditionalRule()
    accepted, rejected, required_rejections = validate_exit_plan(
        (rule,), direction="long", fill_price=100.0,
    )

    assert accepted == ()
    assert rejected == (rule,)
    assert required_rejections == (rule,)


def test_form_opportunity_assigns_location_reference_generation_conflict_key() -> None:
    observed_at = datetime.fromisoformat("2026-10-01T03:50:00+00:00")
    response = DirectionalResponse(
        direction=Direction.LONG,
        anchor=LocationTemplate.OR_LOW,
        reference_level=22590.0,
        structure_boundary=22595.0,
        invalidation_boundary=22580.0,
        entry_boundary=22595.0,
        evidence_ids=("evidence-1",),
        observed_at=observed_at,
        available_at=observed_at,
    )
    life, _ = form_opportunity(
        response,
        OpportunityContext(
            lifecycle_id="life-1",
            session_date="2026-10-01",
            instrument="NIFTY",
            strategy="orderflow",
            candidate_id="candidate-1",
            context_candle=SimpleNamespace(low=22580.0, high=22600.0),
            clock=observed_at,
            regime_version="regime-1",
            policy_version="policy-1",
            reference_generation=2,
        ),
        TRADE_TEMPLATES[("5m_recovery_breakout", "OR_LOW")],
    )

    assert life.conflict_key == ("2026-10-01", "NIFTY", "OR_LOW", 22590.0, 2)
    assert life.comparison_key == (
        "2026-10-01", "NIFTY", "OR_LOW", "long", 22590.0, 2, "policy-1",
    )


def test_parity_artifact_normalizes_embedded_execution_group_timestamp() -> None:
    base = {
        "kind": "lifecycle_formed",
        "occurred_at": "2026-10-05T03:50:00Z",
        "observed_at": "2026-10-05T03:50:00Z",
        "available_at": "2026-10-05T03:50:00Z",
        "identity": "life-1",
        "causal_parent_id": None,
        "unavailable": [],
    }
    canonical = normalize_phase_record({
        **base,
        "values": {
            "execution_group_id": (
                "execution-group:2026-10-05:NIFTY:high-short-recovery:2026-10-05T09:35"
            ),
        },
    }, sequence=0)
    paper = normalize_phase_record({
        **base,
        "values": {
            "execution_group_id": (
                "execution-group:2026-10-05:NIFTY:high-short-recovery:2026-10-05T04:05:00.000000Z"
            ),
        },
    }, sequence=0)

    assert canonical["values"] == paper["values"]


def _payload(*, expiry: bool) -> dict[str, object]:
    lifecycle = {
        "kind": "setup_expired" if expiry else "lifecycle_formed",
        "available_at": "2026-10-01T09:20:00Z",
        "values": {"lifecycle_id": "life-1"},
    }
    return {
        "parity_phase_facts": {"opportunity_lifecycle": [lifecycle]},
    }


def test_phase7_preserves_fill_identity_and_kind() -> None:
    projection = build_phase7_projection({
        "parity_phase_facts": {
            "execution_fills": [{
                "identity": "fill-1",
                "kind": "partial_exit",
                "occurred_at": "2026-10-01T09:21:00Z",
                "observed_at": "2026-10-01T09:21:00Z",
                "available_at": "2026-10-01T09:21:00Z",
                "values": {"fill_id": "fill-1", "kind": "partial_exit"},
            }],
        },
    })

    assert projection["execution_fills"][0]["identity"] == "fill-1"
    assert projection["execution_fills"][0]["kind"] == "partial_exit"


def test_lifecycle_parity_stream_contains_formations_only() -> None:
    context = OrderflowState(
        location_registry=LocationRegistry("test"),
        execution_engine=ExecutionEngine(),
    )
    events: list[dict[str, Any]] = []

    context.emit({"kind": "lifecycle_formed", "observed_at": "09:20", "available_at": "09:20"}, events)
    context.emit({"kind": "setup_expired", "observed_at": "09:25", "available_at": "09:25"}, events)

    assert [item["kind"] for item in context.parity_streams["opportunity_lifecycle"]] == [
        "lifecycle_formed",
    ]


def test_phase7_fill_identity_uses_fill_id_when_identity_is_transport_metadata() -> None:
    projection = build_phase7_projection({
        "parity_phase_facts": {
            "execution_fills": [{
                "fill_id": "fill-1",
                "kind": "entry",
                "opportunity_id": "life-1",
                "occurred_at": "2026-10-01T09:21:00Z",
                "observed_at": "2026-10-01T09:21:00Z",
                "available_at": "2026-10-01T09:21:00Z",
                "values": {"fill_id": "fill-1", "kind": "entry"},
            }],
        },
    })

    assert projection["execution_fills"][0]["identity"] == "fill_id:fill-1:0"


def test_phase7_reporting_preserves_producer_owned_transition_and_ledger_facts() -> None:
    transition = {
        "kind": "entry_pending",
        "occurred_at": "2026-10-01T09:21:00Z",
        "observed_at": "2026-10-01T09:21:00Z",
        "available_at": "2026-10-01T09:22:00Z",
        "identity": "transition-1",
        "causal_parent_id": "life-1",
        "values": {"lifecycle_id": "life-1"},
    }
    ledger = {
        "kind": "reservation",
        "occurred_at": "2026-10-01T09:20:00Z",
        "observed_at": "2026-10-01T09:20:00Z",
        "available_at": "2026-10-01T09:20:00Z",
        "identity": "reservation-1",
        "causal_parent_id": "life-1",
        "values": {"reservation_id": "reservation:life-1"},
    }

    class Context:
        execution_engine = None
        parity_streams = {
            "execution_transitions": [transition],
            "ledger_mutations": [ledger],
        }
        execution_diagnostics: tuple[dict[str, Any], ...] = ()

    payload = build_replay_payload(Context(), [], [], [])

    assert payload["execution_transitions"][0]["identity"] == transition["identity"]
    assert payload["execution_transitions"][0]["sequence"] == 0
    assert payload["execution_transitions"][0]["occurred_at"] == "2026-10-01T09:21:00.000000Z"
    assert payload["ledger_mutations"][0]["identity"] == ledger["identity"]
    assert payload["ledger_mutations"][0]["sequence"] == 0
    assert payload["ledger_mutations"][0]["occurred_at"] == "2026-10-01T09:20:00.000000Z"
    assert payload["execution_diagnostic_summary"] == {
        "record_count": 0,
        "by_kind": {},
        "lifecycle_count": 0,
        "records_by_lifecycle": {},
        "first_sequence": None,
        "last_sequence": None,
    }


def test_replay_context_records_interactions_and_explicit_phase_facts() -> None:
    context = OrderflowState(execution_engine=ExecutionEngine())
    context.record_event({
        "kind": "interaction_created",
        "identity": "interaction:NIFTY:2026-10-05:OR_HIGH:1m:09:19",
        "observed_at": "2026-10-05T09:19:00+05:30",
        "available_at": "2026-10-05T09:20:00+05:30",
    })
    context.record_parity_fact("source_observation", {
        "kind": "source_observation",
        "occurred_at": "2026-10-05T09:15:00+05:30",
        "identity": "aggregate:1m:2026-10-05T09:15",
        "observed_at": "2026-10-05T09:15:00+05:30",
        "available_at": "2026-10-05T09:16:00+05:30",
        "causal_parent_id": None,
        "values": {
            "aggregate": {"close": 22654.7},
            "source_fingerprint": "fingerprint-1",
        },
        "unavailable": [],
        "lineage": [],
    })

    assert context.parity_streams["interaction_response"][0]["kind"] == "interaction_created"
    assert context.parity_streams["source_observation"][0]["identity"] == "aggregate:1m:2026-10-05T09:15"
    source = context.parity_streams["source_observation"][0]
    assert source["values"] == {
        "aggregate": {"close": 22654.7},
        "source_fingerprint": "fingerprint-1",
    }
    assert source["lineage"] == []


def test_interaction_response_phase_fact_keeps_direct_fields_and_session_date() -> None:
    context = OrderflowState(execution_engine=ExecutionEngine())
    context.current_session_date = "2026-10-05"
    context.record_event({
        "kind": "location_interaction_observed",
        "observed_at": "2026-10-05T09:19:00+05:30",
        "available_at": "2026-10-05T09:20:00+05:30",
        "interaction_id": "interaction-1",
        "location": "OR_HIGH",
    })

    fact = context.parity_streams["interaction_response"][0]
    assert "values" not in fact["values"]
    assert fact["values"]["interaction_id"] == "interaction-1"
    assert fact["values"]["location"] == "OR_HIGH"
    assert fact["values"]["session_date"] == "2026-10-05"


def test_interaction_response_phase_fact_serializes_datetime_causal_times() -> None:
    context = OrderflowState(execution_engine=ExecutionEngine())
    observed_at = datetime(2026, 10, 5, 3, 49, tzinfo=timezone.utc)
    available_at = datetime(2026, 10, 5, 3, 50, tzinfo=timezone.utc)
    context.record_event({
        "kind": "location_interaction_observed",
        "observed_at": observed_at,
        "available_at": available_at,
        "interaction_id": "interaction-1",
    })

    values = context.parity_streams["interaction_response"][0]["values"]
    assert values["causal_observed_at"] == "2026-10-05T03:49:00.000000Z"
    assert values["causal_available_at"] == "2026-10-05T03:50:00.000000Z"


def test_opening_interaction_is_published_as_a_lifecycle_event_only() -> None:
    registry = LocationRegistry("test")
    context = OrderflowState(
        location_registry=registry,
        execution_engine=ExecutionEngine(),
    )
    formation_at = datetime.fromisoformat("2026-10-01T03:50:00+00:00")
    life = SetupOpportunity(
        lifecycle_id="life-1",
        direction=Direction.LONG,
        session_date="2026-10-01",
        instrument="NIFTY",
        strategy="orderflow",
        formation_at=formation_at,
        formation_available_at=formation_at,
        candidate_id="life-1",
        parent_context=None,
        location=None,
        reference_level=100.0,
        context_candle=Aggregate(
            "2026-10-01T09:15",
            "2026-10-01T09:20",
            "2026-10-01T09:20",
            "1m",
            1,
            100.0,
            101.0,
            99.0,
            100.5,
            100,
            "NIFTY",
            "",
            "2026-10-01",
            "opening",
        ),
        breakout_level=101.0,
        failure_boundary=99.0,
        opening_policy_version=f"{OpeningInteractionKind.OPENING_BREAKOUT.value}-1m-v1",
    )
    context.opportunities["life-1"] = life

    class Host:
        def __init__(self) -> None:
            self.context = context
            self.execution = None
            self.location_registry = registry
            self.templates = dict(DEFAULT_TEMPLATES)
            self.interaction_detector = None

        def structure_interactions(self):
            return ()

        def selected_interaction_context(self, interaction_id: str, *, as_of: str):
            return None

    coordinator = OpportunityCoordinator(Host())
    bar_events: list[dict[str, Any]] = []
    coordinator.handle_opening_interaction(
        {"time": "09:21", "open": 100.0, "high": 101.0, "low": 99.5, "close": 100.5},
        "2026-10-01T09:21",
        "2026-10-01T09:22",
        bar_events,
    )

    assert bar_events[0]["kind"] == "opening_interaction"
    assert context.events[0]["kind"] == "opening_interaction"
    event = bar_events[0]
    assert event["observed_at"] == "2026-10-01T09:21"
    assert event["available_at"] == "2026-10-01T09:22"
    assert isinstance(event["observed_at"], str)
    evidence = event["evidence"][0]
    assert evidence["observed_at"] == "2026-10-01T09:21"
    assert evidence["available_at"] == "2026-10-01T09:22"


def _aggregate(
    timeframe: str, time: str, end_time: str, available_at: str,
) -> Aggregate:
    return Aggregate(
        time,
        end_time,
        available_at,
        timeframe,
        1,
        100.0,
        101.0,
        99.0,
        100.5,
        100,
        "NIFTY",
        "",
        "2026-10-01",
        "replay",
    )


def test_pd_role_context_uses_post_contact_evidence_and_has_context_identity() -> None:
    context = OrderflowState(execution_engine=ExecutionEngine())
    creation = _aggregate(
        "5m",
        "2026-10-01T03:45:00+00:00",
        "2026-10-01T03:50:00+00:00",
        "2026-10-01T03:50:00+00:00",
    )
    evidence = _aggregate(
        "5m",
        "2026-10-01T03:50:00+00:00",
        "2026-10-01T03:55:00+00:00",
        "2026-10-01T03:55:00+00:00",
    )
    evidence = Aggregate(
        evidence.time,
        evidence.end_time,
        evidence.available_at,
        evidence.timeframe,
        evidence.source_bar_count,
        evidence.open,
        evidence.high,
        102.0,
        evidence.close,
        evidence.volume,
        evidence.symbol,
        evidence.expiry,
        evidence.session_date,
        evidence.source_fingerprint,
    )
    snapshot = ProjectionSnapshot(
        "5m",
        100.0,
        103.0,
        102.0,
        102.5,
        200.0,
        ("2026-10-01T03:45:00+00:00", "2026-10-01T03:50:00+00:00"),
        creation.time,
        evidence.end_time,
        evidence.available_at,
    )
    interaction = SimpleNamespace(
        interaction_id="NIFTY:2026-10-01:PD_HIGH:5m:09:15",
        timeframe="5m",
        location="PD_HIGH",
        creation_candle=creation,
        template_version="v1",
        synthetic_snapshots=(("5m", (snapshot,)),),
        pd_role=None,
        band_low=100.0,
        band_high=101.0,
    )
    context.aggregates["5m"] = [creation, evidence]

    class Host:
        def __init__(self) -> None:
            self.context = context
            self.interaction_detector = None

    events: list[dict[str, Any]] = []
    OpportunityCoordinator(Host()).publish_interaction_context(interaction, events)

    role_event = next(item for item in events if item["kind"] == "role_context_available")
    assert role_event["role"] == "SUPPORT"
    assert role_event["context_id"] == f"interaction:{interaction.interaction_id}"
    assert role_event["context_version"] == "v1"
    assert sum(
        item["kind"] == "interaction_context_published" for item in events
    ) == 1
    assert any(
        item["kind"] == "role_context_available"
        for item in context.events
    )
    assert "interaction_response" not in context.parity_snapshot()
