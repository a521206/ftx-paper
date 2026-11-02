from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.orderflow.compare_canonical_replay import (
    _exit_diagnostics,
    _first_exit_diagnostic_difference,
    _first_execution_trace_difference,
    _resolve_canonical_artifact,
    _timeout_before_canonical_trigger,
    _execution_trace_diagnostics,
)
from ftx_paper.strategies.orderflow.execution import ExecutionEngine
from ftx_paper.strategies.orderflow import interaction_replay as interaction_replay_module
from ftx_paper.strategies.orderflow.context_location_replay import ContextLocationReplay
from ftx_paper.strategies.orderflow.interaction_replay import (
    InteractionResponseReplay,
    replay_interaction_response,
)
from ftx_paper.strategies.orderflow.interactions import ProjectionSnapshot
from ftx_paper.strategies.orderflow.lifecycle import SetupOpportunity
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.opportunity_coordinator import OpportunityCoordinator
from ftx_paper.strategies.orderflow.phase7_projection import build_phase7_projection
from ftx_paper.strategies.orderflow.state_machine_reporting import build_replay_payload
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry, ReplayContext
from ftx_paper.strategies.orderflow.templates import DEFAULT_TEMPLATES, Direction, OpeningInteractionKind
from ftx_paper.strategies.orderflow.timestamps import parse_session_timestamp, serialize_utc


def test_resolve_canonical_artifact_reuses_retained_canonical_file(tmp_path) -> None:
    session_date = "2026-10-05"
    artifact = tmp_path / session_date / "run" / f"{session_date}-canonical.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text('{"sessions":{"2026-10-05":{}}}', encoding="utf-8")

    assert _resolve_canonical_artifact(tmp_path, session_date) == artifact


def _payload(*, expiry: bool) -> dict[str, object]:
    lifecycle = {
        "kind": "setup_expired" if expiry else "lifecycle_formed",
        "available_at": "2026-10-01T09:20:00Z",
        "values": {"lifecycle_id": "life-1"},
    }
    return {
        "parity_phase_facts": {"opportunity_lifecycle": [lifecycle]},
    }


def test_comparison_rejects_paper_timeout_before_canonical_trigger() -> None:
    canonical = {
        "parity_phase_facts": {
            "opportunity_lifecycle": [_payload(expiry=False)["parity_phase_facts"]["opportunity_lifecycle"][0]],
        },
        "diagnostic": {
            "execution_diagnostics": [{
                "kind": "trigger_observation",
                "lifecycle_id": "life-1",
                "level": "1m",
                "eligible": True,
                "observed_at": "2026-10-01T09:21:00Z",
            }],
        },
    }
    paper = _payload(expiry=True)

    result = _timeout_before_canonical_trigger(canonical, paper)

    assert result is not None
    assert result["status"] == "MISMATCHED"
    assert result["reason"] == "paper_timeout_precedes_first_eligible_canonical_trigger"


def test_comparison_ignores_timeout_after_canonical_trigger() -> None:
    canonical = {
        "replay": {},
        "parity_phase_facts": {
            "opportunity_lifecycle": [_payload(expiry=False)["parity_phase_facts"]["opportunity_lifecycle"][0]],
        },
        "diagnostic": {
            "execution_diagnostics": [{
                "kind": "trigger_observation",
                "lifecycle_id": "life-1",
                "level": "1m",
                "eligible": True,
                "observed_at": "2026-10-01T09:21:00Z",
            }],
        },
    }
    paper = _payload(expiry=True)
    paper["parity_phase_facts"]["opportunity_lifecycle"][0]["available_at"] = "2026-10-01T09:22:00Z"

    assert _timeout_before_canonical_trigger(canonical, paper) is None


def test_comparison_uses_first_paper_timeout_and_requires_explicit_trigger_level() -> None:
    canonical = {
        "parity_phase_facts": {
            "opportunity_lifecycle": [_payload(expiry=False)["parity_phase_facts"]["opportunity_lifecycle"][0]],
        },
        "diagnostic": {
            "execution_diagnostics": [
                {
                    "kind": "trigger_observation",
                    "lifecycle_id": "life-1",
                    "eligible": True,
                    "observed_at": "2026-10-01T09:21:00Z",
                },
                {
                    "kind": "trigger_observation",
                    "lifecycle_id": "life-1",
                    "level": "1m",
                    "eligible": True,
                    "observed_at": "2026-10-01T09:23:00Z",
                },
            ],
        },
    }
    paper = {
        "parity_phase_facts": {
            "opportunity_lifecycle": [
                {
                    "kind": "setup_expired",
                    "available_at": "2026-10-01T09:22:00Z",
                    "values": {"lifecycle_id": "life-1"},
                },
                {
                    "kind": "opportunity_timeout",
                    "available_at": "2026-10-01T09:25:00Z",
                    "values": {"lifecycle_id": "life-1"},
                },
            ],
        },
    }

    result = _timeout_before_canonical_trigger(canonical, paper)

    assert result is not None
    assert result["paper_expiry"] == "2026-10-01T09:22:00.000000Z"
    assert result["canonical_trigger"] == "2026-10-01T09:23:00.000000Z"


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

    assert payload["execution_transitions"] == [transition]
    assert payload["ledger_mutations"] == [ledger]


def test_phase7_exit_diagnostics_normalize_utc_and_preserve_missing_exit_event() -> None:
    canonical = {
        "diagnostic": {
            "execution_diagnostics": [
                {
                    "kind": "execution_lifecycle_tick",
                    "lifecycle_id": "life-1",
                    "observed_at": "09:30",
                    "available_at": "09:31",
                    "bar": {"time": "09:30", "close": 100.0},
                    "before": {"position": {"status": "open"}},
                    "after": {"position": {"status": "open"}},
                },
                {
                    "kind": "exit_signal",
                    "lifecycle_id": "life-1",
                    "observed_at": "09:40",
                    "available_at": "09:41",
                    "reason": "vwap_stop",
                },
            ],
        },
    }
    paper = {
        "execution_diagnostics": [
            {
                "kind": "execution_lifecycle_tick",
                "lifecycle_id": "life-1",
                "observed_at": "09:30",
                "available_at": "09:31",
                "bar": {"time": "09:30", "close": 100.0},
                "before": {"position": {"status": "open"}},
                "after": {"position": {"status": "open"}},
            },
        ],
    }

    canonical_exit = _exit_diagnostics(canonical, "life-1", canonical=True, session_date="2026-10-01")
    paper_exit = _exit_diagnostics(paper, "life-1", canonical=False, session_date="2026-10-01")

    assert canonical_exit[0]["observed_at"] == "2026-10-01T04:00:00.000000Z"
    assert canonical_exit[0]["bar"]["time"] == "2026-10-01T04:00:00.000000Z"
    difference = _first_exit_diagnostic_difference(canonical_exit, paper_exit)
    assert difference is not None
    assert difference["index"] == 1
    assert difference["canonical"]["kind"] == "exit_signal"
    assert difference["paper"] is None


def test_execution_trace_difference_preserves_duplicate_causal_clocks() -> None:
    canonical = [
        {"kind": "execution_lifecycle_tick", "observed_at": "09:30", "available_at": "09:31", "after": {"n": 1}},
        {"kind": "execution_lifecycle_tick", "observed_at": "09:30", "available_at": "09:31", "after": {"n": 2}},
    ]
    paper = [canonical[0]]

    difference = _first_execution_trace_difference(canonical, paper)

    assert difference is not None
    assert difference["occurrence"] == 1
    assert difference["canonical"] == canonical[1]
    assert difference["paper"] is None


def test_execution_trace_diagnostics_fails_closed_when_stream_is_unavailable() -> None:
    with pytest.raises(ValueError, match="canonical execution diagnostics unavailable"):
        _execution_trace_diagnostics(
            {"diagnostic": {}}, "life-1", canonical=True, session_date="2026-10-01",
        )


def test_phase6_projection_preserves_only_formed_lifecycle_facts() -> None:
    replay = InteractionResponseReplay(
        facts=(),
        opportunity_facts=(
            {"kind": "or_directional_seed"},
            {"kind": "lifecycle_formed"},
            {"kind": "setup_invalidated"},
            {"kind": "setup_expired"},
            {"kind": "setup_superseded"},
            {"kind": "opportunity_timeout"},
            {"kind": "opportunity_outcome"},
        ),
    )

    assert [
        item["kind"] for item in replay.parity_phase_facts()["opportunity_lifecycle"]
    ] == [
        "lifecycle_formed",
    ]


def test_opening_interaction_is_published_as_a_lifecycle_event_only() -> None:
    registry = LocationRegistry("test")
    context = ReplayContext(
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
            "FUT",
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
        "FUT",
        "2026-10-01",
        "replay",
    )


def test_opening_interaction_dispatches_without_a_completed_5m_aggregate(
    monkeypatch,
) -> None:
    calls: list[str] = []

    class Coordinator:
        def publish_interaction_context(self, *_args: Any) -> None:
            pass

        def handle_structure(self, *_args: Any) -> None:
            calls.append("structure")

        def handle_opening_interaction(self, *_args: Any) -> None:
            calls.append("opening")

    monkeypatch.setattr(
        interaction_replay_module,
        "build_opportunity_coordinator",
        lambda *_args, **_kwargs: Coordinator(),
    )
    one_minute = _aggregate(
        "1m",
        "2026-10-01T03:50:00+00:00",
        "2026-10-01T03:51:00+00:00",
        "2026-10-01T03:51:00+00:00",
    )

    replay_interaction_response(
        {"1m": (one_minute,), "5m": ()},
        ContextLocationReplay((), ()),
        LocationRegistry("test"),
        atr_by_session={"2026-10-01": 1.0},
    )

    assert calls == ["opening"]


def test_timeout_expires_before_structure_admission(monkeypatch) -> None:
    structure_saw_active: list[bool] = []
    life = SetupOpportunity(
        lifecycle_id="life-timeout",
        direction=Direction.LONG,
        session_date="2026-10-01",
        instrument="NIFTY",
        strategy="orderflow",
        formation_at=datetime.fromisoformat("2026-10-01T03:44:00+00:00"),
        formation_available_at=datetime.fromisoformat("2026-10-01T03:44:00+00:00"),
        candidate_id="life-timeout",
        parent_context=None,
        location=None,
        reference_level=100.0,
        context_candle=None,
        breakout_level=101.0,
        failure_boundary=99.0,
        timeout_limit=1,
    )

    class Coordinator:
        def publish_interaction_context(self, *_args: Any) -> None:
            pass

        def handle_structure(self, *_args: Any) -> None:
            structure_saw_active.append(life.active)

        def handle_opening_interaction(self, *_args: Any) -> None:
            pass

    def build_coordinator(transition: Any, **_kwargs: Any) -> Coordinator:
        assert transition.context.register_opportunity(life)
        return Coordinator()

    monkeypatch.setattr(
        interaction_replay_module,
        "build_opportunity_coordinator",
        build_coordinator,
    )
    one_minute = _aggregate(
        "1m",
        "2026-10-01T03:50:00+00:00",
        "2026-10-01T03:51:00+00:00",
        "2026-10-01T03:51:00+00:00",
    )
    five_minute = _aggregate(
        "5m",
        "2026-10-01T03:45:00+00:00",
        "2026-10-01T03:50:00+00:00",
        "2026-10-01T03:50:00+00:00",
    )

    replay = replay_interaction_response(
        {"1m": (one_minute,), "5m": (five_minute,)},
        ContextLocationReplay((), ()),
        LocationRegistry("test"),
        atr_by_session={"2026-10-01": 1.0},
    )

    assert structure_saw_active == [False]
    assert any(item["kind"] == "setup_expired" for item in replay.opportunity_facts)


def test_pd_role_context_uses_post_contact_evidence_and_has_context_identity() -> None:
    context = ReplayContext(execution_engine=ExecutionEngine())
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
        evidence.contract_type,
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
    assert any(
        item["kind"] == "role_context_available"
        for item in context.events
    )
    assert "interaction_response" not in context.parity_snapshot()


def test_session_timestamp_normalizes_local_seconds_and_offsets() -> None:
    assert serialize_utc(parse_session_timestamp(
        "2026-10-01", "2026-10-01T09:21:00",
    )) == "2026-10-01T03:51:00.000000Z"
    assert serialize_utc(parse_session_timestamp(
        "2026-10-01", "2026-10-01T09:21:00+05:30",
    )) == "2026-10-01T03:51:00.000000Z"
