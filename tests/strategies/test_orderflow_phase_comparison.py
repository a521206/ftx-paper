from datetime import datetime
from typing import Any

from scripts.orderflow.compare_canonical_replay import _timeout_before_canonical_trigger
from ftx_paper.strategies.orderflow.interaction_replay import _handle_opening_interaction
from ftx_paper.strategies.orderflow.phase7_projection import build_phase7_projection
from ftx_paper.strategies.orderflow.state_machine_reporting import build_replay_payload
from ftx_paper.strategies.orderflow.templates import OpeningInteractionKind
from ftx_paper.strategies.orderflow.timestamps import parse_session_timestamp, serialize_utc


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


def test_opening_interaction_is_published_as_a_phase45_fact() -> None:
    class Context:
        opportunities: dict[str, object]

        def __init__(self, life: object) -> None:
            self.opportunities = {"life-1": life}
            self.events: list[dict[str, Any]] = []

        def record_event(self, event: dict[str, Any]) -> None:
            self.events.append(event)

    class Life:
        active = True
        opening_policy_version = f"{OpeningInteractionKind.OPENING_BREAKOUT.value}-1m-v1"
        opening_interaction_kind = None
        opening_interaction_evidence: list[dict[str, object]] = []
        formation_available_at = datetime.fromisoformat("2026-10-01T09:20:00+00:00")
        reference_level = 100.0
        context_candle = object()
        direction = "long"
        lifecycle_id = "life-1"

    context = Context(Life())
    facts: list[dict[str, Any]] = []
    _handle_opening_interaction(
        context,
        {"time": "09:21", "open": 100.0, "high": 101.0, "low": 99.5, "close": 100.5},
        facts,
        timestamp="2026-10-01T09:21:00Z",
        next_time="2026-10-01T09:22:00Z",
    )

    assert facts[0]["kind"] == "opening_interaction"
    assert context.events[0]["kind"] == "opening_interaction"


def test_session_timestamp_normalizes_local_seconds_and_offsets() -> None:
    assert serialize_utc(parse_session_timestamp(
        "2026-10-01", "2026-10-01T09:21:00",
    )) == "2026-10-01T03:51:00.000000Z"
    assert serialize_utc(parse_session_timestamp(
        "2026-10-01", "2026-10-01T09:21:00+05:30",
    )) == "2026-10-01T03:51:00.000000Z"
