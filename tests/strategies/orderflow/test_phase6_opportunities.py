from dataclasses import replace

import pytest

from ftx_paper.strategies.orderflow.hierarchy import ContextVersionRef
from ftx_paper.strategies.orderflow.interactions import LocationInteraction
from ftx_paper.strategies.orderflow.lifecycle import LifecycleStage, OpportunityStatus, SetupOpportunity
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.opportunity import OpportunityContext, OpportunityOutcomeStatus, evaluate_response, form_opportunity, identify_opportunity, opportunity_identity
from ftx_paper.strategies.orderflow.opportunity_timeout import advance_opportunity_timeout
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.parity_artifacts import build_phase4_artifact, canonical_hash, compare_phase4_manifests
from ftx_paper.strategies.orderflow.policies import PDPolicy
from ftx_paper.strategies.orderflow.templates import Direction, TRADE_TEMPLATES


def _bar(minute: str, *, close: float = 101.0) -> Aggregate:
    time = f"2026-09-15T09:{minute}"
    return Aggregate(time, time, time, "5m", 5, 100.0, 102.0, 99.0, close, 1, "NIFTY", "", "FUT", "2026-09-15", f"fp-{minute}")


def _interaction(bar: Aggregate) -> LocationInteraction:
    item = LocationInteraction.create(instrument="NIFTY", session_date=bar.session_date, location="PD_LOW", template_version="1", candle=bar, reference_level=100.0, atr=4.0, band=LocationBand(99.8, 100.2), created_at=bar.available_at, available_at=bar.available_at)
    assert item is not None
    return item


def test_identity_is_deterministic_and_template_scoped() -> None:
    args = dict(session_date="2026-09-15", instrument="NIFTY", direction=Direction.LONG, interaction_id="i", reference_generation=2, response_evidence_ids=("b",), formation_at="2026-09-15T09:20", formation_available_at="2026-09-15T09:21")
    assert opportunity_identity(**args) == opportunity_identity(**args)
    assert opportunity_identity(**args, template_id="t") != opportunity_identity(**args)


def test_opportunity_rejects_future_evidence_and_forms_setup_with_parent() -> None:
    contact = _bar("20")
    future = _bar("25")
    interaction = _interaction(contact).evaluation_snapshot()
    template = TRADE_TEMPLATES[("5m_recovery_breakout", "PD_LOW")]
    with pytest.raises(ValueError, match="outside"):
        identify_opportunity(template, interaction, [contact, future], as_of=contact.available_at, response_evidence=[future])
    outcome, identity = identify_opportunity(template, interaction, [contact], as_of=contact.available_at)
    assert identity is not None and outcome.response is not None
    life, setup = form_opportunity(outcome.response, OpportunityContext(lifecycle_id="life", session_date="2026-09-15", instrument="NIFTY", strategy="orderflow", candidate_id="candidate", context_candle=contact, clock=contact.available_at, regime_version="r", policy_version="p", parent_context=ContextVersionRef("ctx", "1")), template)
    assert life.causal_identity is None
    assert setup.parent_context_id == "ctx"
    assert life.stage is LifecycleStage.RESPONSE_CONFIRMED


def test_response_requires_contact_candle_and_matching_instrument() -> None:
    contact = _bar("20")
    interaction = _interaction(contact).evaluation_snapshot()
    template = TRADE_TEMPLATES[("5m_recovery_breakout", "PD_LOW")]
    later = replace(contact, time="2026-09-15T09:25", end_time="2026-09-15T09:29", available_at="2026-09-15T09:30", symbol="BANKNIFTY")
    outcome = evaluate_response(template, interaction, [later], as_of="2026-09-15T09:30")
    assert outcome.status is OpportunityOutcomeStatus.PENDING
    assert outcome.reason == "contact_not_available"


def test_pd_policy_publishes_role_after_causal_bars_are_evaluated() -> None:
    contact = _bar("20")
    interaction = _interaction(contact)
    post_contact = replace(
        contact, time="2026-09-15T09:25", end_time="2026-09-15T09:29",
        available_at="2026-09-15T09:30", high=99.5, low=99.0, close=99.2,
    )
    template = TRADE_TEMPLATES[("5m_recovery_breakout", "PD_LOW")]
    events: list[dict[str, object]] = []

    PDPolicy().evaluate_hypotheses(
        (template,), interaction.evaluation_snapshot(), (post_contact,),
        as_of=post_contact.available_at, state=interaction, emit=events.append,
    )

    assert interaction.pd_role == "RESISTANCE"
    assert events[0]["kind"] == "role_transition"
    assert events[0]["role"] == "RESISTANCE"
    assert events[1]["kind"] == "role_context_available"
    assert events[1]["role"] == "RESISTANCE"
    assert events[1]["available_at"] == post_contact.available_at

    no_evidence_events: list[dict[str, object]] = []
    PDPolicy().evaluate_hypotheses(
        (template,), interaction.evaluation_snapshot(), (),
        as_of=contact.available_at, state=interaction, emit=no_evidence_events.append,
    )
    assert no_evidence_events == []


def test_response_expires_after_carried_session_boundary() -> None:
    contact = _bar("20")
    source = _interaction(contact)
    source.sessions_carried = 2
    interaction = source.evaluation_snapshot()
    template = TRADE_TEMPLATES[("5m_recovery_breakout", "PD_LOW")]
    outcome = evaluate_response(template, interaction, [contact], as_of="2026-09-15T09:30")
    assert outcome.status is OpportunityOutcomeStatus.EXPIRED
    assert outcome.reason == "response_window_expired"


def test_timeout_duplicate_retry_is_idempotent_and_lifecycle_close_reasons_are_explicit() -> None:
    bar = _bar("20")
    life = SetupOpportunity("life", Direction.LONG, bar.session_date, bar.symbol, "orderflow", bar.time, bar.available_at, "candidate", None, "PD_LOW", 100.0, bar, 102.0, 99.0)
    later = replace(bar, time="2026-09-15T09:25", end_time="2026-09-15T09:29", available_at="2026-09-15T09:30")
    first = advance_opportunity_timeout(life, interaction=None, evidence=[later], as_of="2026-09-15T09:30")
    second = advance_opportunity_timeout(life, interaction=None, evidence=[later], as_of="2026-09-15T09:30")
    assert first.added_units == 1 and second.added_units == 0
    life.invalidate(at="2026-09-15T09:31", reason="timeout", expired=True)
    assert life.status is OpportunityStatus.EXPIRED
    assert life.events[-1].payload["reason"] == "timeout"


def test_retest_and_supersession_are_causal_and_execution_group_eligibility_is_explicit() -> None:
    bar = _bar("20")
    life = SetupOpportunity("life", Direction.LONG, bar.session_date, bar.symbol, "orderflow", bar.time, bar.available_at, "candidate", None, "PD_LOW", 100.0, bar, 102.0, 99.0, execution_group_id="group-1")
    assert life.execution_group_eligible({})
    life.mark_retest(at="2026-09-15T09:21", evidence={"bar": "09:20"})
    assert life.stage is LifecycleStage.WAITING_FOR_RETEST
    life.mark_retest(at="2026-09-15T09:22", confirmed=True)
    assert life.stage is LifecycleStage.RETEST_CONFIRMED
    assert not life.execution_group_eligible({"group-1": "winner"})
    life.supersede(at="2026-09-15T09:23", reason="replacement")
    assert life.status is OpportunityStatus.SUPERSEDED
    assert life.events[-1].payload["stage_after"] == LifecycleStage.SUPERSEDED.value
    with pytest.raises(ValueError, match="precede formation"):
        SetupOpportunity("future", Direction.LONG, bar.session_date, bar.symbol, "orderflow", bar.time, bar.available_at, "candidate", None, "PD_LOW", 100.0, bar, 102.0, 99.0).supersede(at="2026-09-15T09:19")


def test_incomplete_timeout_projection_does_not_advance_count() -> None:
    bar = _bar("20")
    life = SetupOpportunity("life", Direction.LONG, bar.session_date, bar.symbol, "orderflow", bar.time, bar.available_at, "candidate", None, "PD_LOW", 100.0, bar, 102.0, 99.0)
    from ftx_paper.strategies.orderflow.interactions import ProjectionSnapshot
    interaction = _interaction(bar)
    interaction.synthetic["5m"].snapshots = [ProjectionSnapshot("5m", 100, 101, 99, 100, 1, (bar.time,), bar.time, "2026-09-15T09:26", "2026-09-15T09:25")]
    result = advance_opportunity_timeout(life, interaction=interaction.evaluation_snapshot(), evidence=(), as_of="2026-09-15T09:30")
    assert result.added_units == 0
    assert life.timeout_count == 0


def _lifecycle_fact(*, identity: str = "opp-1", timeout_count: int = 2, close_reason: str | None = None) -> dict[str, object]:
    return {
        "kind": "opportunity_lifecycle",
        "occurred_at": "2026-09-15T09:30:00+00:00",
        "observed_at": "2026-09-15T09:30:00+00:00",
        "available_at": "2026-09-15T09:31:00+00:00",
        "identity": identity,
        "causal_parent_id": "response-1",
        "values": {
            "lifecycle_id": "life-1",
            "setup_id": "candidate-1",
            "stage": "EXPIRED",
            "status": "EXPIRED",
            "timeout_count": timeout_count,
            "evidence_ids": ["2026-09-15T09:25"],
            "close_reason": close_reason or "timeout",
        },
        "unavailable": [],
    }


def test_opportunity_parity_is_manifest_first_and_reports_first_field_mismatch() -> None:
    streams = {"source_observation": [], "context_location": [], "interaction_response": [], "opportunity_lifecycle": [_lifecycle_fact()], "intent_admission": [], "trigger_exit": []}
    canonical = build_phase4_artifact(run_id="canonical", producer={"name": "canonical"}, scope={}, payload={"parity_phase_facts": streams})
    paper = build_phase4_artifact(run_id="paper", producer={"name": "paper"}, scope={}, payload={"parity_phase_facts": {**streams, "opportunity_lifecycle": [_lifecycle_fact(timeout_count=3)]}})
    result = compare_phase4_manifests(canonical, paper, canonical_records=lambda stream: canonical["phase_streams"][stream], paper_records=lambda stream: paper["phase_streams"][stream])
    assert result["status"] == "MISMATCHED"
    assert result["stream"] == "opportunity_lifecycle"
    assert result["field"] == "values.timeout_count"
    assert result["canonical_value"] == 2
    assert result["paper_value"] == 3


def test_opportunity_duplicate_retry_produces_the_same_normalized_hash() -> None:
    fact = _lifecycle_fact()
    first = build_phase4_artifact(run_id="run", producer={"name": "paper"}, scope={}, payload={"parity_phase_facts": {"opportunity_lifecycle": [fact]}})
    retry = build_phase4_artifact(run_id="run", producer={"name": "paper"}, scope={}, payload={"parity_phase_facts": {"opportunity_lifecycle": [fact]}})
    assert first["manifest"]["streams"]["opportunity_lifecycle"] == retry["manifest"]["streams"]["opportunity_lifecycle"]
    assert canonical_hash(first["phase_streams"]["opportunity_lifecycle"]) == canonical_hash(retry["phase_streams"]["opportunity_lifecycle"])
