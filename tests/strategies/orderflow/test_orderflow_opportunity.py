from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.strategy import (
    IntentStatus, OpportunityStatus, SetupOpportunity, TradeIntent, arm_intent, record_trigger,
)
from ftx_paper.strategy.orderflow import OrderflowPolicy
from ftx_paper.strategy.orderflow_trigger import TriggerObservation


UTC = timezone.utc


def _opportunity():
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=UTC)
    return SetupOpportunity.create(
        interaction_id="interaction-1", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=15), entry_lower=99,
        entry_upper=100, failure_boundary=98, evidence_ids=("e1",),
    )


def test_intent_lifecycle_preserves_setup_and_trigger_causality() -> None:
    opportunity = _opportunity()
    intent = TradeIntent(
        intent_id="intent-1", setup_id=opportunity.setup.setup_id, regime_version="1",
        direction="long", status=IntentStatus.CONFIRMATION_FOUND,
        created_at=opportunity.setup.formed_at, trigger_price=100.0,
        failure_boundary=98.0, expiry=opportunity.setup.expires_at,
        setup_version=opportunity.setup.version, lifecycle_id=opportunity.lifecycle_id,
    )
    armed = arm_intent(
        intent=intent, setup=opportunity.setup, regime="2",
        at=opportunity.setup.formed_at,
    )
    triggered = record_trigger(
        intent=armed,
        observation=TriggerObservation(
            intent_id=armed.intent_id, observed_at="2026-01-01T09:21:00+00:00",
            available_at="2026-01-01T09:21:00+00:00", crossed=True, eligible=True,
            price=100.5, reason="entry boundary crossed",
        ),
    )
    assert armed.status is IntentStatus.ARMED
    assert armed.regime_version == "2"
    assert triggered.status is IntentStatus.TRIGGERED
    assert triggered.trigger_price == 100.5


def test_filled_execution_group_invalidates_unfilled_siblings() -> None:
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=UTC)
    winner = SetupOpportunity.create(
        interaction_id="winner", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=15), entry_lower=99,
        entry_upper=100, failure_boundary=98, evidence_ids=("e1",),
        conflict_group_id="execution:shared",
    )
    sibling = SetupOpportunity.create(
        interaction_id="sibling", direction="short", formed_at=formed,
        expires_at=formed + timedelta(minutes=15), entry_lower=99,
        entry_upper=100, failure_boundary=101, evidence_ids=("e2",),
        conflict_group_id="execution:shared",
    )
    policy = object.__new__(OrderflowPolicy)
    policy._opportunities = {winner.lifecycle_id: winner, sibling.lifecycle_id: sibling}
    policy.on_execution_event(ExecutionNotification(
        "FILLED", f"orderflow-entry:{winner.lifecycle_id}", "FILLED",
        event_id="fill-1", occurred_at="2026-01-01T09:25:00+00:00",
    ))
    assert policy._opportunities[sibling.lifecycle_id].status is OpportunityStatus.INVALIDATED
    assert policy._opportunities[sibling.lifecycle_id].transitions[-1].reason == "execution_group_claimed"


def test_opportunity_timeout_counts_only_configured_timeframe() -> None:
    opportunity = _opportunity()
    at = datetime(2026, 1, 1, 9, 25, tzinfo=UTC)
    assert opportunity.advance_timeout(timeframe="1m", at=at) == opportunity
    expired = opportunity.advance_timeout(timeframe="5m", at=at)
    assert expired.status is OpportunityStatus.ACTIVE
    assert expired.timeout_count == 1
    assert expired.advance_timeout(timeframe="5m", at=at) == expired
    expired = expired.advance_timeout(timeframe="5m", at=at + timedelta(minutes=20))
    assert expired.status is OpportunityStatus.EXPIRED
    assert expired.transitions[-1].reason == "evidence_timeout"
    assert expired.transitions[-1].evidence_ids == ("2026-01-01T09:45:00+00:00",)


def test_template_identity_scopes_opportunity_and_conflict_group() -> None:
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=UTC)
    first = SetupOpportunity.create(
        interaction_id="interaction-1", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=15), entry_lower=99, entry_upper=100,
        failure_boundary=98, evidence_ids=("e1",), template_id="acceptance-v1",
        policy_version="p1", conflict_group_id="or-low",
    )
    second = SetupOpportunity.create(
        interaction_id="interaction-1", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=15), entry_lower=99, entry_upper=100,
        failure_boundary=98, evidence_ids=("e1",), template_id="rejection-v1",
        policy_version="p1", conflict_group_id="or-low",
    )
    assert first.lifecycle_id != second.lifecycle_id
    assert first.execution_group_id == second.execution_group_id == "or-low"


def test_templated_opportunity_source_current_matches_its_interaction() -> None:
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=UTC)
    opportunity = SetupOpportunity.create(
        interaction_id="interaction-1", direction="long",
        formed_at=formed, expires_at=formed + timedelta(minutes=5),
        entry_lower=99.0, entry_upper=101.0, failure_boundary=98.0,
        evidence_ids=("evidence-1",), evidence_timeframe="5m",
        regime_version=3, template_id="acceptance", policy_version="v1",
    )

    assert opportunity.source_is_current(interaction_id="interaction-1", context_version=3)


def test_terminal_transition_is_idempotent_but_conflicts_are_rejected() -> None:
    opportunity = _opportunity()
    at = datetime(2026, 1, 1, 9, 25, tzinfo=UTC)
    expired = opportunity.transition(OpportunityStatus.EXPIRED, at=at, reason="expired")
    assert expired.transition(OpportunityStatus.EXPIRED, at=at, reason="expired") == expired
    try:
        expired.transition(OpportunityStatus.INVALIDATED, at=at, reason="gap")
    except ValueError as exc:
        assert "conflicting transition" in str(exc)
    else:
        raise AssertionError("conflicting terminal transition was accepted")


def test_snapshot_rejects_inconsistent_transition_history() -> None:
    opportunity = _opportunity()
    snapshot = opportunity.snapshot()
    snapshot["status"] = OpportunityStatus.EXPIRED.value
    try:
        SetupOpportunity.from_snapshot(snapshot)
    except ValueError as exc:
        assert "status does not match" in str(exc)
    else:
        raise AssertionError("inconsistent snapshot was accepted")

    duplicate = opportunity.snapshot()
    transitions = duplicate["transitions"]
    assert isinstance(transitions, list)
    duplicate["transitions"] = [*transitions, transitions[0]]
    try:
        SetupOpportunity.from_snapshot(duplicate)
    except ValueError as exc:
        assert "duplicate transitions" in str(exc)
    else:
        raise AssertionError("duplicate transition history was accepted")


def test_promotion_requires_five_unique_confirmation_observations() -> None:
    opportunity = _opportunity()
    at = datetime(2026, 1, 1, 9, 25, tzinfo=UTC)
    evidence = tuple(
        SimpleNamespace(
            observation_id=f"one-{index}", timeframe="1m",
            observed_at=at + timedelta(minutes=index + 1),
            available_at=at + timedelta(minutes=index + 1),
        )
        for index in range(5)
    )
    try:
        opportunity.promote(at=at, confirmation_evidence=evidence[:1])
    except ValueError as exc:
        assert "exactly five" in str(exc)
    else:
        raise AssertionError("promotion accepted incomplete confirmation")

    promoted = opportunity.promote(
        at=evidence[-1].available_at, confirmation_evidence=evidence,
    )
    assert promoted.status is OpportunityStatus.ACTIVE
    assert promoted.setup.confirmation_ids == tuple(item.observation_id for item in evidence)
    assert promoted.transitions[-1].reason == "promoted"


def test_promotion_rejects_non_consecutive_or_pre_formation_evidence() -> None:
    opportunity = _opportunity()
    formed = opportunity.setup.formed_at
    evidence = tuple(
        SimpleNamespace(
            observation_id=f"bar-{index}", timeframe="1m",
            observed_at=formed + timedelta(minutes=index),
            available_at=formed + timedelta(minutes=index),
        )
        for index in range(5)
    )
    try:
        opportunity.promote(at=formed + timedelta(minutes=5), confirmation_evidence=evidence)
    except ValueError as exc:
        assert "after formation" in str(exc)
    else:
        raise AssertionError("promotion accepted pre-formation evidence")


def test_promotion_scopes_instrument_session_and_expiry() -> None:
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=UTC)
    opportunity = SetupOpportunity.create(
        interaction_id="interaction-scoped", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=10), entry_lower=99,
        entry_upper=100, failure_boundary=98, evidence_ids=("e1",),
        source_instrument="NIFTYFUT", source_session_id="session-1",
    )

    def evidence(instrument: str = "NIFTYFUT", session_id: str = "session-1") -> tuple[SimpleNamespace, ...]:
        return tuple(
            SimpleNamespace(
                observation_id=f"scoped-{index}", timeframe="1m", instrument=instrument,
                observed_at=formed + timedelta(minutes=index + 1),
                available_at=formed + timedelta(minutes=index + 1),
                context=SimpleNamespace(session_id=session_id),
            )
            for index in range(5)
        )

    for wrong in (evidence(instrument="OTHER"), evidence(session_id="other-session")):
        try:
            opportunity.promote(at=wrong[-1].available_at, confirmation_evidence=wrong)
        except ValueError as exc:
            assert "causally available" in str(exc)
        else:
            raise AssertionError("promotion accepted out-of-scope evidence")

    late = tuple(
        SimpleNamespace(
            observation_id=item.observation_id, timeframe=item.timeframe,
            instrument=item.instrument,
            observed_at=item.observed_at + timedelta(minutes=10),
            available_at=item.available_at + timedelta(minutes=10), context=item.context,
        )
        for item in evidence()
    )
    try:
        opportunity.promote(at=late[-1].available_at, confirmation_evidence=late)
    except ValueError as exc:
        assert "causally available" in str(exc)
    else:
        raise AssertionError("promotion accepted expired evidence")


def test_promotion_keeps_lifecycle_active_for_later_timeout() -> None:
    opportunity = _opportunity()
    formed = opportunity.setup.formed_at
    evidence = tuple(
        SimpleNamespace(
            observation_id=f"one-{index}", timeframe="1m",
            observed_at=formed + timedelta(minutes=index + 1),
            available_at=formed + timedelta(minutes=index + 1),
        )
        for index in range(5)
    )
    promoted = opportunity.promote(at=evidence[-1].available_at, confirmation_evidence=evidence)
    assert promoted.status is OpportunityStatus.ACTIVE
    expired = promoted
    for index in range(6):
        expired = expired.advance_timeout(
            timeframe="5m", at=formed + timedelta(minutes=25 + index),
            evidence_id=f"5m-{index}",
        )
    assert expired.status is OpportunityStatus.EXPIRED
