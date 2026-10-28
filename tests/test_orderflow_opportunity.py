from datetime import datetime, timedelta, timezone

from ftx_paper.strategy import OpportunityStatus, SetupOpportunity


UTC = timezone.utc


def _opportunity():
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=UTC)
    return SetupOpportunity.create(
        interaction_id="interaction-1", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=15), entry_lower=99,
        entry_upper=100, failure_boundary=98, evidence_ids=("e1",),
    )


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
    duplicate["transitions"] = [*duplicate["transitions"], duplicate["transitions"][0]]
    try:
        SetupOpportunity.from_snapshot(duplicate)
    except ValueError as exc:
        assert "duplicate transitions" in str(exc)
    else:
        raise AssertionError("duplicate transition history was accepted")
