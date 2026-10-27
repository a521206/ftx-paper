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


def test_opportunity_identities_are_deterministic_and_distinct() -> None:
    first = _opportunity()
    second = _opportunity()
    assert first == second
    assert first.lifecycle_id != first.setup.setup_id
    assert first.execution_group_id


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
