from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from ftx_paper.strategies.research.opportunity_timeout import advance_opportunity_timeout
from ftx_paper.strategies.orderflow.orderflow_interaction import InteractionStatus, ProjectionSnapshot
from ftx_paper.strategies.orderflow.orderflow_opportunity import SetupOpportunity


UTC = timezone.utc


def _opportunity() -> SetupOpportunity:
    formed = datetime(2026, 1, 1, 9, 20, tzinfo=UTC)
    return SetupOpportunity.create(
        interaction_id="interaction-timeout", direction="long", formed_at=formed,
        expires_at=formed + timedelta(minutes=40), entry_lower=99, entry_upper=100,
        failure_boundary=98, evidence_ids=("response",), evidence_timeframe="5m",
    )


def _snapshot(*ids: str, available: int) -> ProjectionSnapshot:
    at = datetime(2026, 1, 1, 9, available, tzinfo=UTC)
    return ProjectionSnapshot(
        "5m", 100, 101, 99, 100, ids, (), at - timedelta(minutes=5), at, at,
    )


def test_timeout_counts_cumulative_projection_once_then_real_source_after_departure() -> None:
    snapshots = (_snapshot("obs-1", available=25), _snapshot("obs-1", "obs-2", available=30))
    active = SimpleNamespace(
        interaction_id="interaction-timeout", status=InteractionStatus.ACTIVE,
        synthetic_snapshots=(("5m", snapshots),),
    )
    opportunity = _opportunity()
    first = advance_opportunity_timeout(
        opportunity, interaction=active, evidence=(), as_of=datetime(2026, 1, 1, 9, 30, tzinfo=UTC),
    )
    assert first.opportunity is not None
    assert first.added_units == 2
    assert first.opportunity.timeout_counted_snapshot_ids == (
        "interaction-timeout:5m:obs-1", "interaction-timeout:5m:obs-1,obs-2",
    )
    ended = SimpleNamespace(
        interaction_id="interaction-timeout", status=InteractionStatus.ENDED,
        synthetic_snapshots=(("5m", snapshots),),
    )
    real = SimpleNamespace(timeframe="5m", observation_id="obs-3", available_at=datetime(2026, 1, 1, 9, 35, tzinfo=UTC))
    second = advance_opportunity_timeout(
        first.opportunity, interaction=ended, evidence=(real,), as_of=real.available_at,
    )
    assert second.opportunity is not None
    assert second.added_units == 1
    assert second.opportunity.timeout_counted_source_ids == ("obs-1", "obs-2", "obs-3")


def test_six_real_evidence_candles_expire_without_counting_1m() -> None:
    evidence = tuple(
        SimpleNamespace(
            timeframe="5m",
            observation_id=f"obs-{index}",
            available_at=datetime(2026, 1, 1, 9, 35, tzinfo=UTC) + timedelta(minutes=index * 5),
        )
        for index in range(6)
    )

    result = advance_opportunity_timeout(
        _opportunity(), interaction=None, evidence=evidence,
        as_of=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
    )

    assert result.opportunity is not None
    assert result.opportunity.timeout_count == 6
    assert result.opportunity.timeout_counted_source_ids == tuple(
        item.observation_id for item in evidence
    )
    assert result.expiry_at == evidence[-1].available_at


def test_transition_to_real_evidence_does_not_reset_or_double_count() -> None:
    snapshots = (
        _snapshot("obs-1", "obs-2", available=30),
    )
    active = SimpleNamespace(
        interaction_id="interaction-timeout",
        status=InteractionStatus.ACTIVE,
        synthetic_snapshots=(("5m", snapshots),),
    )
    opportunity = _opportunity()
    first = advance_opportunity_timeout(
        opportunity, interaction=active, evidence=(),
        as_of=datetime(2026, 1, 1, 9, 35, tzinfo=UTC),
    )
    assert first.opportunity is not None
    assert first.added_units == 1

    ended = SimpleNamespace(
        interaction_id="interaction-timeout",
        status=InteractionStatus.ENDED,
        synthetic_snapshots=(("5m", snapshots),),
    )
    evidence = tuple(
        SimpleNamespace(
            timeframe="5m",
            observation_id=observation_id,
            available_at=available_at,
        )
        for observation_id, available_at in (
            ("obs-1", datetime(2026, 1, 1, 9, 25, tzinfo=UTC)),
            ("obs-2", datetime(2026, 1, 1, 9, 30, tzinfo=UTC)),
            ("obs-3", datetime(2026, 1, 1, 9, 35, tzinfo=UTC)),
        )
    )
    second = advance_opportunity_timeout(
        first.opportunity, interaction=ended, evidence=evidence,
        as_of=evidence[-1].available_at,
    )

    assert second.opportunity is not None
    assert second.added_units == 1
    assert second.opportunity.timeout_count == 2
    assert second.opportunity.timeout_counted_source_ids == ("obs-1", "obs-2", "obs-3")
