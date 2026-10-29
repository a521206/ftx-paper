from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.strategy import (
    ContextHistory, ContextVersionRef, MarketContext, OpeningGapState,
    SessionLevels, ValueReference, advance_opening_gap, classify_context_relation,
)


UTC = timezone.utc


def _version(number: int, available_at: datetime) -> MarketContext:
    return MarketContext(
        "ctx-2026-01-01", number, available_at,
        SessionLevels("2026-01-01"), OpeningGapState("NONE"), "BOTH",
    )


def test_context_history_resolves_latest_available_version() -> None:
    start = datetime(2026, 1, 1, 9, 15, tzinfo=UTC)
    history = ContextHistory((_version(1, start), _version(2, start + timedelta(minutes=5))))

    first = history.as_of(start + timedelta(minutes=1))
    assert first is not None
    assert first.version == 1
    assert history.as_of(start - timedelta(seconds=1)) is None
    second = history.as_of(start + timedelta(minutes=6))
    assert second is not None
    assert second.version == 2


def test_context_history_rejects_future_or_duplicate_versions() -> None:
    start = datetime(2026, 1, 1, 9, 15, tzinfo=UTC)
    history = ContextHistory((_version(1, start),))
    with pytest.raises(ValueError, match="increase"):
        history.publish(_version(1, start))
    with pytest.raises(ValueError, match="availability"):
        history.publish(_version(2, start - timedelta(seconds=1)))


def test_opening_gap_recovery_is_explicit_and_causal() -> None:
    state = OpeningGapState("GAP_ACCEPTED", 2.0, 100.0)
    recovered = advance_opening_gap(
        state, close=99.5, available_at=datetime(2026, 1, 1, 9, 20, tzinfo=UTC),
        evidence_id="obs-1",
    )
    assert recovered.status == "RETURN_TO_VALUE"
    assert recovered.evidence_ids == ("obs-1",)


def test_opening_gap_expires_after_five_completed_bars() -> None:
    state = OpeningGapState("GAP_ACCEPTED", -2.0, 100.0, completed_bars=1, reference_id="PD_LOW")
    for index in range(2, 6):
        state = advance_opening_gap(
            state, close=98.0, available_at=datetime(2026, 1, 1, 9, index, tzinfo=UTC),
            evidence_id=f"obs-{index}",
        )
    assert state.status == "EXPIRED"
    assert state.completed_bars == 5
    assert state.reference_id == "PD_LOW"


def test_value_reference_requires_complete_causal_publication() -> None:
    with pytest.raises(ValueError, match="both bounds"):
        ValueReference("pd", 101.0, None, datetime(2026, 1, 1, 9, 15, tzinfo=UTC), ("profile",), True)
    with pytest.raises(ValueError, match="source IDs"):
        ValueReference("pd", 101.0, 99.0, datetime(2026, 1, 1, 9, 15, tzinfo=UTC), (), True)

    reference = ValueReference(
        "pd", 101.0, 99.0, datetime(2026, 1, 1, 9, 15, tzinfo=UTC), ("profile",), True,
    )
    assert reference.complete
    assert reference.source_ids == ("profile",)


def test_market_context_preserves_parent_child_identity_and_location_as_of() -> None:
    available = datetime(2026, 1, 1, 9, 20, tzinfo=UTC)
    parent = MarketContext(
        "context:2026-01-01", 1, available, SessionLevels("2026-01-01"),
        OpeningGapState("NONE"), "BOTH", location=None,
    )
    child = MarketContext(
        "interaction:OR_LOW:2026-01-01", 2, available, parent.levels, parent.opening_gap,
        parent.permission, parent_context_id=parent.context_id, location="OR_LOW",
    )
    history = ContextHistory((parent, child))

    selected = history.as_of_location("OR_LOW", available)
    assert selected is child
    assert history.by_reference(ContextVersionRef(child.context_id, child.version)) is child


def test_context_relation_is_directional_and_does_not_treat_weak_evidence_as_confirmation() -> None:
    assert classify_context_relation(
        location="OR_LOW", direction="long", evidence={"confirmed": True},
    ).value == "confirmed"
    assert classify_context_relation(
        location="OR_LOW", direction="short", evidence={"confirmed": True},
    ).value == "conflicted"
    assert classify_context_relation(
        location="OR_LOW", direction="long", evidence={},
    ).value == "neutral"
