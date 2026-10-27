from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.strategy import (
    ContextHistory, ContextVersion, OpeningGapState, SessionLevels,
    advance_opening_gap, classify_opening_gap,
)


UTC = timezone.utc


def _version(number: int, available_at: datetime) -> ContextVersion:
    return ContextVersion(
        "ctx-2026-01-01", number, available_at,
        SessionLevels("2026-01-01"), OpeningGapState("NONE"), "BOTH",
    )


def test_context_history_resolves_latest_available_version() -> None:
    start = datetime(2026, 1, 1, 9, 15, tzinfo=UTC)
    history = ContextHistory((_version(1, start), _version(2, start + timedelta(minutes=5))))

    assert history.as_of(start + timedelta(minutes=1)).version == 1
    assert history.as_of(start - timedelta(seconds=1)) is None
    assert history.as_of(start + timedelta(minutes=6)).version == 2


def test_context_history_rejects_future_or_duplicate_versions() -> None:
    start = datetime(2026, 1, 1, 9, 15, tzinfo=UTC)
    history = ContextHistory((_version(1, start),))
    with pytest.raises(ValueError, match="increase"):
        history.publish(_version(1, start))
    with pytest.raises(ValueError, match="availability"):
        history.publish(_version(2, start - timedelta(seconds=1)))


def test_opening_gap_classification_preserves_unavailable_state() -> None:
    assert classify_opening_gap(opening_price=None, prior_close=100).status == "UNAVAILABLE"
    assert classify_opening_gap(opening_price=102, prior_close=100).status == "UP"
    assert classify_opening_gap(opening_price=98, prior_close=100).status == "DOWN"
    assert classify_opening_gap(opening_price=100, prior_close=100).status == "NONE"


def test_opening_gap_recovery_is_explicit_and_causal() -> None:
    state = OpeningGapState("GAP_ACCEPTED", 2.0, 100.0)
    recovered = advance_opening_gap(
        state, close=99.5, available_at=datetime(2026, 1, 1, 9, 20, tzinfo=UTC),
        evidence_id="obs-1",
    )
    assert recovered.status == "RETURN_TO_VALUE"
    assert recovered.evidence_ids == ("obs-1",)
