from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from ftx_paper.strategies.orderflow.execution import (
    AdmissionStatus,
    ExecutionEngine,
    ReservationStatus,
)
from ftx_paper.strategies.orderflow.lifecycle import SetupOpportunity
from ftx_paper.strategies.orderflow.templates import Direction


UTC = timezone.utc


def _life() -> SetupOpportunity:
    now = datetime(2026, 9, 29, 9, 20, tzinfo=UTC)
    return SetupOpportunity(
        lifecycle_id="life-1", direction=Direction.LONG, session_date="2026-09-29",
        instrument="NIFTY", strategy="orderflow-1m_5m", formation_at=now,
        formation_available_at=now, candidate_id="candidate-1", parent_context=None,
        location="OR_LOW", reference_level=100.0, context_candle=None,
        breakout_level=101.0, failure_boundary=99.0,
    )


def _setup() -> SimpleNamespace:
    return SimpleNamespace(
        setup_id="setup-1", version="v1", regime_version="r1",
        entry_zone_low=100.0, entry_zone_high=101.0, failure_boundary=99.0,
        expires_at="2026-09-29T10:00:00+05:30", required_confirmation=(),
        lifecycle_id="life-1",
    )


def test_admission_preserves_unavailable_regime_and_arms_allowed_intent() -> None:
    engine = ExecutionEngine()
    life = _life()
    engine.register(life.lifecycle_id)
    assert engine.admit_opportunity(life, _setup(), regime=None, decided_at="2026-09-29T09:20:00+05:30") is None
    assert engine.state_for(life.lifecycle_id).admission.reason == "regime_unresolved"

    regime = SimpleNamespace(permits=lambda direction: direction == "long")
    intent = engine.admit_opportunity(life, _setup(), regime=regime, decided_at="2026-09-29T09:20:00+05:30")
    assert intent is not None
    assert intent.status.value == "armed"
    assert engine.state_for(life.lifecycle_id).admission.status is AdmissionStatus.NOT_EVALUATED


def test_trigger_fill_is_idempotent_and_releases_reservation() -> None:
    engine = ExecutionEngine()
    life = _life()
    engine.register(life.lifecycle_id)
    regime = SimpleNamespace(permits=lambda direction: True)
    engine.admit_opportunity(life, _setup(), regime=regime, decided_at="2026-09-29T09:20:00+05:30")
    triggered = engine.select_entry(life, price=101.0, available_at="2026-09-29T09:21:00+05:30")
    assert triggered.status.value == "triggered"
    assert engine.state_for(life.lifecycle_id).reservation is not None
    filled = engine.fill_entry(life, price=101.5, quantity=1.0, filled_at="2026-09-29T09:22:00+05:30")
    assert filled.status.value == "filled"
    state = engine.state_for(life.lifecycle_id)
    assert state.reservation is None
    assert state.reservation_history[-1].status is ReservationStatus.RELEASED
    assert engine.select_entry(life, price=101.0, available_at="2026-09-29T09:23:00+05:30") == filled
    with pytest.raises(ValueError, match="entry fill requires"):
        engine.fill_entry(life, price=101.5, quantity=1.0, filled_at="2026-09-29T09:23:00+05:30")
