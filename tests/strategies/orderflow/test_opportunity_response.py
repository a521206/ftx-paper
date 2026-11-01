from types import SimpleNamespace

from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.opportunity import (
    DirectionalResponse,
    OpportunityOutcome,
    OpportunityOutcomeStatus,
    evaluate_response,
)
from ftx_paper.strategies.orderflow.opportunity_coordinator import OpportunityCoordinator
import ftx_paper.strategies.orderflow.opportunity_coordinator as coordinator_module
from ftx_paper.strategies.orderflow.policies.base import HypothesisEvaluation
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import LocationRegistry
from ftx_paper.strategies.orderflow.templates import Direction, LONG_RECOVERY_LOW, TRADE_TEMPLATES
from ftx_paper.strategies.orderflow.timestamps import parse_nifty_label


def bar(minute: str, open_: float, high: float, low: float, close: float) -> Aggregate:
    time = parse_nifty_label(f"2026-10-01T{minute}")
    end = time.replace(minute=time.minute + 4)
    available = end
    return Aggregate(
        time, end, available, "5m", 5, open_, high, low, close, 1,
        "NIFTY", "", "FUT", "2026-10-01", f"fp-{minute}",
    )


def interaction(creation: Aggregate, *, carried: int = 0, terminal: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        interaction_id="interaction-1", instrument="NIFTY", session_date="2026-10-01",
        location="OR_LOW", creation_candle=creation, reference_level=100.0,
        band=LocationBand(99.0, 100.0), pre_contact_side=None,
        interaction_started_at=creation.time, available_at=creation.available_at,
        reference_generation=1, sessions_carried=carried,
        terminal_candle_id=terminal,
    )


def test_response_confirms_after_contact_and_preserves_evidence() -> None:
    contact = bar("09:30", 99.4, 100.0, 99.0, 99.5)
    confirmation = bar("09:35", 99.8, 100.5, 99.2, 100.0)

    outcome = evaluate_response(
        LONG_RECOVERY_LOW, interaction(contact), [contact, confirmation],
        as_of="2026-10-01T09:39",
    )

    assert outcome.status is OpportunityOutcomeStatus.RESOLVED
    assert outcome.reason == "causal_rejection"
    assert outcome.response is not None
    assert outcome.response.evidence_ids == ("interaction-1", "2026-10-01T09:35")


def test_carry_session_expiry_is_preserved() -> None:
    contact = bar("09:30", 99.4, 100.0, 99.0, 99.5)

    outcome = evaluate_response(
        LONG_RECOVERY_LOW, interaction(contact, carried=2), [contact],
        as_of="2026-10-02T09:40",
    )

    assert outcome.status is OpportunityOutcomeStatus.EXPIRED
    assert outcome.reason == "response_window_expired"


def test_missing_contact_is_pending_even_when_other_evidence_is_available() -> None:
    creation = bar("09:30", 99.5, 100.0, 99.0, 99.5)
    later = bar("09:35", 99.8, 100.5, 99.2, 100.0)

    outcome = evaluate_response(
        LONG_RECOVERY_LOW, interaction(creation), [later],
        as_of="2026-10-01T09:39",
    )

    assert outcome.status is OpportunityOutcomeStatus.PENDING
    assert outcome.reason == "contact_not_available"


def test_terminal_candle_cuts_off_later_confirmation() -> None:
    contact = bar("09:30", 99.4, 100.0, 99.0, 99.5)
    terminal = bar("09:35", 99.4, 100.2, 99.2, 99.7)
    confirmation = bar("09:40", 99.8, 100.5, 99.2, 100.0)

    outcome = evaluate_response(
        LONG_RECOVERY_LOW, interaction(contact, terminal="2026-10-01T09:35"),
        [contact, terminal, confirmation], as_of="2026-10-01T09:44",
    )

    assert outcome.status is OpportunityOutcomeStatus.PENDING
    assert outcome.reason == "response_window_open"


def test_unavailable_confirmation_evidence_is_not_evaluated() -> None:
    contact = bar("09:30", 99.4, 100.0, 99.0, 99.5)
    future = bar("09:35", 99.8, 100.5, 99.2, 100.0)

    outcome = evaluate_response(
        LONG_RECOVERY_LOW, interaction(contact), [contact, future],
        as_of="2026-10-01T09:36",
    )

    assert outcome.status is OpportunityOutcomeStatus.PENDING
    assert outcome.reason == "response_window_open"


def test_opportunity_coordinator_forms_one_canonical_lifecycle() -> None:
    contact = bar("09:30", 99.4, 100.0, 99.0, 99.5)
    confirmation = bar("09:35", 99.8, 100.5, 99.2, 100.0)
    response = DirectionalResponse(
        Direction.LONG, "OR_LOW", 100.0, 100.5, 99.0, 100.0,
        ("interaction-1", confirmation.time), confirmation.end_time,
        confirmation.available_at, band=LocationBand(99.0, 100.0),
        evidence=(contact, confirmation),
    )
    outcome = OpportunityOutcome(
        OpportunityOutcomeStatus.RESOLVED, "causal_rejection", response.observed_at,
        response.available_at, (contact, confirmation), response,
        policy_version=LONG_RECOVERY_LOW.policy_version,
        template=LONG_RECOVERY_LOW, as_of=confirmation.available_at,
    )
    evaluation = HypothesisEvaluation(
        LONG_RECOVERY_LOW, outcome, "canonical-identity", selected=True,
    )
    interaction_value = interaction(contact)
    coordinator = OpportunityCoordinator(fill_permission_margin=None)

    first = coordinator.form_resolved(
        interaction=interaction_value, evaluation=evaluation, outcome=outcome,
            as_of=confirmation.available_at,
            aggregates={"1m": [], "5m": [contact, confirmation]},
            raw_rows=[], atr=1.0,
    )
    duplicate = coordinator.form_resolved(
            interaction=interaction_value, evaluation=evaluation, outcome=outcome,
            as_of=confirmation.available_at,
            aggregates={"1m": [], "5m": [contact, confirmation]},
            raw_rows=[], atr=1.0,
    )

    assert first is not None
    assert first.identity == "canonical-identity"
    assert first.values["lifecycle_id"] == "lifecycle-2026-10-01-NIFTY-1m_5m-long-candidate-1"
    assert first.values["strategy"] == "orderflow-1m_5m"
    assert first.values["context_candle_id"] == contact.time
    assert duplicate is None


def test_opening_seed_uses_canonical_inputs_and_retries_until_formed(monkeypatch) -> None:
    candle = bar("09:15", 100.0, 102.0, 99.0, 101.0)
    registry = LocationRegistry("stream")
    raw_rows = [{"time": "09:15", "volume": 10}]
    seen: dict[str, object] = {}

    def scores(aggregate, *, raw_rows, aggregates, atr):
        seen["raw_rows"] = raw_rows
        seen["atr"] = atr
        return 0.8, 0.2

    monkeypatch.setattr(coordinator_module, "build_context_scores", scores)
    monkeypatch.setattr(coordinator_module, "daily_atr", lambda session, rows: 7.5)
    coordinator = OpportunityCoordinator()

    assert coordinator.form_opening_seed(
        candle=candle, registry=registry, templates=TRADE_TEMPLATES,
        aggregates={"1m": [], "5m": [candle]}, raw_rows=raw_rows,
        rows_by_date={"2026-10-01": tuple(raw_rows)},
    ) is None

    registry.register_opening_range(99.0, 101.0, candle.available_at, atr=1.0)
    result = coordinator.form_opening_seed(
        candle=candle, registry=registry, templates=TRADE_TEMPLATES,
        aggregates={"1m": [], "5m": [candle]}, raw_rows=raw_rows,
        rows_by_date={"2026-10-01": tuple(raw_rows)},
    )

    assert result is not None
    outcome, formation = result
    assert outcome["interaction_id"].startswith("OR_SEED:")
    assert seen == {"raw_rows": raw_rows, "atr": 7.5}
    assert coordinator.form_opening_seed(
        candle=candle, registry=registry, templates=TRADE_TEMPLATES,
        aggregates={"1m": [], "5m": [candle]}, raw_rows=raw_rows,
        rows_by_date={"2026-10-01": tuple(raw_rows)},
    ) is None
