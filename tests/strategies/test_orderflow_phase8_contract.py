from __future__ import annotations

import pytest

from ftx_paper.strategies.orderflow.parity_artifacts import (
    ARTIFACT_TYPE,
    CONTRACT_ID,
    CONTRACT_VERSION,
    DECISION_STAGES,
    EXECUTION_KINDS,
    LAYERS,
    LEDGER_KINDS,
    PARITY_PAYLOAD_KEYS,
    PHASE_STREAMS,
    SCHEMA_VERSION,
    phase_facts_from_payload,
    validate_contract_declarations,
)


def test_phase8_contract_surface_matches_canonical_order() -> None:
    assert (ARTIFACT_TYPE, SCHEMA_VERSION) == ("producer_parity", 1)
    assert (CONTRACT_ID, CONTRACT_VERSION) == ("orderflow_producer_parity", 1)
    assert PHASE_STREAMS == (
        "source_observation",
        "context_location",
        "interaction_response",
        "opportunity_lifecycle",
        "intent_admission",
        "trigger_exit",
    )
    assert LAYERS == (
        "run_scope",
        "decision",
        "execution_settlement",
        "portfolio_ledger",
    )
    assert DECISION_STAGES == ("interaction", "candidate", "admission", "trigger", "exit")
    assert "synthetic_settlement" in EXECUTION_KINDS
    assert "closeout" in LEDGER_KINDS
    assert PARITY_PAYLOAD_KEYS == ("parity_phase_facts", "parity_layers", "parity_coverage")


def test_contract_requires_every_phase_stream_and_layer() -> None:
    with pytest.raises(ValueError, match="all phase streams"):
        validate_contract_declarations(
            {name: [] for name in PHASE_STREAMS[:-1]},
            {name: [] for name in LAYERS},
        )

    with pytest.raises(ValueError, match="all final layers"):
        validate_contract_declarations(
            {name: [] for name in PHASE_STREAMS},
            {name: [] for name in LAYERS[:-1]},
        )


def test_missing_phase_stream_is_unavailable_not_observed_empty() -> None:
    records, coverage = phase_facts_from_payload(
        {"parity_phase_facts": {"source_observation": []}},
        "context_location",
    )

    assert records == ()
    assert coverage == {
        "status": "UNAVAILABLE",
        "observed": False,
        "reason": "missing_context_location_facts",
    }


def test_declared_empty_phase_stream_is_observed_empty() -> None:
    records, coverage = phase_facts_from_payload(
        {"parity_phase_facts": {"context_location": []}},
        "context_location",
    )

    assert records == ()
    assert coverage == {
        "status": "MATCHED",
        "observed": True,
        "coverage": "observed_empty",
    }
