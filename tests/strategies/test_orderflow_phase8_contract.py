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
    build_parity_artifact,
    build_parity_artifact_from_payload,
    phase_facts_from_payload,
    validate_contract_declarations,
    validate_parity_artifact,
)
from ftx_paper.strategies.orderflow.state_machine_reporting import build_replay_payload
from ftx_paper.strategies.orderflow.state_machine_types import ReplayContext


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


def test_replay_payload_does_not_derive_execution_facts_from_terminal_state() -> None:
    class Fill:
        def __init__(self) -> None:
            self.fill_id = "fill-1"

    class State:
        fills = (Fill(),)
        reservation_history = ()
        admission_history = ()
        opportunity_id = "life-1"

    class Engine:
        def snapshot(self) -> dict[str, State]:
            return {"life-1": State()}

    class Context:
        execution_engine = Engine()
        parity_streams: dict[str, list[dict[str, object]]] = {}
        execution_diagnostics: tuple[dict[str, object], ...] = ()

    payload = build_replay_payload(Context(), [], [], [])

    assert set(payload["parity_phase_facts"]) == set(PHASE_STREAMS)
    assert set(payload["parity_layers"]) == set(LAYERS)
    assert all(
        entry["status"] == "UNAVAILABLE"
        for entry in payload["parity_coverage"]["phase_streams"].values()
    )
    assert all(
        entry["status"] == "UNAVAILABLE"
        for entry in payload["parity_coverage"]["layers"].values()
    )
    assert "execution_fills" not in payload
    assert "execution_transitions" not in payload
    assert "ledger_mutations" not in payload


def _empty_coverage(names: tuple[str, ...]) -> dict[str, dict[str, object]]:
    return {
        name: {"status": "MATCHED", "observed": True, "coverage": "observed_empty"}
        for name in names
    }


def test_unified_artifact_is_deterministic_and_payload_driven() -> None:
    coverage = {
        "phase_streams": _empty_coverage(PHASE_STREAMS),
        "layers": _empty_coverage(LAYERS),
    }
    payload = {
        "parity_phase_facts": {name: [] for name in PHASE_STREAMS},
        "parity_layers": {name: [] for name in LAYERS},
        "parity_coverage": coverage,
        "scope": {"timezone": "Asia/Kolkata"},
    }

    first = build_parity_artifact_from_payload(
        payload, run_id="run-1", producer={"name": "paper"},
    )
    second = build_parity_artifact(
        run_id="run-1", producer={"name": "paper"},
        scope={"timezone": "Asia/Kolkata"},
        phase_streams=payload["parity_phase_facts"],
        layers=payload["parity_layers"], coverage=coverage,
    )

    assert first == second
    validate_parity_artifact(first)


def test_unified_artifact_rejects_invalid_layer_kind_and_tampering() -> None:
    record = {
        "kind": "not_a_ledger_kind",
        "occurred_at": "2026-10-01T09:20:00+05:30",
        "observed_at": "2026-10-01T09:20:00+05:30",
        "available_at": "2026-10-01T09:20:00+05:30",
        "sequence": 0,
        "identity": "ledger-1",
        "causal_parent_id": None,
        "values": {},
        "unavailable": [],
    }
    phase_coverage = _empty_coverage(PHASE_STREAMS)
    layer_coverage = _empty_coverage(LAYERS)
    layers = {name: [] for name in LAYERS}
    layers["portfolio_ledger"] = [record]
    layer_coverage["portfolio_ledger"] = {
        "status": "MATCHED", "observed": True, "coverage": "observed",
    }

    with pytest.raises(ValueError, match="invalid .*ledger.*kind"):
        build_parity_artifact(
            run_id="run-1", producer={"name": "paper"},
            scope={"timezone": "Asia/Kolkata"},
            phase_streams={name: [] for name in PHASE_STREAMS},
            layers=layers,
            coverage={"phase_streams": phase_coverage, "layers": layer_coverage},
        )

    valid = build_parity_artifact(
        run_id="run-1", producer={"name": "paper"},
        scope={"timezone": "Asia/Kolkata"},
        phase_streams={name: [] for name in PHASE_STREAMS},
        layers={name: [] for name in LAYERS},
        coverage={"phase_streams": phase_coverage, "layers": _empty_coverage(LAYERS)},
    )
    valid["manifest"]["layers"]["run_scope"]["count"] = 1
    with pytest.raises(ValueError, match="manifest count or hash"):
        validate_parity_artifact(valid)


def test_producer_emits_execution_and_ledger_layer_facts() -> None:
    context = ReplayContext(execution_engine=object())
    context.emit(
        {
            "kind": "filled",
            "event_id": "event-1",
            "neutral_event_key": "fill-1",
            "observed_at": "2026-10-01T09:20:00+05:30",
            "available_at": "2026-10-01T09:20:00+05:30",
        },
        [],
    )

    assert context.parity_layers["execution_settlement"][0]["kind"] == "fill"
    assert context.parity_layers["portfolio_ledger"][0]["kind"] == "fill"
