from __future__ import annotations

from scripts.orderflow.phase5_parity_harness import compare


def _fact(kind: str, identity: str) -> dict[str, object]:
    return {
        "kind": kind,
        "occurred_at": "2026-09-29T09:20:00+00:00",
        "observed_at": "2026-09-29T09:20:00+00:00",
        "available_at": "2026-09-29T09:21:00+00:00",
        "identity": identity,
        "causal_parent_id": "source:1",
        "values": {"identity": identity},
        "unavailable": [],
    }


def test_harness_compares_explicit_source_and_context_streams() -> None:
    source = [_fact("source_observation", "source:1")]
    context = [_fact("context_location_observed", "context:1")]
    complete = {
        "source_observation": source,
        "context_location": context,
        "interaction_response": [],
        "opportunity_lifecycle": [],
        "intent_admission": [],
        "trigger_exit": [],
    }
    result = compare(
        canonical_facts=complete,
        paper_facts=complete,
        session_date="2026-09-29",
    )

    assert result["status"] == "MATCHED"


def test_harness_does_not_turn_missing_source_into_an_empty_match() -> None:
    fact = _fact("context_location_observed", "context:1")
    result = compare(
        canonical_facts={"context_location": [fact]},
        paper_facts={"context_location": [fact]},
        session_date="2026-09-29",
    )

    assert result["status"] == "UNAVAILABLE"
    assert result["stream"] == "source_observation"
