"""Compare canonical Phase 5 replay facts with Paper domain output.

The harness intentionally bypasses runtime/database integration. It loads the
canonical replay artifact and compares it with facts emitted by the Paper
causal interaction boundary. Paper facts are required explicitly: this tool
must not infer strategy outcomes from canonical snapshots or select templates
outside the canonical policy-dispatch path.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from ftx_paper.strategies.orderflow.parity_artifacts import (
    build_phase4_artifact, compare_phase4_manifests,
)

ALLOWED_DATES = {"2026-09-15", "2026-10-01"}
_INTERACTION_KINDS = {
    "interaction_created", "interaction_ended", "location_interaction_observed",
}
_RESPONSE_KINDS = {"response_confirmed", "response_invalidated", "response_expired"}
_REQUIRED_PHASE_FIELDS = {
    "kind", "occurred_at", "observed_at", "available_at", "identity",
    "causal_parent_id", "values", "unavailable",
}


def _aware(value: str) -> str:
    return value if "+" in value or value.endswith("Z") else f"{value}+05:30"


def _canonical_facts(root: Path, session_date: str, python: str) -> dict[str, Any]:
    source = """
import json
import sys
from src.orderflow.research_replay import load_session_inputs, replay_sessions
from src.orderflow.state_machine_types import ExecutionConstraints
session_date = sys.argv[1]
number = int(session_date.replace('-', ''))
result = replay_sessions(load_session_inputs([number], [session_date]),
                         constraints=ExecutionConstraints(), replay_mode='diagnostic')[session_date]
facts = result.get('parity_phase_facts', {})
evidence = result.get('report_evidence', {})
facts = dict(facts)
facts.setdefault('_canonical_records', evidence.get('records', []))
print(json.dumps(facts, default=str))
"""
    completed = subprocess.run(
        [python, "-c", source, session_date], cwd=root, check=True,
        capture_output=True, text=True,
    )
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict):
        raise ValueError("canonical replay did not return parity facts")
    records = payload.pop("_canonical_records", None)
    if isinstance(records, list):
        payload["_canonical_records"] = records
    return payload


def _normalized_canonical(canonical: dict[str, Any]) -> dict[str, Any]:
    output: list[dict[str, Any]] = []
    for event in canonical.get("_canonical_records", []):
        if not isinstance(event, dict) or event.get("kind") not in _INTERACTION_KINDS | _RESPONSE_KINDS:
            continue
        observed = event.get("observed_at") or event.get("time")
        available = event.get("available_at") or observed
        identity = event.get("interaction_id")
        if not all(isinstance(value, str) for value in (observed, available, identity)):
            continue
        observed_text = str(observed)
        available_text = str(available)
        parent = event.get("causal_parent_id") or event.get("causal_parent_key")
        if parent is None and isinstance(event.get("values"), dict):
            parent = event["values"].get("causal_parent_key")
        output.append({"kind": str(event["kind"]), "occurred_at": _aware(observed_text),
                       "observed_at": _aware(observed_text), "available_at": _aware(available_text),
                       "identity": identity, "causal_parent_id": parent,
                       "values": event, "unavailable": []})
    return {"interaction_response": output}


def _phase_records(records: Any) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        return []
    output: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        if _REQUIRED_PHASE_FIELDS <= record.keys():
            output.append(record)
            continue
        observed = record.get("observed_at") or record.get("timestamp") or record.get("time")
        available = record.get("available_at") or observed
        identity = record.get("identity") or record.get("observation_id") or record.get("event_id")
        if not all(isinstance(value, str) for value in (observed, available, identity)):
            continue
        parent = record.get("causal_parent_id") or record.get("causal_parent_key")
        output.append({
            "kind": str(record.get("kind", "phase_fact")),
            "occurred_at": _aware(str(observed)),
            "observed_at": _aware(str(observed)),
            "available_at": _aware(str(available)),
            "identity": identity,
            "causal_parent_id": parent,
            "values": record,
            "unavailable": list(record.get("unavailable", ())),
        })
    return output


def compare(*, canonical_facts: dict[str, Any], paper_facts: dict[str, Any], session_date: str) -> dict[str, Any]:
    canonical_payload = {
        stream: _phase_records(canonical_facts[stream])
        for stream in canonical_facts
        if stream in {"source_observation", "context_location", "opportunity_lifecycle", "intent_admission", "trigger_exit"}
    }
    canonical_payload["interaction_response"] = _normalized_canonical(canonical_facts)["interaction_response"]
    paper_payload = {
        stream: _phase_records(paper_facts[stream])
        for stream in paper_facts
        if stream in {"source_observation", "context_location", "interaction_response", "opportunity_lifecycle", "intent_admission", "trigger_exit"}
    }
    canonical = build_phase4_artifact(run_id=f"canonical-{session_date}", producer={"name": "NiftyZoning"},
                                      scope={"date": session_date}, payload={"parity_phase_facts": canonical_payload})
    paper = build_phase4_artifact(run_id=f"paper-{session_date}", producer={"name": "ftx-paper"},
                                  scope={"date": session_date}, payload={"parity_phase_facts": paper_payload})
    return compare_phase4_manifests(canonical, paper,
                                    canonical_records=lambda stream: canonical["phase_streams"][stream],
                                    paper_records=lambda stream: paper["phase_streams"][stream])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, choices=sorted(ALLOWED_DATES))
    parser.add_argument("--canonical-root", type=Path, required=True)
    parser.add_argument("--paper-facts", type=Path, required=True)
    parser.add_argument("--canonical-python", default=sys.executable)
    args = parser.parse_args(argv)
    canonical = _canonical_facts(args.canonical_root, args.date, args.canonical_python)
    paper = json.loads(args.paper_facts.read_text(encoding="utf-8"))
    if isinstance(paper, dict) and isinstance(paper.get("parity_phase_facts"), dict):
        paper = paper["parity_phase_facts"]
    if not isinstance(paper, dict):
        raise ValueError("Paper facts must be an object or parity_phase_facts object")
    result = compare(canonical_facts=canonical, paper_facts=paper, session_date=args.date)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result.get("status") == "MATCHED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
