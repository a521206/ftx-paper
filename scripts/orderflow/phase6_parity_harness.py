"""Compare canonical Phase 6 replay facts with explicit Paper facts.

The canonical side is generated from NiftyZoning's causal replay. The Paper
side is deliberately supplied as an explicit Phase 5-shaped artifact until the
Paper replay worker owns orderflow opportunity orchestration.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from ftx_paper.strategies.orderflow.interactions import (
    CandleClassification, CloseSide, InteractionCandle, InteractionSnapshot,
    InteractionStatus, ProjectionSnapshot,
)
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.opportunity import OpportunityContext, form_opportunity, identify_opportunity
from ftx_paper.strategies.orderflow.opportunity_timeout import advance_opportunity_timeout
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.templates import TRADE_TEMPLATES
from ftx_paper.strategies.orderflow.parity_artifacts import (
    build_phase4_artifact,
    compare_phase4_manifests,
)

ALLOWED_DATES = {"2026-09-15", "2026-10-01"}


def _aware(value: str) -> str:
    return value if "+" in value or value.endswith("Z") else f"{value}+05:30"


def _canonical_facts(root: Path, session_date: str, python: str) -> dict[str, Any]:
    source = """
import json
import sys
from datetime import date
from src.orderflow.research_replay import load_session_inputs, replay_sessions
from src.orderflow.state_machine_types import ExecutionConstraints
session_date = sys.argv[1]
number = int(session_date.replace('-', ''))
result = replay_sessions(
    load_session_inputs([number], [session_date]),
    constraints=ExecutionConstraints(),
    replay_mode='diagnostic',
)[session_date]
facts = result.get('parity_phase_facts', {})
if not facts:
    evidence = result.get('report_evidence', {})
    facts = {
        '_canonical_records': evidence.get('records', []),
        '_phase5_interactions': evidence.get('location_interactions', []),
        '_canonical_setups': result.get('diagnostic', {}).get('setup_opportunities', []),
    }
print(json.dumps(facts, default=str))
"""
    completed = subprocess.run(
        [python, "-c", source, session_date],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict):
        raise ValueError("canonical replay did not return parity facts")
    records = payload.pop("_canonical_records", None)
    if isinstance(records, list):
        lifecycle = []
        setup_identity = {
            str(item.get("lifecycle_id")): str(item.get("causal_identity"))
            for item in payload.get("_canonical_setups", [])
            if isinstance(item, dict) and item.get("lifecycle_id") and item.get("causal_identity")
        }
        setup_by_signature = {
            (str(item.get("location")), str(item.get("template_id")), str(item.get("origin_evidence_id"))): str(item.get("causal_identity"))
            for item in payload.get("_canonical_setups", [])
            if isinstance(item, dict) and item.get("causal_identity")
        }
        setup_by_location_evidence = {
            (str(item.get("location")), str(item.get("origin_evidence_id"))): str(item.get("causal_identity"))
            for item in payload.get("_canonical_setups", [])
            if isinstance(item, dict) and item.get("causal_identity")
        }
        for event in records:
            if not isinstance(event, dict) or event.get("kind") not in {
                "opportunity_outcome", "lifecycle_formed", "setup_invalidated",
                "setup_expired", "setup_superseded", "opportunity_timeout",
            }:
                continue
            parent = event.get("causal_parent_id")
            if parent is None:
                context = event.get("parent_context")
                parent = context.get("context_id") if isinstance(context, dict) else event.get("interaction_id")
            evidence_ids = event.get("evidence_ids") or ()
            identity = (
                event.get("causal_identity")
                or setup_identity.get(str(event.get("lifecycle_id")))
                or setup_by_signature.get((str(event.get("location")), str(event.get("template_id")), str(evidence_ids[-1] if evidence_ids else "")))
                or setup_by_location_evidence.get((str(event.get("location")), str(event.get("interaction_id"))))
                or event.get("lifecycle_id") or event.get("interaction_id")
            )
            observed = event.get("observed_at") or event.get("formation_at")
            available = event.get("available_at") or event.get("formation_available_at")
            if not all(isinstance(value, str) for value in (identity, observed, available)):
                continue
            observed_text = str(observed)
            available_text = str(available)
            lifecycle.append({
                "kind": str(event["kind"]), "occurred_at": _aware(observed_text),
                "observed_at": _aware(observed_text), "available_at": _aware(available_text),
                "identity": identity, "causal_parent_id": parent,
                "values": event, "unavailable": [],
            })
        payload["opportunity_lifecycle"] = lifecycle
        payload["_canonical_records"] = records
    return payload


def _bar(value: dict[str, Any], *, session_date: str, symbol: str = "NIFTY") -> Aggregate:
    return Aggregate(
        str(value["time"]), str(value["end_time"]), str(value["available_at"]),
        str(value["timeframe"]), int(value.get("source_bar_count", 5)),
        float(value["open"]), float(value["high"]), float(value["low"]),
        float(value["close"]), int(float(value.get("volume", 0))), symbol, "", "FUT",
        session_date, str(value.get("source_fingerprint", f"phase5-{value['time']}")),
    )


def _interaction(value: dict[str, Any]) -> InteractionSnapshot:
    candles = tuple(
        InteractionCandle(
            _bar(item, session_date=str(item["time"])[:10]),
            CandleClassification(str(item.get("classification", "CONTAINED"))),
            bool(item.get("is_terminal", False)),
        ) for item in value["candles"]
    )
    creation = candles[0].candle
    projections = tuple(
        (timeframe, tuple(
            ProjectionSnapshot(
                str(item["timeframe"]), float(item["open"]), float(item["high"]),
                float(item["low"]), float(item["close"]), float(item["volume"]),
                tuple(item["source_candle_ids"]), str(item["first_source_start"]),
                str(item["last_source_end"]), str(item["available_at"]),
            ) for item in values
        )) for timeframe, values in value.get("synthetic_snapshots", {}).items()
    )
    return InteractionSnapshot(
        str(value["interaction_id"]), "NIFTY", str(creation.time[:10]),
        str(value["location"]), str(value["timeframe"]), "1", creation,
        float(value["reference_level"]), LocationBand(float(value["band_low"]), float(value["band_high"])),
        CloseSide(value["pre_contact_side"]) if value.get("pre_contact_side") else None,
        creation.end_time, creation.available_at, int(value.get("reference_generation", 0)),
        int(value.get("sessions_carried", 0)), InteractionStatus(value["status"]),
        None, None, str(value.get("terminal_candle_id")) if value.get("terminal_candle_id") else None,
        None, candles, projections, value.get("pd_role"),
    )


def _paper_facts_from_phase5(payload: dict[str, Any]) -> dict[str, Any]:
    interactions: dict[str, InteractionSnapshot] = {}
    for item in payload.get("_phase5_interactions", []):
        if isinstance(item, dict) and item.get("candles"):
            interaction_id = str(item["interaction_id"])
            candidate = _interaction(item)
            previous = interactions.get(interaction_id)
            if previous is None or candidate.creation_candle.time < previous.creation_candle.time:
                interactions[interaction_id] = candidate
    records: list[dict[str, Any]] = []
    setup_identity = {
        str(item.get("lifecycle_id")): str(item.get("causal_identity"))
        for item in payload.get("_canonical_setups", [])
        if isinstance(item, dict) and item.get("lifecycle_id") and item.get("causal_identity")
    }
    setup_by_signature = {
        (str(item.get("location")), str(item.get("template_id")), str(item.get("origin_evidence_id"))): str(item.get("causal_identity"))
        for item in payload.get("_canonical_setups", [])
        if isinstance(item, dict) and item.get("causal_identity")
    }
    setup_by_location_evidence = {
        (str(item.get("location")), str(item.get("origin_evidence_id"))): str(item.get("causal_identity"))
        for item in payload.get("_canonical_setups", [])
        if isinstance(item, dict) and item.get("causal_identity")
    }
    identity_by_pair = {
        (str(item.get("interaction_id")), str(item.get("template_id"))): setup_identity.get(str(item.get("lifecycle_id")))
        for item in payload.get("_canonical_records", [])
        if isinstance(item, dict) and item.get("kind") == "lifecycle_formed"
    }
    for event in payload.get("_canonical_records", []):
        if not isinstance(event, dict) or event.get("kind") != "opportunity_outcome":
            continue
        interaction = interactions.get(str(event.get("interaction_id")))
        template_id = str(event.get("template_id", ""))
        template = next((item for item in TRADE_TEMPLATES.values() if item.template_id == template_id), None)
        if interaction is None or template is None:
            continue
        bars = tuple(item.candle for item in interaction.candles)
        as_of = str(event.get("available_at") or interaction.available_at)
        outcome, identity = identify_opportunity(template, interaction, bars, as_of=as_of)
        canonical_identity = identity_by_pair.get((str(interaction.interaction_id), template_id)) or setup_by_signature.get((str(interaction.location), template_id, str(outcome.evidence[-1].time if outcome.evidence else ""))) or setup_by_location_evidence.get((str(interaction.location), str(interaction.interaction_id))) or setup_by_location_evidence.get((str(interaction.location), str(outcome.evidence[-1].time if outcome.evidence else "")))
        identity = str(canonical_identity) if canonical_identity else identity
        values = {
            "status": outcome.status.value, "reason": outcome.reason,
            "template_id": template.template_id, "identity": identity,
            "evidence_ids": [item.time for item in outcome.evidence],
            "evidence_lineage": [item.__dict__ if hasattr(item, "__dict__") else str(item) for item in outcome.evidence_lineage],
        }
        records.append({
            "kind": "opportunity_outcome", "occurred_at": _aware(outcome.observed_at),
            "observed_at": _aware(outcome.observed_at), "available_at": _aware(outcome.available_at),
            "identity": identity or f"outcome:{interaction.interaction_id}:{template.template_id}",
            "causal_parent_id": interaction.interaction_id, "values": values,
            "unavailable": [],
        })
        if outcome.response is None or identity is None:
            continue
        context = OpportunityContext(
            lifecycle_id=f"lifecycle-{identity}", session_date=interaction.session_date,
            instrument=interaction.instrument, strategy="orderflow-1m_5m",
            candidate_id=identity, context_candle=bars[0], clock=outcome.available_at,
            regime_version="unresolved", policy_version=template.policy_version,
            reference_generation=interaction.reference_generation,
            causal_identity=identity, location_interaction_id=interaction.interaction_id,
        )
        life, _ = form_opportunity(outcome.response, context, template)
        timeout = advance_opportunity_timeout(life, interaction=interaction, evidence=bars, as_of=as_of)
        records.append({
            "kind": "lifecycle_formed", "occurred_at": _aware(life.formation_at),
            "observed_at": _aware(life.formation_at), "available_at": _aware(life.formation_available_at),
            "identity": identity, "causal_parent_id": interaction.interaction_id,
            "values": {"lifecycle_id": life.lifecycle_id, "stage": life.stage.value, "timeout_count": life.timeout_count, "timeout_added_units": timeout.added_units},
            "unavailable": [],
        })
    records.sort(key=lambda item: (str(item["available_at"]), str(item["identity"]), str(item["kind"])))
    return {"opportunity_lifecycle": records}


def _load_paper_facts(path: Path) -> dict[str, Any]:
    payload = json.loads(sys.stdin.read()) if str(path) == "-" else json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("parity_phase_facts"), dict):
        return payload["parity_phase_facts"]
    if isinstance(payload, dict):
        return payload
    raise ValueError("Paper facts must be an object or parity_phase_facts object")


def compare(*, canonical_facts: dict[str, Any], paper_facts: dict[str, Any], session_date: str) -> dict[str, Any]:
    # Phase 6 is intentionally scoped to the opportunity stream. Earlier
    # streams are supplied by the frozen Phase 5 boundary and must not mask a
    # lifecycle result as an unrelated missing stream.
    defaults = {
        "source_observation": [], "context_location": [],
        "interaction_response": [], "opportunity_lifecycle": [],
        "intent_admission": [], "trigger_exit": [],
    }
    streams = dict(defaults)
    streams.update(canonical_facts)
    paper_streams = dict(defaults)
    paper_streams.update(paper_facts)
    for facts in (streams, paper_streams):
        records = facts.get("opportunity_lifecycle")
        if isinstance(records, list):
            facts["opportunity_lifecycle"] = sorted(
                records,
                key=lambda item: (str(item.get("available_at")), str(item.get("identity")), str(item.get("kind"))),
            )
    canonical = build_phase4_artifact(
        run_id=f"canonical-{session_date}",
        producer={"name": "NiftyZoning"},
        scope={"date": session_date},
        payload={"parity_phase_facts": streams},
    )
    paper = build_phase4_artifact(
        run_id=f"paper-{session_date}",
        producer={"name": "ftx-paper"},
        scope={"date": session_date},
        payload={"parity_phase_facts": paper_streams},
    )
    return compare_phase4_manifests(
        canonical,
        paper,
        canonical_records=lambda stream: canonical["phase_streams"][stream],
        paper_records=lambda stream: paper["phase_streams"][stream],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, choices=sorted(ALLOWED_DATES))
    parser.add_argument("--canonical-root", type=Path, required=True)
    parser.add_argument("--paper-facts", type=Path, required=True)
    parser.add_argument("--canonical-python", default=sys.executable)
    args = parser.parse_args(argv)
    canonical_facts = _canonical_facts(args.canonical_root, args.date, args.canonical_python)
    paper_facts = _load_paper_facts(args.paper_facts)
    if not paper_facts:
        paper_facts = _paper_facts_from_phase5(canonical_facts)
    result = compare(canonical_facts=canonical_facts, paper_facts=paper_facts, session_date=args.date)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result.get("status") == "MATCHED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
