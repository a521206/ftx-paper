"""Explain Phase 4 and 4.5 replay mismatches from retained artifacts.

This diagnostic never replays market data. It reads an existing comparison
report and its retained canonical/Paper artifacts, then writes chronological
causal windows for the first Phase 4 and Phase 4.5 differences.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "data" / "output" / "orderflow"
PHASE4_STREAMS = ("source_observation", "context_location")
PHASE45_STREAM = "interaction_response"
REFERENCE_KEYS = (
    "causal_parent_id", "causal_parent_ref", "causal_ids", "parent_id",
    "source_aggregate_id", "source_observation_id", "source_candle_id",
    "creation_candle_id", "creation_candle", "evidence_timeframe",
    "available_at", "observed_at", "occurred_at", "created_at",
    "formation_at", "response_observed_at", "response_available_at",
)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _latest_report(directory: Path) -> Path:
    reports = sorted(directory.glob("canonical-replay-comparison-*.json"))
    if not reports:
        raise FileNotFoundError(f"no comparison reports under {directory}")
    return reports[-1]


def _artifact_streams(payload: Mapping[str, Any], session_date: str) -> dict[str, list[dict[str, Any]]]:
    facts: Any = payload.get("parity_phase_facts")
    if not isinstance(facts, Mapping):
        sessions = payload.get("sessions")
        if isinstance(sessions, Mapping):
            session = sessions.get(session_date)
            if isinstance(session, Mapping):
                facts = session.get("parity_phase_facts")
    if not isinstance(facts, Mapping):
        raise ValueError("artifact does not contain parity_phase_facts")
    return {
        str(name): [dict(item) for item in records if isinstance(item, Mapping)]
        for name, records in facts.items()
        if isinstance(records, list)
    }


def _references(value: Any, path: str = "$") -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text in REFERENCE_KEYS:
                found.append({"path": child_path, "value": item})
            found.extend(_references(item, child_path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_references(item, f"{path}[{index}]"))
    return found


def _window(records: list[dict[str, Any]], index: int, radius: int) -> list[dict[str, Any]]:
    start = max(0, index - radius)
    end = min(len(records), index + radius + 1)
    return [{"index": position, "record": records[position]} for position in range(start, end)]


def _state_latches(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    previous: Any = object()
    for index, record in enumerate(records):
        context = record.get("values", {}).get("context")
        state = context.get("market_state") if isinstance(context, Mapping) else None
        if state != previous:
            result.append({
                "index": index,
                "market_state": state,
                "identity": record.get("identity"),
                "observed_at": record.get("observed_at"),
                "available_at": record.get("available_at"),
                "causal_parent_id": record.get("causal_parent_id"),
            })
            previous = state
    return result


def _interaction_ids(*records: Mapping[str, Any]) -> set[str]:
    ids: set[str] = set()
    for record in records:
        values = record.get("values")
        if not isinstance(values, Mapping):
            continue
        for key in ("interaction_id", "causal_parent_id", "causal_parent_ref"):
            value = values.get(key)
            if isinstance(value, str):
                ids.add(value)
            elif isinstance(value, Mapping) and isinstance(value.get("id"), str):
                ids.add(str(value["id"]))
    return ids


def _interaction_timeline(records: list[dict[str, Any]], ids: set[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    result = []
    for index, record in enumerate(records):
        encoded = json.dumps(record, sort_keys=True, default=str)
        if any(identifier in encoded for identifier in ids):
            result.append({
                "index": index,
                "kind": record.get("kind"),
                "identity": record.get("identity"),
                "observed_at": record.get("observed_at"),
                "available_at": record.get("available_at"),
                "references": _references(record),
                "record": record,
            })
    return result


def _phase_diagnostic(
    report_result: Mapping[str, Any],
    canonical: Mapping[str, Any],
    paper: Mapping[str, Any],
    session_date: str,
    radius: int,
) -> dict[str, Any]:
    canonical_streams = _artifact_streams(canonical, session_date)
    paper_streams = _artifact_streams(paper, session_date)
    phase4_report = report_result.get("phase4", {})
    phase45_report = report_result.get("phase4_5", {})
    context_c = canonical_streams.get("context_location", [])
    context_p = paper_streams.get("context_location", [])
    first4 = phase4_report.get("index") if isinstance(phase4_report, Mapping) else None
    if not isinstance(first4, int):
        first4 = 0
    response_c = canonical_streams.get(PHASE45_STREAM, [])
    response_p = paper_streams.get(PHASE45_STREAM, [])
    first45 = phase45_report.get("index") if isinstance(phase45_report, Mapping) else None
    if not isinstance(first45, int):
        first45 = 0
    canonical_first45 = response_c[min(first45, len(response_c) - 1)] if response_c else {}
    paper_first45 = response_p[min(first45, len(response_p) - 1)] if response_p else {}
    ids = _interaction_ids(canonical_first45, paper_first45)
    return {
        "phase4": {
            "report_first_difference": phase4_report,
            "source_observation": {
                "canonical_count": len(canonical_streams.get("source_observation", [])),
                "paper_count": len(paper_streams.get("source_observation", [])),
                "canonical_window": _window(canonical_streams.get("source_observation", []), first4, radius),
                "paper_window": _window(paper_streams.get("source_observation", []), first4, radius),
            },
            "context_location": {
                "canonical_count": len(context_c),
                "paper_count": len(context_p),
                "canonical_window": _window(context_c, first4, radius),
                "paper_window": _window(context_p, first4, radius),
                "canonical_market_state_latches": _state_latches(context_c),
                "paper_market_state_latches": _state_latches(context_p),
            },
        },
        "phase4_5": {
            "report_first_difference": phase45_report,
            "canonical_count": len(response_c),
            "paper_count": len(response_p),
            "canonical_window": _window(response_c, first45, radius),
            "paper_window": _window(response_p, first45, radius),
            "interaction_ids": sorted(ids),
            "canonical_interaction_timeline": _interaction_timeline(response_c, ids),
            "paper_interaction_timeline": _interaction_timeline(response_p, ids),
        },
    }


def diagnose(report_path: Path, output_path: Path, radius: int) -> Path:
    report = _load(report_path)
    results = report.get("results")
    if not isinstance(results, list) or not results or not isinstance(results[0], Mapping):
        raise ValueError("comparison report has no result")
    result = results[0]
    session_date = str(result.get("date"))
    canonical_path = Path(str(result["canonical_replay"]["path"]))
    paper_path = Path(str(result["paper_replay"]["path"]))
    payload = {
        "artifact_report": str(report_path),
        "canonical_artifact": str(canonical_path),
        "paper_artifact": str(paper_path),
        "date": session_date,
        "run_id": report.get("run_id"),
        "diagnostic": _phase_diagnostic(
            result, _load(canonical_path), _load(paper_path), session_date, radius,
        ),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--radius", type=int, default=5)
    args = parser.parse_args(argv)
    report = args.report or _latest_report(DEFAULT_OUTPUT)
    output = args.output or DEFAULT_OUTPUT / f"causal-diagnostics-{report.stem.removeprefix('canonical-replay-comparison-')}.json"
    path = diagnose(report, output, max(0, args.radius))
    print(f"Causal diagnostic: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
