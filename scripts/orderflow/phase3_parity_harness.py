"""Compare Phase 3 artifacts and supplied Phase 4A publications.

The harness runs both repositories' Phase 3 exporters first.  Phase 4A then
consumes the resulting aggregate artifacts and explicitly supplied evidence
publications.  It does not reconstruct evidence or run Phase 4B replay.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any

from ftx_paper.config import PaperConfig
from ftx_paper.strategies.orderflow.evidence import (
    SerializedEvidenceAdapter,
    evidence_payload,
    produce_evidence,
)

SECTIONS = (
    "source_fingerprint",
    "aggregates",
    "source_gaps",
    "issues",
    "session_boundaries",
)
PHASE4A_FIELDS = (
    "atr",
    "locations",
    "vwap_measurements",
    "prior_value",
    "contexts",
    "unavailable",
    "context_stream_id",
    "location_stream_id",
)
ALLOWED_DATES = ("2026-09-15", "2026-10-01")
DEFAULT_DATES = list(ALLOWED_DATES)
_AGGREGATE_FIELDS = {
    "time", "end_time", "available_at", "timeframe", "source_bar_count",
    "open", "high", "low", "close", "volume", "symbol", "expiry",
    "contract_type", "session_date", "source_fingerprint",
}


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _first_difference(left: Any, right: Any, path: str = "") -> dict[str, Any] | None:
    if type(left) is type(right) and left == right:
        return None
    if isinstance(left, dict) and isinstance(right, dict):
        for key in sorted(set(left) | set(right)):
            difference = _first_difference(left.get(key), right.get(key), f"{path}.{key}")
            if difference:
                return difference
    elif isinstance(left, list) and isinstance(right, list):
        for index, (left_item, right_item) in enumerate(zip(left, right, strict=False)):
            difference = _first_difference(left_item, right_item, f"{path}[{index}]")
            if difference:
                return difference
    return {"path": path or "$", "canonical": left, "paper": right}


def compare_payloads(canonical: dict[str, Any], paper: dict[str, Any]) -> dict[str, Any]:
    """Compare the Layer 1 sections of two exporter payloads."""
    missing = {
        side: [name for name in SECTIONS if name not in payload]
        for side, payload in (("canonical", canonical), ("paper", paper))
    }
    if any(missing.values()):
        return {"verdict": "INCOMPLETE", "missing_sections": missing}
    manifest = {
        name: {"canonical": _digest(canonical.get(name)), "paper": _digest(paper.get(name))}
        for name in SECTIONS
    }
    for name, hashes in manifest.items():
        if hashes["canonical"] != hashes["paper"]:
            return {
                "verdict": "MISMATCHED",
                "first_mismatch": {
                    "section": name,
                    **(_first_difference(canonical.get(name), paper.get(name)) or {}),
                },
                "manifest": manifest,
            }
    return {"verdict": "MATCHED", "manifest": manifest}


def _validate_phase3_artifact(payload: dict[str, Any]) -> None:
    """Reject malformed Phase 3 artifacts before they reach Phase 4A."""
    required = {"artifact_type", "schema_version", "source_fingerprint", "aggregates", *SECTIONS}
    if payload.get("artifact_type") != "canonical_p2_market_observation":
        raise ValueError("invalid Phase 3 artifact type")
    if payload.get("schema_version") != 1:
        raise ValueError("invalid Phase 3 schema version")
    if not required <= set(payload):
        raise ValueError("Phase 3 artifact envelope is incomplete")
    if not isinstance(payload["source_fingerprint"], str) or not payload["source_fingerprint"]:
        raise ValueError("Phase 3 source_fingerprint is required")
    aggregates = payload["aggregates"]
    if not isinstance(aggregates, dict) or set(aggregates) != {"1m", "5m"}:
        raise ValueError("Phase 3 aggregates must contain exactly 1m and 5m")
    for timeframe, items in aggregates.items():
        if not isinstance(items, list):
            raise ValueError(f"Phase 3 {timeframe} aggregates must be a list")
        for item in items:
            if not isinstance(item, dict) or not _AGGREGATE_FIELDS <= set(item):
                raise ValueError(f"Phase 3 {timeframe} aggregate is incomplete")
            if item["timeframe"] != timeframe:
                raise ValueError(f"Phase 3 {timeframe} aggregate has mismatched timeframe")


def _publication_payload(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("phase4a_evidence", payload.get("evidence", payload))
    if not isinstance(value, dict):
        raise ValueError("Phase 4A publication must be an object")
    return value


def _phase4a_manifest(publication: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    manifest: dict[str, Any] = {}
    for field in PHASE4A_FIELDS:
        if field not in publication:
            manifest[field] = {
                "status": "UNAVAILABLE", "observed": False, "reason": f"missing_{field}_publication",
                "count": 0, "sha256": _digest(None),
            }
            continue
        value = publication[field]
        if field in {"locations", "vwap_measurements", "contexts", "unavailable"} and not isinstance(value, list):
            return {}, f"Phase 4A {field} must be a list"
        if field in {"context_stream_id", "location_stream_id"} and value is not None and not isinstance(value, str):
            return {}, f"Phase 4A {field} must be a string or null"
        manifest[field] = {
            "status": "MATCHED", "observed": True,
            "count": len(value) if isinstance(value, list) else 1,
            "sha256": _digest(value),
        }
    return manifest, None


def compare_phase4a_evidence(
    canonical: dict[str, Any], paper: dict[str, Any],
) -> dict[str, Any]:
    """Compare supplied Phase 4A publications, manifest before records."""
    try:
        canonical_value = _publication_payload(canonical)
        paper_value = _publication_payload(paper)
        canonical_manifest, canonical_error = _phase4a_manifest(canonical_value)
        paper_manifest, paper_error = _phase4a_manifest(paper_value)
    except ValueError as exc:
        return {"status": "INCOMPLETE", "reason": str(exc)}
    if canonical_error or paper_error:
        return {
            "status": "INCOMPLETE", "reason": canonical_error or paper_error,
            "canonical": canonical_error, "paper": paper_error,
        }
    for field in PHASE4A_FIELDS:
        left = canonical_manifest[field]
        right = paper_manifest[field]
        if left["status"] != "MATCHED" or right["status"] != "MATCHED":
            statuses = {left["status"], right["status"]}
            status = "INCOMPLETE" if "INCOMPLETE" in statuses else "UNAVAILABLE"
            return {"status": status, "field": field, "canonical": left, "paper": right}
        if (left["count"], left["sha256"]) == (right["count"], right["sha256"]):
            continue
        difference = _first_difference(canonical_value.get(field), paper_value.get(field))
        return {
            "status": "MISMATCHED", "field": field, "manifest": {
                "canonical": canonical_manifest, "paper": paper_manifest,
            }, "first_mismatch": difference,
        }
    return {"status": "MATCHED", "manifest": {"canonical": canonical_manifest, "paper": paper_manifest}}


def _load_publication(path: Path, session_date: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("phase4a_evidence"), dict):
        payload = payload["phase4a_evidence"]
    if isinstance(payload, dict) and session_date in payload:
        payload = payload[session_date]
    if not isinstance(payload, dict):
        raise ValueError("supplied Phase 4A publication is not an object")
    return payload


def _run_exporter(
    command: list[str],
    *,
    cwd: Path,
    output: Path,
    env: dict[str, str],
) -> tuple[str, dict[str, Any] | None]:
    result = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True)
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        return f"INCOMPLETE: {detail or 'exporter failed'}", None
    try:
        payload = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return f"INCOMPLETE: invalid exporter output: {exc}", None
    if not isinstance(payload, dict):
        return "INCOMPLETE: exporter output is not an object", None
    return "MATCHED", payload


def _environment_for(root: Path) -> dict[str, str]:
    environment = os.environ.copy()
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = str(root) if not existing else os.pathsep.join((str(root), existing))
    return environment


def compare_date(
    session_date: str,
    *,
    canonical_root: Path,
    paper_root: Path,
    paper_runtime_dir: Path,
    canonical_python: str,
    paper_python: str,
    canonical_evidence: Path | None = None,
    paper_evidence: Path | None = None,
) -> dict[str, Any]:
    """Run both exporters and compare one trading date."""
    with tempfile.TemporaryDirectory(prefix="orderflow-phase3-parity-") as temporary:
        directory = Path(temporary)
        canonical_output = directory / "canonical.json"
        paper_output = directory / "paper.json"
        canonical_status, canonical = _run_exporter(
            [
                canonical_python,
                "scripts/orderflow/export_market_observation.py",
                "--date",
                session_date,
                "--output",
                str(canonical_output),
            ],
            cwd=canonical_root,
            output=canonical_output,
            env=_environment_for(canonical_root),
        )
        paper_status, paper = _run_exporter(
            [
                paper_python,
                "scripts/orderflow/export_market_observation.py",
                "--date",
                session_date,
                "--runtime-dir",
                str(paper_runtime_dir),
                "--output",
                str(paper_output),
            ],
            cwd=paper_root,
            output=paper_output,
            env=_environment_for(paper_root),
        )
        if canonical is None or paper is None:
            return {
                "date": session_date,
                "verdict": "INCOMPLETE",
                "canonical": canonical_status,
                "paper": paper_status,
            }
        try:
            _validate_phase3_artifact(canonical)
            _validate_phase3_artifact(paper)
        except ValueError as exc:
            return {"date": session_date, "phase3": compare_payloads(canonical, paper), "phase4a": {"status": "INCOMPLETE", "reason": str(exc)}}
        phase3_result = compare_payloads(canonical, paper)
        if (canonical_evidence is None) != (paper_evidence is None):
            phase35_result = {
                "status": "INCOMPLETE", "reason": "canonical_and_paper_evidence_are_required_together",
            }
            phase4_result = {
                "status": "INCOMPLETE", "reason": "canonical_and_paper_evidence_are_required_together",
            }
        elif canonical_evidence is None or paper_evidence is None:
            phase35_result = {
                "status": "UNAVAILABLE", "reason": "supplied_phase3_5_publications_required",
            }
            phase4_result = {"status": "UNAVAILABLE", "reason": "supplied_phase4a_publications_required"}
        else:
            try:
                canonical_publication = _load_publication(canonical_evidence, session_date)
                paper_publication = _load_publication(paper_evidence, session_date)
                canonical_phase35 = produce_evidence(
                    canonical, session_date=session_date,
                    adapter=SerializedEvidenceAdapter(canonical_publication),
                )
                paper_phase35 = produce_evidence(
                    paper, session_date=session_date,
                    adapter=SerializedEvidenceAdapter(paper_publication),
                )
                phase35_result = compare_phase4a_evidence(
                    evidence_payload(canonical_phase35)["evidence"],
                    evidence_payload(paper_phase35)["evidence"],
                )
                phase4_result = compare_phase4a_evidence(
                    canonical_publication, paper_publication,
                )
            except (OSError, json.JSONDecodeError, ValueError) as exc:
                phase35_result = {"status": "INCOMPLETE", "reason": str(exc)}
                phase4_result = {"status": "INCOMPLETE", "reason": str(exc)}
        return {
            "date": session_date,
            "phase3": phase3_result,
            "phase35": phase35_result,
            "phase4a": phase4_result,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", action="append", dest="dates", choices=ALLOWED_DATES, default=None)
    parser.add_argument("--canonical-evidence", type=Path, default=None)
    parser.add_argument("--paper-evidence", type=Path, default=None)
    args = parser.parse_args(argv)
    dates = args.dates or DEFAULT_DATES
    paper_root = Path(__file__).resolve().parents[2]
    canonical_root = paper_root.parent / "NiftyZoning"
    canonical_python_path = canonical_root / ".venv" / "Scripts" / "python.exe"
    canonical_python = str(canonical_python_path if canonical_python_path.exists() else sys.executable)
    paper_runtime_dir = PaperConfig.from_env().runtime_dir
    results = [
        compare_date(
            date,
            canonical_root=canonical_root,
            paper_root=paper_root,
            paper_runtime_dir=paper_runtime_dir,
            canonical_python=canonical_python,
            paper_python=sys.executable,
            canonical_evidence=args.canonical_evidence,
            paper_evidence=args.paper_evidence,
        )
        for date in dates
    ]
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if all(
        item.get("phase3", {}).get("verdict") == "MATCHED"
        and item.get("phase35", {}).get("status") == "MATCHED"
        and item.get("phase4a", {}).get("status") == "MATCHED"
        for item in results
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
