"""Compare canonical and Paper Layer 1 market-observation artifacts.

The harness runs both repositories' exporters for one or more session dates,
compares the deterministic source-observation sections, and reports the first
field-level mismatch after hash-first manifest comparison.
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


SECTIONS = (
    "source_fingerprint",
    "aggregates",
    "source_gaps",
    "issues",
    "session_boundaries",
)
DEFAULT_DATE = "2026-09-29"


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
        return {"date": session_date, **compare_payloads(canonical, paper)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", action="append", dest="dates", default=None)
    parser.add_argument("--canonical-root", type=Path, required=True)
    parser.add_argument("--paper-runtime-dir", type=Path, required=True)
    parser.add_argument(
        "--paper-root", type=Path,
        default=Path(__file__).resolve().parents[2],
    )
    parser.add_argument("--canonical-python", default=sys.executable)
    parser.add_argument("--paper-python", default=sys.executable)
    args = parser.parse_args(argv)
    dates = args.dates or [DEFAULT_DATE]
    results = [
        compare_date(
            date,
            canonical_root=args.canonical_root,
            paper_root=args.paper_root,
            paper_runtime_dir=args.paper_runtime_dir,
            canonical_python=args.canonical_python,
            paper_python=args.paper_python,
        )
        for date in dates
    ]
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if all(item["verdict"] == "MATCHED" for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
