"""Compare the Paper evidence replay with NiftyZoning for one session.

The two repositories are executed in isolated subprocesses.  Phase 3.5
evidence projections are compared.  No NiftyZoning package is imported into
the Paper process.
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


PHASE35_FIELDS = (
    "atr", "locations", "vwap_measurements", "prior_value", "contexts",
    "unavailable", "context_stream_id", "location_stream_id",
)


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
        for index in range(min(len(left), len(right), 20)):
            difference = _first_difference(left[index], right[index], f"{path}[{index}]")
            if difference:
                return difference
        if len(left) > 20 or len(right) > 20:
            return {
                "path": path or "$",
                "canonical_count": len(left),
                "paper_count": len(right),
                "canonical_sha256": _digest(left),
                "paper_sha256": _digest(right),
            }
        if len(left) != len(right):
            return {
                "path": f"{path}[{min(len(left), len(right))}]",
                "canonical": left[min(len(left), len(right)):] if len(left) > len(right) else [],
                "paper": right[min(len(left), len(right)):] if len(right) > len(left) else [],
            }
    return {"path": path or "$", "canonical": left, "paper": right}


def _compare_fields(
    canonical: dict[str, Any], paper: dict[str, Any], fields: tuple[str, ...], *, label: str,
) -> dict[str, Any]:
    missing = {
        side: [field for field in fields if field not in payload]
        for side, payload in (("canonical", canonical), ("paper", paper))
    }
    if any(missing.values()):
        return {"status": "INCOMPLETE", "layer": label, "missing": missing}
    manifest = {
        field: {"canonical": _digest(canonical[field]), "paper": _digest(paper[field])}
        for field in fields
    }
    for field in fields:
        if manifest[field]["canonical"] != manifest[field]["paper"]:
            return {
                "status": "MISMATCHED",
                "layer": label,
                "field": field,
                "manifest": manifest,
                "first_mismatch": _first_difference(canonical[field], paper[field], field),
            }
    return {"status": "MATCHED", "layer": label, "manifest": manifest}


def _compare_phase35(canonical: dict[str, Any], paper: dict[str, Any]) -> dict[str, Any]:
    canonical_value = canonical.get("evidence", canonical)
    paper_value = paper.get("evidence", paper)
    if not isinstance(canonical_value, dict) or not isinstance(paper_value, dict):
        return {"status": "INCOMPLETE", "layer": "phase3_5", "reason": "evidence is not an object"}
    return _compare_fields(canonical_value, paper_value, PHASE35_FIELDS, label="phase3_5")


def _environment(root: Path, *, paper_root: Path | None = None) -> dict[str, str]:
    environment = os.environ.copy()
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = str(root) if not existing else os.pathsep.join((str(root), existing))
    if paper_root is not None:
        environment["FTX_PAPER_ROOT"] = str(paper_root)
    return environment


def _run_json(
    command: list[str], *, cwd: Path, output: Path | None = None,
    paper_root: Path | None = None,
) -> tuple[Any | None, str | None]:
    result = subprocess.run(
        command, cwd=cwd, env=_environment(cwd, paper_root=paper_root), capture_output=True, text=True,
    )
    if result.returncode:
        return None, (result.stderr or result.stdout).strip() or "subprocess failed"
    try:
        payload = json.loads(output.read_text(encoding="utf-8")) if output is not None else json.loads(result.stdout)
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"invalid JSON output: {exc}"
    return payload if isinstance(payload, (dict, list)) else None, None


def _run_command(command: list[str], *, cwd: Path) -> str | None:
    result = subprocess.run(
        command, cwd=cwd, env=_environment(cwd), capture_output=True, text=True,
    )
    if result.returncode:
        return (result.stderr or result.stdout).strip() or "subprocess failed"
    return None


def _canonical_projection_command(
    canonical_python: str, replay_path: Path, session_date: str,
) -> list[str]:
    code = (
        "import json, sys; "
        "from src.orderflow.phase35_projection import build_phase35_projection; "
        "payload=json.load(open(sys.argv[1], encoding='utf-8')); "
        "print(json.dumps(build_phase35_projection(payload['sessions'][sys.argv[2]]), default=str))"
    )
    return [canonical_python, "-c", code, str(replay_path), session_date]


def compare_date(
    session_date: str,
    *,
    canonical_root: Path,
    paper_root: Path,
    paper_runtime_dir: Path,
    canonical_python: str,
    paper_python: str,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="orderflow-cross-repo-") as directory_name:
        directory = Path(directory_name)
        paper_replay_path = directory / "paper-replay.json"
        paper_replay, replay_error = _run_json(
            [paper_python, "scripts/orderflow/orderflow_replay.py", "--date", session_date, "--runtime-dir", str(paper_runtime_dir), "--output", str(paper_replay_path)],
            cwd=paper_root, output=paper_replay_path,
        )
        canonical_output_dir = directory / "canonical-replay"
        canonical_error = _run_command(
            [
                canonical_python,
                "scripts/orderflow/experiments/run_causal_orderflow_replay.py",
                "--date", session_date.replace("-", ""),
                "--replay-mode", "full",
                "--output", str(canonical_output_dir),
            ],
            cwd=canonical_root,
        )
        replay_files = list(canonical_output_dir.rglob("orderflow-state-machine-multi-session-replay.json"))
        canonical_path = replay_files[0] if replay_files else None
        canonical_projection, projection_error = (
            _run_json(
                _canonical_projection_command(canonical_python, canonical_path, session_date),
                cwd=canonical_root,
            )
            if canonical_path is not None and canonical_error is None
            else (None, canonical_error or "canonical replay artifact was not produced")
        )
        if paper_replay is None or canonical_projection is None:
            phase35 = {
                "status": "INCOMPLETE",
                "layer": "phase3_5",
                "canonical": projection_error,
                "paper": replay_error,
            }
        else:
            phase35 = _compare_phase35(
                canonical_projection, paper_replay,
            )
        return {
            "date": session_date,
            "phase3_5": phase35,
            "canonical_replay": {"status": "MATCHED" if canonical_projection else "INCOMPLETE"},
            "paper_replay": {"status": "MATCHED" if paper_replay else "INCOMPLETE"},
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", action="append", dest="dates", required=True)
    parser.add_argument("--canonical-root", type=Path, default=None)
    parser.add_argument("--canonical-python", default=None)
    parser.add_argument("--runtime-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    paper_root = Path(__file__).resolve().parents[2]
    canonical_root = args.canonical_root or paper_root.parent / "NiftyZoning"
    canonical_python_path = canonical_root / ".venv" / "Scripts" / "python.exe"
    canonical_python = args.canonical_python or str(
        canonical_python_path if canonical_python_path.exists() else sys.executable
    )
    paper_runtime_dir = args.runtime_dir or PaperConfig.from_env().runtime_dir
    results = [compare_date(
        session_date,
        canonical_root=canonical_root,
        paper_root=paper_root,
        paper_runtime_dir=paper_runtime_dir,
        canonical_python=canonical_python,
        paper_python=sys.executable,
    ) for session_date in args.dates]
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if all(
        item.get("phase3_5", {}).get("status") == "MATCHED"
        for item in results
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
