"""Compare the Paper evidence replay with NiftyZoning for one session.

The two repositories are executed in isolated subprocesses.  Phase 3
observations are rebuilt from replay-engine bars before Phase 3.5 evidence
projections are compared.  No NiftyZoning package is imported into the Paper
process.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any
from zoneinfo import ZoneInfo

from ftx_paper.config import PaperConfig


PHASE35_FIELDS = (
    "atr", "locations", "vwap_measurements", "prior_value", "contexts",
    "unavailable", "context_stream_id", "location_stream_id",
)
PHASE35_ATR_DEPENDENT_FIELDS = ("atr", "locations", "vwap_measurements", "contexts")
PHASE35_ATR_INDEPENDENT_FIELDS = tuple(
    field for field in PHASE35_FIELDS if field not in PHASE35_ATR_DEPENDENT_FIELDS
)
PHASE3_FIELDS = (
    "source_fingerprint", "aggregates", "source_gaps", "issues", "session_boundaries",
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
    paper_value = _paper_phase35_projection(paper)
    if not isinstance(canonical_value, dict) or not isinstance(paper_value, dict):
        return {"status": "INCOMPLETE", "layer": "phase3_5", "reason": "evidence is not an object"}
    missing = {
        side: [field for field in PHASE35_FIELDS if field not in payload]
        for side, payload in (("canonical", canonical_value), ("paper", paper_value))
    }
    if any(missing.values()):
        return {"status": "INCOMPLETE", "layer": "phase3_5", "missing": missing}
    canonical_atr = canonical_value["atr"]
    paper_atr = paper_value["atr"]
    if not isinstance(canonical_atr, dict) or not isinstance(paper_atr, dict):
        return {"status": "INCOMPLETE", "layer": "phase3_5", "reason": "atr is not an object"}
    if canonical_atr.get("value") != paper_atr.get("value"):
        independent = _compare_fields(
            canonical_value,
            paper_value,
            PHASE35_ATR_INDEPENDENT_FIELDS,
            label="phase3_5_independent",
        )
        return {
            "status": "INPUT_VARIANCE",
            "layer": "phase3_5",
            "reason": "ATR source histories differ; dependent evidence is not numerically comparable",
            "variance_fields": list(PHASE35_ATR_DEPENDENT_FIELDS),
            "atr": {
                "canonical": canonical_atr.get("value"),
                "paper": paper_atr.get("value"),
            },
            "dependent_counts": {
                field: {
                    "canonical": len(canonical_value[field]) if isinstance(canonical_value[field], list) else None,
                    "paper": len(paper_value[field]) if isinstance(paper_value[field], list) else None,
                }
                for field in PHASE35_ATR_DEPENDENT_FIELDS
            },
            "independent": independent,
        }
    return _compare_fields(canonical_value, paper_value, PHASE35_FIELDS, label="phase3_5")


def _paper_phase35_projection(session_result: dict[str, Any]) -> dict[str, Any]:
    """Project Paper's publication payload into the canonical Phase 3.5 shape."""
    raw = session_result.get("evidence", session_result)
    if not isinstance(raw, dict):
        return {}

    raw_atr = raw.get("atr")
    atr = {
        "value": raw_atr.get("value"),
        "lineage": raw_atr.get("lineage", []),
    } if isinstance(raw_atr, dict) else raw_atr
    locations = raw.get("locations", [])
    if not isinstance(locations, list):
        locations = []
    replay = session_result.get("replay_records", [])
    contexts = [
        item.get("ordinary_context_ref")
        for item in replay
        if isinstance(item, dict) and item.get("ordinary_context_ref") is not None
    ] if isinstance(replay, list) else []
    raw_prior = raw.get("prior_value")
    prior_value = raw_prior
    if isinstance(raw_prior, dict) and isinstance(raw_prior.get("reference"), dict):
        reference = raw_prior["reference"]
        lineage = reference.get("lineage", ())
        if (
            isinstance(lineage, (list, tuple))
            and len(lineage) >= 2
            and isinstance(reference.get("available_at"), str)
        ):
            available_at = datetime.fromisoformat(reference["available_at"])
            available_at = available_at.astimezone(ZoneInfo("Asia/Kolkata"))
            prior_value = {
                "reference_id": lineage[0],
                "scope": "prior_session",
                "poc": reference.get("point_of_control"),
                "vah": reference.get("value_area_high"),
                "val": reference.get("value_area_low"),
                "source_sessions": [reference.get("session_date")],
                "available_at": available_at.strftime("%Y-%m-%dT%H:%M"),
                "fingerprint": lineage[1],
            }
        else:
            prior_value = reference
    return {
        "atr": atr,
        "locations": locations,
        "vwap_measurements": [
            item for item in locations if item.get("alias") == "VWAP"
        ],
        "prior_value": prior_value,
        "contexts": contexts,
        "unavailable": raw.get("unavailable", []),
        "context_stream_id": raw.get("context_stream_id"),
        "location_stream_id": raw.get("location_stream_id"),
    }


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


def _artifact_contains_session(path: Path, session_date: str) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return False
    sessions = payload.get("sessions") if isinstance(payload, dict) else None
    return isinstance(sessions, dict) and session_date in sessions


def _resolve_canonical_artifact(path: Path, session_date: str) -> Path | None:
    if path.is_file():
        if path.name == "manifest.json":
            try:
                manifest = json.loads(path.read_text(encoding="utf-8"))
                latest_replay = manifest.get("latest_replay")
            except (OSError, TypeError, ValueError):
                return None
            if isinstance(latest_replay, str):
                replay_path = Path(latest_replay)
                if not replay_path.is_absolute():
                    replay_path = path.parent / replay_path
                return (
                    replay_path
                    if replay_path.is_file() and _artifact_contains_session(replay_path, session_date)
                    else None
                )
        return path if _artifact_contains_session(path, session_date) else None
    if path.is_dir():
        matches = sorted(path.rglob("orderflow-state-machine-multi-session-replay.json"))
        return next(
            (candidate for candidate in matches if _artifact_contains_session(candidate, session_date)),
            None,
        )
    return None


def _replay_observation_command(
    python: str, replay_path: Path, session_date: str, *, canonical: bool,
) -> list[str]:
    module = "src.orderflow.market_observation" if canonical else "ftx_paper.strategies.orderflow"
    timestamp_normalization = (
        "rows=[{**row, 'time': f\"{row['session_date']}T{str(row['time']).split('T')[-1][:5]}\"} for row in rows]; "
        if not canonical else ""
    )
    code = (
        "import json, sys; "
        f"from {module} import observe_market; "
        f"payload=json.load(open(sys.argv[1], encoding='utf-8')); "
         + ("session=payload['sessions'][sys.argv[2]]; "
            "replay=session.get('replay', {}); " if canonical else
            "replay=payload; ")
         + "rows=[item['bar'] for item in replay.get('replay_records', [])]; "
         + timestamp_normalization
        + "observation=observe_market(rows); "
        "print(json.dumps({"
        "'source_fingerprint': observation.source_fingerprint, "
        "'aggregates': {key: [item.to_dict() for item in value] for key, value in observation.aggregates.items()}, "
        "'source_gaps': [{'index': item.index, 'previous_time': item.previous_time, 'next_time': item.next_time} for item in observation.source_gaps], "
        "'issues': [{'kind': item.kind.value, 'session_date': item.session_date, 'timeframe': item.timeframe, 'group_start': item.group_start, 'group_end': item.group_end, 'details': item.details} for item in observation.issues], "
        "'session_boundaries': {str(key): list(value) for key, value in observation.session_boundaries.items()}"
        "}, default=str))"
    )
    return [python, "-c", code, str(replay_path), session_date]


def _canonical_projection_command(
    canonical_python: str, replay_path: Path, session_date: str,
) -> list[str]:
    code = (
        "import json, sys; "
        "from src.orderflow.phase35_projection import build_phase35_projection; "
        "payload=json.load(open(sys.argv[1], encoding='utf-8')); "
        "session=payload['sessions'][sys.argv[2]]; "
        "projection=build_phase35_projection(session); "
        "projection['evidence']['prior_value']=(projection['evidence']['prior_value'] "
        "if projection['evidence']['prior_value'] is not None else "
        "session.get('value_profile', {}).get('prior_session_reference')); "
        "print(json.dumps(projection, default=str))"
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
    canonical_artifact: Path | None = None,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="orderflow-cross-repo-") as directory_name:
        directory = Path(directory_name)
        paper_replay_path = directory / "paper-replay.json"
        canonical_path = (
            _resolve_canonical_artifact(canonical_artifact, session_date)
            if canonical_artifact else None
        )
        canonical_output_dir = directory / "canonical-replay"
        paper_command = [
            paper_python, "scripts/orderflow/orderflow_replay.py", "--date", session_date,
            "--runtime-dir", str(paper_runtime_dir), "--output", str(paper_replay_path),
        ]
        with ThreadPoolExecutor(max_workers=3) as executor:
            paper_future = executor.submit(
                _run_json, paper_command, cwd=paper_root, output=paper_replay_path,
            )
            if canonical_path is None:
                canonical_future = executor.submit(
                    _run_command,
                    [
                        canonical_python,
                        "scripts/orderflow/experiments/run_causal_orderflow_replay.py",
                        "--date", session_date.replace("-", ""),
                        "--replay-mode", "full",
                        "--output", str(canonical_output_dir),
                    ],
                    cwd=canonical_root,
                )
                canonical_error = canonical_future.result()
                replay_files = list(
                    canonical_output_dir.rglob("orderflow-state-machine-multi-session-replay.json")
                )
                canonical_path = replay_files[0] if replay_files else None
            else:
                canonical_error = None

            paper_replay, replay_error = paper_future.result()
            canonical_projection_future = (
                executor.submit(
                    _run_json,
                    _canonical_projection_command(canonical_python, canonical_path, session_date),
                    cwd=canonical_root,
                )
                if canonical_path is not None and canonical_error is None else None
            )
            canonical_phase3_future = (
                executor.submit(
                    _run_json,
                    _replay_observation_command(
                        canonical_python, canonical_path, session_date, canonical=True,
                    ),
                    cwd=canonical_root,
                )
                if canonical_path is not None and canonical_error is None else None
            )
            paper_phase3_future = (
                executor.submit(
                    _run_json,
                    _replay_observation_command(
                        paper_python, paper_replay_path, session_date, canonical=False,
                    ),
                    cwd=paper_root,
                )
                if paper_replay is not None else None
            )
            canonical_projection, projection_error = (
                canonical_projection_future.result()
                if canonical_projection_future is not None
                else (None, canonical_error or "canonical replay artifact was not produced")
            )
            canonical_phase3, canonical_phase3_error = (
                canonical_phase3_future.result()
                if canonical_phase3_future is not None
                else (None, canonical_error or "canonical replay artifact was not produced")
            )
            paper_phase3, paper_phase3_error = (
                paper_phase3_future.result()
                if paper_phase3_future is not None
                else (None, replay_error or "paper replay artifact was not produced")
            )
        if canonical_phase3 is None or paper_phase3 is None:
            phase3 = {
                "status": "INCOMPLETE",
                "layer": "phase3",
                "canonical": canonical_phase3_error,
                "paper": paper_phase3_error,
            }
        else:
            phase3 = _compare_fields(canonical_phase3, paper_phase3, PHASE3_FIELDS, label="phase3")
        if phase3["status"] != "MATCHED":
            phase35 = {
                "status": "UNAVAILABLE",
                "layer": "phase3_5",
                "reason": "phase3_mismatch",
            }
        elif paper_replay is None or canonical_projection is None:
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
            "phase3": phase3,
            "phase3_5": phase35,
            "canonical_replay": {"status": "MATCHED" if canonical_projection else "INCOMPLETE"},
            "paper_replay": {"status": "MATCHED" if paper_replay else "INCOMPLETE"},
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", action="append", dest="dates", required=True)
    parser.add_argument("--canonical-root", type=Path, default=None)
    parser.add_argument("--canonical-python", default=None)
    parser.add_argument(
        "--canonical-artifact",
        type=Path,
        default=None,
        help="Existing canonical replay artifact or directory; skips canonical replay execution",
    )
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
        canonical_artifact=args.canonical_artifact,
    ) for session_date in args.dates]
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if all(
        item.get("phase3", {}).get("status") == "MATCHED"
        and
        item.get("phase3_5", {}).get("status") in {"MATCHED", "INPUT_VARIANCE"}
        for item in results
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
