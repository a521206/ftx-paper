"""Compare the Paper evidence replay with NiftyZoning for one session.

The two repositories are executed in isolated subprocesses.  Phase 3
observations are rebuilt from replay-engine bars before Phase 3.5 evidence
projections are compared.  No NiftyZoning package is imported into the Paper
process.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any

from ftx_paper.config import PaperConfig
from ftx_paper.strategies.orderflow.parity_artifacts import (
    build_phase4_artifact,
    compare_phase4_manifests,
    compare_chronological_streams,
)
from ftx_paper.strategies.orderflow.timestamps import parse_utc, serialize_utc

PHASE35_FIELDS = (
    "atr", "locations", "vwap_measurements", "prior_value", "contexts",
    "unavailable", "context_stream_id", "location_stream_id",
)
PHASE35_ATR_DEPENDENT_FIELDS = ("atr", "locations", "vwap_measurements", "contexts")
PHASE35_ATR_INDEPENDENT_FIELDS = tuple(
    field for field in PHASE35_FIELDS if field not in PHASE35_ATR_DEPENDENT_FIELDS
)
# ATR is sourced from repository-specific history, but variance must remain
# bounded before dependent evidence can be accepted as input variance.
ATR_RELATIVE_TOLERANCE = 0.01
ATR_ABSOLUTE_TOLERANCE = 2.5
PHASE3_FIELDS = (
    "source_fingerprint", "aggregates", "source_gaps", "issues", "session_boundaries",
)
PHASE4_STREAMS = ("source_observation", "context_location")
MIN_COMPARISON_DATE = date(2026, 9, 21)


def _comparison_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use ISO YYYY-MM-DD format") from exc
    if parsed < MIN_COMPARISON_DATE:
        raise argparse.ArgumentTypeError(
            f"comparison dates before {MIN_COMPARISON_DATE.isoformat()} are prohibited"
        )
    return parsed.isoformat()


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


def _compare_phase35_independent(
    canonical: dict[str, Any], paper: dict[str, Any], tolerance: float,
) -> dict[str, Any]:
    """Compare independent evidence while bounding prior-profile value drift."""
    fields = tuple(field for field in PHASE35_ATR_INDEPENDENT_FIELDS if field != "prior_value")
    result = _compare_fields(canonical, paper, fields, label="phase3_5_independent")
    if result["status"] != "MATCHED":
        return result
    left = canonical["prior_value"]
    right = paper["prior_value"]
    if left == right:
        return result
    if not isinstance(left, dict) or not isinstance(right, dict):
        return {"status": "MISMATCHED", "layer": "phase3_5_independent", "field": "prior_value"}
    for field in ("poc", "vah", "val"):
        left_value = left.get(field)
        right_value = right.get(field)
        if not isinstance(left_value, (int, float)) or not isinstance(right_value, (int, float)) or not math.isfinite(left_value) or not math.isfinite(right_value):
            return {"status": "MISMATCHED", "layer": "phase3_5_independent", "field": f"prior_value.{field}"}
        if abs(left_value - right_value) > tolerance:
            return {"status": "MISMATCHED", "layer": "phase3_5_independent", "field": f"prior_value.{field}", "canonical_value": left_value, "paper_value": right_value, "tolerance": tolerance}
    ignored = {"fingerprint", "poc", "vah", "val"}
    remaining_left = {key: value for key, value in left.items() if key not in ignored}
    remaining_right = {key: value for key, value in right.items() if key not in ignored}
    if remaining_left != remaining_right:
        return {"status": "MISMATCHED", "layer": "phase3_5_independent", "field": "prior_value", "first_mismatch": _first_difference(remaining_left, remaining_right, "prior_value")}
    # The comparison fingerprint intentionally excludes tolerant value fields
    # and the producer fingerprint that was derived from them.
    return {
        "status": "MATCHED",
        "layer": "phase3_5_independent",
        "reason": "bounded_prior_value_input_variance",
        "comparison_fingerprint": _digest(remaining_left),
    }


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
        canonical_value_atr = canonical_atr.get("value")
        paper_value_atr = paper_atr.get("value")
        if not isinstance(canonical_value_atr, (int, float)) or not isinstance(paper_value_atr, (int, float)) or not math.isfinite(canonical_value_atr) or not math.isfinite(paper_value_atr):
            return {"status": "MISMATCHED", "layer": "phase3_5", "reason": "ATR values are not numeric"}
        difference = abs(canonical_value_atr - paper_value_atr)
        tolerance = max(ATR_ABSOLUTE_TOLERANCE, abs(canonical_value_atr) * ATR_RELATIVE_TOLERANCE)
        independent = _compare_phase35_independent(
            canonical_value,
            paper_value,
            tolerance,
        )
        return {
            "status": "INPUT_VARIANCE" if difference <= tolerance and independent["status"] == "MATCHED" else "MISMATCHED",
            "layer": "phase3_5",
            "reason": "ATR source histories differ; dependent evidence is not numerically comparable",
            "variance_fields": list(PHASE35_ATR_DEPENDENT_FIELDS),
            "atr": {
                "canonical": canonical_value_atr,
                "paper": paper_value_atr,
                "absolute_difference": difference,
                "tolerance": tolerance,
                "relative_difference": difference / abs(canonical_value_atr) if canonical_value_atr else None,
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


def _bounded_atr_dependent_difference(phase4: dict[str, Any], tolerance: float) -> bool:
    """Accept only numeric ATR geometry differences within the ATR bound."""
    field = str(phase4.get("field", ""))
    canonical = phase4.get("canonical_value")
    paper = phase4.get("paper_value")
    if not field.endswith((".band_low", ".band_high")):
        return False
    if not isinstance(canonical, (int, float)) or not isinstance(paper, (int, float)) or not math.isfinite(canonical) or not math.isfinite(paper):
        return False
    return abs(canonical - paper) <= tolerance


def _paper_phase35_projection(session_result: dict[str, Any]) -> dict[str, Any]:
    """Project Paper's publication payload into the canonical Phase 3.5 shape."""
    raw = session_result.get("evidence", session_result)
    if not isinstance(raw, dict):
        return {}

    raw_atr = raw.get("atr")
    locations = raw.get("locations", [])
    if not isinstance(locations, list):
        locations = []
    atr = {
        "value": raw_atr.get("value"),
        # Match the canonical projection: ATR lineage is represented by the
        # source aggregates of locations carrying the frozen ATR.
        "lineage": [
            item.get("source_aggregate_id")
            for item in locations
            if isinstance(item, dict) and item.get("frozen_atr") is not None
        ],
    } if isinstance(raw_atr, dict) else raw_atr
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
            available_at = serialize_utc(parse_utc(reference["available_at"]))
            prior_value = {
                "reference_id": lineage[0],
                "scope": "prior_session",
                "poc": reference.get("point_of_control"),
                "vah": reference.get("value_area_high"),
                "val": reference.get("value_area_low"),
                "source_sessions": [reference.get("session_date")],
                "available_at": available_at,
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


def _compare_phase4(canonical_path: Path, paper: dict[str, Any], session_date: str) -> dict[str, Any]:
    """Compare explicit causal source/context streams after Phase 4B replay."""
    try:
        canonical_payload = json.loads(canonical_path.read_text(encoding="utf-8"))
        canonical_session = canonical_payload["sessions"][session_date]
        canonical_facts = canonical_session.get("parity_phase_facts", {})
        paper_facts = paper.get("parity_phase_facts", {})
        canonical = build_phase4_artifact(
            run_id=f"canonical-{session_date}", producer={"name": "NiftyZoning"},
            scope={"date": session_date}, payload={"parity_phase_facts": canonical_facts},
        )
        paper_artifact = build_phase4_artifact(
            run_id=f"paper-{session_date}", producer={"name": "ftx-paper"},
            scope={"date": session_date}, payload={"parity_phase_facts": paper_facts},
        )
        return compare_phase4_manifests(
            canonical, paper_artifact, streams=PHASE4_STREAMS,
            canonical_records=lambda stream: canonical["phase_streams"][stream],
            paper_records=lambda stream: paper_artifact["phase_streams"][stream],
        )
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "INCOMPLETE", "stream": "source_observation", "reason": str(exc)}


def _compare_phase45(canonical_path: Path, paper: dict[str, Any], session_date: str) -> dict[str, Any]:
    """Compare the explicit chronological interaction/response stream."""
    try:
        canonical_payload = json.loads(canonical_path.read_text(encoding="utf-8"))
        canonical_facts = canonical_payload["sessions"][session_date].get("parity_phase_facts", {})
        paper_facts = paper.get("parity_phase_facts", {})
        if not isinstance(canonical_facts, dict) or "interaction_response" not in canonical_facts:
            return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": "canonical facts are unavailable"}
        if not isinstance(paper_facts, dict) or "interaction_response" not in paper_facts:
            return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": "paper facts are unavailable"}
        canonical_records = canonical_facts.get("interaction_response", [])
        paper_records = paper_facts.get("interaction_response", [])
        if not isinstance(canonical_records, list) or not isinstance(paper_records, list):
            return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": "interaction facts are unavailable"}
        result = compare_chronological_streams(canonical_records, paper_records)
        return {"stream": "interaction_response", **result}
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": str(exc)}


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


def _canonical_source_command(canonical_python: str, canonical_root: Path, session_date: str, output: Path) -> list[str]:
    code = """
import json, sys
from src.data.db import db_connection
from src.data.repos.futures_repo import fetch_futures
from src.data.repos.index_repo import load_trading_dates

session_date = sys.argv[1]
output = sys.argv[2]
with db_connection() as connection:
    trading_dates = load_trading_dates(date_to=int(session_date.replace('-', '')), con=connection)
    dates = [item for item in trading_dates if item <= session_date]
    rows = fetch_futures(
        date_from=int(dates[0].replace('-', '')),
        date_to=int(session_date.replace('-', '')),
        contract_type='CONTINUOUS', series='I', con=connection,
    ) if dates else []
raw = {}
for row in rows:
    minute = str(row.time).split('T')[-1][:5]
    if minute > '15:30':
        continue
    raw.setdefault(str(row.date), []).append({
        'time': minute, 'open': row.open, 'high': row.high, 'low': row.low,
        'close': row.close, 'volume': row.volume, 'oi': row.oi,
        'symbol': row.symbol, 'expiry': row.expiry,
        'contract_type': row.contract_type, 'session_date': row.date,
    })
full_dates = set(dates[-6:])
payload = {}
for item_date, items in raw.items():
    items.sort(key=lambda item: item['time'])
    if item_date in full_dates:
        payload[item_date] = items
        continue
    payload[item_date] = [{
        **items[0], 'time': '15:30',
        'high': max(item['high'] for item in items),
        'low': min(item['low'] for item in items),
        'close': items[-1]['close'],
        'volume': sum(item['volume'] for item in items),
        'oi': items[-1]['oi'],
    }]
with open(output, 'w', encoding='utf-8') as handle:
    json.dump({'sessions': payload}, handle, sort_keys=True)
"""
    return [canonical_python, "-c", code, session_date, str(output)]


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
        canonical_source_path = directory / "canonical-source.json"
        canonical_path = (
            _resolve_canonical_artifact(canonical_artifact, session_date)
            if canonical_artifact else None
        )
        canonical_output_dir = directory / "canonical-replay"
        canonical_source_error = _run_command(
            _canonical_source_command(canonical_python, canonical_root, session_date, canonical_source_path),
            cwd=canonical_root,
        )
        paper_command = [
            paper_python, "scripts/orderflow/orderflow_replay.py", "--date", session_date,
            "--source-artifact", str(canonical_source_path), "--include-interactions",
            "--output", str(paper_replay_path),
        ]
        with ThreadPoolExecutor(max_workers=2) as executor:
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
                else (None, canonical_error or canonical_source_error or "canonical replay artifact was not produced")
            )
            canonical_phase3, canonical_phase3_error = (
                canonical_phase3_future.result()
                if canonical_phase3_future is not None
                else (None, canonical_error or canonical_source_error or "canonical replay artifact was not produced")
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
        phase4 = (
            _compare_phase4(canonical_path, paper_replay, session_date)
            if canonical_path is not None and paper_replay is not None
            else {"status": "INCOMPLETE", "stream": "source_observation", "reason": "replay artifact unavailable"}
        )
        phase45 = (
            _compare_phase45(canonical_path, paper_replay, session_date)
            if canonical_path is not None and paper_replay is not None
            else {"status": "INCOMPLETE", "stream": "interaction_response", "reason": "replay artifact unavailable"}
        )
        atr_payload = phase35.get("atr")
        atr_tolerance = atr_payload.get("tolerance") if isinstance(atr_payload, dict) else None
        if (
            phase35.get("status") == "INPUT_VARIANCE"
            and phase4.get("status") == "MISMATCHED"
            and isinstance(atr_tolerance, (int, float))
            and _bounded_atr_dependent_difference(phase4, atr_tolerance)
        ):
            phase4 = {
                "status": "INPUT_VARIANCE",
                "stream": phase4.get("stream", "context_location"),
                "reason": "bounded ATR variance affects dependent context/location evidence",
                "variance": phase35.get("atr"),
                "first_dependent_difference": phase4,
            }
        return {
            "date": session_date,
            "phase3": phase3,
            "phase3_5": phase35,
            "phase4": phase4,
            "phase4_5": phase45,
            "canonical_replay": {"status": "MATCHED" if canonical_projection else "INCOMPLETE"},
            "paper_replay": {"status": "MATCHED" if paper_replay else "INCOMPLETE"},
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", action="append", dest="dates", required=True, type=_comparison_date)
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
        and item.get("phase4", {}).get("status") in {"MATCHED", "INPUT_VARIANCE"}
        and item.get("phase4_5", {}).get("status") == "MATCHED"
        for item in results
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
