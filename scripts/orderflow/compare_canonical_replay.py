"""Compare the Paper evidence replay with NiftyZoning for one session.

The two repositories are executed in isolated subprocesses.  Phase 3
observations are rebuilt from replay-engine bars before Phase 3.5 evidence
projections are compared.  No NiftyZoning package is imported into the Paper
process.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import uuid4

from ftx_paper.strategies.orderflow.parity_artifacts import (
    build_phase4_artifact,
    build_phase7_artifact,
    build_parity_artifact_from_payload,
    compare_parity_artifacts,
    compare_phase4_manifests,
    compare_phase7_manifests,
)
from ftx_paper.strategies.orderflow.phase7_projection import PHASE7_STREAMS, build_phase7_projection
from ftx_paper.strategies.orderflow.timestamps import parse_session_timestamp, parse_utc, serialize_utc

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
PHASE6_STREAMS = ("opportunity_lifecycle",)
PHASE6_EVENT_KINDS = frozenset({
    "lifecycle_formed", "setup_invalidated",
    "setup_expired", "setup_superseded", "opportunity_timeout",
})
PHASE6_PROVENANCE_FIELDS = (
    "candidate_id", "lifecycle_id", "location", "direction", "template_id",
    "response_episode_id", "context_candle_reference", "response_candle_reference",
    "response_observed_at", "response_available_at", "formation_at",
    "formation_available_at", "stage",
)
MIN_COMPARISON_DATE = date(2026, 9, 21)
_CAUSAL_TIMESTAMP_KEYS = frozenset({
    "as_of", "available_at", "current_available_at", "end_time", "formation_at",
    "formation_available_at", "observed_at", "occurred_at", "response_available_at",
    "response_observed_at", "causal_available_at", "causal_observed_at",
    "context_candle_available_at", "time",
    "filled_at", "reserved_at", "released_at", "decided_at", "effective_at",
    "triggered_at",
})
_TRANSPORT_METADATA_KEYS = frozenset({
    "identity", "neutral_event_key", "event_id", "sequence", "stream_sequence",
    "_publication_available_at",
    "causal_parent_key",
})
_CAUSAL_METADATA_KEYS = frozenset({
    "causal_available_at", "causal_observed_at", "causal_parent_id", "parent_id",
})
_ENVELOPE_DUPLICATE_KEYS = frozenset({
    "kind", "occurred_at", "observed_at", "available_at", "unavailable",
})
_PRODUCER_DERIVED_KEYS = frozenset({"parity_stream", "source_fingerprint"})
_PHASE45_NON_SEMANTIC_KEYS = frozenset({
    "band_low", "band_high", "source_aggregate_id", "terminal_candle_id", "terminal_close_side",
})


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


def _write_comparison_output(
    results: list[dict[str, Any]], output_dir: Path, *, status: str | None = None,
    run_id: str | None = None,
) -> Path:
    generated_at = datetime.now(timezone.utc)
    run_id = run_id or f"{generated_at:%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"canonical-replay-comparison-{run_id}.json"
    payload = {
        "run_id": run_id,
        "generated_at": generated_at.isoformat(),
        "status": status or "FAILED",
        "results": results,
    }
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _retain_replay_artifacts(
    artifact_dir: Path | None,
    session_date: str,
    run_id: str,
    canonical_path: Path | None,
    paper_path: Path,
    canonical_source_path: Path,
) -> dict[str, dict[str, Any]]:
    """Copy subprocess replay artifacts out of the temporary work directory."""
    if artifact_dir is None:
        return {
            "canonical_source": {"status": "NOT_RETAINED", "path": None},
            "canonical": {"status": "NOT_RETAINED", "path": None},
            "paper": {"status": "NOT_RETAINED", "path": None},
        }
    destination = artifact_dir / session_date / run_id
    result: dict[str, dict[str, Any]] = {}
    for name, source in (
        ("canonical_source", canonical_source_path),
        ("canonical", canonical_path),
        ("paper", paper_path),
    ):
        if source is None or not source.is_file():
            result[name] = {"status": "UNAVAILABLE", "path": None}
            continue
        target = destination / f"{session_date}-{name}.json"
        try:
            destination.mkdir(parents=True, exist_ok=True)
            if source.resolve() != target.resolve():
                shutil.copy2(source, target)
            result[name] = {"status": "RETAINED", "path": str(target)}
        except OSError as exc:
            result[name] = {"status": "RETENTION_FAILED", "path": None, "reason": str(exc)}
    return result


def _phase_accepted(phase: Mapping[str, Any], *, allow_variance: bool = False) -> bool:
    accepted = {"MATCHED", "INPUT_VARIANCE"} if allow_variance else {"MATCHED"}
    return phase.get("status") in accepted


def _gate_result(result: Mapping[str, Any]) -> dict[str, Any]:
    phases = ("phase3", "phase3_5", "phase4", "phase4_5", "phase6", "phase7", "parity")
    accepted = {
        "phase3": _phase_accepted(result.get("phase3", {})),
        "phase3_5": _phase_accepted(result.get("phase3_5", {}), allow_variance=True),
        "phase4": _phase_accepted(result.get("phase4", {}), allow_variance=True),
        "phase4_5": _phase_accepted(result.get("phase4_5", {})),
        "phase6": _phase_accepted(result.get("phase6", {}), allow_variance=True),
        "phase7": _phase_accepted(result.get("phase7", {}), allow_variance=True),
        "parity": _phase_accepted(result.get("parity", {})),
    }
    failed = [name for name in phases if not accepted[name]]
    first_failure = next((index for index, name in enumerate(phases) if not accepted[name]), None)
    blocked = phases[first_failure + 1:] if first_failure is not None else ()
    return {
        "status": "PASSED" if not failed else "FAILED",
        "accepted_phases": [name for name in phases if accepted[name]],
        "blocked_phases": list(blocked),
        "failed_phases": failed,
        "reasons": [result.get(name, {}).get("reason") for name in failed if result.get(name, {}).get("reason")],
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


def _canonical_timestamp(value: Any, session_date: str) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if len(text) == 5 and text[2] == ":":
        text = f"{session_date}T{text}"
    if len(text) >= 10 and text[4] == "-" and text[7] == "-" and text[10] in {"T", " "}:
        try:
            return serialize_utc(parse_session_timestamp(session_date, text))
        except ValueError:
            return value
    return value


def _normalize_canonical_timestamps(value: Any, session_date: str, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            item_key: _normalize_canonical_timestamps(
                item_value, session_date, str(item_key) if str(item_key) in _CAUSAL_TIMESTAMP_KEYS else None,
            )
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [_normalize_canonical_timestamps(item, session_date, key) for item in value]
    return _canonical_timestamp(value, session_date) if key is not None else value


def _normalize_observation_payload(payload: dict[str, Any], session_date: str) -> dict[str, Any]:
    result = dict(payload)
    aggregates = {}
    for timeframe, items in payload.get("aggregates", {}).items():
        normalized_items = []
        for item in items:
            normalized = _normalize_canonical_timestamps(item, session_date)
            normalized.pop("time_label", None)
            normalized.pop("end_time_label", None)
            normalized_items.append(normalized)
        aggregates[timeframe] = normalized_items
    result["aggregates"] = aggregates
    return result


def _normalize_phase_record_for_compare(record: dict[str, Any], session_date: str) -> dict[str, Any]:
    normalized = _normalize_canonical_timestamps(record, session_date)

    candle_reference_keys = {
        "context_candle_reference", "response_candle_reference",
        "context_candle_ref", "response_candle_ref",
    }

    def normalize_candle_references(value: Any) -> Any:
        if isinstance(value, Mapping):
            result = {}
            for item_key, item_value in value.items():
                key = str(item_key)
                if key in candle_reference_keys and isinstance(item_value, Mapping):
                    result[key] = {
                        str(ref_key): (
                            _canonical_timestamp(ref_value, session_date)
                            if str(ref_key) in {"start_at", "end_at"}
                            else ref_value
                        )
                        for ref_key, ref_value in item_value.items()
                    }
                else:
                    result[key] = normalize_candle_references(item_value)
            return result
        if isinstance(value, list):
            return [normalize_candle_references(item) for item in value]
        return value

    normalized = normalize_candle_references(normalized)

    def remove_labels(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: remove_labels(item)
                for key, item in value.items()
                if key not in {"time_label", "end_time_label"}
            }
        if isinstance(value, list):
            return [remove_labels(item) for item in value]
        return value

    return remove_labels(normalized)


def _phase45_semantic_projection(record: Mapping[str, Any], session_date: str) -> dict[str, Any]:
    """Project producer envelopes onto stable interaction semantics.

    Canonical and Paper use different generated identities and parent hashes.
    Those values are useful diagnostics, but are not part of the cross-producer
    contract.  Causal metadata is folded into ``values`` so a producer that
    promotes it to the envelope compares the same as one that nests it.
    """
    normalized = _normalize_phase_record_for_compare(dict(record), session_date)

    def normalize_causal_timestamps(value: Any, key: str | None = None) -> Any:
        if isinstance(value, Mapping):
            return {
                str(item_key): normalize_causal_timestamps(
                    item_value, str(item_key) if str(item_key) in _CAUSAL_TIMESTAMP_KEYS else None,
                )
                for item_key, item_value in value.items()
            }
        if isinstance(value, list):
            return [normalize_causal_timestamps(item, key) for item in value]
        if key in _CAUSAL_TIMESTAMP_KEYS:
            return serialize_utc(parse_utc(value))
        return value

    normalized = normalize_causal_timestamps(normalized)
    raw_values = normalized.get("values")
    values: dict[str, Any] = dict(raw_values) if isinstance(raw_values, Mapping) else {}
    for key, value in normalized.items():
        if key in {"kind", "occurred_at", "observed_at", "available_at", "unavailable", "values"}:
            continue
        if key in _TRANSPORT_METADATA_KEYS or key in _CAUSAL_METADATA_KEYS:
            if key in _CAUSAL_METADATA_KEYS and key not in values:
                values[key] = value
            continue
        if key not in values:
            values[key] = value

    # Parent hashes are producer-local. Resolve the relationship through the
    # stable source/interaction identity carried by the semantic payload.
    parent_ref = next(
        (
            {"kind": name, "id": values[name]}
            for name in ("interaction_id", "source_observation_id", "source_aggregate_id")
            if values.get(name) is not None
        ),
        None,
    )
    values.pop("causal_parent_id", None)
    values.pop("parent_id", None)
    values.pop("causal_parent_key", None)
    if parent_ref is not None:
        values["causal_parent_ref"] = parent_ref

    def strip_transport(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                str(key): strip_transport(item)
                for key, item in value.items()
                if key not in _TRANSPORT_METADATA_KEYS
                and key not in _ENVELOPE_DUPLICATE_KEYS
                and key not in _PRODUCER_DERIVED_KEYS
                and key not in _PHASE45_NON_SEMANTIC_KEYS
                and key not in {"causal_parent_key", "causal_parent_id", "parent_id"}
            }
        if isinstance(value, list):
            return [strip_transport(item) for item in value]
        return value

    return {
        "kind": normalized.get("kind"),
        "occurred_at": normalized.get("occurred_at"),
        "observed_at": normalized.get("observed_at"),
        "available_at": normalized.get("available_at"),
        "values": strip_transport(values),
        "unavailable": strip_transport(normalized.get("unavailable", [])),
    }


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
    # Snapshot identity and fingerprint belong to the producer's profile
    # store.  Phase 3.5 parity compares the published value geometry and
    # causal availability, not repository-specific source provenance.
    ignored = {"fingerprint", "reference_id", "poc", "vah", "val"}
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
        canonical_facts = _normalize_canonical_timestamps(
            canonical_session.get("parity_phase_facts", {}), session_date,
        )
        paper_facts = paper.get("parity_phase_facts", {})
        canonical_facts = {
            stream: [_normalize_phase_record_for_compare(item, session_date) for item in records]
            for stream, records in canonical_facts.items()
        }
        paper_facts = {
            stream: [_normalize_phase_record_for_compare(item, session_date) for item in records]
            for stream, records in paper_facts.items()
        }
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
        canonical_facts = _normalize_canonical_timestamps(
            canonical_payload["sessions"][session_date].get("parity_phase_facts", {}), session_date,
        )
        paper_facts = paper.get("parity_phase_facts", {})
        if not isinstance(canonical_facts, dict) or "interaction_response" not in canonical_facts:
            return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": "canonical facts are unavailable"}
        if not isinstance(paper_facts, dict) or "interaction_response" not in paper_facts:
            return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": "paper facts are unavailable"}
        canonical_records = canonical_facts.get("interaction_response", [])
        paper_records = paper_facts.get("interaction_response", [])
        if not isinstance(canonical_records, list) or not isinstance(paper_records, list):
            return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": "interaction facts are unavailable"}
        canonical_semantics = [
            _phase45_semantic_projection(item, session_date)
            for item in canonical_records
        ]
        paper_semantics = [
            _phase45_semantic_projection(item, session_date)
            for item in paper_records
        ]
        result: dict[str, Any]
        for index, (canonical_value, paper_value) in enumerate(zip(canonical_semantics, paper_semantics)):
            if canonical_value != paper_value:
                difference = _first_difference(canonical_value, paper_value)
                assert difference is not None
                result = {
                    "status": "MISMATCHED", "index": index, "field": difference["path"],
                    "canonical_value": difference["canonical"], "paper_value": difference["paper"],
                    "canonical_record": canonical_value, "paper_record": paper_value,
                }
                break
        else:
            if len(canonical_semantics) != len(paper_semantics):
                result = {
                    "status": "MISMATCHED", "index": min(len(canonical_semantics), len(paper_semantics)),
                    "reason": "count", "canonical_count": len(canonical_semantics),
                    "paper_count": len(paper_semantics),
                }
            else:
                result = {"status": "MATCHED", "count": len(canonical_semantics)}
                diagnostics = [
                    {
                        "index": index,
                        "canonical_identity": canonical.get("identity"),
                        "paper_identity": paper.get("identity"),
                        "canonical_parent": canonical.get("causal_parent_id") or canonical.get("causal_parent_key"),
                        "paper_parent": paper.get("causal_parent_id") or paper.get("causal_parent_key"),
                    }
                    for index, (canonical, paper) in enumerate(zip(canonical_records, paper_records))
                    if canonical != paper
                ]
                if diagnostics:
                    result["ignored_producer_metadata"] = diagnostics
        return {"stream": "interaction_response", **result}
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        reason = str(exc)
        if "timezone-aware" in reason and reason != "parity timestamps must be timezone-aware":
            reason = "parity timestamps must be timezone-aware"
        return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": reason}


def _canonical_phase6_records(session: Mapping[str, Any], session_date: str) -> list[dict[str, Any]] | None:
    facts = session.get("parity_phase_facts")
    lifecycle_facts = facts.get("opportunity_lifecycle") if isinstance(facts, Mapping) else None
    if isinstance(lifecycle_facts, list) and lifecycle_facts:
        projected = [dict(item) for item in lifecycle_facts if isinstance(item, Mapping)]
        raw_records = session.get("records")
        if not isinstance(raw_records, list):
            raw_records = session.get("replay_records")
        if not isinstance(raw_records, list):
            report = session.get("report_evidence")
            raw_records = report.get("records") if isinstance(report, Mapping) else None
        if isinstance(raw_records, list):
            opening_records = [
                item for item in raw_records
                if isinstance(item, Mapping)
                and item.get("kind") == "lifecycle_formed"
                and str(
                    item.get("location")
                    or (item.get("values") or {}).get("location", "")
                ).startswith("OR_")
            ]
            opening_projection = _canonical_phase6_records(
                {"records": opening_records, "diagnostic": session.get("diagnostic")},
                session_date,
            )
            existing = {
                str(
                    item.get("values", {}).get("lifecycle_id")
                    if isinstance(item.get("values"), Mapping)
                    else item.get("lifecycle_id")
                )
                for item in projected
                if item.get("identity") is not None
            }
            projected.extend(
                item for item in opening_projection or ()
                if str(
                    item.get("values", {}).get("lifecycle_id")
                    if isinstance(item.get("values"), Mapping)
                    else item.get("lifecycle_id")
                ) not in existing
            )
        return projected

    raw_records = session.get("records")
    if not isinstance(raw_records, list):
        report = session.get("report_evidence")
        raw_records = report.get("records") if isinstance(report, Mapping) else None
    if not isinstance(raw_records, list):
        return None
    if not raw_records:
        return None

    diagnostic = session.get("diagnostic")
    setups = diagnostic.get("setup_opportunities", ()) if isinstance(diagnostic, Mapping) else ()
    setup_identity = {
        str(item.get("lifecycle_id")): str(item.get("causal_identity"))
        for item in setups if isinstance(item, Mapping)
        and item.get("lifecycle_id") and item.get("causal_identity")
    }
    setup_by_signature = {
        (str(item.get("location")), str(item.get("template_id")), str(item.get("origin_evidence_id"))): str(item.get("causal_identity"))
        for item in setups if isinstance(item, Mapping) and item.get("causal_identity")
    }
    setup_by_location_evidence = {
        (str(item.get("location")), str(item.get("origin_evidence_id"))): str(item.get("causal_identity"))
        for item in setups if isinstance(item, Mapping) and item.get("causal_identity")
    }
    lifecycle: list[dict[str, Any]] = []
    for raw in raw_records:
        if not isinstance(raw, Mapping) or raw.get("kind") not in PHASE6_EVENT_KINDS:
            continue
        event = _normalize_phase_record_for_compare(dict(raw), session_date)
        evidence_ids = event.get("evidence_ids") or ()
        identity = (
            event.get("causal_identity")
            or setup_identity.get(str(event.get("lifecycle_id")))
            or setup_by_signature.get((str(event.get("location")), str(event.get("template_id")), str(evidence_ids[-1] if evidence_ids else "")))
            or setup_by_location_evidence.get((str(event.get("location")), str(event.get("interaction_id"))))
            or event.get("lifecycle_id") or event.get("interaction_id")
            or event.get("response_episode_id") or event.get("execution_group_id")
            or event.get("event_id")
        )
        observed = event.get("observed_at") or event.get("formation_at")
        available = event.get("available_at") or event.get("formation_available_at")
        if not isinstance(identity, str) or not isinstance(observed, str) or not isinstance(available, str):
            raise ValueError("canonical lifecycle event is missing causal timestamps or identity")
        parent = event.get("causal_parent_id")
        if parent is None:
            context = event.get("parent_context")
            parent = context.get("context_id") if isinstance(context, Mapping) else event.get("interaction_id")
        lifecycle.append({
            "kind": str(event["kind"]), "occurred_at": observed,
            "observed_at": observed, "available_at": available,
            "identity": identity, "causal_parent_id": parent,
            "values": event, "unavailable": [],
        })
    return lifecycle


def _phase6_provenance(record: Mapping[str, Any]) -> dict[str, Any]:
    values = record.get("values")
    values = values if isinstance(values, Mapping) else {}
    return {
        field: values.get(field)
        for field in PHASE6_PROVENANCE_FIELDS
        if values.get(field) is not None
    }


def _phase6_structural_difference(
    canonical: Sequence[Mapping[str, Any]], paper: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    if len(canonical) != len(paper):
        index = min(len(canonical), len(paper))
        return {
            "reason": "count",
            "index": index,
            "field": "count",
            "canonical_count": len(canonical),
            "paper_count": len(paper),
            "canonical_record": canonical[index] if index < len(canonical) else None,
            "paper_record": paper[index] if index < len(paper) else None,
        }
    for index, (left, right) in enumerate(zip(canonical, paper)):
        left_provenance = _phase6_provenance(left)
        right_provenance = _phase6_provenance(right)
        difference = _first_difference(left_provenance, right_provenance, "provenance")
        if difference is not None:
            return {
                "index": index,
                "field": difference["path"],
                "canonical_value": difference["canonical"],
                "paper_value": difference["paper"],
                "canonical_record": left_provenance,
                "paper_record": right_provenance,
            }
    return None


def _phase6_lifecycle_id(record: Mapping[str, Any] | None) -> str | None:
    if not isinstance(record, Mapping):
        return None
    values = record.get("values")
    if isinstance(values, Mapping):
        value = values.get("lifecycle_id") or values.get("opportunity_id")
        if value is not None:
            return str(value)
    for key in ("lifecycle_id", "opportunity_id", "causal_parent_id"):
        value = record.get(key)
        if value is not None and str(value).startswith("lifecycle-"):
            return str(value)
    return None


def _phase6_causal_diagnostics(
    payload: Mapping[str, Any], lifecycle_ids: Sequence[str], *, canonical: bool,
) -> dict[str, Any]:
    """Summarize candidate-specific diagnostics without copying full traces."""
    diagnostic = payload.get("diagnostic")
    source: Any = (
        diagnostic.get("execution_diagnostics", [])
        if canonical and isinstance(diagnostic, Mapping)
        else payload.get("execution_diagnostics", [])
    )
    if not isinstance(source, list):
        replay = payload.get("replay")
        source = replay.get("execution_diagnostics", []) if isinstance(replay, Mapping) else []
    wanted = set(lifecycle_ids)
    records = [
        dict(item) for item in source
        if isinstance(item, Mapping) and str(item.get("lifecycle_id")) in wanted
    ]
    by_kind: dict[str, int] = {}
    for item in records:
        kind = str(item.get("kind", ""))
        by_kind[kind] = by_kind.get(kind, 0) + 1
    return {
        "lifecycle_ids": sorted(wanted),
        "record_count": len(records),
        "by_kind": dict(sorted(by_kind.items())),
        "first_records": records[:10],
        "last_records": records[-10:] if len(records) > 10 else [],
    }


def _record_contains(value: Any, needle: str) -> bool:
    if isinstance(value, Mapping):
        return any(_record_contains(item, needle) for item in value.values())
    if isinstance(value, list):
        return any(_record_contains(item, needle) for item in value)
    return value == needle


def _decision_clock_diagnostics(
    replay: Mapping[str, Any], lifecycle_id: str,
) -> list[dict[str, Any]]:
    records = replay.get("replay_records")
    if not isinstance(records, list):
        return []
    diagnostics: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping) or not _record_contains(record, lifecycle_id):
            continue
        clock = record.get("decision_clock")
        if isinstance(clock, Mapping):
            available_1m = clock.get("available_1m")
            available_5m = clock.get("available_5m")
            diagnostics.append({
                "sequence": record.get("sequence"),
                "timestamp": record.get("timestamp"),
                "available_at": record.get("available_at"),
                "decision_at": clock.get("decision_at"),
                "next_decision_at": clock.get("next_decision_at"),
                "new_5m": clock.get("new_5m"),
                "source_1m_time": clock.get("source_1m_time"),
                "available_1m": _bounded_diagnostic_values(available_1m),
                "available_5m": _bounded_diagnostic_values(available_5m),
            })
    if len(diagnostics) <= 8:
        return diagnostics
    return [
        *diagnostics[:4],
        {"omitted": len(diagnostics) - 8},
        *diagnostics[-4:],
    ]


def _execution_diagnostics(
    payload: Mapping[str, Any], lifecycle_id: str, *, canonical: bool,
    session_date: str,
) -> list[dict[str, Any]]:
    diagnostic = payload.get("diagnostic")
    source: Any = (
        diagnostic.get("execution_diagnostics", [])
        if canonical and isinstance(diagnostic, Mapping)
        else payload.get("execution_diagnostics", [])
    )
    if not isinstance(source, list):
        return []
    identity_keys = {"lifecycle_id", "opportunity_id", "causal_parent_id"}
    records = [
        item for item in source
        if isinstance(item, Mapping)
        and any(item.get(key) == lifecycle_id for key in identity_keys)
    ]
    keep = {
        "sequence", "kind", "lifecycle_id", "observed_at", "available_at",
        "location_interaction_id", "entry_at", "active_positions", "gate_result",
        "blocking_reason", "gate_evidence", "vwap_source", "synthetic_snapshots",
        "synthetic_snapshot_count", "continuation", "completed_1m_count",
        "completed_5m_count", "reason", "evidence_time", "evidence_available_at",
        "bar", "before", "after", "fill_price", "quantity", "remaining_quantity",
        "stop", "stop_trigger", "risk_reward", "components", "owner", "level",
    }
    result = [
        _normalize_execution_diagnostic(
            {str(key): value for key, value in item.items() if key in keep},
            session_date=session_date,
        )
        for item in records
    ]
    if len(result) <= 12:
        return result
    return [*result[:6], {"omitted": len(result) - 12}, *result[-6:]]


def _normalize_execution_diagnostic(value: Any, *, session_date: str | None) -> Any:
    """Normalize diagnostic timestamps recursively for cross-repository comparison."""
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_execution_diagnostic(item, session_date=session_date)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _normalize_execution_diagnostic(item, session_date=session_date)
            for item in value
        ]
    if isinstance(value, str) and session_date is not None:
        try:
            return _canonical_timestamp(value, session_date)
        except (TypeError, ValueError):
            return value
    return value


def _exit_diagnostics(
    payload: Mapping[str, Any], lifecycle_id: str, *, canonical: bool,
    session_date: str,
) -> list[dict[str, Any]]:
    """Return the causal exit evidence, including ticks with no exit action."""
    diagnostic = payload.get("diagnostic")
    source: Any = (
        diagnostic.get("execution_diagnostics", [])
        if canonical and isinstance(diagnostic, Mapping)
        else payload.get("execution_diagnostics", [])
    )
    if not isinstance(source, list):
        return []
    exit_kinds = {
        "exit_signal", "exit_order", "exit_trigger_observed", "exit_decision",
        "position_closed", "execution_lifecycle_tick", "failed_continuation_evaluated",
    }
    records = [
        item for item in source
        if isinstance(item, Mapping)
        and item.get("kind") in exit_kinds
        and any(item.get(key) == lifecycle_id for key in ("lifecycle_id", "opportunity_id", "causal_parent_id"))
    ]
    keep = {
        "sequence", "kind", "level", "reason", "owner", "lifecycle_id",
        "observed_at", "available_at", "evidence_time", "evidence_available_at",
        "bar", "before", "after", "fill_price", "quantity", "remaining_quantity",
        "stop", "stop_trigger", "risk_reward", "components", "vwap_source",
        "continuation", "position", "decision_context",
        "location_interaction_id", "entry_at", "completed_1m_count",
        "completed_5m_count", "synthetic_snapshot_count", "synthetic_snapshots",
    }
    return [
        _normalize_execution_diagnostic(
            {str(key): value for key, value in item.items() if key in keep},
            session_date=session_date,
        )
        for item in records
    ]


def _first_exit_diagnostic_difference(
    canonical: list[dict[str, Any]], paper: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Compare exit evidence in causal order, preserving missing-event diagnostics."""
    geometry_fields = {
        "failure_boundary", "protective_stop", "entry_boundary", "band_low", "band_high",
    }
    diagnostic_metadata = {"sequence"}

    def semantic(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                str(key): semantic(item)
                for key, item in value.items()
                if key not in geometry_fields and key not in diagnostic_metadata
            }
        if isinstance(value, list):
            return [semantic(item) for item in value]
        return value

    limit = max(len(canonical), len(paper))
    for index in range(limit):
        left = canonical[index] if index < len(canonical) else None
        right = paper[index] if index < len(paper) else None
        if semantic(left) != semantic(right):
            difference = _first_difference(semantic(left), semantic(right), "exit_diagnostic")
            return {
                "index": index,
                "canonical": left,
                "paper": right,
                "field": difference,
            }
    return None


def _execution_trace_diagnostics(
    payload: Mapping[str, Any], lifecycle_id: str, *, canonical: bool,
    session_date: str,
) -> list[dict[str, Any]]:
    diagnostic = payload.get("diagnostic")
    source: Any = (
        diagnostic.get("execution_diagnostics")
        if canonical and isinstance(diagnostic, Mapping)
        else payload.get("execution_diagnostics")
    )
    if not isinstance(source, list):
        producer = "canonical" if canonical else "paper"
        raise ValueError(f"{producer} execution diagnostics unavailable")

    def normalize(value: Any, key: str | None = None) -> Any:
        if isinstance(value, Mapping):
            return {str(k): normalize(v, str(k)) for k, v in value.items()}
        if isinstance(value, list):
            return [normalize(item, key) for item in value]
        if key in {"observed_at", "available_at", "time", "triggered_at"}:
            try:
                return _canonical_timestamp(value, session_date)
            except (TypeError, ValueError):
                return value
        return value

    return [
        normalize(item)
        for item in source
        if isinstance(item, Mapping)
        and item.get("kind") == "execution_lifecycle_tick"
        and item.get("lifecycle_id") == lifecycle_id
    ]


def _first_execution_trace_difference(
    canonical: list[dict[str, Any]], paper: list[dict[str, Any]],
) -> dict[str, Any] | None:
    def key(item: Mapping[str, Any]) -> tuple[Any, Any]:
        return item.get("observed_at"), item.get("available_at")

    def indexed(
        values: list[dict[str, Any]],
    ) -> dict[tuple[tuple[Any, Any], int], dict[str, Any]]:
        occurrences: dict[tuple[Any, Any], int] = {}
        result: dict[tuple[tuple[Any, Any], int], dict[str, Any]] = {}
        for item in values:
            clock = key(item)
            occurrence = occurrences.get(clock, 0)
            occurrences[clock] = occurrence + 1
            result[(clock, occurrence)] = item
        return result

    canonical_by_clock = indexed(canonical)
    paper_by_clock = indexed(paper)
    clocks = sorted(set(canonical_by_clock) | set(paper_by_clock), key=str)
    for index, (clock, occurrence) in enumerate(clocks):
        left = canonical_by_clock.get((clock, occurrence))
        right = paper_by_clock.get((clock, occurrence))
        if left != right:
            return {
                "index": index,
                "clock": {"observed_at": clock[0], "available_at": clock[1]},
                "occurrence": occurrence,
                "canonical": left,
                "paper": right,
            }
    return None


def _bounded_diagnostic_values(value: Any, limit: int = 12) -> list[Any]:
    if not isinstance(value, list):
        return []
    if len(value) <= limit:
        return list(value)
    head = limit // 2
    return [*value[:head], {"omitted": len(value) - limit}, *value[-(limit - head):]]


def _lifecycle_records(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    facts = payload.get("parity_phase_facts")
    records = facts.get("opportunity_lifecycle") if isinstance(facts, Mapping) else None
    return [item for item in records if isinstance(item, Mapping)] if isinstance(records, list) else []


def _lifecycle_id(record: Mapping[str, Any]) -> str | None:
    values = record.get("values")
    if isinstance(values, Mapping):
        value = values.get("lifecycle_id") or values.get("opportunity_id")
        if value is not None:
            return str(value)
    value = record.get("lifecycle_id") or record.get("opportunity_id")
    return str(value) if value is not None else None


def _causal_time(record: Mapping[str, Any]) -> str | None:
    value = record.get("observed_at") or record.get("occurred_at") or record.get("available_at")
    if isinstance(value, str):
        try:
            return serialize_utc(parse_utc(value))
        except ValueError:
            return serialize_utc(parse_session_timestamp(value[:10], value))
    values = record.get("values")
    if isinstance(values, Mapping):
        value = values.get("observed_at") or values.get("available_at") or values.get("time")
        if isinstance(value, str):
            try:
                return serialize_utc(parse_utc(value))
            except ValueError:
                return serialize_utc(parse_session_timestamp(value[:10], value))
    return None


def _timeout_before_canonical_trigger(
    canonical_session: Mapping[str, Any], paper: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Reject Paper expiry that preempts Canonical's first eligible 1m trigger."""
    paper_expiries: dict[str, Mapping[str, Any]] = {}
    for record in _lifecycle_records(paper):
        lifecycle_id = _lifecycle_id(record)
        if lifecycle_id is not None and record.get("kind") in {"setup_expired", "opportunity_timeout"}:
            previous = paper_expiries.get(lifecycle_id)
            current_time = _causal_time(record)
            previous_time = _causal_time(previous) if previous is not None else None
            if previous is None or (
                current_time is not None
                and (previous_time is None or parse_utc(current_time) < parse_utc(previous_time))
            ):
                paper_expiries[lifecycle_id] = record
    if not paper_expiries:
        return None
    canonical_lifecycles: dict[str, Mapping[str, Any]] = {}
    for record in _lifecycle_records(canonical_session):
        lifecycle_id = _lifecycle_id(record)
        if lifecycle_id is not None:
            canonical_lifecycles[lifecycle_id] = record
    for lifecycle_id, expiry in paper_expiries.items():
        if lifecycle_id not in canonical_lifecycles:
            continue
        diagnostic = canonical_session.get("diagnostic", {})
        diagnostics = diagnostic.get("execution_diagnostics", []) if isinstance(diagnostic, Mapping) else []
        triggers = [
            item for item in diagnostics
            if isinstance(item, Mapping)
            and (
                item.get("lifecycle_id") == lifecycle_id
                or item.get("opportunity_id") == lifecycle_id
                or item.get("causal_parent_id") == lifecycle_id
            )
            if item.get("kind") == "trigger_observation"
            and item.get("level") == "1m"
            and item.get("eligible") is True
        ]
        if not triggers:
            continue
        trigger_times = [
            value for item in triggers
            if (value := _causal_time(item)) is not None
        ]
        trigger_time = min(trigger_times) if trigger_times else None
        expiry_time = _causal_time(expiry)
        if trigger_time is None or expiry_time is None:
            continue
        if parse_utc(expiry_time) <= parse_utc(trigger_time):
            return {
                "status": "MISMATCHED",
                "stream": "trigger_exit",
                "reason": "paper_timeout_precedes_first_eligible_canonical_trigger",
                "lifecycle_id": lifecycle_id,
                "paper_expiry": expiry_time,
                "canonical_trigger": serialize_utc(parse_utc(trigger_time)),
            }
    return None


def _compare_phase6(canonical_path: Path, paper: dict[str, Any], session_date: str) -> dict[str, Any]:
    """Compare explicit opportunity/setup lifecycle facts."""
    try:
        payload = json.loads(canonical_path.read_text(encoding="utf-8"))
        session = payload["sessions"][session_date]
        canonical_records = _canonical_phase6_records(session, session_date)
        if canonical_records is None:
            return {
                "status": "INCOMPLETE",
                "stream": "opportunity_lifecycle",
                "reason": "canonical lifecycle records are unavailable",
            }
        paper_facts = paper.get("parity_phase_facts", {})
        paper_records = paper_facts.get("opportunity_lifecycle") if isinstance(paper_facts, Mapping) else None
        if not isinstance(paper_records, list):
            return {"status": "INCOMPLETE", "stream": "opportunity_lifecycle", "reason": "paper facts are unavailable"}
        def lifecycle_order(item: Mapping[str, Any]) -> tuple[str, int, str, str]:
            values = item.get("values")
            candidate_id = values.get("candidate_id") if isinstance(values, Mapping) else None
            suffix = str(candidate_id).rsplit("-candidate-", 1)[-1] if candidate_id else ""
            candidate_number = int(suffix) if suffix.isdigit() else 2**31 - 1
            return (
                str(item.get("available_at")), candidate_number,
                str(item.get("identity")), str(item.get("kind")),
            )

        canonical_records.sort(key=lifecycle_order)
        paper_records = [
            dict(item) for item in paper_records
            if isinstance(item, Mapping) and item.get("kind") in PHASE6_EVENT_KINDS
        ]
        paper_records.sort(key=lifecycle_order)
        def semantic_lifecycle(record: dict[str, Any]) -> dict[str, Any]:
            projected = _phase45_semantic_projection(record, session_date)
            ignored = {
                "available_at", "observed_at", "occurred_at", "kind",
                "causal_available_at", "causal_observed_at", "causal_ids",
                "causal_parent_key", "causal_identity", "event_id", "lifecycle_sequence", "sequence",
                "neutral_event_key", "provenance", "confirmation_evidence",
                "long_score", "short_score", "stage_before", "stage_after", "events",
            }
            values = projected.get("values")
            if isinstance(values, Mapping):
                projected["values"] = {
                    key: value for key, value in values.items() if key not in ignored
                }
            projected["identity"] = record.get("identity")
            projected["causal_parent_id"] = record.get("causal_parent_id")
            return projected

        canonical_records = [semantic_lifecycle(item) for item in canonical_records]
        paper_records = [semantic_lifecycle(item) for item in paper_records]
        structural_difference = _phase6_structural_difference(canonical_records, paper_records)
        if structural_difference is not None:
            boundary_records = (
                structural_difference.get("canonical_record"),
                structural_difference.get("paper_record"),
            )
            lifecycle_ids = [
                lifecycle_id
                for lifecycle_id in (
                    _phase6_lifecycle_id(record) for record in boundary_records
                )
                if lifecycle_id is not None
            ]
            return {
                "status": "MISMATCHED",
                "stream": "opportunity_lifecycle",
                "reason": "structural_lifecycle_divergence",
                "first_difference": structural_difference,
                "causal_diagnostics": {
                    "canonical": _phase6_causal_diagnostics(
                        session, lifecycle_ids, canonical=True,
                    ),
                    "paper": _phase6_causal_diagnostics(
                        paper, lifecycle_ids, canonical=False,
                    ),
                },
            }
        canonical = build_phase4_artifact(
            run_id=f"canonical-{session_date}", producer={"name": "NiftyZoning"},
            scope={"date": session_date}, payload={"parity_phase_facts": {"opportunity_lifecycle": canonical_records}},
        )
        paper_artifact = build_phase4_artifact(
            run_id=f"paper-{session_date}", producer={"name": "ftx-paper"},
            scope={"date": session_date}, payload={"parity_phase_facts": {"opportunity_lifecycle": paper_records}},
        )
        result = compare_phase4_manifests(
            canonical, paper_artifact, streams=PHASE6_STREAMS,
            canonical_records=lambda stream: canonical["phase_streams"][stream],
            paper_records=lambda stream: paper_artifact["phase_streams"][stream],
        )
        if (
            result.get("status") == "MISMATCHED"
            and str(result.get("field", "")).endswith(("failure_boundary", "entry_boundary"))
            and isinstance(result.get("canonical_value"), (int, float))
            and isinstance(result.get("paper_value"), (int, float))
            and abs(float(result["canonical_value"]) - float(result["paper_value"])) <= ATR_ABSOLUTE_TOLERANCE
        ):
            return {
                "status": "INPUT_VARIANCE",
                "stream": "opportunity_lifecycle",
                "reason": "bounded ATR variance affects lifecycle boundaries",
                "first_difference": result,
            }
        return result
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "INCOMPLETE", "stream": "opportunity_lifecycle", "reason": str(exc)}


def _compare_phase7(canonical_path: Path, paper: dict[str, Any], session_date: str) -> dict[str, Any]:
    """Compare explicit execution facts in causal stream order."""
    try:
        payload = json.loads(canonical_path.read_text(encoding="utf-8"))
        canonical_session = payload["sessions"][session_date]
        canonical_replay = canonical_session.get("replay")
        if not isinstance(canonical_replay, Mapping):
            return {"status": "INCOMPLETE", "stream": PHASE7_STREAMS[0], "reason": "canonical_replay_missing"}
        canonical_facts = canonical_replay.get("parity_phase_facts")
        # The replay payload also carries the earlier phase streams.  Only
        # Phase 7 facts belong in this projection; including those other keys
        # causes a lookup failure against the Phase 7-only stream mapping.
        explicit_keys = {
            key for key in PHASE7_STREAMS
            if isinstance(canonical_facts, Mapping) and key in canonical_facts
        }
        if not explicit_keys:
            return {"status": "INCOMPLETE", "stream": PHASE7_STREAMS[0], "reason": "canonical_phase7_facts_missing"}
        canonical_streams = build_phase7_projection(canonical_replay)
        paper_streams = build_phase7_projection(paper)
        paper_facts = paper.get("parity_phase_facts")
        paper_explicit_keys = {
            key for key in PHASE7_STREAMS
            if isinstance(paper_facts, Mapping) and key in paper_facts
        }
        def normalize(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [_normalize_phase_record_for_compare(item, session_date) for item in values]
        canonical_artifact = build_phase7_artifact(
            run_id=f"canonical-{session_date}", producer={"name": "NiftyZoning"},
            scope={"date": session_date},
            payload={"parity_phase_facts": {
                name: normalize(canonical_streams[name]) for name in explicit_keys
            }},
        )
        paper_artifact = build_phase7_artifact(
            run_id=f"paper-{session_date}", producer={"name": "ftx-paper"},
            scope={"date": session_date},
            payload={"parity_phase_facts": {
                name: normalize(paper_streams[name]) for name in paper_explicit_keys
            }},
        )
        result = compare_phase7_manifests(
            canonical_artifact, paper_artifact,
            canonical_records=lambda stream: canonical_artifact["phase_streams"][stream],
            paper_records=lambda stream: paper_artifact["phase_streams"][stream],
        )
        ordering = _timeout_before_canonical_trigger(canonical_session, paper)
        if ordering is not None and result.get("status") == "MATCHED":
            result = ordering
        elif ordering is not None:
            result["causal_ordering"] = ordering
        if result.get("status") == "MISMATCHED":
            records = (result.get("canonical_record"), result.get("paper_record"))
            lifecycle_id = next(
                (
                    str(
                        item.get("values", {}).get("lifecycle_id")
                        or item.get("values", {}).get("opportunity_id")
                    )
                    for item in records
                    if isinstance(item, Mapping)
                    and isinstance(item.get("values"), Mapping)
                    and (
                        item["values"].get("lifecycle_id") is not None
                        or item["values"].get("opportunity_id") is not None
                    )
                ),
                None,
            )
            if lifecycle_id is not None:
                result["lifecycle_id"] = lifecycle_id
                result["decision_clock"] = {
                    "canonical": _decision_clock_diagnostics(canonical_replay, lifecycle_id),
                    "paper": _decision_clock_diagnostics(paper, lifecycle_id),
                }
                result["execution_diagnostics"] = {
                    "canonical": _execution_diagnostics(
                        canonical_session, lifecycle_id, canonical=True,
                        session_date=session_date,
                    ),
                    "paper": _execution_diagnostics(
                        paper, lifecycle_id, canonical=False,
                        session_date=session_date,
                    ),
                }
                canonical_exit = _exit_diagnostics(
                    canonical_session, lifecycle_id, canonical=True,
                    session_date=session_date,
                )
                paper_exit = _exit_diagnostics(
                    paper, lifecycle_id, canonical=False,
                    session_date=session_date,
                )
                result["exit_diagnostics"] = {
                    "canonical": canonical_exit,
                    "paper": paper_exit,
                    "first_difference": _first_exit_diagnostic_difference(
                        canonical_exit, paper_exit,
                    ),
                }
                canonical_trace = _execution_trace_diagnostics(
                    canonical_session, lifecycle_id, canonical=True,
                    session_date=session_date,
                )
                paper_trace = _execution_trace_diagnostics(
                    paper, lifecycle_id, canonical=False,
                    session_date=session_date,
                )
                result["execution_trace"] = {
                    "canonical": canonical_trace,
                    "paper": paper_trace,
                    "first_difference": _first_execution_trace_difference(
                        canonical_trace, paper_trace,
                    ),
                }
        if (
            result.get("status") == "MISMATCHED"
            and str(result.get("field", "")).startswith("values.")
            and str(result.get("field", "")).rsplit(".", 1)[-1]
            in {
                "price", "quantity", "requested_price", "failure_boundary",
                "protective_stop", "stop",
            }
            and isinstance(result.get("canonical_value"), (int, float))
            and isinstance(result.get("paper_value"), (int, float))
            and abs(float(result["canonical_value"]) - float(result["paper_value"]))
            <= ATR_ABSOLUTE_TOLERANCE
        ):
            result = {
                "status": "INPUT_VARIANCE",
                "stream": result.get("stream", "execution_fills"),
                "reason": "bounded ATR variance affects execution price",
                "first_difference": result,
            }
        return result
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "INCOMPLETE", "stream": PHASE7_STREAMS[0], "reason": str(exc)}


def _environment(root: Path, *, paper_root: Path | None = None) -> dict[str, str]:
    environment = os.environ.copy()
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = str(root) if not existing else os.pathsep.join((str(root), existing))
    if paper_root is not None:
        environment["FTX_PAPER_ROOT"] = str(paper_root)
    return environment


def _run_json(
    command: list[str], *, cwd: Path, output: Path | None = None,
    paper_root: Path | None = None, debug_dir: Path | None = None, label: str = "subprocess",
) -> tuple[Any | None, str | None]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=_environment(cwd, paper_root=paper_root),
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        return None, f"could not start subprocess: {exc} ({_command_label(command)})"
    log_paths = _write_process_logs(result, debug_dir=debug_dir, label=label)
    if result.returncode:
        diagnostic = _process_diagnostic(result, command)
        if log_paths:
            diagnostic += f"; logs: {log_paths}"
        if output is not None and output.is_file():
            diagnostic += f"; output artifact exists at {output}"
        return None, diagnostic
    try:
        payload = json.loads(output.read_text(encoding="utf-8")) if output is not None else json.loads(result.stdout)
    except (OSError, json.JSONDecodeError) as exc:
        diagnostic = f"invalid JSON output: {exc}"
        return None, f"{diagnostic}; logs: {log_paths}" if log_paths else diagnostic
    return payload if isinstance(payload, (dict, list)) else None, None


def _write_process_logs(
    result: subprocess.CompletedProcess[str], *, debug_dir: Path | None, label: str,
) -> str | None:
    if debug_dir is None:
        return None
    debug_dir.mkdir(parents=True, exist_ok=True)
    stem = "".join(character if character.isalnum() or character in "-_" else "_" for character in label)
    stdout_path = debug_dir / f"{stem}.stdout.log"
    stderr_path = debug_dir / f"{stem}.stderr.log"
    stdout_path.write_text(result.stdout, encoding="utf-8")
    stderr_path.write_text(result.stderr, encoding="utf-8")
    return f"{stdout_path}, {stderr_path}"


def _command_label(command: list[str]) -> str:
    return " ".join(command)


def _process_diagnostic(result: subprocess.CompletedProcess[str], command: list[str]) -> str:
    parts = [
        f"subprocess exited with code {result.returncode}",
        f"command: {_command_label(command)}",
    ]
    if result.stderr.strip():
        parts.append(f"stderr: {result.stderr.strip()}")
    if result.stdout.strip():
        parts.append(f"stdout: {result.stdout.strip()}")
    return "; ".join(parts)


def _run_command(
    command: list[str], *, cwd: Path, debug_dir: Path | None = None, label: str = "subprocess",
) -> str | None:
    try:
        result = subprocess.run(
            command, cwd=cwd, env=_environment(cwd), capture_output=True, text=True,
        )
    except OSError as exc:
        return f"could not start subprocess: {exc} ({_command_label(command)})"
    log_paths = _write_process_logs(result, debug_dir=debug_dir, label=label)
    if result.returncode:
        diagnostic = _process_diagnostic(result, command)
        return f"{diagnostic}; logs: {log_paths}" if log_paths else diagnostic
    return None


def _artifact_contains_session(path: Path, session_date: str) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return False
    sessions = payload.get("sessions") if isinstance(payload, dict) else None
    return isinstance(sessions, dict) and session_date in sessions


def _resolve_canonical_artifact(path: Path, session_date: str) -> Path | None:
    path = path.resolve()
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
        matches = sorted(
            (
                *path.rglob("orderflow-state-machine-multi-session-replay.json"),
                *path.rglob(f"{session_date}-canonical.json"),
            ),
            key=lambda candidate: candidate.stat().st_mtime,
            reverse=True,
        )
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
        "from ftx_paper.strategies.orderflow.timestamps import session_minute; "
        "rows=[{**row, 'time': session_minute(row['session_date'], row['time'])} for row in rows]; "
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


def _canonical_source_command(canonical_python: str, session_date: str, output: Path) -> list[str]:
    code = """
import json, sys
from datetime import datetime, time
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
    value = row.time
    if isinstance(value, datetime):
        minute = value.strftime('%H:%M')
    elif isinstance(value, time):
        minute = value.strftime('%H:%M')
    elif isinstance(value, str):
        text = value.strip()
        parsed = datetime.fromisoformat(text.replace('Z', '+00:00')) if 'T' in text else time.fromisoformat(text)
        minute = parsed.strftime('%H:%M')
    else:
        raise TypeError('canonical source row time must be a datetime, time, or string')
    if minute > '15:30':
        continue
    raw.setdefault(str(row.date), []).append({
        'time': f'{row.date}T{minute}', 'open': row.open, 'high': row.high, 'low': row.low,
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
        **items[0], 'time': f'{item_date}T15:30',
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


def _compare_unified_artifact(
    canonical_path: Path | None, paper: Mapping[str, Any] | None, session_date: str,
) -> dict[str, Any]:
    """Run the Phase 8 acceptance comparison; split phases remain diagnostics."""
    if canonical_path is None or paper is None:
        return {"status": "INCOMPLETE", "reason": "replay artifact unavailable"}
    try:
        canonical_payload = json.loads(canonical_path.read_text(encoding="utf-8"))
        canonical_session = canonical_payload["sessions"][session_date]
        if not isinstance(canonical_session, Mapping):
            raise ValueError("canonical session payload is unavailable")
        canonical_artifact = build_parity_artifact_from_payload(
            canonical_session, run_id=f"canonical-{session_date}",
            producer={"name": "NiftyZoning"},
        )
        paper_artifact = build_parity_artifact_from_payload(
            paper, run_id=f"paper-{session_date}", producer={"name": "ftx-paper"},
        )
        return compare_parity_artifacts(canonical_artifact, paper_artifact)
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "INCOMPLETE", "reason": str(exc)}


def compare_date(
    session_date: str,
    *,
    canonical_root: Path,
    paper_root: Path,
    canonical_python: str,
    paper_python: str,
    canonical_artifact: Path | None = None,
    debug_dir: Path | None = None,
    artifact_dir: Path | None = None,
    run_id: str | None = None,
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
            _canonical_source_command(canonical_python, session_date, canonical_source_path),
            cwd=canonical_root, debug_dir=debug_dir, label=f"{session_date}-canonical-source",
        )
        paper_command = [
            paper_python, "scripts/orderflow/orderflow_replay.py", "--date", session_date,
            "--source-artifact", str(canonical_source_path), "--include-interactions",
            "--output", str(paper_replay_path),
        ]
        with ThreadPoolExecutor(max_workers=2) as executor:
            paper_future = executor.submit(
                _run_json, paper_command, cwd=paper_root, output=paper_replay_path,
                debug_dir=debug_dir, label=f"{session_date}-paper-replay",
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
                    cwd=canonical_root, debug_dir=debug_dir,
                    label=f"{session_date}-canonical-replay",
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
                    debug_dir=debug_dir, label=f"{session_date}-canonical-projection",
                )
                if canonical_path is not None and canonical_error is None else None
            )
            canonical_phase3_future = (
                executor.submit(
                    _run_json,
                    _replay_observation_command(
                        canonical_python, canonical_path, session_date, canonical=True,
                    ),
                    cwd=canonical_root, debug_dir=debug_dir,
                    label=f"{session_date}-canonical-observation",
                )
                if canonical_path is not None and canonical_error is None else None
            )
            paper_phase3_future = (
                executor.submit(
                    _run_json,
                    _replay_observation_command(
                        paper_python, paper_replay_path, session_date, canonical=False,
                    ),
                    cwd=paper_root, debug_dir=debug_dir,
                    label=f"{session_date}-paper-observation",
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
            phase3 = _compare_fields(
                _normalize_observation_payload(canonical_phase3, session_date),
                _normalize_observation_payload(paper_phase3, session_date),
                PHASE3_FIELDS,
                label="phase3",
            )
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
                _normalize_canonical_timestamps(canonical_projection, session_date), paper_replay,
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
        phase6 = (
            _compare_phase6(canonical_path, paper_replay, session_date)
            if canonical_path is not None and paper_replay is not None
            else {"status": "INCOMPLETE", "stream": "opportunity_lifecycle", "reason": "replay artifact unavailable"}
        )
        phase7 = (
            _compare_phase7(canonical_path, paper_replay, session_date)
            if canonical_path is not None and paper_replay is not None
            else {"status": "INCOMPLETE", "stream": PHASE7_STREAMS[0], "reason": "replay artifact unavailable"}
        )
        parity = _compare_unified_artifact(canonical_path, paper_replay, session_date)
        if phase6.get("status") not in {"MATCHED", "INPUT_VARIANCE"}:
            phase7 = {
                "status": "UNAVAILABLE",
                "stream": PHASE7_STREAMS[0],
                "reason": "phase6_gate_not_accepted",
            }
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
        retained_artifacts = _retain_replay_artifacts(
            artifact_dir, session_date, run_id or uuid4().hex[:8], canonical_path, paper_replay_path,
            canonical_source_path,
        )
        result = {
            "date": session_date,
            "phase3": phase3,
            "phase3_5": phase35,
            "phase4": phase4,
            "phase4_5": phase45,
            "phase6": phase6,
            "phase7": phase7,
            "phase7_status": phase7.get("status"),
            "parity": parity,
            "canonical_replay": {
                "status": "MATCHED" if canonical_projection else "INCOMPLETE",
                **retained_artifacts["canonical"],
            },
            "paper_replay": {
                "status": "MATCHED" if paper_replay else "INCOMPLETE",
                **retained_artifacts["paper"],
            },
            "canonical_source": retained_artifacts["canonical_source"],
        }
        result["gate"] = _gate_result(result)
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Exit status is 0 only when every requested date passes the parity gate. "
            "A reported FAILED comparison is an expected diagnostic result, not an "
            "unhandled script error."
        ),
    )
    parser.add_argument("--date", action="append", dest="dates", required=True, type=_comparison_date)
    parser.add_argument("--canonical-python", default=None)
    parser.add_argument(
        "--canonical-artifact",
        type=Path,
        default=None,
        help="Existing canonical replay artifact or directory; skips canonical replay execution",
    )
    args = parser.parse_args(argv)
    paper_root = Path(__file__).resolve().parents[2]
    canonical_root = paper_root.parent / "NiftyZoning"
    canonical_python_path = canonical_root / ".venv" / "Scripts" / "python.exe"
    canonical_python = args.canonical_python or str(
        canonical_python_path if canonical_python_path.exists() else sys.executable
    )
    output_dir = paper_root / "data" / "output" / "orderflow"
    generated_at = datetime.now(timezone.utc)
    run_id = f"{generated_at:%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    artifact_dir = output_dir / "artifacts"
    debug_dir = artifact_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    results = [compare_date(
        session_date,
        canonical_root=canonical_root,
        paper_root=paper_root,
        canonical_python=canonical_python,
        paper_python=sys.executable,
        canonical_artifact=args.canonical_artifact,
        debug_dir=debug_dir,
        artifact_dir=artifact_dir,
        run_id=run_id,
    ) for session_date in args.dates]
    overall = "PASSED" if all(
        _gate_result(item)["status"] == "PASSED" for item in results
    ) else "FAILED"
    report_path = _write_comparison_output(results, output_dir, status=overall, run_id=run_id)
    print(f"Comparison report: {report_path}")
    print(f"Artifact directory: {artifact_dir}")
    for result in results:
        gate = result["gate"]
        phase_statuses = ", ".join(
            f"{phase}={result.get(phase, {}).get('status', 'MISSING')}"
            for phase in ("phase3", "phase3_5", "phase4", "phase4_5", "phase6", "phase7", "parity")
        )
        print(f"{result['date']}: gate={gate['status']} ({phase_statuses})")
        if gate["failed_phases"]:
            print(f"  failed phases: {', '.join(gate['failed_phases'])}")
    print(
        "Comparison completed with parity failures; see the report for first differences."
        if overall == "FAILED"
        else "Comparison passed for all requested dates."
    )
    return 0 if overall == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
