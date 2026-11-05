"""Compare the Paper evidence replay with NiftyZoning for one session.

The canonical repository and the Paper API client are executed in isolated
subprocesses. Phase 3 observations are rebuilt from replay-engine bars before
Phase 3.5 evidence projections are compared. No NiftyZoning package is
imported into the Paper process.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from functools import lru_cache
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
    build_phase7_artifact,
    build_parity_artifact_from_payload,
    compare_chronological_streams,
    compare_parity_artifacts,
    compare_phase7_manifests,
)
from ftx_paper.strategies.orderflow.phase7_projection import PHASE7_STREAMS
from ftx_paper.strategies.orderflow.timestamps import parse_session_timestamp, parse_utc, serialize_utc

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
PHASE4_STREAMS = ("source_observation", "context_location")
MIN_COMPARISON_DATE = date(2026, 9, 15)
_CAUSAL_TIMESTAMP_KEYS = frozenset({
    "as_of", "available_at", "created_at", "current_available_at", "end_time", "formation_at",
    "formation_available_at", "observed_at", "occurred_at", "response_available_at",
    "response_observed_at", "causal_available_at", "causal_observed_at",
    "context_candle_available_at", "time",
    "filled_at", "reserved_at", "released_at", "decided_at", "effective_at",
    "triggered_at", "pivot_time",
})
_TRANSPORT_METADATA_KEYS = frozenset({
    "identity", "neutral_event_key", "event_id", "sequence", "stream_sequence",
    "_publication_available_at",
    "causal_parent_key",
})
_CAUSAL_METADATA_KEYS = frozenset({
    "causal_available_at", "causal_observed_at", "causal_parent_id", "parent_id",
})


@lru_cache(maxsize=8)
def _load_canonical_payload(path: str) -> dict[str, Any]:
    """Load a canonical artifact once per comparison process.

    Canonical replay artifacts are intentionally detailed and can exceed 150 MB.
    The phase comparisons only read them, so reparsing the same artifact for each
    phase needlessly dominates comparison time and memory churn.
    """
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("canonical replay payload must be an object")
    return payload
_ENVELOPE_DUPLICATE_KEYS = frozenset({
    "kind", "occurred_at", "observed_at", "available_at", "unavailable",
})
_PRODUCER_DERIVED_KEYS = frozenset({"parity_stream", "source_fingerprint"})
_PHASE45_NON_SEMANTIC_KEYS = frozenset({
    "terminal_candle_id", "terminal_close_side",
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
) -> dict[str, dict[str, Any]]:
    """Copy subprocess replay artifacts out of the temporary work directory."""
    if artifact_dir is None:
        return {
            "canonical": {"status": "NOT_RETAINED", "path": None},
            "paper": {"status": "NOT_RETAINED", "path": None},
        }
    destination = artifact_dir / session_date / run_id
    result: dict[str, dict[str, Any]] = {}
    for name, source in (
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


def _phase_accepted(phase: Mapping[str, Any]) -> bool:
    return phase.get("status") == "MATCHED"


def _gate_result(result: Mapping[str, Any]) -> dict[str, Any]:
    phases = ("phase3", "phase3_5", "phase4", "phase4_5", "phase6", "phase7", "parity")
    observed = {
        "phase3": _phase_accepted(result.get("phase3", {})),
        "phase3_5": _phase_accepted(result.get("phase3_5", {})),
        "phase4": _phase_accepted(result.get("phase4", {})),
        "phase4_5": _phase_accepted(result.get("phase4_5", {})),
        "phase6": _phase_accepted(result.get("phase6", {})),
        "phase7": _phase_accepted(result.get("phase7", {})),
        "parity": _phase_accepted(result.get("parity", {})),
    }
    first_failure = next((index for index, name in enumerate(phases) if not observed[name]), None)
    accepted = {
        name: first_failure is None or index < first_failure
        for index, name in enumerate(phases)
    }
    failed = [name for name in phases if not observed[name]]
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
            if text.endswith("Z") or "+" in text[10:] or "-" in text[10:]:
                return serialize_utc(parse_utc(text))
            # Embedded causal values can reference a prior session, so use the
            # value's own date rather than forcing it into the replay date.
            return serialize_utc(parse_session_timestamp(text[:10], text))
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


def _normalize_unified_payload_for_compare(
    payload: Mapping[str, Any], session_date: str,
) -> dict[str, Any]:
    """Normalize canonical local labels before unified artifact construction."""
    normalized = dict(payload)
    for group_name in ("parity_phase_facts", "parity_layers"):
        raw_group = payload.get(group_name)
        if not isinstance(raw_group, Mapping):
            continue
        normalized[group_name] = {
            str(stream): [
                _normalize_phase_record_for_compare(dict(record), session_date)
                if isinstance(record, Mapping) else record
                for record in records
            ]
            if isinstance(records, Sequence) and not isinstance(records, (str, bytes))
            else records
            for stream, records in raw_group.items()
        }
    return normalized


def _explicit_phase_records(
    payload: Mapping[str, Any], stream: str,
) -> tuple[list[dict[str, Any]] | None, dict[str, Any] | None]:
    """Read one declared parity stream without reconstructing producer facts."""
    facts = payload.get("parity_phase_facts")
    coverage = payload.get("parity_coverage")
    phase_coverage = coverage.get("phase_streams") if isinstance(coverage, Mapping) else None
    entry = phase_coverage.get(stream) if isinstance(phase_coverage, Mapping) else None
    records = facts.get(stream) if isinstance(facts, Mapping) else None
    if not isinstance(entry, Mapping):
        return None, {"status": "INCOMPLETE", "reason": f"missing_{stream}_coverage"}
    status = str(entry.get("status"))
    if status != "MATCHED":
        return None, dict(entry)
    if not isinstance(records, list):
        return None, {"status": "INCOMPLETE", "reason": f"invalid_{stream}_facts"}
    if not bool(entry.get("observed")):
        return None, {"status": "INCOMPLETE", "reason": f"{stream}_not_observed"}
    if not records and entry.get("coverage") != "observed_empty":
        return None, {"status": "INCOMPLETE", "reason": f"{stream}_empty_without_observed_coverage"}
    normalized = [dict(item) for item in records if isinstance(item, Mapping)]
    if len(normalized) != len(records):
        return None, {"status": "INCOMPLETE", "reason": f"invalid_{stream}_record"}
    return normalized, None


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
    canonical: dict[str, Any], paper: dict[str, Any],
) -> dict[str, Any]:
    """Compare independent evidence exactly."""
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
        if left_value != right_value:
            return {"status": "MISMATCHED", "layer": "phase3_5_independent", "field": f"prior_value.{field}", "canonical_value": left_value, "paper_value": right_value}
    # Snapshot identity and fingerprint belong to the producer's profile store;
    # compare the published value geometry and causal availability instead.
    ignored = {"fingerprint", "reference_id", "poc", "vah", "val"}
    remaining_left = {key: value for key, value in left.items() if key not in ignored}
    remaining_right = {key: value for key, value in right.items() if key not in ignored}
    if remaining_left != remaining_right:
        return {"status": "MISMATCHED", "layer": "phase3_5_independent", "field": "prior_value", "first_mismatch": _first_difference(remaining_left, remaining_right, "prior_value")}
    return {
        "status": "MATCHED",
        "layer": "phase3_5_independent",
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
        return {"status": "MISMATCHED", "layer": "phase3_5", "field": "atr.value",
                "canonical_value": canonical_atr.get("value"),
                "paper_value": paper_atr.get("value")}
    # Prior-value geometry and causal availability are semantic evidence;
    # reference IDs and fingerprints are producer-owned profile-store lineage.
    independent = _compare_phase35_independent(canonical_value, paper_value)
    if independent["status"] != "MATCHED":
        return {
            **independent,
            "layer": "phase3_5",
        }
    dependent = _compare_fields(
        canonical_value,
        paper_value,
        PHASE35_ATR_DEPENDENT_FIELDS,
        label="phase3_5",
    )
    if dependent["status"] != "MATCHED":
        return dependent
    return {
        "status": "MATCHED",
        "layer": "phase3_5",
        "manifest": dependent["manifest"],
        "prior_value": independent,
    }


def _paper_phase35_projection(session_result: dict[str, Any]) -> dict[str, Any]:
    """Project Paper's publication payload into the canonical Phase 3.5 shape."""
    raw = session_result.get("evidence", session_result)
    if not isinstance(raw, dict):
        return {}

    raw_atr = raw.get("atr")
    locations = raw.get("locations", [])
    if not isinstance(locations, list):
        locations = []
    locations = _normalize_phase35_locations(locations, str(session_result.get("session_date", "")))
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
    # Canonical Phase 3.5 emits the current ordinary-context reference on each
    # replay record. Resolve Paper publications at those same causal bar times.
    contexts: list[dict[str, Any]] = []
    raw_contexts = raw.get("contexts", [])
    ordinary_contexts = []
    if isinstance(raw_contexts, list):
        for item in raw_contexts:
            if not isinstance(item, dict):
                continue
            context_id = item.get("context_id")
            version = item.get("version")
            available_at = item.get("available_at")
            if (
                isinstance(context_id, str)
                and not context_id.startswith("interaction:")
                and isinstance(version, str)
                and isinstance(available_at, str)
            ):
                ordinary_contexts.append((
                    parse_utc(available_at),
                    {"context_id": context_id, "version": version},
                ))
    replay_records = session_result.get("replay_records", [])
    if isinstance(replay_records, list):
        ordinary_contexts.sort(key=lambda item: item[0])
        for record in replay_records:
            if not isinstance(record, dict):
                continue
            explicit_reference = record.get("ordinary_context_ref")
            if isinstance(explicit_reference, dict):
                if isinstance(explicit_reference.get("context_id"), str) and isinstance(explicit_reference.get("version"), str):
                    contexts.append({
                        "context_id": explicit_reference["context_id"],
                        "version": explicit_reference["version"],
                    })
                continue
            bar = record.get("bar")
            if not isinstance(bar, dict) or not isinstance(bar.get("time"), str):
                continue
            available = parse_session_timestamp(
                str(session_result.get("session_date", "")), bar["time"],
            )
            current = None
            for published_at, reference in ordinary_contexts:
                if published_at > available:
                    break
                current = reference
            if current is not None:
                contexts.append(current)
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


def _normalize_phase35_locations(
    locations: list[Any], session_date: str,
) -> list[dict[str, Any]]:
    """Normalize Paper location records to the canonical causal projection."""
    # Canonical registration publishes the high side before the low side for
    # each paired location.  Paper evidence is accumulated through helpers
    # that naturally append the low side first, so use the producer-neutral
    # semantic order here rather than retaining publication insertion order.
    priority = {
        "PD_HIGH": 0, "PD_LOW": 1,
        "OR_HIGH": 2, "OR_LOW": 3,
        "SESSION_HIGH": 4, "SESSION_LOW": 5,
    }
    normalized: list[dict[str, Any]] = []
    for item in locations:
        if not isinstance(item, dict):
            continue
        value = _normalize_canonical_timestamps(dict(item), session_date)
        band = value.get("band")
        if isinstance(band, dict):
            value.setdefault("band_low", band.get("low"))
            value.setdefault("band_high", band.get("high"))
        normalized.append(value)
    normalized.sort(key=lambda item: (
        str(item.get("available_at", "")),
        priority.get(str(item.get("alias", "")), 99),
        0 if item.get("alias") == "VWAP" and item.get("timeframe") == "5m" else 1,
        str(item.get("source_aggregate_id") or ""),
        int(item.get("generation", 0)),
    ))
    return normalized


def _compare_phase4(canonical_path: Path, paper: dict[str, Any], session_date: str) -> dict[str, Any]:
    """Compare explicit causal source/context streams after Phase 4B replay."""
    try:
        canonical_payload = _load_canonical_payload(str(canonical_path))
        canonical_session = canonical_payload["sessions"][session_date]
        for stream in PHASE4_STREAMS:
            canonical_records, canonical_error = _explicit_phase_records(canonical_session, stream)
            paper_records, paper_error = _explicit_phase_records(paper, stream)
            if canonical_error or paper_error:
                return {"status": "INCOMPLETE", "stream": stream,
                        "canonical": canonical_error, "paper": paper_error}
            result = compare_chronological_streams(
                canonical_records or (), paper_records or (), stream=stream,
                session_date=session_date,
            )
            if result["status"] != "MATCHED":
                return {"stream": stream, **result}
        return {"status": "MATCHED"}
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "INCOMPLETE", "stream": "source_observation", "reason": str(exc)}


def _compare_phase45(canonical_path: Path, paper: dict[str, Any], session_date: str) -> dict[str, Any]:
    """Compare the explicit chronological interaction/response stream."""
    try:
        canonical_payload = _load_canonical_payload(str(canonical_path))
        canonical_records, canonical_error = _explicit_phase_records(
            canonical_payload["sessions"][session_date], "interaction_response",
        )
        paper_records, paper_error = _explicit_phase_records(paper, "interaction_response")
        if canonical_error or paper_error:
            return {"status": "INCOMPLETE", "stream": "interaction_response",
                    "canonical": canonical_error, "paper": paper_error}
        return {
            "stream": "interaction_response",
            **compare_chronological_streams(
                canonical_records or (), paper_records or (), stream="interaction_response",
                session_date=session_date,
            ),
        }
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        reason = str(exc)
        if "timezone-aware" in reason and reason != "parity timestamps must be timezone-aware":
            reason = "parity timestamps must be timezone-aware"
        return {"status": "INCOMPLETE", "stream": "interaction_response", "reason": reason}


def _compare_phase6(canonical_path: Path, paper: dict[str, Any], session_date: str) -> dict[str, Any]:
    """Compare explicit opportunity/setup lifecycle facts."""
    try:
        payload = _load_canonical_payload(str(canonical_path))
        session = payload["sessions"][session_date]
        canonical_records, canonical_error = _explicit_phase_records(session, "opportunity_lifecycle")
        paper_records, paper_error = _explicit_phase_records(paper, "opportunity_lifecycle")
        if canonical_error or paper_error:
            return {"status": "INCOMPLETE", "stream": "opportunity_lifecycle",
                    "canonical": canonical_error, "paper": paper_error}
        result = compare_chronological_streams(
            canonical_records or (), paper_records or (),
            stream="opportunity_lifecycle", session_date=session_date,
        )
        if result["status"] != "MATCHED":
            return {
                "status": "MISMATCHED",
                "stream": "opportunity_lifecycle",
                **result,
            }
        return result
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "INCOMPLETE", "stream": "opportunity_lifecycle", "reason": str(exc)}


def _compare_phase7(canonical_path: Path, paper: dict[str, Any], session_date: str) -> dict[str, Any]:
    """Compare explicit execution facts in causal stream order."""
    try:
        payload = _load_canonical_payload(str(canonical_path))
        canonical_session = payload["sessions"][session_date]
        canonical_replay = canonical_session.get("replay")
        if not isinstance(canonical_replay, Mapping):
            return {"status": "INCOMPLETE", "stream": PHASE7_STREAMS[0], "reason": "canonical_replay_missing"}
        def phase7_records(
            source: Mapping[str, Any], stream: str,
        ) -> tuple[list[dict[str, Any]] | None, dict[str, Any] | None]:
            if stream in {"intent_admission", "trigger_exit"}:
                return _explicit_phase_records(source, stream)
            records = source.get(stream)
            if not isinstance(records, list):
                return None, {"status": "INCOMPLETE", "reason": f"missing_{stream}_facts"}
            normalized = [dict(item) for item in records if isinstance(item, Mapping)]
            if len(normalized) != len(records):
                return None, {"status": "INCOMPLETE", "reason": f"invalid_{stream}_record"}
            return normalized, None

        for stream in PHASE7_STREAMS:
            _, canonical_error = phase7_records(canonical_replay, stream)
            _, paper_error = phase7_records(paper, stream)
            if canonical_error or paper_error:
                return {"status": "INCOMPLETE", "stream": stream,
                        "canonical": canonical_error, "paper": paper_error}
        canonical_streams = {
            stream: phase7_records(canonical_replay, stream)[0] or []
            for stream in PHASE7_STREAMS
        }
        paper_streams = {
            stream: phase7_records(paper, stream)[0] or []
            for stream in PHASE7_STREAMS
        }
        def normalize(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [_normalize_phase_record_for_compare(item, session_date) for item in values]
        canonical_artifact = build_phase7_artifact(
            run_id=f"canonical-{session_date}", producer={"name": "NiftyZoning"},
            scope={"date": session_date},
            payload={"parity_phase_facts": {
                name: normalize(canonical_streams[name]) for name in PHASE7_STREAMS
            }},
        )
        paper_artifact = build_phase7_artifact(
            run_id=f"paper-{session_date}", producer={"name": "ftx-paper"},
            scope={"date": session_date},
            payload={"parity_phase_facts": {
                name: normalize(paper_streams[name]) for name in PHASE7_STREAMS
            }},
        )
        result = compare_phase7_manifests(
            canonical_artifact, paper_artifact,
            canonical_records=lambda stream: canonical_artifact["phase_streams"][stream],
            paper_records=lambda stream: paper_artifact["phase_streams"][stream],
        )
        return result
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "INCOMPLETE", "stream": PHASE7_STREAMS[0], "reason": str(exc)}


def _environment(root: Path) -> dict[str, str]:
    environment = os.environ.copy()
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = str(root) if not existing else os.pathsep.join((str(root), existing))
    return environment


def _run_json(
    command: list[str], *, cwd: Path, output: Path | None = None,
    debug_dir: Path | None = None, label: str = "subprocess",
) -> tuple[Any | None, str | None]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=_environment(cwd),
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


def _paper_api_payload(result: Any) -> dict[str, Any] | None:
    """Extract the orderflow replay payload from the API result envelope."""
    if not isinstance(result, Mapping):
        return None
    replay_result = result.get("result")
    if not isinstance(replay_result, Mapping):
        return None
    report = replay_result.get("report")
    if not isinstance(report, Mapping):
        return None
    extensions = report.get("extensions")
    orderflow_extension = (
        extensions.get("ftx-paper-orderflow")
        if isinstance(extensions, Mapping)
        else None
    )
    payload = (
        orderflow_extension.get("orderflow_payload")
        if isinstance(orderflow_extension, Mapping)
        else None
    )
    return dict(payload) if isinstance(payload, Mapping) else None


def _paper_api_command(
    paper_python: str, session_date: str, *, base_url: str,
) -> list[str]:
    return [
        paper_python, "-m", "ftx_paper.replay_cli", session_date,
        "--base-url", base_url,
        "--strategy-id", "ftx-paper-orderflow",
        "--include-parity-layers", "--json",
    ]


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


def _compare_unified_artifact(
    canonical_path: Path | None, paper: Mapping[str, Any] | None, session_date: str,
) -> dict[str, Any]:
    """Run the Phase 8 acceptance comparison; split phases remain diagnostics."""
    if canonical_path is None or paper is None:
        return {"status": "INCOMPLETE", "reason": "replay artifact unavailable"}
    try:
        canonical_payload = _load_canonical_payload(str(canonical_path))
        canonical_session = canonical_payload["sessions"][session_date]
        if not isinstance(canonical_session, Mapping):
            raise ValueError("canonical session payload is unavailable")
        canonical_session = _normalize_unified_payload_for_compare(
            canonical_session, session_date,
        )
        paper = _normalize_unified_payload_for_compare(paper, session_date)
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
    paper_api_url: str = "http://127.0.0.1:8501",
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="orderflow-cross-repo-") as directory_name:
        directory = Path(directory_name)
        paper_replay_path = directory / "paper-replay.json"
        canonical_path = (
            _resolve_canonical_artifact(canonical_artifact, session_date)
            if canonical_artifact else None
        )
        canonical_output_dir = directory / "canonical-replay"
        paper_command = _paper_api_command(
            paper_python, session_date, base_url=paper_api_url,
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            paper_future = executor.submit(
                _run_json, paper_command, cwd=paper_root,
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

            paper_api_result, replay_error = paper_future.result()
            paper_replay = _paper_api_payload(paper_api_result)
            if paper_replay is not None:
                paper_replay_path.write_text(
                    json.dumps(paper_replay, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            elif replay_error is None:
                replay_error = "API result did not contain an orderflow replay payload"
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
            if phase35.get("status") == "MATCHED" and canonical_path is not None and paper_replay is not None
            else {"status": "UNAVAILABLE", "stream": "source_observation", "reason": "phase3_5_gate_not_accepted"}
        )
        if canonical_path is not None and paper_replay is not None:
            phase45_comparison = _compare_phase45(canonical_path, paper_replay, session_date)
        else:
            phase45_comparison = {
                "status": "INCOMPLETE",
                "stream": "interaction_response",
                "reason": "replay_artifact_unavailable",
            }
        phase45 = (
            phase45_comparison
            if phase4.get("status") == "MATCHED"
            else {
                "status": "UNAVAILABLE",
                "stream": "interaction_response",
                "reason": "phase4_gate_not_accepted",
                "diagnostic": phase45_comparison,
            }
        )
        phase6 = (
            _compare_phase6(canonical_path, paper_replay, session_date)
            if phase45.get("status") == "MATCHED" and canonical_path is not None and paper_replay is not None
            else {"status": "UNAVAILABLE", "stream": "opportunity_lifecycle", "reason": "phase4_5_gate_not_accepted"}
        )
        phase7 = (
            _compare_phase7(canonical_path, paper_replay, session_date)
            if phase6.get("status") == "MATCHED" and canonical_path is not None and paper_replay is not None
            else {"status": "UNAVAILABLE", "stream": PHASE7_STREAMS[0], "reason": "phase6_gate_not_accepted"}
        )
        parity = (
            _compare_unified_artifact(canonical_path, paper_replay, session_date)
            if phase7.get("status") == "MATCHED"
            else {"status": "UNAVAILABLE", "reason": "phase7_gate_not_accepted"}
        )
        retained_artifacts = _retain_replay_artifacts(
            artifact_dir, session_date, run_id or uuid4().hex[:8], canonical_path, paper_replay_path,
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
        "--paper-api-url",
        default=os.getenv("FTX_API_URL", "http://127.0.0.1:8501"),
        help="Paper API base URL used for the orderflow replay",
    )
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
    lock_path = output_dir / ".canonical-replay-comparison.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            owner = int(lock_path.read_text(encoding="ascii").strip())
            os.kill(owner, 0)
        except (OSError, ValueError):
            lock_path.unlink(missing_ok=True)
            try:
                lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                print(
                    "A canonical replay comparison started while recovering a stale lock.",
                    file=sys.stderr,
                )
                return 2
        else:
            print(
                f"A canonical replay comparison is already running (PID {owner}).",
                file=sys.stderr,
            )
            return 2
    try:
        os.write(lock_fd, str(os.getpid()).encode("ascii"))
    finally:
        os.close(lock_fd)
    try:
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
            paper_api_url=args.paper_api_url,
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
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
