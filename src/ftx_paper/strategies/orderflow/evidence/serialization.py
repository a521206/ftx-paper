"""Deterministic serialization for Phase 3.5 parity projections."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
import json
from types import MappingProxyType
from collections.abc import Mapping
from typing import Any


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, MappingProxyType):
        return _json_value(dict(value))
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def evidence_payload(evidence: Mapping[str, Any]) -> dict[str, Any]:
    publication = evidence.get("publication")
    if publication is None:
        raise ValueError("evidence publication is required")
    return {
        "schema_version": 1,
        "artifact_type": "canonical_phase3_5_evidence",
        "session_date": evidence.get("session_date"),
        "source_fingerprint": evidence.get("source_fingerprint"),
        "evidence": _json_value(publication),
    }


def serialize_evidence(evidence: Mapping[str, Any]) -> str:
    return json.dumps(evidence_payload(evidence), sort_keys=True, separators=(",", ":"))
