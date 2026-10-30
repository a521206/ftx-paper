"""Deterministic serialization for Phase 3.5 parity projections."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
import json
from types import MappingProxyType
from typing import Any

from .producer import Phase35Evidence


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return _json_value(asdict(value))
    if isinstance(value, MappingProxyType):
        return _json_value(dict(value))
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def evidence_payload(evidence: Phase35Evidence) -> dict[str, Any]:
    publication = evidence.publication
    return {
        "schema_version": 1,
        "artifact_type": "canonical_phase3_5_evidence",
        "session_date": evidence.session_date,
        "source_fingerprint": evidence.source_fingerprint,
        "evidence": _json_value(publication),
    }


def serialize_evidence(evidence: Phase35Evidence) -> str:
    return json.dumps(evidence_payload(evidence), sort_keys=True, separators=(",", ":"))
