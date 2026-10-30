"""Canonical evidence publication boundary."""

from .producer import (
    EvidenceAdapter,
    Phase35Evidence,
    Phase35Producer,
    UnavailableEvidenceAdapter,
    produce_evidence,
)
from .adapters import SerializedEvidenceAdapter
from .serialization import evidence_payload, serialize_evidence

__all__ = [
    "EvidenceAdapter",
    "Phase35Evidence",
    "Phase35Producer",
    "UnavailableEvidenceAdapter",
    "evidence_payload",
    "produce_evidence",
    "serialize_evidence",
    "SerializedEvidenceAdapter",
]
