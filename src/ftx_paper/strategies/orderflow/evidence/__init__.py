"""Canonical evidence publication boundary."""

from .producer import (
    EvidenceAdapter,
    produce_evidence,
)
from .adapters import SerializedEvidenceAdapter
from .features_adapter import PaperFeatureAdapter
from .serialization import evidence_payload, serialize_evidence

__all__ = [
    "EvidenceAdapter",
    "evidence_payload",
    "produce_evidence",
    "serialize_evidence",
    "SerializedEvidenceAdapter",
    "PaperFeatureAdapter",
]
