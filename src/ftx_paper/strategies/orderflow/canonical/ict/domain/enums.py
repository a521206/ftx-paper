"""Vocabulary for the ICT market-state model."""

from enum import StrEnum


class Condition(StrEnum):
    UNKNOWN = "unknown"
    BALANCE_CANDIDATE = "balance_candidate"
    BALANCED = "balanced"
    EXPANSION = "expansion"
    RETRACEMENT = "retracement"
    RESUMED_EXPANSION = "resumed_expansion"
    REVERSAL_ATTEMPT = "reversal_attempt"


class StructuralDirection(StrEnum):
    UNKNOWN = "unknown"
    UP = "up"
    DOWN = "down"


class Movement(StrEnum):
    UNKNOWN = "unknown"
    ROTATION = "rotation"
    DRIFT = "drift"
    DIRECTIONAL = "directional"


class EvidenceStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"


class EpisodeLifecycle(StrEnum):
    FORMING = "forming"
    ACTIVE = "active"
    ENDED = "ended"


class TransitionResolution(StrEnum):
    UNRESOLVED = "unresolved"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    EXPIRED = "expired"
