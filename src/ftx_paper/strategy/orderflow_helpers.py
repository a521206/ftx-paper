"""Pure orderflow parsing, scoring adapters, timestamp, and ID helpers."""

from __future__ import annotations

from datetime import datetime
import hashlib

from .orderflow_contract import FeatureValue, OrderflowObservation


def feature_number(value: FeatureValue) -> float | None:
    """Convert an available numeric feature without masking unavailable values."""
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def observation_number(observation: OrderflowObservation, name: str) -> float | None:
    return feature_number(observation.features.get(name))


def volume_average(observations: list[OrderflowObservation]) -> float | None:
    values = [feature_number(item.features.get("volume")) for item in observations]
    numeric_values = [value for value in values if value is not None]
    if len(numeric_values) != len(values):
        return None
    return sum(numeric_values) / len(numeric_values) if numeric_values else None


def score_rejection(
    *, direction: str, long_score: float | None, short_score: float | None,
    margin: float | None,
) -> str | None:
    """Apply the canonical permission margin to a complete score pair."""
    if margin is None:
        return None
    if long_score is None or short_score is None:
        return "setup_scores_unavailable"
    own = long_score if direction == "long" else short_score
    opposing = short_score if direction == "long" else long_score
    return "fill_permission_mismatch" if own - opposing < margin else None


def opening_gap_context_available_at(observation: OrderflowObservation) -> datetime:
    return observation.field_available_at("open")


def decision_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()[:24]


__all__ = [
    "decision_id", "feature_number", "observation_number", "opening_gap_context_available_at",
    "score_rejection", "volume_average",
]
