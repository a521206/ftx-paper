"""Minimal canonical source/aggregate types required by Layer 1."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SessionLevels:
    opening_range_low: float
    opening_range_high: float
    prior_day_low: float | None
    prior_day_high: float | None
    opening_range_available_at: str
    prior_day_available_at: str
