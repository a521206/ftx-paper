"""Causal session and opening-gap policy helpers."""

from __future__ import annotations

from typing import Any

from .opening_gap_policy import OpeningGapPolicy
from .orderflow_common import Aggregate
from .state_machine_types import LocationRegistry


def classify_opening_gap(
    *,
    opening: Aggregate,
    registry: LocationRegistry | None,
    prior_value: Any = None,
) -> dict[str, Any]:
    """Classify an opening gap from references available at the opening."""
    if registry is None and prior_value is None:
        return {}
    available_at = opening.available_at
    prior_low_record = (
        registry.resolve_as_of("PD_LOW", available_at) if registry is not None else None
    )
    prior_high_record = (
        registry.resolve_as_of("PD_HIGH", available_at) if registry is not None else None
    )
    reference = getattr(prior_value, "reference", None)
    return OpeningGapPolicy().classify(
        opening={"time": opening.time, "open": opening.open},
        prior_low=prior_low_record.price if prior_low_record is not None else None,
        prior_high=prior_high_record.price if prior_high_record is not None else None,
        prior_value_low=getattr(reference, "value_area_low", None),
        prior_value_high=getattr(reference, "value_area_high", None),
        available_at=available_at,
    )


__all__ = ["classify_opening_gap"]
