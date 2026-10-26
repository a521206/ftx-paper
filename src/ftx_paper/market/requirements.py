"""Neutral market feature requirement contracts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FeatureRequirement:
    name: str
    timeframe: str | None = None
    required: bool = True

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("feature requirement name is required")
        if self.timeframe is not None and not self.timeframe.strip():
            raise ValueError("feature requirement timeframe cannot be blank")


def feature_union(policies: Sequence[object]) -> tuple[FeatureRequirement, ...]:
    result: dict[tuple[str, str | None], FeatureRequirement] = {}
    for policy in policies:
        for requirement in getattr(policy, "required_features", ()):
            key = (requirement.name, requirement.timeframe)
            previous = result.get(key)
            if previous is None or (not previous.required and requirement.required):
                result[key] = requirement
    return tuple(result.values())


__all__ = ["FeatureRequirement", "feature_union"]
