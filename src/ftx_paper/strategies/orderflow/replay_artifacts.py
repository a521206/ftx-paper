"""Value objects for replay artifact projection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class InteractionResponseReplay:
    """Projected owner facts retained for parity artifact consumers."""

    facts: tuple[dict[str, Any], ...]
    opportunity_facts: tuple[dict[str, Any], ...] = ()
    formations: tuple[Any, ...] = ()
    regime_publications: tuple[tuple[str, Any], ...] = ()
    context_publications: tuple[dict[str, Any], ...] = ()
    execution_phase_facts: dict[str, tuple[dict[str, Any], ...]] = field(default_factory=dict)
    parity_layers: dict[str, tuple[dict[str, Any], ...]] = field(default_factory=dict)
    execution_diagnostics: tuple[dict[str, Any], ...] = ()
    replay_records: tuple[dict[str, Any], ...] = ()

    def parity_phase_facts(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "interaction_response": [dict(item) for item in self.facts],
            "opportunity_lifecycle": [
                dict(item) for item in self.opportunity_facts
                if item.get("kind") == "lifecycle_formed"
            ],
        }


__all__ = ["InteractionResponseReplay"]
