"""Wire opportunity coordination to explicit Paper state ports."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from .hierarchy import MarketContext
from .interaction_detector import InteractionDetector
from .interactions import InteractionSnapshot
from .opportunity_coordinator import OpportunityCoordinator, OpportunityReplayHost
from .state_machine_types import LocationRegistry, OrderflowState
from .templates import DEFAULT_TEMPLATES, TradeTemplate


class OrderflowOpportunityHost:
    """Explicit opportunity state and policy ports without an engine reference."""

    def __init__(
        self,
        *,
        context: OrderflowState,
        execution: Any,
        location_registry: LocationRegistry,
        detector: InteractionDetector,
        selected_context: Callable[..., MarketContext | None],
        structure_interactions: Callable[[], tuple[InteractionSnapshot, ...]],
        sync_interaction: Callable[[InteractionSnapshot], None],
        templates: Mapping[str, TradeTemplate] | None = None,
    ) -> None:
        self._context = context
        self._execution = execution
        self._location_registry = location_registry
        self._detector = detector
        self._selected_context = selected_context
        self._structure_interactions = structure_interactions
        self._sync_interaction = sync_interaction
        self._templates = dict(templates or DEFAULT_TEMPLATES)

    @property
    def context(self) -> OrderflowState:
        return self._context

    @property
    def execution(self) -> Any:
        return self._execution

    @property
    def location_registry(self) -> LocationRegistry:
        return self._location_registry

    @property
    def templates(self) -> dict[str, TradeTemplate]:
        return self._templates

    @property
    def interaction_detector(self) -> InteractionDetector:
        return self._detector

    def structure_interactions(self) -> tuple[InteractionSnapshot, ...]:
        return self._structure_interactions()

    def sync_interaction_snapshot(self, interaction: InteractionSnapshot) -> None:
        self._sync_interaction(interaction)

    def selected_interaction_context(
        self, interaction_id: str, *, as_of: datetime | str,
    ) -> MarketContext | None:
        return self._selected_context(interaction_id, as_of=as_of)


def build_opportunity_coordinator(
    *,
    context: OrderflowState,
    execution: Any,
    location_registry: LocationRegistry,
    detector: InteractionDetector,
    selected_context: Callable[..., MarketContext | None],
    structure_interactions: Callable[[], tuple[InteractionSnapshot, ...]],
    sync_interaction: Callable[[InteractionSnapshot], None],
    templates: Mapping[str, TradeTemplate] | None = None,
) -> OpportunityCoordinator:
    host: OpportunityReplayHost = OrderflowOpportunityHost(
        context=context,
        execution=execution,
        location_registry=location_registry,
        detector=detector,
        selected_context=selected_context,
        structure_interactions=structure_interactions,
        sync_interaction=sync_interaction,
        templates=templates,
    )
    return OpportunityCoordinator(host)


__all__ = ["OrderflowOpportunityHost", "build_opportunity_coordinator"]
