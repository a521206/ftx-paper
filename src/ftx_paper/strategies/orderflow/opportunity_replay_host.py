"""Wire opportunity coordination to explicit Paper state ports."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .hierarchy import MarketContext
from .interaction_detector import InteractionDetector
from .interactions import InteractionSnapshot
from .opportunity_coordinator import OpportunityCoordinator, OpportunityReplayHost
from .state_machine_types import LocationRegistry, OrderflowState
from .templates import DEFAULT_TEMPLATES, TradeTemplate
from .timestamps import coerce_timestamp


class OrderflowOpportunityHost:
    """Explicit opportunity state and policy ports without an engine reference."""

    def __init__(
        self,
        *,
        context: OrderflowState,
        execution: Any,
        location_registry: LocationRegistry,
        detector: InteractionDetector,
        templates: Mapping[str, TradeTemplate] | None = None,
    ) -> None:
        self._context = context
        self._execution = execution
        self._location_registry = location_registry
        self._detector = detector
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
        return tuple(
            state.evaluation_snapshot()
            for state in self._detector.interactions.values()
            if state.timeframe == "5m"
        )

    def sync_interaction_snapshot(self, interaction: InteractionSnapshot) -> None:
        detector_state = self._detector.interactions_for(
            interaction.timeframe,
        ).get(interaction.interaction_id)
        self._context.interactions[interaction.interaction_id] = (
            detector_state.evaluation_snapshot()
            if detector_state is not None else interaction
        )

    def selected_interaction_context(
        self, interaction_id: str, *, as_of: datetime | str,
    ) -> MarketContext | None:
        available_at = coerce_timestamp(as_of)
        return max(
            (
                item for item in self._context.market_contexts.versions.get(
                    f"interaction:{interaction_id}", (),
                )
                if item.available_at <= available_at
            ),
            key=lambda item: item.available_at,
            default=None,
        )


def build_opportunity_coordinator(
    *,
    context: OrderflowState,
    execution: Any,
    location_registry: LocationRegistry,
    detector: InteractionDetector,
    templates: Mapping[str, TradeTemplate] | None = None,
) -> OpportunityCoordinator:
    host: OpportunityReplayHost = OrderflowOpportunityHost(
        context=context,
        execution=execution,
        location_registry=location_registry,
        detector=detector,
        templates=templates,
    )
    return OpportunityCoordinator(host)


__all__ = ["OrderflowOpportunityHost", "build_opportunity_coordinator"]
