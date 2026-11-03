"""Wire canonical opportunity coordination to the Paper replay transition owner."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from .hierarchy import MarketContext
from .interaction_detector import InteractionDetector
from .interactions import InteractionSnapshot
from .opportunity_coordinator import OpportunityCoordinator, OpportunityReplayHost
from .state_machine_types import LocationRegistry
from .templates import DEFAULT_TEMPLATES, TradeTemplate
from .timestamps import coerce_timestamp
from .transition import OrderflowTransitionOwner


class OrderflowOpportunityHost:
    """Canonical replay host: context, execution, and structure interactions."""

    def __init__(
        self,
        transition: OrderflowTransitionOwner,
        *,
        detector: InteractionDetector,
        templates: Mapping[str, TradeTemplate] | None = None,
    ) -> None:
        self._transition = transition
        self._detector = detector
        self._templates = dict(templates or DEFAULT_TEMPLATES)

    @property
    def context(self):
        return self._transition.context

    @property
    def execution(self):
        return self._transition.execution_owner

    @property
    def location_registry(self) -> LocationRegistry:
        return self._transition.location_registry

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

    def selected_interaction_context(
        self, interaction_id: str, *, as_of: datetime | str,
    ) -> MarketContext | None:
        available_at = coerce_timestamp(as_of)
        return max(
            (
                item for item in self.context.market_contexts.versions.get(
                    f"interaction:{interaction_id}", (),
                )
                if item.available_at <= available_at
            ),
            key=lambda item: item.available_at,
            default=None,
        )


def build_opportunity_coordinator(
    transition: OrderflowTransitionOwner,
    *,
    detector: InteractionDetector,
    templates: Mapping[str, TradeTemplate] | None = None,
) -> OpportunityCoordinator:
    host: OpportunityReplayHost = OrderflowOpportunityHost(
        transition, detector=detector, templates=templates,
    )
    return OpportunityCoordinator(host)


__all__ = [
    "OrderflowOpportunityHost",
    "build_opportunity_coordinator",
]
