"""Wire canonical opportunity coordination to the Paper replay transition owner."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .hierarchy import MarketContext
from .interaction_detector import InteractionDetector
from .interactions import InteractionSnapshot
from .opportunity_coordinator import OpportunityCoordinator, OpportunityReplayHost
from .state_machine_types import LocationRegistry
from .templates import DEFAULT_TEMPLATES, TradeTemplate
from .timestamps import coerce_timestamp, serialize_utc
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
            for state in reversed(tuple(self._detector.interactions.values()))
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


def bar_events_to_facts(
    events: list[dict[str, Any]],
    *,
    session_date: str,
) -> list[dict[str, Any]]:
    """Project canonical bar events onto parity fact envelopes."""
    facts: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, Mapping):
            continue
        observed = event.get("observed_at", event.get("time"))
        available = event.get("available_at", observed)
        if observed is None or available is None:
            continue
        parent = (
            event.get("interaction_id")
            or event.get("causal_parent_id")
            or event.get("lifecycle_id")
        )
        identity = str(event.get("neutral_event_key", event.get("event_id", "")))
        values = {
            key: value
            for key, value in event.items()
            if key not in {
                "kind", "observed_at", "available_at", "causal_observed_at",
                "causal_available_at", "event_id", "sequence", "neutral_event_key",
            }
        }
        facts.append({
            "kind": str(event.get("kind", "")),
            "occurred_at": serialize_utc(observed) if not isinstance(observed, str) else observed,
            "observed_at": serialize_utc(observed) if not isinstance(observed, str) else observed,
            "available_at": serialize_utc(available) if not isinstance(available, str) else available,
            "causal_observed_at": event.get("causal_observed_at", observed),
            "causal_available_at": event.get("causal_available_at", available),
            "policy_version": event.get("policy_version", "orderflow-v1"),
            "provenance": event.get("provenance", "causal_replay"),
            "session_date": event.get("session_date", session_date),
            "identity": identity,
            "causal_parent_id": parent,
            "values": values,
            "unavailable": [],
        })
    return facts


__all__ = [
    "OrderflowOpportunityHost",
    "build_opportunity_coordinator",
    "bar_events_to_facts",
]
