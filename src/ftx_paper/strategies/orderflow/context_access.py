"""Decision-context access used by orderflow execution policy."""

from __future__ import annotations

from typing import Any

from .hierarchy import classify_context_relation
from .timestamps import coerce_timestamp


class DecisionContextAccess:
    """Resolve causal context without exposing the transition owner to policy."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    @property
    def context(self) -> Any:
        return self.engine.context

    def for_life(self, life: Any, as_of: str) -> Any | None:
        return self.for_location(getattr(life, "location", None), as_of=as_of)

    def for_location(self, location: str | None, *, as_of: str) -> Any | None:
        store = self.context.market_contexts
        clock = coerce_timestamp(as_of)
        if location is not None:
            candidates = []
            for interaction_id in store.interaction_contexts:
                interaction = self.context.interactions.get(interaction_id)
                if (
                    interaction is None
                    or interaction.location != location
                    or interaction.timeframe != "5m"
                ):
                    continue
                candidate = store.latest_as_of(f"interaction:{interaction_id}", clock)
                if candidate is not None:
                    candidates.append(candidate)
            if candidates:
                return max(candidates, key=lambda item: item.available_at)

        reference = store.ordinary_context
        ordinary = store.resolve(reference) if reference is not None else None
        if (
            ordinary is not None
            and ordinary.available_at <= clock
            and (location is None or ordinary.location in (None, location))
        ):
            return ordinary
        return None

    def scores_for(self, reference: Any | None) -> tuple[float | None, float | None]:
        context = self.context.context(reference) if reference is not None else None
        return (
            getattr(context, "long_score", None),
            getattr(context, "short_score", None),
        )

    def relation_for(self, life: Any, context: Any) -> Any:
        observation = self.context.child_observation
        evidence = (
            observation.evidence
            if observation is not None
            and observation.context_id == context.context_id
            else {}
        )
        return classify_context_relation(
            parent=context,
            location=(
                getattr(life, "location", None)
                or getattr(context, "location", None)
            ),
            direction=getattr(getattr(life, "direction", None), "value", None),
            evidence=evidence,
        )

    def record_exit(self, life: Any, rule: Any, *, timestamp: str, reason: str) -> Any | None:
        reference = self.for_location(
            getattr(rule, "location", None), as_of=timestamp,
        )
        self.context.record_decision_context({
            "lifecycle_id": life.lifecycle_id,
            "rule_id": getattr(rule, "rule_id", None),
            "observed_at": timestamp,
            "reason": reason,
        })
        return getattr(reference, "reference", None)
