from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence

from ..opportunity import OpportunityOutcome, identify_opportunity
from ..interactions import InteractionSnapshot
from ..orderflow_common import Aggregate
from ..templates import TradeTemplate


@dataclass(frozen=True, slots=True)
class HypothesisEvaluation:
    template: TradeTemplate
    outcome: OpportunityOutcome
    identity: str | None
    selected: bool = False
    arbitration_reason: str | None = None


class LocationPolicy:
    @staticmethod
    def templates_for(templates: Sequence[TradeTemplate], interaction: InteractionSnapshot) -> tuple[TradeTemplate, ...]:
        return tuple(
            template for template in templates
            if interaction.location in {location.value for location in template.locations}
            and template.response_timeframe == interaction.timeframe
        )

    def identify(self, template: TradeTemplate, interaction: InteractionSnapshot,
                 bars: Sequence[Aggregate], *, as_of: str) -> tuple[OpportunityOutcome, str | None]:
        raise NotImplementedError


class StandardLocationPolicy(LocationPolicy):
    def identify(self, template: TradeTemplate, interaction: InteractionSnapshot,
                 bars: Sequence[Aggregate], *, as_of: str) -> tuple[OpportunityOutcome, str | None]:
        return identify_opportunity(template, interaction, bars, as_of=as_of)

    def arbitrate(self, templates: Sequence[TradeTemplate], interaction: InteractionSnapshot,
                  bars: Sequence[Aggregate], *, as_of: str) -> tuple[HypothesisEvaluation, ...]:
        evaluations = tuple(
            HypothesisEvaluation(template, *self.identify(template, interaction, bars, as_of=as_of))
            for template in sorted(templates, key=lambda item: item.template_id)
        )
        resolved = [item for item in evaluations if item.outcome.response is not None and item.outcome.status.value == "RESOLVED"]
        if not resolved:
            return evaluations
        latest = max(item.outcome.response.available_at for item in resolved if item.outcome.response)
        winner = min((item for item in resolved if item.outcome.response and item.outcome.response.available_at == latest),
                     key=lambda item: (item.template.response_type.value != "rejection", item.template.template_id))
        return tuple(HypothesisEvaluation(item.template, item.outcome, item.identity, item is winner,
                                          "same_time_rejection_precedence" if item is winner else "same_interaction_execution_arbitration")
                     for item in evaluations)
