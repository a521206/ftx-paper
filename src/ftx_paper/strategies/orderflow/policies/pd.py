from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from collections.abc import Callable

from ..interactions import InteractionSnapshot, LocationInteraction, candle_identity
from ..opportunity import DirectionalResponse, OpportunityOutcome, OpportunityOutcomeStatus, opportunity_identity
from ..orderflow_common import Aggregate
from ..response import ResponseCandle, build_response_spec, evaluate_response_hypotheses
from ..templates import Direction, ResponseType, TradeTemplate
from .base import HypothesisEvaluation


class PDPolicy:
    """Canonical prior-day dispatch with dynamic role and arbitration."""

    def evaluate_hypotheses(self, templates: Sequence[TradeTemplate], interaction: InteractionSnapshot,
                            bars: Sequence[Aggregate], *, as_of: str,
                            state: LocationInteraction | None = None,
                            emit: Callable[[dict[str, Any]], None] | None = None) -> tuple[HypothesisEvaluation, ...]:
        ordered = tuple(sorted(templates, key=lambda item: item.template_id))
        if not ordered:
            return ()
        role = state.pd_role if state and state.pd_role else ("SUPPORT" if interaction.location == "PD_LOW" else "RESISTANCE")
        eligible = self._eligible(interaction, bars, as_of, expiry_at=ordered[0].expiry_at)

        def reset_before(bar: Aggregate) -> bool:
            nonlocal role
            next_role = self._strict_role(interaction, bar)
            if next_role is None or next_role == role:
                return False
            role = next_role
            if state is not None:
                state.pd_role = role
                state.pd_role_transition_candle_id = candle_identity(bar)
                state.pd_role_transition_available_at = bar.available_at
            if emit is not None:
                emit({
                    "kind": "role_transition", "interaction_id": interaction.interaction_id,
                    "location": interaction.location, "role": role,
                    "candle_id": candle_identity(bar), "observed_at": bar.end_time,
                    "available_at": bar.available_at,
                })
            return True

        results = evaluate_response_hypotheses(
            eligible, ordered,
            to_candle=lambda bar: ResponseCandle(bar.open, bar.high, bar.low, bar.close),
            spec_for=lambda template, _bar: build_response_spec(
                template,
                direction=(Direction.LONG if role == "SUPPORT" else Direction.SHORT)
                if template.response_type is ResponseType.REJECTION
                else (Direction.SHORT if role == "SUPPORT" else Direction.LONG),
                band=interaction.band,
            ), armed=False, pre_contact_side=interaction.pre_contact_side,
            reset_before=reset_before,
            invalidation_allowed=lambda spec, bar: spec.response_type is ResponseType.ACCEPTANCE and spec.touches(high=bar.high, low=bar.low),
        )
        evaluations = tuple(
            HypothesisEvaluation(template, self._outcome(template, interaction, result, as_of),
                                 self._identity(template, interaction, result))
            for template, result in zip(ordered, results)
        )
        resolved = [item for item in evaluations if item.outcome.status is OpportunityOutcomeStatus.RESOLVED and item.outcome.response]
        winner = min(resolved, key=lambda item: (
            item.outcome.response.available_at if item.outcome.response else "",
            item.outcome.response.observed_at if item.outcome.response else "",
            item.template.template_id,
        ), default=None)
        return tuple(HypothesisEvaluation(item.template, item.outcome, item.identity, item is winner,
                                          "earliest_causal_pd_response" if item is winner else "same_interaction_execution_arbitration" if item.outcome.response else None)
                     for item in evaluations)

    def arbitrate(self, templates: Sequence[TradeTemplate], interaction: InteractionSnapshot,
                  bars: Sequence[Aggregate], *, as_of: str, state: LocationInteraction | None = None):
        selected = next((item for item in self.evaluate_hypotheses(templates, interaction, bars, as_of=as_of, state=state) if item.selected), None)
        return None if selected is None else (selected.template, selected.outcome, selected.identity)

    @staticmethod
    def _eligible(interaction: InteractionSnapshot, bars: Sequence[Aggregate], as_of: str, *, expiry_at: str | None = None) -> tuple[Aggregate, ...]:
        eligible = tuple(sorted((bar for bar in bars if bar.timeframe == "5m" and bar.symbol == interaction.instrument and bar.available_at <= as_of and bar.available_at >= bar.end_time and bar.available_at > interaction.available_at and bar.session_date == interaction.session_date and bar.time > interaction.creation_candle.time and (expiry_at is None or bar.available_at <= f"{interaction.session_date}T{expiry_at}")), key=lambda bar: (bar.time, bar.available_at)))
        return tuple(bar for bar in eligible if interaction.terminal_candle_id is None or bar.time <= interaction.terminal_candle_id)

    @staticmethod
    def _strict_role(interaction: InteractionSnapshot, bar: Aggregate) -> str | None:
        if bar.low > interaction.band_high:
            return "SUPPORT"
        if bar.high < interaction.band_low:
            return "RESISTANCE"
        return None

    @staticmethod
    def _outcome(template: TradeTemplate, interaction: InteractionSnapshot, evaluation: Any, as_of: str) -> OpportunityOutcome:
        status = (OpportunityOutcomeStatus.RESOLVED if evaluation.status == "CONFIRMED"
                  else OpportunityOutcomeStatus.CONTRADICTED if evaluation.status == "INVALIDATED"
                  else OpportunityOutcomeStatus.PENDING)
        evidence = tuple(evaluation.evidence)
        latest = evaluation.confirmation_candle or (evidence[-1] if evidence else interaction.creation_candle)
        response = None
        if status is OpportunityOutcomeStatus.RESOLVED:
            direction = evaluation.direction
            if direction is None or evaluation.confirmation_candle is None:
                raise RuntimeError("confirmed PD response is missing metadata")
            bar = evaluation.confirmation_candle
            response = DirectionalResponse(
                direction, interaction.location, interaction.reference_level,
                bar.high if direction is Direction.LONG else bar.low,
                interaction.band.low if direction is Direction.LONG else interaction.band.high,
                interaction.band.high if direction is Direction.LONG else interaction.band.low,
                (interaction.interaction_id, bar.time), bar.end_time, bar.available_at,
                interaction.band, evidence,
            )
        return OpportunityOutcome(status, f"causal_{template.response_type.value}" if response else
                                 "reference_crossed_after_contact" if status is OpportunityOutcomeStatus.CONTRADICTED else
                                 "response_window_open", latest.end_time, latest.available_at, evidence,
                                 response, policy_version="pd-dynamic-response-spec-v1", template=template, as_of=as_of)

    @staticmethod
    def _identity(template: TradeTemplate, interaction: InteractionSnapshot, evaluation: Any) -> str | None:
        if evaluation.status != "CONFIRMED" or evaluation.confirmation_candle is None or evaluation.direction is None:
            return None
        bar = evaluation.confirmation_candle
        return opportunity_identity(session_date=interaction.session_date, instrument=interaction.instrument,
                                    direction=evaluation.direction, interaction_id=interaction.interaction_id,
                                    reference_generation=interaction.reference_generation,
                                    response_evidence_ids=(interaction.interaction_id, bar.time),
                                    formation_at=bar.end_time, formation_available_at=bar.available_at,
                                    template_id=template.template_id)
