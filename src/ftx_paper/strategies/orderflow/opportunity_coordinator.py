"""Canonical opportunity formation after interaction response arbitration."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from collections.abc import Mapping
from typing import Any
from datetime import datetime

from .interactions import InteractionSnapshot
from .contextual import build_context_scores
from .location_geometry import LocationBand
from .opportunity import (
    DirectionalResponse,
    OpportunityOutcome,
    OpportunityOutcomeStatus,
    OpportunityContext,
    form_opportunity,
    opportunity_identity,
)
from .orderflow_common import Aggregate
from .policies.base import HypothesisEvaluation
from .state_machine_types import LocationRegistry
from .templates import Direction, TradeTemplate
from .timestamps import nifty_label, serialize_utc


@dataclass(frozen=True, slots=True)
class LifecycleFormation:
    """Canonical lifecycle values ready for the parity fact envelope."""

    identity: str
    lifecycle_id: str
    values: dict[str, Any]
    observed_at: Any
    available_at: Any


@dataclass(slots=True)
class OpportunityCoordinator:
    """Own idempotent lifecycle formation, not response policy or execution."""

    setup_number: int = 0
    fill_permission_margin: float | None = 0.05
    _formed_identities: set[str] = field(default_factory=set, init=False, repr=False)
    _opening_seed_sessions: set[str] = field(default_factory=set, init=False, repr=False)
    _opening_seed_candles: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        self._formed_identities.clear()

    def form_resolved(
        self,
        *,
        interaction: InteractionSnapshot,
        evaluation: HypothesisEvaluation,
        outcome: OpportunityOutcome,
        as_of: Any,
        aggregates: Mapping[str, list[Aggregate]],
        atr: float | None,
    ) -> LifecycleFormation | None:
        response = outcome.response
        identity = evaluation.identity
        if (
            outcome.status is not OpportunityOutcomeStatus.RESOLVED
            or response is None
            or identity is None
            or identity in self._formed_identities
            or str(interaction.creation_candle.time) in self._opening_seed_candles
            or (
                isinstance(interaction.creation_candle.time, datetime)
                and nifty_label(interaction.creation_candle.time) in self._opening_seed_candles
            )
        ):
            return None

        score_candle = max(
            aggregates.get("5m", ()),
            key=lambda item: item.available_at,
            default=interaction.creation_candle,
        )
        scores = build_context_scores(
            score_candle,
            raw_rows=(),
            aggregates=dict(aggregates),
            atr=atr,
        )
        if scores is None:
            return None
        long_score, short_score = scores
        own_score = long_score if response.direction is Direction.LONG else short_score
        opposing_score = short_score if response.direction is Direction.LONG else long_score
        if (
            self.fill_permission_margin is not None
            and own_score - opposing_score < self.fill_permission_margin
        ):
            return None
        if not evaluation.selected:
            # Canonical allocates candidate numbers before admission arbitration,
            # but only publishes the selected lifecycle.
            self.setup_number += 1
            return None

        template = evaluation.template
        if template.direction is not response.direction:
            if interaction.location not in {"PD_LOW", "PD_HIGH"}:
                return None
            template = replace(template, direction=Direction(response.direction))

        self.setup_number += 1
        candidate_id = (
            f"{interaction.session_date}-{interaction.instrument}-1m_5m-"
            f"{response.direction.value}-candidate-{self.setup_number}"
        )
        context_candle = (
            response.evidence[-2]
            if len(response.evidence) > 1
            else interaction.creation_candle
        )
        lifecycle, setup = form_opportunity(
            response,
            OpportunityContext(
                lifecycle_id=f"lifecycle-{candidate_id}",
                session_date=interaction.session_date,
                instrument=interaction.instrument,
                strategy="orderflow-1m_5m",
                candidate_id=candidate_id,
                context_candle=context_candle,
                clock=as_of,
                regime_version="unresolved",
                policy_version=outcome.policy_version or template.policy_version,
                reference_generation=interaction.reference_generation,
                causal_identity=identity,
                location_interaction_id=interaction.interaction_id,
            ),
            template,
            formation_available_at=serialize_utc(as_of),
        )
        values = {
            **lifecycle.to_dict(),
            "kind": "lifecycle_formed",
            "template_id": template.template_id,
            "location": interaction.location,
            "level": "5m",
            "direction": response.direction.value,
            "candidate_id": candidate_id,
            "causal_identity": identity,
            "session_date": lifecycle.session_date,
            "instrument": lifecycle.instrument,
            "strategy": lifecycle.strategy,
            "formation_at": lifecycle.formation_at,
            "formation_available_at": lifecycle.formation_available_at,
            "response_episode_id": lifecycle.response_episode_id,
            "execution_group_id": lifecycle.execution_group_id,
            "long_score": long_score,
            "short_score": short_score,
            "opening_score_override": False,
            "decision_context_required": lifecycle.decision_context_required,
            "reference_level": lifecycle.reference_level,
            "entry_boundary": setup.entry_zone_low,
            "failure_boundary": lifecycle.failure_boundary,
            "context_candle_id": context_candle.time,
            "context_candle_available_at": context_candle.available_at,
            "response_candle_id": response.evidence_ids[-1],
            "response_observed_at": response.observed_at,
            "response_available_at": response.available_at,
            "response_evidence_ids": list(response.evidence_ids),
            "location_interaction_id": interaction.interaction_id,
        }
        self._formed_identities.add(identity)
        return LifecycleFormation(
            identity,
            lifecycle.lifecycle_id,
            values,
            response.observed_at,
            response.available_at,
        )

    def form_opening_seed(
        self,
        *,
        candle: Aggregate,
        registry: LocationRegistry,
        templates: Mapping[tuple[str, str], TradeTemplate],
        aggregates: Mapping[str, list[Aggregate]],
        atr: float | None,
        opening_gap_direction: str | None,
        prior_volume: tuple[float, ...],
    ) -> tuple[dict[str, Any], LifecycleFormation] | None:
        """Form only Canonical's explicit first-candle OR acceptance seed."""
        if candle.session_date in self._opening_seed_sessions:
            return None
        # Canonical evaluates the first completed 5m candle exactly once.
        self._opening_seed_sessions.add(candle.session_date)
        if candle.open == candle.close:
            return None
        direction = Direction.LONG if candle.close > candle.open else Direction.SHORT
        expected_gap = "down" if direction is Direction.LONG else "up"
        candle_range = candle.high - candle.low
        opening_signature = False
        if candle_range > 0 and prior_volume:
            body = abs(candle.close - candle.open)
            rejection_wick = (
                candle.open - candle.low
                if direction is Direction.LONG
                else candle.high - candle.open
            )
            five_minute_average = sum(prior_volume) / len(prior_volume) * 5
            opening_signature = (
                opening_gap_direction == expected_gap
                and body / candle_range >= 0.35
                and rejection_wick / candle_range >= 0.25
                and candle.volume >= five_minute_average * 2.0
            )
        scores = build_context_scores(
            candle,
            raw_rows=(),
            aggregates=dict(aggregates),
            atr=atr,
        )
        if scores is None:
            return None
        long_score, short_score = scores
        own_score = long_score if direction is Direction.LONG else short_score
        opposing_score = short_score if direction is Direction.LONG else long_score
        opening_score_override = False
        if own_score <= opposing_score:
            if not opening_signature or self.fill_permission_margin is None:
                return None
            if direction is Direction.LONG:
                long_score = max(long_score, short_score + self.fill_permission_margin)
                own_score = long_score
            else:
                short_score = max(short_score, long_score + self.fill_permission_margin)
                own_score = short_score
            opening_score_override = True
        if own_score <= opposing_score:
            return None
        location = "OR_HIGH" if direction is Direction.LONG else "OR_LOW"
        record = registry.resolve_as_of(location, candle.available_at)
        template = templates.get(("5m_acceptance_breakout", location))
        if record is None or template is None:
            return None
        self._opening_seed_sessions.add(candle.session_date)
        self._opening_seed_candles.add(str(candle.time))
        self._opening_seed_candles.add(nifty_label(candle.time))
        seed_id = (
            f"OR_SEED:{candle.session_date}:{candle.symbol}:{record.generation}:"
            f"{direction.value}:{nifty_label(candle.time)}"
        )
        response = DirectionalResponse(
            direction=direction,
            anchor=location,
            reference_level=record.price,
            structure_boundary=record.price,
            invalidation_boundary=(
                record.band.high if direction is Direction.SHORT else record.band.low
            ),
            entry_boundary=record.price,
            evidence_ids=(seed_id, nifty_label(candle.time)),
            observed_at=candle.end_time,
            available_at=candle.available_at,
            band=LocationBand(record.band.low, record.band.high),
            evidence=(candle,),
        )
        identity = opportunity_identity(
            session_date=candle.session_date,
            instrument=candle.symbol,
            direction=direction,
            interaction_id=seed_id,
            reference_generation=record.generation,
            template_id=template.template_id,
            response_evidence_ids=response.evidence_ids,
            formation_at=response.observed_at,
            formation_available_at=response.available_at,
        )
        self.setup_number += 1
        candidate_id = (
            f"{candle.session_date}-{candle.symbol}-1m_5m-{direction.value}-"
            f"candidate-{self.setup_number}"
        )
        opening_policy = (
            "opening_retest"
            if (
                direction is Direction.SHORT and candle.close <= record.price
            ) or (
                direction is Direction.LONG and candle.close >= record.price
            )
            else "opening_breakout"
        )
        lifecycle, setup = form_opportunity(
            response,
            OpportunityContext(
                lifecycle_id=f"lifecycle-{candidate_id}",
                session_date=candle.session_date,
                instrument=candle.symbol,
                strategy="orderflow-1m_5m",
                candidate_id=candidate_id,
                context_candle=candle,
                clock=candle.available_at,
                regime_version="unresolved",
                policy_version=template.policy_version,
                reference_generation=record.generation,
                causal_identity=identity,
                opening_policy_version=f"{opening_policy}-1m-v1",
                response_episode_id=f"opening-response-episode:{seed_id}",
            ),
            template,
            formation_available_at=serialize_utc(candle.available_at),
            decision_context_required=False,
        )
        values = {
            **lifecycle.to_dict(),
            "kind": "lifecycle_formed",
            "template_id": template.template_id,
            "location": location,
            "level": "5m",
            "direction": direction.value,
            "candidate_id": candidate_id,
            "causal_identity": identity,
            "opening_policy_version": f"{opening_policy}-1m-v1",
            "opening_interaction_kind": None,
            "long_score": long_score,
            "short_score": short_score,
            "opening_score_override": opening_score_override,
            "reference_level": record.price,
            "entry_boundary": setup.entry_zone_low,
            "failure_boundary": response.invalidation_boundary,
            "context_candle_id": candle.time,
            "context_candle_available_at": candle.available_at,
            "response_candle_id": candle.time,
            "response_observed_at": response.observed_at,
            "response_available_at": response.available_at,
            "response_evidence_ids": list(response.evidence_ids),
            "response_episode_id": lifecycle.response_episode_id,
            "execution_group_id": lifecycle.execution_group_id,
        }
        formation = LifecycleFormation(
            identity, lifecycle.lifecycle_id, values,
            response.observed_at, response.available_at,
        )
        outcome = {
            "interaction_id": seed_id,
            "location": location,
            "template_id": template.template_id,
            "response_type": template.response_type.value,
            "direction": direction.value,
            "status": "RESOLVED",
            "reason": "opening_candle_directional_seed",
            "evidence_ids": list(response.evidence_ids),
        }
        self._formed_identities.add(identity)
        return outcome, formation


def opening_gap_direction(candle: Aggregate, registry: LocationRegistry) -> str:
    """Classify the opening candle against causally available prior extremes."""
    prior_low = registry.resolve_as_of("PD_LOW", candle.available_at)
    prior_high = registry.resolve_as_of("PD_HIGH", candle.available_at)
    if prior_low is not None and candle.open < prior_low.price:
        return "down"
    if prior_high is not None and candle.open > prior_high.price:
        return "up"
    return "internal"


__all__ = ["LifecycleFormation", "OpportunityCoordinator", "opening_gap_direction"]
