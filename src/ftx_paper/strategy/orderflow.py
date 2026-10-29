"""Diagnostic orderflow strategy for causal location observations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from typing import Any

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.contracts import MarketBar, MarketRole, OrderIntent
from ftx_paper.market.bundles import DecisionBundle

from .coordinator import SharedDecisionCoordinator
from .orderflow_contract import OrderflowContext, OrderflowObservation, feature_values_to_dict
from .orderflow_context import (
    ContextHistory, MarketContext, ContextVersionRef, OpeningGapState, SessionLevels,
    advance_opening_gap, classify_context_relation, classify_opening_gap,
)
from .orderflow_opportunity import (
    OpportunityStatus, SetupOpportunity,
)
from .opportunity_timeout import advance_opportunity_timeout
from .orderflow_admission import evaluate_admission
from .orderflow_interaction import (
    EvidenceLineage, HypothesisState, InteractionCloseReason, InteractionDetector,
    InteractionStatus, LocationInteraction,
)
from .orderflow_location import (
    LocationBand, LocationObservation, LocationRecord, LocationRegistry,
    is_diagnostic_location,
)
from .orderflow_response import evaluate_policy_responses
from .templates import TRADE_TEMPLATES, TradeTemplate
from .orderflow_vwap import evaluate_vwap_diagnostic_response
from .orderflow_trigger import observe_trigger
from .policy_contract import ExecutionMode, FeatureRequirement, PolicyDecision
from .protocol import StrategyMetadata
from .snapshot_envelope import wrap_snapshot
from .exits import ExitAction
from .orderflow_position_manager import OrderflowPositionManager
from .orderflow_evidence import OrderflowEvidenceAssembler

from .orderflow_codec import policy_snapshot_payload, strategy_snapshot_payload
from .orderflow_snapshot import policy_from_snapshot, strategy_from_snapshot
from .orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig
from .orderflow_helpers import (
    decision_id as _decision_id,
    observation_number as _number,
    opening_gap_context_available_at,
    score_rejection as _score_rejection,
)


class OrderflowPolicy:
    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.config = config
        self._session_id: str | None = None
        self._processed_observation_ids: set[str] = set()
        self._processed_location_observation_ids: set[str] = set()
        self._location_detectors = {timeframe: InteractionDetector(timeframe) for timeframe in ("1m", "5m")}
        self._context_history = ContextHistory()
        self._location_registry = LocationRegistry()
        self._context_version = 0
        self._session_high: float | None = None
        self._session_low: float | None = None
        self._admission_suspended = False
        self._gap_end: datetime | None = None
        self._opening_gap_state = OpeningGapState("UNAVAILABLE")
        self._opening_gap_initialized = False
        self._opening_gap_observation_id: str | None = None
        self._opening_gap_last_evaluated_id: str | None = None
        self._opening_seed: dict[str, object] | None = None
        self._active_position = False
        self._opportunities: dict[str, SetupOpportunity] = {}
        self._pending_triggers: dict[str, dict[str, object]] = {}
        self._evidence = OrderflowEvidenceAssembler(config)

    def set_active_position(self, active: bool) -> None:
        self._active_position = active

    @property
    def execution_mode(self) -> ExecutionMode:
        return self.config.execution_mode

    @property
    def required_features(self) -> tuple[FeatureRequirement, ...]:
        return tuple(
            FeatureRequirement(name, timeframe=self.config.response_timeframe)
            for name in self.config.feature_set
        )

    @property
    def _evaluation_observations(self) -> tuple[OrderflowObservation, ...]:
        return self._evidence.observations

    @_evaluation_observations.setter
    def _evaluation_observations(self, value: tuple[OrderflowObservation, ...]) -> None:
        self._evidence.observations = value

    def _confirmation_scores(
        self, observation: OrderflowObservation,
    ) -> tuple[float, float] | None:
        return self._evidence.confirmation_scores(observation)

    def _prepare_observations(
        self, orderflow: OrderflowContext, context: DecisionContext,
    ) -> tuple[OrderflowObservation, ...]:
        return self._evidence.prepare(orderflow, context, self._location_registry)

    def _reset_session(
        self, orderflow: OrderflowContext, boundary: datetime,
        base: Mapping[str, Any],
    ) -> list[PolicyDecision]:
        transitions = [
            PolicyDecision({
                **base,
                "outcome": "rejected",
                "reason": InteractionCloseReason.SESSION_END.value,
                "decision_id": _decision_id("lifecycle", item.interaction_id, "session_end"),
                "interaction_id": item.interaction_id,
                "location": item.location_alias,
                "reference_generation": item.reference_generation,
                "interaction_state": InteractionStatus.ENDED.value,
                "decision_stage": "lifecycle",
            })
            for detector in self._location_detectors.values()
            for item in detector.close_all(InteractionCloseReason.SESSION_END, boundary)
        ]
        self._location_registry.remove_diagnostic_locations()
        self._location_registry.next_generation(at=boundary, carry=self._session_id is not None)
        self._processed_observation_ids.clear()
        self._processed_location_observation_ids.clear()
        self._evidence.clear_cache()
        self._location_detectors = {timeframe: InteractionDetector(timeframe) for timeframe in ("1m", "5m")}
        self._context_history = ContextHistory()
        if self._session_id is None:
            self._location_registry = LocationRegistry()
        self._context_version = 0
        self._session_high = None
        self._session_low = None
        self._admission_suspended = False
        self._gap_end = None
        self._opening_gap_state = OpeningGapState("UNAVAILABLE")
        self._opening_gap_initialized = False
        self._opening_gap_observation_id = None
        self._opening_gap_last_evaluated_id = None
        self._opening_seed = None
        self._opportunities.clear()
        self._pending_triggers.clear()
        self._session_id = orderflow.session_id
        return transitions

    def _prepare_opening_gap(
        self,
        observations: tuple[OrderflowObservation, ...],
        orderflow: OrderflowContext,
        expected_instrument: str | None,
        causal_cutoff: datetime,
        base: Mapping[str, Any],
    ) -> tuple[
        tuple[PolicyDecision, ...],
        OrderflowObservation | None,
        OrderflowObservation | None,
    ]:
        def is_causally_available(item: OrderflowObservation) -> bool:
            return (
                (expected_instrument is None or item.instrument == expected_instrument)
                and item.available_at <= causal_cutoff
            )

        events: list[PolicyDecision] = []
        if not self._opening_gap_initialized:
            first_1m = next(
                (item for item in observations if item.timeframe == "1m" and is_causally_available(item)),
                None,
            )
            if first_1m is not None:
                opening = _number(first_1m, "open")
                previous_close = _number(first_1m, "previous_close")
                if opening is not None and previous_close is not None:
                    prior_high = _number(first_1m, "prior_value_high")
                    prior_low = _number(first_1m, "prior_value_low")
                    reference = (
                        prior_low if prior_low is not None and opening < prior_low
                        else prior_high if prior_high is not None and opening > prior_high
                        else previous_close
                    )
                    reference_id = (
                        "PD_LOW" if prior_low is not None and opening < prior_low
                        else "PD_HIGH" if prior_high is not None and opening > prior_high
                        else None
                    )
                    classified_gap = classify_opening_gap(
                        opening_price=opening, prior_close=reference,
                        observed_at=first_1m.observed_at, reference_id=reference_id,
                    )
                    self._opening_gap_state = OpeningGapState(
                        "GAP_CANDIDATE" if classified_gap.status in {"UP", "DOWN"}
                        else classified_gap.status,
                        classified_gap.gap_points, classified_gap.reference_price,
                        classified_gap.observed_at, (first_1m.observation_id,),
                        reference_id=classified_gap.reference_id,
                    )
                    self._opening_gap_observation_id = first_1m.observation_id
                    context_available_at = opening_gap_context_available_at(first_1m)
                    events.append(PolicyDecision({
                        **base,
                        "observed_at": first_1m.observed_at.isoformat(),
                        "decision_at": context_available_at.isoformat(),
                        "available_at": context_available_at.isoformat(),
                        "outcome": "recorded", "decision_stage": "interaction",
                        "reason": "opening_gap_context",
                        "decision_id": _decision_id("opening_gap_context", first_1m.observation_id),
                        "direction": "down" if (self._opening_gap_state.gap_points or 0) < 0
                        else "up" if (self._opening_gap_state.gap_points or 0) > 0 else "none",
                        "gap_points": self._opening_gap_state.gap_points,
                        "reference_level": self._opening_gap_state.reference_price,
                        "source_observation_ids": [first_1m.observation_id],
                    }))
                    self._opening_gap_initialized = True

        next_opening_gap_1m = min(
            (
                item for item in observations
                if item.timeframe == "1m"
                and is_causally_available(item)
                and item.observation_id != self._opening_gap_observation_id
                and item.observation_id != self._opening_gap_last_evaluated_id
            ),
            key=lambda item: (item.available_at, item.observed_at, item.observation_id),
            default=None,
        )
        first_causal_5m = next(
            (
                item for item in observations
                if item.timeframe == "5m"
                and is_causally_available(item)
                and (item.context is None or item.context.session_id == orderflow.session_id)
            ),
            None,
        )
        return tuple(events), next_opening_gap_1m, first_causal_5m

    def _advance_opening_gap(
        self,
        observation: OrderflowObservation,
        next_opening_gap_1m: OrderflowObservation | None,
        base: Mapping[str, Any],
    ) -> tuple[PolicyDecision, ...]:
        if (
            observation.timeframe != "1m"
            or not self._opening_gap_initialized
            or next_opening_gap_1m is None
            or observation.observation_id != next_opening_gap_1m.observation_id
        ):
            return ()
        self._opening_gap_last_evaluated_id = observation.observation_id
        advanced_gap = advance_opening_gap(
            self._opening_gap_state,
            close=_number(observation, "close"),
            available_at=observation.available_at,
            evidence_id=observation.observation_id,
            complete=(observation.context is not None and observation.context.completeness == "complete"),
        )
        previous_gap = self._opening_gap_state
        self._opening_gap_state = advanced_gap
        if advanced_gap.status == previous_gap.status:
            return ()
        transition_at = observation.observed_at.isoformat()
        return (
            PolicyDecision({
                **base,
                "observed_at": observation.observed_at.isoformat(),
                "decision_at": transition_at,
                "available_at": transition_at,
                "outcome": "recorded",
                "decision_stage": "interaction",
                "reason": "opening_gap_transition",
                "decision_id": _decision_id("opening_gap_transition", observation.observation_id),
                "from_state": previous_gap.status,
                "state": advanced_gap.status,
                "reference": advanced_gap.reference_id,
                "reference_level": advanced_gap.reference_price,
                "source_observation_ids": [observation.observation_id],
            }),
            PolicyDecision({
                **base,
                "decision_at": transition_at,
                "available_at": transition_at,
                "outcome": "recorded",
                "decision_stage": "interaction",
                "reason": "opening_gap_permission",
                "decision_id": _decision_id("opening_gap_permission", observation.observation_id),
                "direction": "down" if (advanced_gap.gap_points or 0) < 0 else "up",
                "state": advanced_gap.status,
                "reference": advanced_gap.reference_id,
                "source_observation_ids": [observation.observation_id],
            }),
        )

    def _advance_context(
        self,
        observation: OrderflowObservation,
        orderflow: OrderflowContext,
        causal_cutoff: datetime,
        already_processed: bool,
        base: Mapping[str, Any],
    ) -> tuple[tuple[PolicyDecision, ...], bool]:
        if observation.timeframe != "5m" or already_processed or observation.context is None:
            return (), False
        events: list[PolicyDecision] = []
        self._context_version += 1
        high = observation.features.get("high")
        low = observation.features.get("low")
        previous_high = self._session_high
        previous_low = self._session_low
        if isinstance(high, (int, float)):
            self._session_high = max(self._session_high or float(high), float(high))
        if isinstance(low, (int, float)):
            self._session_low = min(self._session_low or float(low), float(low))
        for extreme, previous, current in (
            ("high", previous_high, self._session_high),
            ("low", previous_low, self._session_low),
        ):
            if (
                observation.context.replay_provenance.get("producer") != "ftx-paper"
                or current is None or current == previous
            ):
                continue
            events.append(PolicyDecision({
                **base,
                "observed_at": observation.observed_at.isoformat(),
                "decision_at": observation.available_at.isoformat(),
                "available_at": observation.available_at.isoformat(),
                "decision_stage": "candidate", "outcome": "recorded",
                "reason": "session_extreme_registered",
                "decision_id": _decision_id("session_extreme", extreme, observation.observation_id),
                "extreme": extreme, "value": current,
                "source_observation_ids": [observation.observation_id],
            }))
        if observation.available_at > causal_cutoff:
            return tuple(events), True
        level_values = {
            item.location_alias.upper(): item.reference
            for item in observation.location_observations
            if not is_diagnostic_location(item.location_type)
        }
        opening_gap = classify_opening_gap(
            opening_price=_number(observation, "open"),
            prior_close=_number(observation, "previous_close"),
            observed_at=observation.available_at,
        )
        context_gap = (
            self._opening_gap_state
            if self._opening_gap_state.status != "UNAVAILABLE" else opening_gap
        )
        parent_context = MarketContext(
            context_id=f"context:{orderflow.session_id}", version=self._context_version,
            available_at=observation.available_at,
            levels=SessionLevels(
                orderflow.session_id, level_values.get("OR_HIGH"), level_values.get("OR_LOW"),
                level_values.get("PD_HIGH"), level_values.get("PD_LOW"),
                "PD_HIGH" in level_values and "PD_LOW" in level_values,
                observation.available_at, source_ids=(observation.observation_id,),
            ),
            opening_gap=context_gap if context_gap.status != "UNAVAILABLE" else OpeningGapState(
                "GAP_CANDIDATE" if observation.context.replay_provenance.get("gap_status") == "source_gap" else "UNAVAILABLE",
                observed_at=observation.available_at, evidence_ids=(observation.observation_id,),
            ),
            permission=(
                "UNAVAILABLE" if observation.context.replay_provenance.get("gap_status") == "source_gap"
                else "BOTH"
            ),
            long_score=_number(observation, "long_score"), short_score=_number(observation, "short_score"),
            evidence_ids=(observation.observation_id,),
            evidence=({
                "completed_bar": True, "observation_id": observation.observation_id,
                "long_score": _number(observation, "long_score"),
                "short_score": _number(observation, "short_score"),
            },),
            source_provenance=({
                "source_observation_id": observation.observation_id,
                "score_source_observation_id": observation.observation_id,
            },),
        )
        self._context_history.publish(parent_context)
        parent_reference = ContextVersionRef(parent_context.context_id, parent_context.version)
        for location_observation in observation.location_observations:
            self._context_version += 1
            self._context_history.publish(MarketContext(
                context_id=f"interaction:{location_observation.location_alias}:{orderflow.session_id}",
                version=self._context_version, available_at=observation.available_at,
                levels=parent_context.levels, opening_gap=parent_context.opening_gap,
                permission=parent_context.permission, long_score=parent_context.long_score,
                short_score=parent_context.short_score,
                evidence_ids=(observation.observation_id, location_observation.observation_id),
                value_reference=parent_context.value_reference,
                location=location_observation.location_alias,
                parent_context_id=parent_context.context_id,
                evidence=({"location_observation_id": location_observation.observation_id},),
                source_provenance=({
                    "source_observation_id": observation.observation_id,
                    "source_aggregate_id": location_observation.source_aggregate_id,
                    "parent_context": asdict(parent_reference),
                },),
            ))
        active_interactions = {
            item.interaction_id: item
            for detector in self._location_detectors.values()
            for item in detector.active_interactions
        }
        for opportunity_id, opportunity in tuple(self._opportunities.items()):
            if opportunity.status is not OpportunityStatus.ACTIVE:
                continue
            source = active_interactions.get(opportunity.source_interaction_id or "")
            if source is None:
                continue
            decision_context = self._context_history.as_of_location(
                source.location_alias, observation.available_at,
            )
            if decision_context is not None:
                reference = ContextVersionRef(decision_context.context_id, decision_context.version)
                if opportunity.decision_context != reference:
                    self._opportunities[opportunity_id] = replace(opportunity, decision_context=reference)
        return tuple(events), False

    def _handle_gap(
        self,
        observation: OrderflowObservation,
        gap_status: object,
        base: Mapping[str, Any],
    ) -> tuple[PolicyDecision, ...]:
        events: list[PolicyDecision] = []
        if gap_status == "source_gap":
            self._admission_suspended = True
            self._gap_end = observation.interval_end or observation.available_at
            for opportunity_id, opportunity in tuple(self._opportunities.items()):
                if opportunity.status is not OpportunityStatus.ACTIVE:
                    continue
                invalidated = opportunity.invalidate(
                    at=observation.available_at, reason="source_gap",
                    evidence_ids=(observation.observation_id,),
                )
                self._opportunities[opportunity_id] = invalidated
                events.append(PolicyDecision({
                    **base, "decision_stage": "lifecycle", "outcome": "rejected",
                    "reason": "opportunity_invalidated_by_source_gap",
                    "decision_id": _decision_id(
                        "lifecycle", opportunity.lifecycle_id,
                        observation.observation_id, "source_gap",
                    ),
                    "opportunity_id": opportunity.lifecycle_id,
                    "setup_id": opportunity.setup.setup_id,
                    "source_observation_ids": [observation.observation_id],
                }))
            for interaction in self._location_detectors[observation.timeframe].observe_gap(observation.available_at):
                events.append(PolicyDecision({
                    **base, "decision_stage": "lifecycle", "outcome": "rejected",
                    "reason": InteractionCloseReason.DATA_GAP.value,
                    "decision_id": _decision_id(
                        "lifecycle", interaction.interaction_id,
                        observation.observation_id, "gap",
                    ),
                    "location": interaction.location_alias,
                    "interaction_id": interaction.interaction_id,
                    "timeframe": observation.timeframe,
                    "reference_generation": interaction.reference_generation,
                    "interaction_state": InteractionStatus.ENDED.value,
                    "source_observation_ids": [observation.observation_id],
                }))
        elif (
            self._admission_suspended
            and gap_status == "none"
            and observation.timeframe == "5m"
            and self._gap_end is not None
            and observation.available_at > self._gap_end
        ):
            self._admission_suspended = False
            self._gap_end = None
        return tuple(events)

    def _attach_opening_seed(
        self, observation: OrderflowObservation, base: Mapping[str, Any],
    ) -> tuple[PolicyDecision, ...] | None:
        if (
            observation.timeframe != "1m"
            or self._opening_seed is None
            or self._opening_seed.get("attached_observation_id") is not None
            or observation.observation_id == self._opening_gap_observation_id
        ):
            return ()
        seed = self._opening_seed
        formation_available_at = seed.get("formation_available_at")
        if isinstance(formation_available_at, str):
            formation_available = datetime.fromisoformat(formation_available_at.replace("Z", "+00:00"))
            if observation.available_at <= formation_available:
                return None
        raw_boundary = seed.get("reference_level")
        if not isinstance(raw_boundary, (int, float)) or isinstance(raw_boundary, bool):
            return None
        boundary = float(raw_boundary)
        close = _number(observation, "close")
        high = _number(observation, "high")
        low = _number(observation, "low")
        if close is None or high is None or low is None:
            return ()
        direction = str(seed["direction"])
        crossed = close >= boundary if direction == "long" else close <= boundary
        touched = low <= boundary <= high
        opening_policy = str(seed.get("opening_policy", ""))
        if not touched and not crossed:
            return None
        if opening_policy == "opening_breakout" and not crossed:
            return None
        opening_kind = "opening_breakout" if opening_policy == "opening_breakout" else "opening_rejection"
        self._opening_seed = {
            **seed,
            "attached_observation_id": observation.observation_id,
            "opening_interaction_kind": opening_kind,
        }
        evidence_ids = [str(seed["source_observation_id"]), observation.observation_id]
        attached = PolicyDecision({
            **base,
            "decision_stage": "interaction", "outcome": "recorded",
            "reason": "opening_interaction_attached",
            "decision_id": _decision_id(
                "opening_interaction", str(seed["seed_id"]), observation.observation_id,
            ),
            "candidate_provenance": "opening_hypothesis",
            "interaction_id": str(seed["seed_id"]),
            "opening_interaction_kind": opening_kind,
            "location": seed["location"], "direction": direction,
            "reference_level": boundary,
            "reference_generation": seed["reference_generation"],
            "source_observation_ids": evidence_ids,
            "response_evidence_ids": evidence_ids,
            "available_at": observation.available_at.isoformat(),
        })
        # Keep the opening path producer-neutral: the seed is a hypothesis,
        # while the first causally available one-minute bar is its response.
        response = PolicyDecision({
            **base,
            "decision_stage": "interaction", "outcome": "recorded",
            "reason": "response_confirmed",
            "decision_id": _decision_id(
                "opening_response", str(seed["seed_id"]), observation.observation_id,
            ),
            "interaction_id": str(seed["seed_id"]),
            "location": seed["location"], "direction": direction,
            "response_type": "acceptance",
            "evidence_ids": evidence_ids,
            "source_observation_ids": evidence_ids,
            "observed_at": observation.observed_at.isoformat(),
            "available_at": observation.available_at.isoformat(),
        })
        return (attached, response)

    def _process_location_observation(
        self,
        observation: OrderflowObservation,
        location_observation: LocationObservation,
        base: Mapping[str, Any],
    ) -> tuple[tuple[PolicyDecision, ...], tuple[PolicyDecision, ...], tuple[PolicyDecision, ...]]:
        if not isinstance(location_observation, LocationObservation):
            raise ValueError("invalid causal location observation")
        if location_observation.timeframe and location_observation.timeframe != observation.timeframe:
            raise ValueError("location observation timeframe does not match parent")
        if observation.timeframe == "5m":
            parent_context = observation.context
            if parent_context is None or location_observation.child_observation_ids != parent_context.child_observation_ids:
                raise ValueError("5m location observation lineage does not match parent")
        if location_observation.available_at > observation.available_at:
            return (), (), ()
        location_key = location_observation.observation_id
        if location_key in self._processed_location_observation_ids:
            return (), (), ()
        self._processed_location_observation_ids.add(location_key)
        location = LocationRecord(
            location_observation.location_id, location_observation.location_alias,
            location_observation.reference,
            LocationBand(location_observation.band_lower, location_observation.band_upper),
            location_observation.reference_generation, location_observation.available_at,
            location_observation.reference_source_aggregate_id or location_observation.source_aggregate_id,
            timeframe=observation.timeframe, source_identity=observation.instrument,
            location_type=location_observation.location_type,
            source_observation_id=location_observation.source_observation_id,
            frozen_atr=location_observation.frozen_atr,
        )
        context_events: list[PolicyDecision] = []
        created_events: list[PolicyDecision] = []
        lifecycle_events: list[PolicyDecision] = []
        if not any(item.location_id == location.location_id for item in self._location_registry.records):
            self._location_registry.publish(location)
            if observation.context is not None and observation.context.replay_provenance.get("producer") == "ftx-paper":
                lifecycle_values = {
                    "location": location.alias, "location_id": location.location_id,
                    "reference_generation": location.generation, "reference_level": location.price,
                    "timeframe": observation.timeframe, "source_aggregate_id": location.source_aggregate_id,
                    "source_observation_ids": [location_key],
                }
                lifecycle_at = location.available_at
                context_events.append(PolicyDecision({
                    **base, "decision_at": lifecycle_at.isoformat(),
                    "available_at": location.available_at.isoformat(),
                    "decision_stage": "interaction", "outcome": "recorded",
                    "reason": "interaction_context_published",
                    "decision_id": _decision_id("interaction_context", location.location_id, location.generation),
                    **lifecycle_values,
                }))
                created_events.append(PolicyDecision({
                    **base, "decision_at": lifecycle_at.isoformat(),
                    "available_at": location.available_at.isoformat(),
                    "decision_stage": "interaction", "outcome": "recorded",
                    "reason": "interaction_created",
                    "decision_id": _decision_id("interaction_created", location.location_id, location.generation),
                    **lifecycle_values,
                }))
        detector = self._location_detectors[observation.timeframe]
        closed_before = len(detector.closed_interactions)
        interaction = detector.observe(location, observation, observation_id=location_key)
        for ended in detector.closed_interactions[closed_before:]:
            lifecycle_events.append(PolicyDecision({
                **base, "decision_stage": "lifecycle", "outcome": "rejected",
                "reason": ended.close_reason.value if ended.close_reason else "interaction_ended",
                "decision_id": _decision_id("lifecycle", ended.interaction_id, location_key, "ended"),
                "location": ended.location_alias, "interaction_id": ended.interaction_id,
                "reference_generation": ended.reference_generation, "response_state": "ENDED",
                "source_observation_ids": [location_key],
            }))
            if ended.close_reason is None:
                raise RuntimeError("ended interaction is missing close reason")
            for opportunity_id, opportunity in tuple(self._opportunities.items()):
                if opportunity.status is OpportunityStatus.ACTIVE and opportunity.source_interaction_id == ended.interaction_id:
                    invalidated = opportunity.invalidate(
                        at=ended.closed_at or observation.available_at,
                        reason=f"source_interaction_{ended.close_reason.value.lower()}",
                        evidence_ids=(location_key,),
                    )
                    self._opportunities[opportunity_id] = invalidated
                    lifecycle_events.append(PolicyDecision({
                        **base, "decision_stage": "opportunity", "outcome": "rejected",
                        "reason": invalidated.transitions[-1].reason,
                        "decision_id": _decision_id("opportunity_invalidated", opportunity_id, location_key),
                        "opportunity_id": opportunity_id, "source_observation_ids": [location_key],
                    }))
        if interaction is not None and interaction.status is InteractionStatus.ACTIVE:
            lifecycle_events.append(PolicyDecision({
                **base, "decision_stage": "opportunity", "outcome": "recorded",
                "reason": "location_interaction_observed",
                "decision_id": _decision_id("opportunity", interaction.interaction_id, location_key),
                "location": interaction.location_alias, "interaction_id": interaction.interaction_id,
                "timeframe": observation.timeframe, "reference_generation": interaction.reference_generation,
                "source_observation_ids": [location_key],
                "observed_at": location_observation.observed_at.isoformat(),
                "available_at": location_observation.available_at.isoformat(),
                "feature_values": feature_values_to_dict(observation.features),
            }))
            if is_diagnostic_location(interaction.location_type):
                diagnostic = evaluate_vwap_diagnostic_response(interaction, interaction.contact_candles)
                lifecycle_events.append(PolicyDecision({
                    **base, "decision_stage": "interaction", "outcome": "recorded",
                    "reason": "vwap_diagnostic_response",
                    "decision_id": _decision_id("vwap_diagnostic", interaction.interaction_id, location_key),
                    **diagnostic,
                }))
        return tuple(context_events), tuple(created_events), tuple(lifecycle_events)

    def _advance_interaction_projections(
        self, observation: OrderflowObservation,
        observations: tuple[OrderflowObservation, ...],
    ) -> None:
        if observation.timeframe != "5m":
            return
        projection_sources = {
            timeframe: tuple(
                item for item in observations
                if item.timeframe == timeframe and item.available_at <= observation.available_at
            )
            for timeframe in ("1m", "5m")
        }
        detector5 = self._location_detectors["5m"]
        detector5.advance_projections(projection_sources, as_of=observation.available_at)
        for candidate in detector5.active_interactions:
            snapshots = dict(candidate.synthetic_snapshots).get("5m", ())
            if not snapshots:
                continue
            long_score = _number(observation, "long_score")
            short_score = _number(observation, "short_score")
            if long_score is not None or short_score is not None:
                detector5.record_score(
                    candidate.interaction_id, snapshot=snapshots[-1],
                    context_version=observation.context.generation if observation.context else None,
                    long_score=long_score, short_score=short_score,
                )

    def _advance_opportunity_timeouts(
        self, observation: OrderflowObservation, detector: InteractionDetector,
    ) -> dict[str, SetupOpportunity]:
        previous = dict(self._opportunities)
        if observation.timeframe != "5m":
            return previous
        for key, item in tuple(self._opportunities.items()):
            source_interaction = next(
                (
                    candidate
                    for candidate in (*detector.active_interactions, *detector.closed_interactions)
                    if candidate.interaction_id == item.source_interaction_id
                ),
                None,
            )
            timeout = advance_opportunity_timeout(
                item, interaction=source_interaction,
                evidence=(observation,), as_of=observation.available_at,
            )
            if timeout.opportunity is not None:
                self._opportunities[key] = timeout.opportunity
        return previous

    def _response_observations(
        self,
        interaction: object,
        observations: tuple[OrderflowObservation, ...],
    ) -> tuple[OrderflowObservation, ...]:
        contact_candles = getattr(interaction, "contact_candles", ())
        if not contact_candles:
            return ()
        contact_at = getattr(contact_candles[0], "observed_at", None)
        if not isinstance(contact_at, datetime):
            return ()
        return tuple(sorted(
            (
                item for item in observations
                if item.timeframe == self.config.response_timeframe
                and item.observed_at >= contact_at
            ),
            key=lambda item: (item.observed_at, item.available_at, item.observation_id),
        ))

    def _record_expired_opportunities(
        self,
        previous_opportunities: Mapping[str, SetupOpportunity],
        base: Mapping[str, Any],
    ) -> tuple[PolicyDecision, ...]:
        events: list[PolicyDecision] = []
        for opportunity_id, opportunity in self._opportunities.items():
            previous_opportunity = previous_opportunities.get(opportunity_id)
            if previous_opportunity is None or len(opportunity.transitions) <= len(previous_opportunity.transitions):
                continue
            for transition in opportunity.transitions[len(previous_opportunity.transitions):]:
                if transition.status is not OpportunityStatus.EXPIRED:
                    continue
                events.append(PolicyDecision({
                    **base,
                    "decision_stage": "lifecycle", "outcome": "rejected",
                    "reason": "setup_expired",
                    "decision_id": _decision_id(
                        "lifecycle", opportunity.lifecycle_id,
                        transition.at.isoformat(), "setup_expired",
                    ),
                    "opportunity_id": opportunity.lifecycle_id,
                    "setup_id": opportunity.setup.setup_id,
                    "interaction_id": opportunity.response_episode_id.removeprefix("response-"),
                    "observed_at": transition.at.isoformat(),
                    "available_at": transition.at.isoformat(),
                    "source_observation_ids": list(transition.evidence_ids),
                }))
        return tuple(events)

    def _advance_pending_triggers(
        self, observation: OrderflowObservation, context: DecisionContext,
        base: Mapping[str, Any], futures: MarketBar | None,
    ) -> tuple[PolicyDecision, ...]:
        if observation.timeframe != "1m":
            return ()
        events: list[PolicyDecision] = []
        for lifecycle_id, pending in tuple(self._pending_triggers.items()):
            opportunity = self._opportunities.get(lifecycle_id)
            template = pending.get("template")
            interaction = pending.get("interaction")
            if (
                opportunity is None or opportunity.status is not OpportunityStatus.ACTIVE
                or not isinstance(template, TradeTemplate)
                or not isinstance(interaction, LocationInteraction)
            ):
                self._pending_triggers.pop(lifecycle_id, None)
                continue
            trigger = observe_trigger(
                observation,
                boundary=(opportunity.setup.entry_upper if str(pending["direction"]) == "long"
                          else opportunity.setup.entry_lower),
                execution_mode=ExecutionMode.EXECUTABLE.value,
                trigger_type=template.trigger_type.value,
                effective_at=opportunity.setup.formed_at.isoformat(),
                expires_at=opportunity.setup.expires_at.isoformat(),
                setup_version=opportunity.setup.setup_id, armed=True,
            )
            if not trigger.eligible:
                continue
            direction = str(pending["direction"])
            admission = evaluate_admission(
                opportunity, context_available=observation.context is not None,
                opening_gap_allowed=not self._admission_suspended,
                active_position_allowed=(
                    not self._active_position
                    or interaction.location_alias not in {"SESSION_LOW", "SESSION_HIGH"}
                ),
                direction_matches=True, confirmations_available=True,
                current_at=observation.available_at,
                context_relation=opportunity.context_relation,
                evidence_ids=(observation.observation_id,),
                evaluate_portfolio_gates=False,
            )
            proposal = self._execution_proposal(
                accepted=admission.status.value == "PASSED", direction=direction,
                observation=observation, opportunity=opportunity,
                template=template, interaction=interaction, context=context, futures=futures,
            )
            common = {
                **base, "opportunity_id": lifecycle_id,
                "setup_id": opportunity.setup.setup_id,
                "interaction_id": interaction.interaction_id,
                "location": interaction.location_alias, "direction": direction,
                "source_observation_ids": [observation.observation_id],
            }
            events.extend((
                PolicyDecision({
                    **common, "decision_stage": "trigger", "outcome": "accepted",
                    "reason": trigger.reason,
                    "decision_id": _decision_id("trigger", lifecycle_id, observation.observation_id),
                    "trigger": asdict(trigger), "observed_at": trigger.observed_at,
                    "available_at": trigger.available_at,
                }),
                PolicyDecision({
                    **common, "decision_stage": "admission",
                    "outcome": "accepted" if admission.status.value == "PASSED" else "rejected",
                    "reason": "orderflow_admission", "admission_reason": admission.reason,
                    "decision_id": admission.decision_id,
                    "admission_predicates": [asdict(item) for item in admission.predicates],
                    "trigger": asdict(trigger), "trigger_eligible": True,
                    "execution_proposal": proposal, "observed_at": trigger.observed_at,
                    "available_at": trigger.available_at,
                }),
            ))
            self._pending_triggers.pop(lifecycle_id, None)
        return tuple(events)

    def _advance_prior_day_role(
        self,
        interaction: LocationInteraction,
        response_observations: tuple[OrderflowObservation, ...],
        base: Mapping[str, Any],
        detector: InteractionDetector,
    ) -> tuple[LocationInteraction, tuple[PolicyDecision, ...]]:
        alias = getattr(interaction, "location_alias", None)
        if alias not in {"PD_LOW", "PD_HIGH"}:
            return interaction, ()
        role = getattr(interaction, "pd_role", None) or (
            "SUPPORT" if alias == "PD_LOW" else "RESISTANCE"
        )
        transition_at = getattr(interaction, "pd_role_transition_available_at", None)
        candidates = tuple(
            candidate for candidate in response_observations
            # The role transition candle is causally available evidence and
            # must remain eligible for the canonical response window.
            if transition_at is None or candidate.available_at >= transition_at
        )
        events: list[PolicyDecision] = []
        for candidate in candidates:
            high = candidate.features.get("high")
            low = candidate.features.get("low")
            if not isinstance(high, (int, float)) or not isinstance(low, (int, float)):
                continue
            raw_upper = getattr(interaction, "band_upper", None)
            raw_lower = getattr(interaction, "band_lower", None)
            reference_level = getattr(interaction, "reference_level")
            upper = raw_upper if raw_upper is not None else reference_level
            lower = raw_lower if raw_lower is not None else reference_level
            transitioned = "SUPPORT" if low > upper else "RESISTANCE" if high < lower else None
            if transitioned is None or transitioned == role:
                continue
            transition_evidence = (candidate.observation_id,)
            for opportunity_id, opportunity in tuple(self._opportunities.items()):
                if (
                    opportunity.source_interaction_id == getattr(interaction, "interaction_id")
                    and opportunity.status is OpportunityStatus.ACTIVE
                    and opportunity.setup.formed_at <= candidate.available_at
                ):
                    invalidated = opportunity.invalidate(
                        at=candidate.available_at, reason="prior_day_role_transition",
                        evidence_ids=transition_evidence,
                    )
                    self._opportunities[opportunity_id] = invalidated
                    events.append(PolicyDecision({
                        **base, "decision_stage": "opportunity", "outcome": "rejected",
                        "reason": "prior_day_role_transition",
                        "decision_id": _decision_id(
                            "opportunity_invalidated", opportunity_id, candidate.observation_id,
                        ),
                        "opportunity_id": opportunity_id,
                        "source_observation_ids": list(transition_evidence),
                    }))
            updated = detector.update_prior_day_role(
                getattr(interaction, "interaction_id"), transitioned,
                candidate.observation_id, candidate.available_at,
            )
            if updated is not None:
                interaction = updated
            role = transitioned
        return interaction, tuple(events)

    def _execution_proposal(
        self,
        *,
        accepted: bool,
        direction: str,
        observation: OrderflowObservation,
        opportunity: SetupOpportunity,
        template: TradeTemplate,
        interaction: object,
        context: DecisionContext,
        futures: MarketBar | None,
    ) -> dict[str, object]:
        return {
            "eligible": accepted,
            "direction": direction,
            "instrument": observation.instrument,
            "exchange": futures.instrument.exchange if futures is not None else None,
            "instrument_type": futures.instrument.instrument_type if futures is not None else None,
            "lifecycle_id": opportunity.lifecycle_id,
            "stop_price": opportunity.setup.failure_boundary,
            "trigger_price": (
                opportunity.setup.entry_upper
                if direction == "long" else opportunity.setup.entry_lower
            ),
            "stop_policy": template.stop_policy.value,
            "exit_mode": self.config.exit_policy,
            "cell": str(getattr(interaction, "location_alias", "orderflow")).lower(),
            "entry_bar": context.as_of.minute,
            "strategy_id": self.config.name,
            "execution_group_id": opportunity.execution_group_id,
            "setup_availability": opportunity.setup.formed_at.isoformat(),
        }

    def _form_opening_candidate(
        self,
        observation: OrderflowObservation,
        seed: Mapping[str, object],
        orderflow: OrderflowContext,
        context: DecisionContext,
        base: Mapping[str, Any],
        futures: MarketBar | None,
    ) -> tuple[PolicyDecision, ...]:
        direction = str(seed["direction"])
        alias = str(seed["location"])
        template = TRADE_TEMPLATES.get(("5m_acceptance_breakout", alias))
        reference = seed.get("reference_level")
        lower = seed.get("band_lower", reference)
        upper = seed.get("band_upper", reference)
        if (
            template is None
            or not isinstance(reference, (int, float))
            or not isinstance(lower, (int, float))
            or not isinstance(upper, (int, float))
        ):
            return ()
        child_ids = tuple(
            observation.context.child_observation_ids
            if observation.context is not None else ()
        )
        children = tuple(
            item for item in orderflow.observations_for("1m")
            if item.observation_id in child_ids
        )
        response_observation = max(
            children, key=lambda item: item.observed_at, default=observation,
        )
        evidence_ids = (
            str(seed.get("source_observation_id", observation.observation_id)),
            observation.observation_id,
        )
        formed_at = observation.available_at
        self._opening_seed = {
            **seed,
            "attached_observation_id": response_observation.observation_id,
            "opening_interaction_kind": None,
        }
        opportunity = SetupOpportunity.create(
            interaction_id=str(seed["seed_id"]), direction=direction,
            formed_at=formed_at,
            expires_at=formed_at + timedelta(minutes=self.config.response_window_candles * 5),
            entry_lower=float(lower), entry_upper=float(upper),
            failure_boundary=float(upper if direction == "short" else lower),
            evidence_ids=evidence_ids, evidence_timeframe="5m",
            template_id=template.template_id, policy_version=template.version,
            conflict_group_id=template.execution_conflict_group,
            source_instrument=observation.instrument,
            source_session_id=orderflow.session_id,
        )
        if opportunity.lifecycle_id in self._opportunities:
            return ()
        self._opportunities[opportunity.lifecycle_id] = opportunity
        proposal = {
            "eligible": True,
            "direction": direction,
            "instrument": observation.instrument,
            "exchange": futures.instrument.exchange if futures is not None else None,
            "instrument_type": futures.instrument.instrument_type if futures is not None else None,
            "lifecycle_id": opportunity.lifecycle_id,
            "stop_price": opportunity.setup.failure_boundary,
            "trigger_price": opportunity.setup.entry_upper if direction == "long" else opportunity.setup.entry_lower,
            "stop_policy": template.stop_policy.value,
            "exit_mode": self.config.exit_policy,
            "cell": alias.lower(),
            "entry_bar": context.as_of.minute,
            "strategy_id": self.config.name,
            "execution_group_id": opportunity.execution_group_id,
            "setup_availability": formed_at.isoformat(),
        }
        return (
            PolicyDecision({
                **base, "decision_stage": "opportunity", "outcome": "recorded",
                "reason": "setup_candidate_formed",
                "decision_id": _decision_id("candidate", opportunity.lifecycle_id),
                "opportunity_id": opportunity.lifecycle_id,
                "setup_id": opportunity.setup.setup_id,
                "interaction_id": None, "location": alias,
                "opening_seed_id": str(seed["seed_id"]),
                "direction": direction, "template_id": template.template_id,
                "candidate_provenance": "opening_hypothesis",
                "response_evidence_ids": list(evidence_ids),
                "source_observation_ids": list(evidence_ids),
                "observed_at": response_observation.observed_at.isoformat(),
                "available_at": formed_at.isoformat(),
                "response_type": template.response_type.value,
                "execution_proposal": proposal,
            }),
            PolicyDecision({
                **base, "decision_stage": "candidate", "outcome": "recorded",
                "reason": "hypothesis_arbitrated",
                "decision_id": _decision_id("opening_arbitration", opportunity.lifecycle_id),
                "interaction_id": None, "opening_seed_id": str(seed["seed_id"]),
                "location": alias, "direction": direction,
                "template_id": template.template_id, "arbitration_selected": True,
                "arbitration_reason": "opening_candle_directional_seed",
                "source_observation_ids": list(evidence_ids),
                "observed_at": response_observation.observed_at.isoformat(),
                "available_at": formed_at.isoformat(),
            }),
            PolicyDecision({
                **base, "decision_stage": "candidate", "outcome": "recorded",
                "reason": "opening_candle_directional_seed",
                "decision_id": _decision_id("opening_outcome", opportunity.lifecycle_id),
                "interaction_id": None, "opening_seed_id": str(seed["seed_id"]),
                "location": alias, "direction": direction,
                "template_id": template.template_id, "response_type": template.response_type.value,
                "status": "RESOLVED", "response_evidence_ids": list(evidence_ids),
                "source_observation_ids": list(evidence_ids),
                "observed_at": response_observation.observed_at.isoformat(),
                "available_at": formed_at.isoformat(),
            }),
    )

    def _evaluate_response(
        self,
        interaction: LocationInteraction,
        observation: OrderflowObservation,
        observations: tuple[OrderflowObservation, ...],
        orderflow: OrderflowContext,
        context: DecisionContext,
        base: Mapping[str, Any],
        detector: InteractionDetector,
        futures: MarketBar | None,
    ) -> tuple[PolicyDecision, ...]:
        if interaction.timeframe != self.config.response_timeframe:
            return ()
        decision_cutoff = min(context.as_of, orderflow.cutoff)
        response_observations = self._response_observations(interaction, observations)
        if not response_observations:
            return ()
        available = tuple(item for item in response_observations if item.available_at <= decision_cutoff)
        interaction, role_events = self._advance_prior_day_role(interaction, available, base, detector)
        results = list(role_events)
        evaluations = evaluate_policy_responses(
            interaction, available, max_candles=self.config.response_window_candles,
            cutoff=decision_cutoff,
            initial_armed={item.hypothesis_id: item.armed for item in interaction.hypothesis_states},
        )
        for template, evaluation in evaluations:
            results.extend(self._record_response_evaluation(
                interaction, observation, response_observations, template, evaluation,
                decision_cutoff, context, base, detector, futures,
            ))
        return tuple(results)

    def _record_response_evaluation(
        self,
        interaction: LocationInteraction,
        observation: OrderflowObservation,
        response_observations: tuple[OrderflowObservation, ...],
        template: TradeTemplate,
        evaluation: Any,
        decision_cutoff: datetime,
        context: DecisionContext,
        base: Mapping[str, Any],
        detector: InteractionDetector,
        futures: MarketBar | None,
    ) -> tuple[PolicyDecision, ...]:
        hypothesis_id = template.template_id
        future = next((item for item in response_observations if item.available_at > decision_cutoff), None)
        if future is not None and evaluation.status is None:
            next_status, reason, evidence_ids = "UNAVAILABLE", "response_evidence_unavailable_at_cutoff", tuple(
                future.evidence_ids or (future.observation_id,)
            )
            expected_available_at, direction, confirmation_id = future.available_at, None, None
        else:
            next_status = evaluation.status.value if evaluation.status is not None else "PENDING_RESPONSE"
            reason = evaluation.reason or next_status.lower()
            evidence_ids = evaluation.evidence_ids
            expected_available_at = getattr(evaluation, "expected_available_at", None)
            direction = evaluation.direction.value if evaluation.direction is not None else None
            confirmation_id = evaluation.confirmation_observation_id
        if next_status == "PENDING":
            next_status = "PENDING_RESPONSE"
        source_ids = tuple(item.observation_id for item in response_observations if set(item.evidence_ids).intersection(evidence_ids))
        if not source_ids and confirmation_id is not None:
            source_ids = (confirmation_id,)
        current = next((item for item in interaction.hypothesis_states if item.hypothesis_id == hypothesis_id), None)
        if current is not None and (
            tuple(evidence_ids) == current.evidence_ids and next_status == current.status
            and evaluation.armed == current.armed and direction == current.direction
            and confirmation_id == current.confirmation_observation_id
            and expected_available_at == current.expected_available_at
        ):
            return ()
        state = HypothesisState(
            hypothesis_id, template.template_id, next_status, evaluation.armed, direction,
            tuple(evidence_ids), confirmation_id,
            evaluation.confirmation_available_at if next_status == "CONFIRMED" else None,
            expected_available_at, reason if next_status == "INVALIDATED" else None,
            reason if next_status == "EXPIRED" else None,
            tuple(EvidenceLineage(
                item.observation_id, item.source_observation_id or item.observation_id,
                item.timeframe, item.observed_at, item.available_at,
                str(item.context.replay_provenance.get("source_fingerprint"))
                if item.context is not None and item.context.replay_provenance.get("source_fingerprint") is not None else None,
            ) for item in response_observations if item.observation_id in source_ids),
        )
        updated = detector.update_response(interaction.interaction_id, next_status, hypothesis_id, state=state)
        if updated is None:
            return ()
        results = [PolicyDecision({
            **base, "decision_stage": "response",
            "outcome": "recorded" if next_status == "CONFIRMED" else "unavailable" if next_status == "UNAVAILABLE" else "rejected",
            "reason": reason, "decision_id": _decision_id("response", interaction.interaction_id,
                confirmation_id or (evidence_ids[-1] if evidence_ids else observation.observation_id), next_status),
            "location": interaction.location_alias, "interaction_id": interaction.interaction_id,
            "reference_generation": interaction.reference_generation, "response_state": next_status,
            "template_id": template.template_id, "hypothesis_state": asdict(state),
            "response_evidence_ids": list(evidence_ids), "source_observation_ids": list(source_ids),
            "expected_available_at": expected_available_at.isoformat() if expected_available_at else None,
        })]
        if next_status == "CONFIRMED":
            results.extend(self._handle_confirmed_response(
                interaction, observation, response_observations, template, evaluation,
                direction, confirmation_id, evidence_ids, context, base, futures,
            ))
        return tuple(results)

    def _handle_confirmed_response(
        self,
        interaction: LocationInteraction,
        observation: OrderflowObservation,
        response_observations: tuple[OrderflowObservation, ...],
        template: TradeTemplate,
        evaluation: Any,
        direction: str | None,
        confirmation_id: str | None,
        evidence_ids: tuple[str, ...],
        context: DecisionContext,
        base: Mapping[str, Any],
        futures: MarketBar | None,
    ) -> tuple[PolicyDecision, ...]:
        if direction is None:
            return (PolicyDecision({**base, "decision_stage": "admission", "outcome": "unavailable",
                "reason": "response_direction_unavailable",
                "decision_id": _decision_id("unavailable", "direction", interaction.interaction_id),
                "interaction_id": interaction.interaction_id, "source_observation_ids": list(evidence_ids)}),)
        formed_at = evaluation.confirmation_available_at or observation.available_at
        formation_context = self._context_history.as_of_location(interaction.location_alias, formed_at)
        confirmation = next((item for item in response_observations if item.observation_id == confirmation_id), None)
        close = _number(confirmation, "close") if confirmation is not None else None
        acceptance = close is not None and (close > (interaction.band_upper or interaction.reference_level)
            if direction == "long" else close < (interaction.band_lower or interaction.reference_level))
        relation = classify_context_relation(location=interaction.location_alias, direction=direction,
            evidence={"acceptance": acceptance, "evidence_ids": evidence_ids})
        scores = self._confirmation_scores(confirmation) if confirmation is not None else None
        long_score, short_score = scores if scores is not None else (None, None)
        rejection = _score_rejection(direction=direction, long_score=long_score, short_score=short_score,
            margin=self.config.fill_permission_margin)
        if rejection is not None:
            return (PolicyDecision({**base, "decision_stage": "candidate", "outcome": "rejected",
                "observed_at": confirmation.observed_at.isoformat() if confirmation is not None
                else formed_at.isoformat(),
                "decision_at": formed_at.isoformat(), "available_at": formed_at.isoformat(),
                "reason": "setup_score_mismatch" if rejection == "fill_permission_mismatch" else rejection,
                "decision_id": _decision_id("score_rejected", interaction.interaction_id, confirmation_id),
                "interaction_id": interaction.interaction_id,
                "location": interaction.location_alias, "direction": direction,
                "template_id": template.template_id, "long_score": long_score, "short_score": short_score,
                "score_margin": self.config.fill_permission_margin,
                "response_evidence_ids": list(evidence_ids), "source_observation_ids": list(evidence_ids),
                "evidence": {"metrics": {"long_score": long_score, "short_score": short_score,
                    "fill_permission_margin": self.config.fill_permission_margin},
                    "provenance": {"context_version": formation_context.version if formation_context else None,
                        "source_observation_ids": list(evidence_ids)}}}),)
        opportunity = SetupOpportunity.create(
            interaction_id=interaction.interaction_id, direction=direction, formed_at=formed_at,
            expires_at=formed_at + timedelta(minutes=self.config.response_window_candles * (5 if interaction.timeframe == "5m" else 1)),
            entry_lower=interaction.band_lower or interaction.reference_level,
            entry_upper=interaction.band_upper or interaction.reference_level,
            failure_boundary=(interaction.band_lower if direction == "long" else interaction.band_upper) or interaction.reference_level,
            evidence_ids=evidence_ids, evidence_timeframe=interaction.timeframe,
            regime_version=formation_context.version if formation_context else None, template_id=template.template_id,
            policy_version=template.version, conflict_group_id=template.execution_conflict_group,
            context_relation=relation.value,
            formation_context=ContextVersionRef(formation_context.context_id, formation_context.version) if formation_context else None,
            source_instrument=observation.instrument,
            source_session_id=observation.context.session_id if observation.context else None,
        )
        if opportunity.lifecycle_id in self._opportunities:
            return ()
        current_context = formation_context
        trigger_observation = next((item for item in response_observations if item.observation_id == confirmation_id), observation)
        trigger = observe_trigger(trigger_observation,
            boundary=(opportunity.setup.entry_upper if direction == "long" else opportunity.setup.entry_lower),
            execution_mode=ExecutionMode.EXECUTABLE.value, trigger_type=template.trigger_type.value,
            effective_at=formed_at.isoformat(), expires_at=opportunity.setup.expires_at.isoformat(),
            setup_version=opportunity.setup.setup_id, permission_version=str(current_context.version) if current_context else None,
            expected_setup_version=opportunity.setup.setup_id,
            expected_permission_version=str(current_context.version) if current_context else None, armed=True)
        admission = evaluate_admission(opportunity,
            context_available=current_context is not None,
            opening_gap_allowed=(current_context is not None and current_context.opening_gap.status != "UNAVAILABLE" and
                (current_context.opening_gap.status not in {"UP", "DOWN"} or
                 (direction == "long" and (current_context.opening_gap.gap_points or 0) >= 0) or
                 (direction == "short" and (current_context.opening_gap.gap_points or 0) <= 0))
                and not self._admission_suspended),
            direction_matches=True, confirmations_available=confirmation_id is not None,
            current_at=formed_at, context_version=str(current_context.version) if current_context else None,
            context_relation=relation.value, evidence_ids=evidence_ids,
            active_position_allowed=(
                not self._active_position
                or interaction.location_alias not in {"SESSION_LOW", "SESSION_HIGH"}
            ),
            evaluate_portfolio_gates=False)
        self._opportunities[opportunity.lifecycle_id] = opportunity
        accepted = admission.status.value == "PASSED"
        if accepted and not trigger.eligible:
            self._pending_triggers[opportunity.lifecycle_id] = {
                "direction": direction, "template": template, "interaction": interaction,
            }
        proposal = self._execution_proposal(
            accepted=accepted and trigger.eligible,
            direction=direction, observation=observation,
            opportunity=opportunity, template=template, interaction=interaction, context=context, futures=futures)
        return (
            PolicyDecision({**base, "decision_stage": "opportunity", "outcome": "recorded", "reason": "setup_candidate_formed",
                "decision_id": _decision_id("candidate", opportunity.lifecycle_id), "opportunity_id": opportunity.lifecycle_id,
                "setup_id": opportunity.setup.setup_id, "interaction_id": interaction.interaction_id,
                "location": interaction.location_alias, "direction": direction, "template_id": template.template_id,
                "source_observation_ids": list(evidence_ids), "observed_at": formed_at.isoformat(), "available_at": formed_at.isoformat(),
                "long_score": long_score, "short_score": short_score}),
            PolicyDecision({**base, "decision_stage": "trigger", "outcome": "accepted" if trigger.eligible else "rejected",
                "reason": trigger.reason, "decision_id": _decision_id("trigger", opportunity.lifecycle_id, trigger_observation.observation_id),
                "opportunity_id": opportunity.lifecycle_id, "setup_id": opportunity.setup.setup_id,
                "interaction_id": interaction.interaction_id, "location": interaction.location_alias, "direction": direction,
                "trigger": asdict(trigger), "source_observation_ids": list(evidence_ids),
                "observed_at": trigger.observed_at, "available_at": trigger.available_at}),
            PolicyDecision({**base, "decision_stage": "admission", "outcome": "accepted" if accepted else "rejected",
                "reason": "orderflow_admission", "decision_id": admission.decision_id, "opportunity_id": opportunity.lifecycle_id,
                "setup_id": opportunity.setup.setup_id, "interaction_id": interaction.interaction_id,
                "location": interaction.location_alias, "direction": direction, "template_id": template.template_id,
                "admission_status": admission.status.value, "admission_reason": admission.reason,
                "admission_predicates": [asdict(item) for item in admission.predicates],
                "formation_context": asdict(opportunity.formation_context) if opportunity.formation_context else None,
                "decision_context": asdict(opportunity.decision_context) if opportunity.decision_context else None,
                "trigger": asdict(trigger), "trigger_eligible": trigger.eligible,
                "source_observation_ids": list(evidence_ids), "execution_proposal": proposal}),
        )

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext) -> tuple[PolicyDecision, ...]:
        orderflow = bundle.orderflow_context
        base: dict[str, Any] = {
            "strategy_id": self.config.name,
            "strategy_name": self.config.name,
            "strategy_version": self.config.version,
            "config_hash": self.config.config_hash,
            "bundle_id": bundle.bundle_id,
            "decision_at": context.as_of.isoformat(),
            "available_at": context.as_of.isoformat(),
            "availability_cutoff": orderflow.cutoff.isoformat() if orderflow else None,
            "decision_stage": "trigger",
            "cell": "orderflow",
            "subject": {"vehicle": "futures", "location": "orderflow"},
            "policy": {},
            "evidence": {"metrics": {}, "state": {}, "provenance": {}},
        }
        if orderflow is None or orderflow.session_id != bundle.trading_date:
            return (PolicyDecision({**base, "outcome": "unavailable", "reason": "orderflow_context_unavailable",
                                     "decision_id": _decision_id(
                                         "unavailable", bundle.trading_date, "context", bundle.bundle_id,
                                     )}),)
        session_transitions: list[PolicyDecision] = (
            self._reset_session(orderflow, context.as_of, base)
            if self._session_id != orderflow.session_id else []
        )

        observations = self._prepare_observations(orderflow, context)
        if not observations:
            return (*session_transitions, PolicyDecision({**base, "outcome": "unavailable", "reason": "required_features_unavailable",
                                     "decision_id": _decision_id(
                                         "unavailable", orderflow.session_id, "features", bundle.bundle_id,
                                     )}))

        results: list[PolicyDecision] = list(session_transitions)
        futures = bundle.bars.get(MarketRole.FUTURES)
        expected_instrument = futures.instrument.symbol if futures is not None else None
        causal_cutoff = min(context.as_of, orderflow.cutoff)

        opening_events, next_opening_gap_1m, first_causal_5m = self._prepare_opening_gap(
            observations, orderflow, expected_instrument, causal_cutoff, base,
        )
        results.extend(opening_events)
        results.extend(self._observe_causally(
            observations, orderflow, context, base, futures, expected_instrument,
            causal_cutoff, next_opening_gap_1m, first_causal_5m,
        ))
        if not results and all(observation.observation_id in self._processed_observation_ids for observation in observations):
            return ()
        return self._finalize_lifecycle_events(results)

    def _observe_causally(
        self, observations: tuple[OrderflowObservation, ...], orderflow: OrderflowContext,
        context: DecisionContext, base: Mapping[str, Any], futures: MarketBar | None,
        expected_instrument: str | None, causal_cutoff: datetime,
        next_opening_gap_1m: OrderflowObservation | None,
        first_causal_5m: OrderflowObservation | None,
    ) -> list[PolicyDecision]:
        results: list[PolicyDecision] = []
        ordered_observations = sorted(
            observations,
            # Causal availability is the primary clock.  Do not process all
            # completed parents ahead of earlier one-minute evidence; that
            # makes a later aggregate appear to be the first causal context.
            key=lambda item: (
                item.available_at,
                0 if item.timeframe == "5m" else 1,
                item.observed_at,
                item.observation_id,
            ),
        )
        for observation in ordered_observations:
            already_processed = observation.observation_id in self._processed_observation_ids
            if already_processed and not any(
                location.observation_id not in self._processed_location_observation_ids
                for location in observation.location_observations
            ):
                continue
            if observation.timeframe not in self._location_detectors:
                continue
            if expected_instrument is not None and observation.instrument != expected_instrument:
                continue
            if observation.observed_at > causal_cutoff or observation.available_at > causal_cutoff:
                continue
            results.extend(self._advance_opening_gap(observation, next_opening_gap_1m, base))
            if observation.context is None or observation.context.session_id != orderflow.session_id:
                continue
            results.extend(self._advance_pending_triggers(observation, context, base, futures))
            opening_seed_events = self._attach_opening_seed(observation, base)
            if opening_seed_events is not None:
                results.extend(opening_seed_events)
                if any(
                    item.payload.get("reason") == "opening_interaction_attached"
                    for item in opening_seed_events
                ) and self._opening_seed is not None:
                    results.extend(self._form_opening_candidate(
                        observation, self._opening_seed, orderflow, context, base, futures,
                    ))
            gap_status = observation.context.replay_provenance.get("gap_status")
            context_events, skip_observation = self._advance_context(
                observation, orderflow, causal_cutoff, already_processed, base,
            )
            results.extend(context_events)
            if skip_observation:
                continue
            results.extend(self._handle_gap(observation, gap_status, base))
            location_observations = observation.location_observations
            if observation.timeframe == "5m" and not orderflow.validate_complete_parent(observation):
                if location_observations:
                    raise ValueError("5m location observation parent is incomplete")
                continue
            opening_parent = first_causal_5m is not None and observation.observation_id == first_causal_5m.observation_id
            if opening_parent:
                opening_close = _number(observation, "close")
                opening_open = _number(observation, "open")
                if opening_close is not None and opening_open is not None and opening_close != opening_open:
                    direction = "short" if opening_close < opening_open else "long"
                    long_score = _number(observation, "long_score")
                    short_score = _number(observation, "short_score")
                    own_score = short_score if direction == "short" else long_score
                    opposing_score = long_score if direction == "short" else short_score
                    opening_alias = "OR_LOW" if direction == "short" else "OR_HIGH"
                    seed_location = next((item for item in location_observations
                                          if item.location_alias.upper() == opening_alias), None)
                    if (seed_location is not None and self._opening_seed is None
                            and own_score is not None and opposing_score is not None
                            and own_score > opposing_score):
                        seed_id = _decision_id("opening_seed", orderflow.session_id,
                                               observation.observation_id, opening_alias, direction)
                        self._opening_seed = {
                            "seed_id": seed_id, "direction": direction, "location": opening_alias,
                            "reference_level": seed_location.reference,
                            "band_lower": seed_location.band_lower,
                            "band_upper": seed_location.band_upper,
                            "reference_generation": seed_location.reference_generation,
                            "source_observation_id": observation.observation_id,
                            "attached_observation_id": None,
                            "formation_available_at": observation.available_at.isoformat(),
                            "opening_policy": ("opening_retest" if (
                                direction == "short" and opening_close <= seed_location.reference
                            ) or (direction == "long" and opening_close >= seed_location.reference)
                            else "opening_breakout"),
                        }
                        results.append(PolicyDecision({
                            **base, "decision_stage": "candidate", "outcome": "recorded",
                            "reason": "opening_candle_directional_seed", "decision_id": seed_id,
                            "candidate_provenance": "opening_hypothesis", "direction": direction,
                            "location": opening_alias, "reference_level": seed_location.reference,
                            "reference_generation": seed_location.reference_generation,
                            "source_observation_ids": [observation.observation_id],
                            "long_score": long_score, "short_score": short_score,
                            "available_at": observation.available_at.isoformat(),
                        }))
                        # Form the canonical opening lifecycle at the
                        # completed 5m parent. Later 1m opening evidence is
                        # attached to this lifecycle rather than creating it.
                        results.extend(self._form_opening_candidate(
                            observation, self._opening_seed, orderflow, context, base, futures,
                        ))
                location_observations = tuple(item for item in location_observations
                                              if item.location_alias.upper() not in {"OR_HIGH", "OR_LOW"})
            self._processed_observation_ids.add(observation.observation_id)
            interaction_context_events: list[PolicyDecision] = []
            interaction_created_events: list[PolicyDecision] = []
            for location_observation in sorted(
                location_observations,
                key=lambda item: (is_diagnostic_location(item.location_type), item.location_alias),
            ):
                context_events, created_events, lifecycle_events = self._process_location_observation(
                    observation, location_observation, base,
                )
                interaction_context_events.extend(context_events)
                interaction_created_events.extend(created_events)
                results.extend(lifecycle_events)
            results.extend(interaction_context_events)
            results.extend(interaction_created_events)
            self._advance_interaction_projections(observation, observations)
            detector = self._location_detectors[observation.timeframe]
            previous_opportunities = self._advance_opportunity_timeouts(observation, detector)
            response_interactions = list(detector.active_interactions)
            response_interactions.extend(item for item in detector.closed_interactions
                                         if item.closed_at == observation.available_at)
            for interaction in response_interactions:
                results.extend(self._evaluate_response(
                    interaction, observation, observations, orderflow, context,
                    base, detector, futures,
                ))
            results.extend(self._record_expired_opportunities(previous_opportunities, base))
            if self._admission_suspended:
                results.append(PolicyDecision({
                    **base, "outcome": "unavailable", "reason": "admission_suspended_after_source_gap",
                    "decision_id": _decision_id("unavailable", "gap", observation.observation_id),
                    "source_observation_ids": [observation.observation_id],
                    "expected_available_at": self._gap_end.isoformat() if self._gap_end else None,
                }))
        return results

    @staticmethod
    def _finalize_lifecycle_events(results: list[PolicyDecision]) -> tuple[PolicyDecision, ...]:
        lifecycle_reasons = {"interaction_context_published", "interaction_created"}
        lifecycle_events = [item for item in results if item.payload.get("reason") in lifecycle_reasons]
        if lifecycle_events:
            lifecycle_events.sort(key=lambda item: item.payload.get("reason") != "interaction_context_published")
            first_lifecycle_index = min(
                index for index, item in enumerate(results)
                if item.payload.get("reason") in lifecycle_reasons
            )
            results = [item for item in results if item.payload.get("reason") not in lifecycle_reasons]
            results[first_lifecycle_index:first_lifecycle_index] = lifecycle_events
        return tuple(results)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        if event.event_type != "FILLED" or event.status != "FILLED":
            return None
        prefix = "orderflow-entry:"
        if not event.client_order_id.startswith(prefix):
            return None
        winner_id = event.client_order_id[len(prefix):]
        winner = self._opportunities.get(winner_id)
        if winner is None:
            return None
        try:
            at = datetime.fromisoformat(
                (event.occurred_at or event.available_at or "").replace("Z", "+00:00")
            )
        except ValueError:
            return None
        evidence_id = event.event_id or event.client_order_id
        for lifecycle_id, opportunity in tuple(self._opportunities.items()):
            if (
                lifecycle_id != winner_id
                and opportunity.status is OpportunityStatus.ACTIVE
                and opportunity.execution_group_id == winner.execution_group_id
            ):
                self._opportunities[lifecycle_id] = opportunity.transition(
                    OpportunityStatus.INVALIDATED, at=at,
                    reason="execution_group_claimed", evidence_ids=(evidence_id,),
                )
        return None

    def snapshot(self) -> Mapping[str, object]:
        return policy_snapshot_payload(self)

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "OrderflowPolicy":
        return policy_from_snapshot(cls, snapshot)


class OrderflowStrategy:
    """Strategy facade preserving the public Strategy protocol."""

    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.policy = OrderflowPolicy(config)
        self.coordinator = SharedDecisionCoordinator(self.policy)
        self._position_manager = OrderflowPositionManager(config)

    @property
    def _decision_positions(self):
        return self._position_manager.positions

    @property
    def _pending_exits(self):
        return self._position_manager.pending_exits

    @property
    def _pending_exit_actions(self):
        return self._position_manager.pending_exit_actions

    @property
    def _completed_bars(self):
        return self._position_manager.completed_bars

    @property
    def _stop_update_evidence(self):
        return self._position_manager.stop_update_evidence

    def bind_account_context(self, portfolio: object, capital_context: object) -> None:
        del portfolio, capital_context

    def set_active_position(self, active: bool) -> None:
        self.policy._active_position = active

    @property
    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(self.policy.config.name, self.policy.config.version, self.policy.config.config_hash)

    def on_bundle(self, bundle: DecisionBundle):
        cutoff = (
            bundle.orderflow_context.cutoff
            if bundle.orderflow_context is not None
            else self._fallback_cutoff(bundle)
        )
        context = DecisionContext(
            as_of=cutoff,
            account_revision=0,
            capital=CapitalSnapshot(0.0, 0.0, 0.0, 0.0),
            margin=MarginSnapshot(0.0, 0.0, 0.0),
            exposure=ExposureSnapshot(0.0, 0),
        )
        return self.evaluate(bundle, context)

    @staticmethod
    def _fallback_cutoff(bundle: DecisionBundle):
        from ftx_paper.config import IST

        raw = bundle.minute if "T" in bundle.minute else f"{bundle.trading_date}T{bundle.minute}"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
        return parsed

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext):
        self.policy.set_active_position(context.exposure.open_positions > 0)
        return self.coordinator.evaluate(bundle, context)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.coordinator.on_execution_event(event)

    def register_entry(self, order: OrderIntent, *, fill_price: float | None = None,
                       entry_fill_time: datetime | str | None = None,
                       reference_price: float | None = None) -> None:
        self._position_manager.register_entry(
            order, fill_price=fill_price, entry_fill_time=entry_fill_time,
            reference_price=reference_price,
        )
        if order.client_order_id in self._position_manager.positions:
            self.policy.set_active_position(True)

    def on_closed_bar(self, bar: MarketBar) -> tuple[ExitAction, ...]:
        return self._position_manager.on_closed_bar(bar)

    @property
    def stop_update_evidence(self) -> tuple[Mapping[str, object], ...]:
        return tuple(self._stop_update_evidence)

    def settle_exit(self, exit_order_id: str, *, filled: bool, **_kwargs: object) -> None:
        self._position_manager.settle_exit(exit_order_id, filled=filled)
        if filled and not self._position_manager.positions:
            self.policy.set_active_position(False)

    def snapshot(self) -> Mapping[str, object]:
        return wrap_snapshot(
            strategy_id=ORDERFLOW_NAME,
            metadata={"name": self.metadata.name, "version": self.metadata.version,
                      "config_hash": self.metadata.config_hash},
            payload=strategy_snapshot_payload(self),
        )

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object], *, capital_profile=None) -> "OrderflowStrategy":
        del capital_profile
        return strategy_from_snapshot(cls, snapshot)


__all__ = [
    "OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy",
    "ORDERFLOW_NAME", "ORDERFLOW_VERSION",
]
