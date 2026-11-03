"""Replay coordination for template-derived opportunity lifecycles."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime
from typing import Any, Protocol

from .contextual import build_context_scores
from .execution import (
    AdmissionStatus,
    ExecutionAdmission,
    ExecutionCoordinator,
    _opposite_open_exposure,
)
from .hierarchy import (
    ContextObservation,
    ContextRelation,
    ContextVersionRef,
    MarketContext,
    classify_context_relation,
)
from .interactions import (
    InteractionSnapshot,
    InteractionStatus,
    LocationTemplate,
    candle_identity,
)
from .interaction_detector import InteractionDetector
from .lifecycle import (
    Direction,
    OpeningInteractionEvidence,
    SetupOpportunity,
)
from .opportunity import (
    DirectionalResponse,
    EvidenceRef,
    OpportunityContext,
    OpportunityOutcome,
    form_opportunity,
    opportunity_identity,
)
from .orderflow_common import Aggregate, candle_reference, canonical_timestamp
from .policies import PDPolicy, LocationPolicy, StandardLocationPolicy
from .policies.base import HypothesisEvaluation
from .state_machine_types import LocationRegistry, ReplayContext
from .templates import (
    OpeningInteractionKind,
    TradeTemplate,
)
from .timestamps import nifty_label, serialize_utc
from .timestamps import coerce_timestamp

LIQUIDITY_PROBE = "LIQUIDITY_PROBE"
TIMEFRAME_VARIANT = "1m_5m"
STRUCTURE_TIMEFRAME = "5m"


@dataclass(frozen=True, slots=True)
class LifecycleFormation:
    """Replay transition envelope for a formed setup lifecycle."""

    identity: str
    lifecycle_id: str
    values: dict[str, Any]
    observed_at: Any
    available_at: Any
    lifecycle: Any = None
    setup: Any = None
    context_references: tuple[Any, ...] = ()
    evidence_lineage: tuple[str, ...] = ()
    execution_group: str | None = None
    availability: str = "available"


def policy_for(location: LocationTemplate) -> LocationPolicy:
    """Resolve location-specific response rules without splitting the replay."""
    if location in {LocationTemplate.PD_LOW, LocationTemplate.PD_HIGH}:
        return PDPolicy()
    return StandardLocationPolicy()


class OpportunityReplayHost(Protocol):
    @property
    def context(self) -> ReplayContext: ...

    @property
    def execution(self) -> ExecutionCoordinator: ...

    @property
    def location_registry(self) -> LocationRegistry | None: ...

    @property
    def templates(self) -> dict[str, TradeTemplate]: ...

    @property
    def interaction_detector(self) -> InteractionDetector | None: ...

    def selected_interaction_context(
        self, interaction_id: str, *, as_of: datetime | str,
    ) -> MarketContext | None: ...

    def structure_interactions(self) -> tuple[InteractionSnapshot, ...]: ...


class OpportunityCoordinator:
    """Own opportunity identification, formation, and invalidation decisions."""

    def __init__(self, host: OpportunityReplayHost) -> None:
        self.host = host
        self._last_invalidation_5m_available_at: datetime | None = None
        self._formations: list[LifecycleFormation] = []

    def formations_for(self, clock: datetime | str) -> tuple[LifecycleFormation, ...]:
        """Return owner-published formations causally available at ``clock``."""
        available = coerce_timestamp(clock)
        return tuple(
            item for item in self._formations
            if coerce_timestamp(item.available_at) <= available
        )

    def publish_interaction_context(
        self, interaction: InteractionSnapshot, bar_events: list[dict[str, Any]],
    ) -> None:
        projection = dict(interaction.synthetic_snapshots).get(interaction.timeframe, ())
        if not projection:
            return
        snapshot = projection[-1]
        context_id = f"interaction:{interaction.interaction_id}"
        versions = self.host.context.market_contexts.versions.get(context_id, ())
        version = f"v{len(versions) + 1}"
        score_candle = replace(
            interaction.creation_candle,
            time=snapshot.first_source_start,
            end_time=snapshot.last_source_end,
            available_at=snapshot.available_at,
            open=snapshot.open,
            high=snapshot.high,
            low=snapshot.low,
            close=snapshot.close,
            volume=int(snapshot.volume),
        )
        scores = build_context_scores(
            score_candle,
            raw_rows=self.host.context.rows,
            aggregates=self.host.context.aggregates,
            atr=self.host.context.atr_by_session.get(score_candle.session_date),
        )
        observed_at = snapshot.last_source_end
        available_at = snapshot.available_at
        reference = self.host.context.publish_context(MarketContext(
            context_id=context_id,
            version=version,
            timestamp=observed_at,
            available_at=available_at,
            location=str(interaction.location),
            market_state=self.host.context.market_state,
            definition_version=interaction.template_version,
            source_timeframe=interaction.timeframe,
            long_score=scores[0] if scores is not None else None,
            short_score=scores[1] if scores is not None else None,
            source_provenance=({"source_candle_ids": list(snapshot.source_candle_ids)},),
        ))
        self.host.context.market_contexts.interaction_contexts[
            interaction.interaction_id
        ] = reference
        if interaction.timeframe == "5m" and interaction.location in {"PD_LOW", "PD_HIGH"}:
            role = interaction.pd_role or (
                "SUPPORT" if interaction.location == "PD_LOW" else "RESISTANCE"
            )
            for bar in self.host.context.aggregates.get("5m", ()):
                if bar.available_at > available_at or bar.time <= interaction.creation_candle.time:
                    continue
                if bar.low > interaction.band_high:
                    role = "SUPPORT"
                elif bar.high < interaction.band_low:
                    role = "RESISTANCE"
            detector_state = (
                self.host.interaction_detector.interactions_for(
                    interaction.timeframe,
                ).get(interaction.interaction_id)
                if self.host.interaction_detector is not None else None
            )
            role_already_published = (
                detector_state is not None
                and detector_state.pd_role_context_available_at is not None
            )
            if detector_state is not None and not role_already_published:
                detector_state.pd_role = role
                detector_state.pd_role_context_available_at = available_at
            if not role_already_published:
                self.host.context.emit({
                    "kind": "role_context_available",
                    "interaction_id": interaction.interaction_id,
                    "location": interaction.location,
                    "role": role,
                    "context_id": context_id,
                    "context_version": version,
                    "observed_at": observed_at,
                    "available_at": available_at,
                }, bar_events)
        self.host.context.emit({
            "kind": "interaction_context_published",
            "interaction_id": interaction.interaction_id,
            "context_id": context_id,
            "context_version": version,
            "observed_at": observed_at,
            "available_at": available_at,
            "long_score": scores[0] if scores is not None else None,
            "short_score": scores[1] if scores is not None else None,
        }, bar_events)
        if self.host.interaction_detector is not None:
            self.host.interaction_detector.record_score(
                interaction.interaction_id,
                snapshot=snapshot,
                context_version=version,
                long_score=scores[0] if scores is not None else None,
                short_score=scores[1] if scores is not None else None,
            )
            self.host.context.interactions[interaction.interaction_id] = (
                self.host.interaction_detector.interactions_for(
                    interaction.timeframe,
                )[interaction.interaction_id].evaluation_snapshot()
            )

    def _register_candidate(
        self, life: SetupOpportunity, bar_events: list[dict[str, Any]],
    ) -> bool:
        ctx = self.host.context
        # A predefined execution group is single-fill, but candidates formed
        # before that fill must remain eligible; only a consumed claim denies.
        claim = ctx.execution_engine.execution_group_claim(life.execution_group_id)
        if claim is not None:
            ctx.emit(
                {
                    "kind": "execution_group_candidate_denied",
                    "response_episode_id": life.response_episode_id,
                    "execution_group_id": life.execution_group_id,
                    "winning_lifecycle_id": claim[0],
                    "claim_fill_time": claim[1],
                    "reason": "execution_group_already_filled",
                    "observed_at": life.formation_at,
                    "available_at": life.formation_available_at,
                },
                bar_events,
                life,
            )
            return False
        return ctx.register_opportunity(life)

    def _attach_setup(self, life: SetupOpportunity, setup: Any) -> None:
        formation = LifecycleFormation(
            identity=life.causal_identity or life.lifecycle_id,
            lifecycle_id=life.lifecycle_id,
            values={},
            observed_at=life.formation_at,
            available_at=life.formation_available_at,
            lifecycle=life,
            setup=setup,
        )
        self._formations.append(formation)
        self.host.execution.attach_setup(formation)

    def _seed_or_directional_episode(
        self, candle: Aggregate, row: dict[str, Any], bar_events: list[dict[str, Any]],
    ) -> None:
        """Create the special pre-interaction OR episode from the first 5m candle."""
        ctx = self.host.context
        if self.host.location_registry is None or candle.time in ctx.or_seed_candles:
            return
        if candle.close == candle.open:
            return
        direction = Direction.SHORT if candle.close < candle.open else Direction.LONG
        location = LocationTemplate.OR_LOW if direction is Direction.SHORT else LocationTemplate.OR_HIGH
        record = self.host.location_registry.resolve_as_of(location.value, candle.available_at)
        if record is None:
            return
        scores = build_context_scores(
            candle, raw_rows=ctx.rows, aggregates=ctx.aggregates,
            atr=ctx.atr_by_session.get(candle.session_date),
        )
        if scores is None:
            return
        long_score, short_score = scores
        own_score = short_score if direction is Direction.SHORT else long_score
        opposing_score = long_score if direction is Direction.SHORT else short_score
        if own_score is None or opposing_score is None or own_score <= opposing_score:
            return
        template = next(
            (
                item for item in self.host.templates.values()
                if location in item.locations
                and item.response_type.value == "acceptance"
            ),
            None,
        )
        if template is None:
            return
        ctx.or_seed_candles.add(candle.time)
        seed_id = (
            f"OR_SEED:{candle.session_date}:{candle.symbol}:"
            f"{record.generation}:{direction.value}:{nifty_label(candle.time)}"
        )
        response = DirectionalResponse(
            direction=direction,
            anchor=location.value,
            reference_level=record.price,
            band=record.band,
            structure_boundary=record.price,
            invalidation_boundary=(
                record.band.high
                if direction is Direction.SHORT
                else record.band.low
            ),
            evidence_ids=(seed_id, candle_identity(candle)),
            observed_at=candle.end_time,
            available_at=candle.available_at,
            evidence=(candle,),
            evidence_lineage=(EvidenceRef(
                candle_identity(candle), candle.end_time, candle.available_at,
            ),),
            entry_boundary=record.price,
        )
        # The opening seed is provisional. Its 1m interaction policy is
        # attached later and never represented as a normal location template.
        setup_template = template
        opening_policy = (
            OpeningInteractionKind.OPENING_RETEST.value
            if (
                direction is Direction.SHORT and candle.close <= record.price
            ) or (
                direction is Direction.LONG and candle.close >= record.price
            )
            else OpeningInteractionKind.OPENING_BREAKOUT.value
        )
        ctx.setup_number += 1
        symbol = str(row.get("symbol", candle.symbol))
        setup_id = f"{candle.session_date}-{symbol}-{TIMEFRAME_VARIANT}-{direction.value}-candidate-{ctx.setup_number}"
        identity = opportunity_identity(
            session_date=candle.session_date,
            instrument=symbol,
            direction=direction,
            interaction_id=seed_id,
            reference_generation=record.generation,
            response_evidence_ids=response.evidence_ids,
            formation_at=response.observed_at,
            formation_available_at=response.available_at,
            template_id=setup_template.template_id,
        )
        life, setup = form_opportunity(
            response,
            OpportunityContext(
                lifecycle_id=f"lifecycle-{setup_id}",
                session_date=candle.session_date,
                instrument=symbol,
                strategy=f"orderflow-{TIMEFRAME_VARIANT}",
                candidate_id=setup_id,
                parent_context=None,
                context_candle=candle,
                failure_boundary=response.invalidation_boundary,
                clock=candle.available_at,
                regime_version=ctx.regime.version if ctx.regime is not None else "unresolved",
                policy_version=setup_template.policy_version,
                reference_generation=record.generation,
                causal_identity=identity,
                location_interaction_id=None,
                opening_policy_version=f"{opening_policy}-1m-v1",
                response_episode_id=f"opening-response-episode:{seed_id}",
            ),
            setup_template,
            formation_available_at=candle.available_at,
            decision_context_required=False,
        )
        if not self._register_candidate(life, bar_events):
            return
        self._attach_setup(life, setup)
        ctx.emit({
            "kind": "or_directional_seed",
            "lifecycle_id": life.lifecycle_id,
            "causal_identity": identity,
            "location": location.value,
            "direction": direction.value,
            "interaction_id": None,
            "opening_interaction_kind": None,
            "opening_policy_version": f"{opening_policy}-1m-v1",
            "candidate_provenance": "opening_hypothesis",
            "reference_generation": record.generation,
            "observed_at": candle.end_time,
            "available_at": candle.available_at,
            "long_score": long_score,
            "short_score": short_score,
        }, bar_events)
        confirmation_evidence = {
            "context": candle.to_dict(),
            "available_at": candle.available_at,
        }
        ctx.emit({
            "kind": "opportunity_outcome",
            "interaction_id": seed_id,
            "location": location.value,
            "template_id": setup_template.template_id,
            "response_type": setup_template.response_type.value,
            "direction": direction.value,
            "status": "RESOLVED",
            "reason": "opening_candle_directional_seed",
            "observed_at": response.observed_at,
            "available_at": response.available_at,
            "evidence_ids": list(response.evidence_ids),
            "evidence_lineage": [asdict(item) for item in response.evidence_lineage],
            "confirmation_evidence": confirmation_evidence,
        }, bar_events)
        lifecycle_event = {
            "kind": "lifecycle_formed",
            "template_id": setup_template.template_id,
            "location": location.value,
            "level": STRUCTURE_TIMEFRAME,
            "observed_at": response.observed_at,
            "available_at": life.formation_available_at,
            "candidate_id": setup_id,
            "parent_context": None,
            "long_score": long_score,
            "short_score": short_score,
            "reference_level": life.reference_level,
            "failure_boundary": life.failure_boundary,
            "context_candle_id": nifty_label(candle.time),
            "context_candle_reference": candle_reference(candle),
            "context_candle_available_at": candle.available_at,
            "response_candle_id": response.evidence_ids[-1],
            "response_candle_reference": candle_reference(response.evidence[-1]),
            "response_observed_at": response.observed_at,
            "response_available_at": response.available_at,
            "response_evidence_ids": list(response.evidence_ids),
            "location_interaction_id": None,
            "opening_interaction_kind": None,
            "opening_policy_version": f"{opening_policy}-1m-v1",
            "entry_boundary": response.entry_boundary,
            "confirmation_evidence": confirmation_evidence,
        }
        ctx.emit(lifecycle_event, bar_events, life)
        ctx.emit_market_event(
            {
                "kind": "setup_candidate",
                "level": STRUCTURE_TIMEFRAME,
                "setup_id": setup_id,
                "direction": direction.value,
                "template_id": setup_template.template_id,
                "response_type": setup_template.response_type.value,
                "setup_version": f"{setup_id}.v1",
                "regime_version": ctx.regime.version if ctx.regime else None,
                "aggregate_time": candle.time,
                "observed_at": response.observed_at,
                "available_at": life.formation_available_at,
                "predicates": {
                    "location_interaction": False,
                    "completed_bar": True,
                    "confirmation_timeframe": STRUCTURE_TIMEFRAME,
                    "confirmation_bar_count": 1,
                    "confirmation_available_at": candle.available_at,
                },
                "owner": STRUCTURE_TIMEFRAME,
                "evidence_timeframe": STRUCTURE_TIMEFRAME,
                 "response_candle_id": response.evidence_ids[-1],
                 "response_observed_at": response.observed_at,
                 "response_available_at": response.available_at,
                 "response_evidence_ids": list(response.evidence_ids),
                 "location_interaction_id": None,
                 "opening_interaction_kind": None,
                 "opening_policy_version": f"{opening_policy}-1m-v1",
                 "entry_boundary": response.entry_boundary,
                "long_score": long_score,
                "short_score": short_score,
                "lifecycle_id": life.lifecycle_id,
                "lifecycle_stage": life.stage.value,
                "thesis_type": "structural_candidate",
            },
            LIQUIDITY_PROBE,
            bar_events,
            life,
        )
        self.host.execution.admit_opportunity(
            life, serialize_utc(candle.available_at), bar_events,
        )

    def handle_opening_interaction(
        self, row: dict[str, Any], timestamp: str, next_time: str,
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Attach the first completed 1m opening evidence to each seed."""
        ctx = self.host.context
        if not ctx.opportunities:
            return
        timestamp_dt = canonical_timestamp(timestamp)
        timestamp_label = nifty_label(timestamp_dt)
        next_time_label = nifty_label(canonical_timestamp(next_time))
        for life in tuple(ctx.opportunities.values()):
            if (
                not life.active
                or life.opening_policy_version is None
                or life.opening_interaction_kind in {
                    OpeningInteractionKind.OPENING_BREAKOUT.value,
                    OpeningInteractionKind.OPENING_RETEST.value,
                }
                or life.formation_available_at >= timestamp_dt
            ):
                continue
            boundary = life.reference_level
            if boundary is None or life.context_candle is None:
                continue
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])
            direction = life.direction
            crossed = close >= boundary if direction is Direction.LONG else close <= boundary
            touched = low <= boundary <= high
            if not (crossed or touched):
                continue
            policy = life.opening_policy_version or ""
            if policy.startswith(OpeningInteractionKind.OPENING_BREAKOUT.value):
                if not crossed:
                    continue
                kind = OpeningInteractionKind.OPENING_BREAKOUT
            elif not life.opening_interaction_evidence:
                kind = OpeningInteractionKind.OPENING_REJECTION
            else:
                has_recovery = any(
                    item.get("kind") == "opening_recovery"
                    for item in life.opening_interaction_evidence
                )
                recovered = close > boundary if direction is Direction.SHORT else close < boundary
                if not has_recovery and recovered:
                    kind = OpeningInteractionKind.OPENING_REJECTION
                elif has_recovery and crossed:
                    kind = OpeningInteractionKind.OPENING_RETEST
                else:
                    continue
            evidence: OpeningInteractionEvidence = {
                "time": str(row["time"]),
                "observed_at": timestamp_label,
                "available_at": next_time_label,
                "open": float(row["open"]),
                "high": high,
                "low": low,
                "close": close,
                "reference_level": boundary,
                "kind": (
                    "opening_recovery"
                    if kind is OpeningInteractionKind.OPENING_REJECTION
                    and life.opening_interaction_evidence
                    and (close > boundary if direction is Direction.SHORT else close < boundary)
                    else kind.value
                ),
            }
            life.opening_interaction_kind = kind.value
            life.opening_interaction_evidence.append(evidence)
            ctx.emit({
                "kind": "opening_interaction",
                "lifecycle_id": life.lifecycle_id,
                "interaction_kind": kind.value,
                "location_interaction_id": None,
                "evidence": list(life.opening_interaction_evidence),
                "policy_version": life.opening_policy_version,
                "observed_at": timestamp_label,
                "available_at": next_time_label,
            }, bar_events)

    def handle_structure(
        self,
        timestamp: str,
        next_time: str,
        row: dict[str, Any],
        available5: list[Aggregate],
        bar_events: list[dict[str, Any]],
    ) -> None:
        ctx = self.host.context
        latest_structure = available5[-1] if available5 else None
        if latest_structure is None:
            return
        session_candles = [
            item for item in ctx.aggregates[STRUCTURE_TIMEFRAME]
            if item.session_date == latest_structure.session_date
        ]
        first_session_candle = min(session_candles, key=lambda item: item.time) if session_candles else None
        if first_session_candle is not None:
            self._seed_or_directional_episode(first_session_candle, row, bar_events)
        self._invalidate_by_structure(available5, timestamp, bar_events)
        latest = latest_structure
        new_confirmation_context = latest.time != ctx.last_confirmation_context_time
        if new_confirmation_context:
            ctx.last_confirmation_context_time = latest.time
        confirmation_evidence = {
            "context": latest.to_dict(),
            "available_at": latest.available_at,
        }
        for interaction in reversed(self.host.structure_interactions()):
            # VWAP is diagnostic context only and never enters opportunity evaluation.
            if str(interaction.location) == "VWAP":
                continue
            if interaction.creation_candle.available_at > latest.available_at:
                continue
            is_pd = str(interaction.location) in {"PD_LOW", "PD_HIGH"}
            response_already_published = any(
                interaction_id == interaction.interaction_id
                for interaction_id, _template_id in ctx.published_resolved_responses
            )
            if (
                interaction.status is not InteractionStatus.ACTIVE
                and (
                    is_pd
                    or interaction.terminal_candle_id != candle_identity(latest)
                    or not new_confirmation_context
                    or response_already_published
                )
            ):
                continue
            templates = tuple(
                template for template in self.host.templates.values()
                if interaction.location in template.locations
            )
            if not templates:
                continue
            if not new_confirmation_context and not is_pd:
                continue
            interaction_context = self.host.selected_interaction_context(
                interaction.interaction_id, as_of=latest.available_at,
            )
            if is_pd:
                policy = policy_for(LocationTemplate(str(interaction.location)))
                if not isinstance(policy, PDPolicy):
                    raise TypeError("PD locations require PDPolicy")
                interaction_state = self.host.interaction_detector.interactions.get(
                    interaction.interaction_id
                ) if self.host.interaction_detector is not None else None
                if (
                    interaction_state is not None
                    and interaction_state.pd_role is None
                    and interaction_state.pd_role_context_available_at is None
                ):
                    interaction_state.pd_role = (
                        "SUPPORT" if interaction.location == "PD_LOW" else "RESISTANCE"
                    )
                    interaction_state.pd_role_context_available_at = interaction.available_at
                    ctx.emit({
                        "kind": "role_context_available",
                        "interaction_id": interaction.interaction_id,
                        "location": interaction.location,
                        "role": interaction_state.pd_role,
                        "observed_at": interaction.interaction_started_at,
                        "available_at": interaction.available_at,
                    }, bar_events)
                pd_evidence: tuple[Aggregate, ...] = tuple(dict.fromkeys(
                    bar for template in templates for bar in available5
                    if bar.timeframe == template.response_timeframe
                    and bar.symbol == interaction.instrument
                    and bar.available_at >= interaction.creation_candle.available_at
                    and bar.available_at >= bar.end_time
                    and bar.available_at <= latest.available_at
                    and bar.available_at <= canonical_timestamp(
                        f"{bar.session_date}T{template.expiry_at}"
                    )
                ))
                evaluations = policy.evaluate_hypotheses(
                    templates, interaction, pd_evidence,
                    as_of=serialize_utc(latest.available_at),
                    state=interaction_state,
                    emit=(
                        (lambda event: ctx.emit(event, bar_events))
                        if interaction_state is not None else None
                    ),
                )
                for evaluation in evaluations:
                    self._process_observation(
                        evaluation.template, evaluation.outcome, evaluation.identity,
                        interaction,
                        interaction_context, latest,
                        confirmation_evidence, latest.available_at,
                        row, next_time, bar_events, arbitration=evaluation,
                    )
                if interaction_state is not None:
                    ctx.interactions[interaction.interaction_id] = interaction_state.evaluation_snapshot()
                    transition_at = interaction_state.pd_role_transition_available_at
                    if (
                        transition_at is not None
                        and interaction_state.pd_role_transition_processed_at is None
                    ):
                        transition_candle = interaction_state.pd_role_transition_candle_id
                        transition_bar = next(
                            (bar for bar in pd_evidence if bar.time == transition_candle), latest,
                        )
                        for life in tuple(ctx.opportunities.values()):
                            if (
                                life.active
                                and life.location_interaction_id == interaction.interaction_id
                                and life.formation_available_at <= transition_at
                            ):
                                self.host.execution.invalidate_opportunity(
                                    life,
                                    timestamp=serialize_utc(transition_at),
                                    observed_at=serialize_utc(transition_bar.end_time),
                                    available_at=serialize_utc(transition_at),
                                    bar_events=bar_events,
                                )
                                ctx.emit({
                                    "kind": "setup_invalidated",
                                    "interaction_id": interaction.interaction_id,
                                    "lifecycle_id": life.lifecycle_id,
                                    "reason": "pd_role_transition",
                                    "observed_at": transition_bar.end_time,
                                    "available_at": transition_at,
                                    "boundary": life.failure_boundary,
                                }, bar_events)
                        interaction_state.pd_role_transition_processed_at = latest.available_at
                        ctx.interactions[interaction.interaction_id] = interaction_state.evaluation_snapshot()
                continue
            # All standard-location arms see one identical, causally bounded
            # window. Arbitration is explicit and independent of mapping order.
            response_timeframes = {template.response_timeframe for template in templates}
            if len(response_timeframes) != 1:
                raise ValueError("standard-location templates must share a response timeframe")
            response_timeframe = next(iter(response_timeframes))
            response_bars = self.host.context.aggregates.get(response_timeframe, ())
            shared_evidence = tuple(sorted(
                (
                    bar for bar in response_bars
                    if bar.timeframe == response_timeframe
                    and bar.symbol == interaction.instrument
                    and bar.available_at >= interaction.creation_candle.available_at
                    and bar.available_at >= bar.end_time
                    and bar.available_at <= latest.available_at
                    and bar.available_at <= canonical_timestamp(
                        f"{bar.session_date}T15:00"
                    )
                ),
                key=lambda bar: (bar.time, bar.available_at),
            ))
            policy = policy_for(LocationTemplate(str(interaction.location)))
            if not isinstance(policy, StandardLocationPolicy):
                raise TypeError("standard locations require StandardLocationPolicy")
            evaluations = policy.arbitrate(
                templates, interaction, shared_evidence,
                as_of=serialize_utc(latest.available_at),
            )
            for evaluation in evaluations:
                self._process_observation(
                    evaluation.template, evaluation.outcome, evaluation.identity,
                    interaction, interaction_context, latest,
                    confirmation_evidence, latest.available_at,
                    row, next_time, bar_events, arbitration=evaluation,
                )

    def _invalidate_by_structure(
        self, available5: list[Aggregate], timestamp: str, bar_events: list[dict[str, Any]],
    ) -> None:
        new_bars = [
            bar for bar in available5
            if self._last_invalidation_5m_available_at is None
            or bar.available_at > self._last_invalidation_5m_available_at
        ]
        for latest in new_bars:
            for life in tuple(self.host.context.opportunities.values()):
                if not life.active or life.candidate_invalidated:
                    continue
                # A provisional 5m episode is resolved by its 1m owner; an
                # opposing 5m close is evidence, not invalidation.
                if not life.episode_confirmed:
                    continue
                if self.host.execution.setup_invalidated_by_close(life, close=latest.close):
                    self.host.execution.invalidate_opportunity(
                        life,
                        timestamp=timestamp,
                        observed_at=serialize_utc(latest.end_time),
                        available_at=serialize_utc(latest.available_at),
                        bar_events=bar_events,
                    )
        if new_bars:
            self._last_invalidation_5m_available_at = new_bars[-1].available_at

    def _process_observation(
        self,
        template: TradeTemplate,
        observation: OpportunityOutcome,
        identity: str | None,
        interaction: InteractionSnapshot,
        interaction_context: MarketContext | None,
        latest: Aggregate,
        confirmation_evidence: dict[str, Any],
        confirmation_available_at: datetime | str,
        row: dict[str, Any],
        next_time: str,
        bar_events: list[dict[str, Any]],
        arbitration: HypothesisEvaluation | None = None,
    ) -> None:
        ctx = self.host.context
        observation = replace(
            observation,
            confirmation_available_at=confirmation_available_at,
            confirmation_context=latest,
        )
        response = observation.response
        ctx.record_opportunity_outcome(interaction.interaction_id, template.template_id, observation)
        publish_response = response is not None and ctx.record_resolved_response(
            interaction.interaction_id, template.template_id,
        )
        if arbitration is not None and response is not None and publish_response:
            ctx.emit(
                {
                    "kind": "hypothesis_arbitrated",
                    "interaction_id": interaction.interaction_id,
                    "location": str(interaction.location),
                    "template_id": template.template_id,
                    "response_type": template.response_type.value,
                    "direction": response.direction.value,
                    "session_date": interaction.session_date,
                    "arbitration_selected": arbitration.selected,
                    "arbitration_reason": arbitration.arbitration_reason,
                    "observed_at": response.observed_at,
                    "available_at": response.available_at,
                },
                bar_events,
            )
        if (
            response is not None
            and publish_response
            and str(interaction.location) not in {"PD_LOW", "PD_HIGH"}
        ):
            ctx.emit(
                {
                    "kind": "response_confirmed",
                    "interaction_id": interaction.interaction_id,
                    "location": str(interaction.location),
                    "template_id": template.template_id,
                    "response_type": template.response_type.value,
                    "direction": response.direction.value,
                    "observed_at": response.observed_at,
                    "available_at": response.available_at,
                    "evidence_ids": list(response.evidence_ids),
                },
                bar_events,
            )
        ctx.emit(
            {
                "kind": "opportunity_outcome",
                "interaction_id": interaction.interaction_id,
                "location": str(interaction.location),
                "template_id": template.template_id,
                "response_type": template.response_type.value,
                "direction": (
                    response.direction.value
                    if response is not None else template.direction.value
                ),
                "status": observation.status.value,
                "reason": observation.reason,
                "arbitration_selected": (
                    arbitration.selected if arbitration is not None else None
                ),
                "arbitration_reason": (
                    arbitration.arbitration_reason if arbitration is not None else None
                ),
                "as_of": observation.as_of,
                "observed_at": observation.observed_at,
                "available_at": observation.available_at,
                "evidence_ids": [item.time for item in observation.evidence],
                "evidence_lineage": [asdict(item) for item in observation.evidence_lineage],
                "confirmation_evidence": confirmation_evidence,
            },
            bar_events,
        )
        if response is None:
            return
        if str(interaction.location) in {"PD_LOW", "PD_HIGH"}:
            role = interaction.pd_role or (
                "SUPPORT" if interaction.location == "PD_LOW" else "RESISTANCE"
            )
            expected_direction = "long" if role == "SUPPORT" else "short"
            if response.direction.value != expected_direction:
                ctx.emit(
                    {
                        "kind": "opportunity_suppressed_pd_role_direction",
                        "interaction_id": interaction.interaction_id,
                        "template_id": template.template_id,
                        "role": role,
                        "direction": response.direction.value,
                        "expected_direction": expected_direction,
                        "response_evidence_ids": list(response.evidence_ids),
                        "observed_at": response.observed_at,
                        "available_at": response.available_at,
                        "reason": "pd_role_direction_mismatch",
                    },
                    bar_events,
                )
                return
        if (
            response.direction is not template.direction
            and str(interaction.location) in {"PD_LOW", "PD_HIGH"}
        ):
            template = replace(template, direction=response.direction)
            observation = replace(observation, template=template)
        if identity is None:
            raise RuntimeError("resolved opportunity is missing its causal identity")
        duplicate_of = ctx.first_lifecycle_for_identity(identity)
        if duplicate_of is not None:
            ctx.emit(
                {
                    "kind": "opportunity_suppressed_duplicate",
                    "causal_identity": identity,
                    "reason": (
                        "duplicate_causal_identity"
                    ),
                    "interaction_id": interaction.interaction_id,
                    "template_id": template.template_id,
                    "direction": response.direction.value,
                    "response_type": template.response_type.value,
                    "reference_level": response.reference_level,
                    "evidence_ids": list(response.evidence_ids),
                    "formation_at": response.observed_at,
                    "formation_available_at": confirmation_available_at,
                    "duplicate_of_lifecycle_id": duplicate_of,
                    "observed_at": latest.end_time,
                    "available_at": latest.available_at,
                },
                bar_events,
            )
            return
        confirmation_clock = canonical_timestamp(confirmation_available_at)
        setup_expires_at = (
            f"{nifty_label(confirmation_clock)[:10]}T{template.expiry_at}"
        )
        if confirmation_clock >= canonical_timestamp(setup_expires_at):
            ctx.emit({
                "kind": "opportunity_suppressed_expired",
                "interaction_id": interaction.interaction_id,
                "template_id": template.template_id,
                "location_id": str(interaction.location),
                "response_available_at": response.available_at,
                "confirmation_available_at": confirmation_available_at,
                "setup_expires_at": setup_expires_at,
                "reason": "response_available_at_or_after_setup_expiry",
                "observed_at": latest.end_time,
                "available_at": latest.available_at,
            }, bar_events)
            return
        life = self.form_response_generation(
            response, observation, interaction, interaction_context, latest,
            confirmation_evidence,
            confirmation_available_at, row,
            bar_events, identity,
        )
        if life is not None:
            if arbitration is not None and not arbitration.selected:
                self.host.execution.record_admission(
                    life,
                    ExecutionAdmission(
                        AdmissionStatus.DENIED,
                        arbitration.arbitration_reason or "hypothesis_suppressed",
                        serialize_utc(canonical_timestamp(confirmation_available_at)),
                        None,
                    ),
                    bar_events,
                )
            else:
                self.host.execution.admit_opportunity(life, next_time, bar_events)

    def form_response_generation(
        self,
        response: DirectionalResponse,
        observation: OpportunityOutcome,
        confirmed_interaction: InteractionSnapshot,
        strategy_context: MarketContext | None,
        latest: Aggregate,
        confirmation_evidence: dict[str, Any],
        confirmation_available_at: datetime | str,
        row: dict[str, Any],
        bar_events: list[dict[str, Any]],
        identity: str,
    ) -> SetupOpportunity | None:
        ctx = self.host.context
        direction = response.direction.value
        template = observation.template
        if template is None:
            return None
        local_scores = build_context_scores(
            latest, raw_rows=ctx.rows, aggregates=ctx.aggregates,
            atr=ctx.atr_by_session.get(latest.session_date),
        )
        local_long_score, local_short_score = local_scores if local_scores is not None else (None, None)
        score_margin = ctx.execution_constraints.fill_permission_margin
        own_score = local_long_score if direction == "long" else local_short_score
        opposing_score = local_short_score if direction == "long" else local_long_score
        if score_margin is not None and (
            own_score is None
            or opposing_score is None
            or own_score - opposing_score < score_margin
        ):
            ctx.emit(
                {
                    "kind": "setup_score_rejected",
                    "direction": direction,
                    "long_score": local_long_score,
                    "short_score": local_short_score,
                    "score_margin": score_margin,
                    "reason": (
                        "setup_scores_unavailable"
                        if own_score is None or opposing_score is None
                        else "setup_score_mismatch"
                    ),
                    "observed_at": latest.end_time,
                    "available_at": confirmation_available_at,
                },
                bar_events,
            )
            return None
        relation = ContextRelation.NEUTRAL
        if strategy_context is not None:
            relation = classify_context_relation(
                parent=strategy_context,
                location=strategy_context.location,
                direction=direction,
                evidence={"response_type": template.response_type.value},
            )
            ctx.child_observation = ContextObservation(
                context_id=strategy_context.context_id,
                context_version=strategy_context.version,
                observed_at=latest.time,
                available_at=confirmation_available_at,
                relation=relation,
                source_timeframe=STRUCTURE_TIMEFRAME,
                evidence={
                    "direction": direction,
                    "response_type": template.response_type.value,
                    "completed_bar": True,
                    "confirmation_timeframe": STRUCTURE_TIMEFRAME,
                    "confirmation_bar_count": 1,
                },
            )
            ctx.emit(
                {
                    "kind": "context_relation",
                    "level": STRUCTURE_TIMEFRAME,
                    "context_id": strategy_context.context_id,
                    "context_version": strategy_context.version,
                    "relation": relation.value,
                    "observed_at": latest.time,
                    "available_at": confirmation_available_at,
                    "parent_long_score": strategy_context.long_score,
                    "parent_short_score": strategy_context.short_score,
                    "long_score": local_long_score,
                    "short_score": local_short_score,
                    "confirmation_timeframe": STRUCTURE_TIMEFRAME,
                    "confirmation_bar_count": 1,
                    "owner": STRUCTURE_TIMEFRAME,
                    "evidence_timeframe": STRUCTURE_TIMEFRAME,
                },
                bar_events,
            )
        formation_bar = response.evidence[-1] if response.evidence else latest
        ctx.setup_number += 1
        symbol = row.get("symbol", "NIFTY")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("bar symbol must be a non-empty string")
        setup_id = (
            f"{row['session_date']}-{symbol}-{TIMEFRAME_VARIANT}-{direction}"
            f"-candidate-{ctx.setup_number}"
        )
        parent = strategy_context
        life, setup = form_opportunity(
            response,
            OpportunityContext(
                lifecycle_id=f"lifecycle-{setup_id}",
                session_date=str(row["session_date"]), instrument=symbol,
                strategy=f"orderflow-{TIMEFRAME_VARIANT}", candidate_id=setup_id,
                parent_context=(
                    ContextVersionRef(parent.context_id, parent.version)
                    if parent else None
                ),
                 context_candle=(
                     response.evidence[-2] if len(response.evidence) > 1 else formation_bar
                 ),
                clock=canonical_timestamp(confirmation_available_at),
                regime_version=ctx.regime.version if ctx.regime is not None else "unresolved",
                policy_version=observation.policy_version or template.policy_version,
                reference_generation=confirmed_interaction.reference_generation,
                causal_identity=identity,
                location_interaction_id=confirmed_interaction.interaction_id,
            ),
            template,
            formation_available_at=confirmation_available_at,
            decision_context_required=False,
        )
        formation_available_at = life.formation_available_at
        if not self._register_candidate(life, bar_events):
            return None
        self._attach_setup(life, setup)
        ctx.emit(
            {
                "kind": "candidate_formed",
                "template_id": template.template_id,
                "interaction_id": confirmed_interaction.interaction_id,
                "lifecycle_id": life.lifecycle_id,
                "parent_context": ctx.snapshot(strategy_context),
                "observed_at": response.observed_at,
                "available_at": confirmation_available_at,
                "stage": "CANDIDATE_FORMED",
                "evidence": [asdict(bar) for bar in response.evidence],
                "confirmation_available_at": confirmation_available_at,
                "confirmation_timeframe": STRUCTURE_TIMEFRAME,
                "confirmation_bar_count": 1,
            },
            bar_events,
            life,
        )
        opposite_exposure = _opposite_open_exposure(ctx, life.direction)
        if opposite_exposure is not None:
            ctx.emit(
                {
                    "kind": "competing_opportunity_observed",
                    "from_lifecycle_id": opposite_exposure.lifecycle_id,
                    "to_lifecycle_id": life.lifecycle_id,
                    "observed_at": latest.end_time,
                    "available_at": formation_available_at,
                    "reason": "confirmed_opposite_location_thesis_while_position_open",
                    "execution_transfer": False,
                },
                bar_events,
            )
        lifecycle_event = {
            "kind": "lifecycle_formed",
            "template_id": template.template_id,
            "location": getattr(life.location, "value", life.location),
            "level": STRUCTURE_TIMEFRAME,
            "observed_at": response.observed_at,
            "available_at": formation_available_at,
            "candidate_id": setup_id,
            "parent_context": ctx.snapshot(parent),
            "long_score": parent.long_score if parent else None,
            "short_score": parent.short_score if parent else None,
            "reference_level": life.reference_level,
            "failure_boundary": life.failure_boundary,
            "context_candle_id": life.context_candle.time if life.context_candle else None,
            "context_candle_reference": candle_reference(life.context_candle) if life.context_candle else None,
            "context_candle_available_at": (
                life.context_candle.available_at if life.context_candle else None
            ),
            "response_candle_id": response.evidence_ids[-1] if response.evidence_ids else None,
            "response_candle_reference": candle_reference(response.evidence[-1]) if response.evidence else None,
            "response_observed_at": response.observed_at,
            "response_available_at": response.available_at,
            "response_evidence_ids": list(response.evidence_ids),
            "location_interaction_id": confirmed_interaction.interaction_id,
            "opening_interaction_kind": None,
            "entry_boundary": response.entry_boundary,
            "confirmation_evidence": confirmation_evidence,
        }
        ctx.emit(lifecycle_event, bar_events, life)
        ctx.emit_market_event(
            {
                "kind": "setup_candidate", "level": STRUCTURE_TIMEFRAME,
                "setup_id": setup_id, "direction": direction,
                "template_id": template.template_id,
                "response_type": template.response_type.value,
                "setup_version": f"{setup_id}.v1",
                "regime_version": ctx.regime.version if ctx.regime else None,
                "aggregate_time": latest.time,
                "observed_at": response.observed_at,
                "available_at": formation_available_at,
                "predicates": {
                    "location_interaction": confirmed_interaction.interaction_id is not None,
                    "completed_bar": True,
                    "confirmation_timeframe": STRUCTURE_TIMEFRAME,
                    "confirmation_bar_count": 1,
                    "confirmation_available_at": confirmation_available_at,
                },
                "owner": STRUCTURE_TIMEFRAME,
                "evidence_timeframe": STRUCTURE_TIMEFRAME,
                "context_id": parent.context_id if parent else None,
                "context_version": parent.version if parent else None,
                 "context_relation": relation.value,
                 "response_candle_id": response.evidence_ids[-1] if response.evidence_ids else None,
                 "response_observed_at": response.observed_at,
                 "response_available_at": response.available_at,
                 "response_evidence_ids": list(response.evidence_ids),
                 "location_interaction_id": confirmed_interaction.interaction_id,
                 "opening_interaction_kind": None,
                 "entry_boundary": response.entry_boundary,
                 "long_score": parent.long_score if parent else None,
                "short_score": parent.short_score if parent else None,
                "local_long_score": local_long_score,
                "local_short_score": local_short_score,
                "lifecycle_id": life.lifecycle_id,
                "lifecycle_stage": life.stage.value,
                "thesis_type": "structural_candidate",
            },
            LIQUIDITY_PROBE,
            bar_events,
            life,
        )
        return life
