"""Diagnostic orderflow strategy for causal location observations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import hashlib
import json

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.contracts import MarketRole
from ftx_paper.market.bundles import DecisionBundle

from .coordinator import SharedDecisionCoordinator
from .orderflow_contract import OrderflowObservation
from .orderflow_opportunity import (
    ExecutionGroupRegistry, OpportunityStatus, SetupOpportunity,
    advance_timeouts_before_formation,
)
from .orderflow_admission import evaluate_admission
from .orderflow_capture import CaptureMode
from .orderflow_interaction import InteractionCloseReason, InteractionDetector, InteractionStatus
from .orderflow_location import LocationBand, LocationObservation, LocationRecord
from .orderflow_response import ResponseOutcomeStatus, evaluate_response_window
from .policy_contract import ExecutionMode, FeatureRequirement, PolicyDecision
from .protocol import StrategyMetadata
from .snapshot_envelope import unwrap_snapshot, wrap_snapshot


ORDERFLOW_NAME = "ftx-paper-orderflow"
ORDERFLOW_VERSION = "0.1.0"


@dataclass(frozen=True, slots=True)
class OrderflowConfig:
    name: str = ORDERFLOW_NAME
    version: str = ORDERFLOW_VERSION
    enabled_timeframes: tuple[str, ...] = ("1m", "5m")
    session_windows: tuple[tuple[str, str], ...] = (("09:15", "15:30"),)
    response_timeframe: str = "5m"
    session_reset: str = "trading_date"
    response_type: str = "confirmed_rejection"
    response_window_candles: int = 3
    confirmation_rule: str = "completed_response_candle"
    invalidation_rule: str = "close_outside_frozen_band"
    trigger_type: str = "diagnostic_touch"
    admission_policy_version: str = "diagnostic_v1"
    stop_policy: str = "shared_adaptive_stop"
    exit_policy: str = "shared_exit_state_machine"
    feature_set: tuple[str, ...] = ("close", "volume")
    enabled_vehicles: tuple[str, ...] = ("futures",)
    execution_mode: ExecutionMode = ExecutionMode.DIAGNOSTIC
    timezone: str = "Asia/Kolkata"
    market_session: tuple[str, str] = ("09:15", "15:30")
    vehicle: str = "futures"
    replay_mode: str = "causal"
    capture_mode: CaptureMode = CaptureMode.FULL

    def __post_init__(self) -> None:
        if self.response_timeframe not in self.enabled_timeframes:
            raise ValueError("response timeframe must be enabled")
        if not self.feature_set or len(self.feature_set) != len(set(self.feature_set)):
            raise ValueError("orderflow feature_set must be non-empty and unique")
        if "futures" not in self.enabled_vehicles:
            raise ValueError("orderflow requires futures as the enabled vehicle")
        if self.response_window_candles <= 0:
            raise ValueError("response window must be positive")
        if self.execution_mode is not ExecutionMode.DIAGNOSTIC:
            raise ValueError("executable orderflow policy is deferred to a later phase")
        if self.timezone != "Asia/Kolkata":
            raise ValueError("orderflow currently requires the Asia/Kolkata timezone")
        if len(self.market_session) != 2 or self.market_session[0] >= self.market_session[1]:
            raise ValueError("market_session must be an increasing start/end pair")
        if self.vehicle != "futures":
            raise ValueError("orderflow currently supports futures only")
        if self.replay_mode != "causal":
            raise ValueError("orderflow currently requires causal replay mode")

    @property
    def config_hash(self) -> str:
        payload = asdict(self)
        payload["execution_mode"] = self.execution_mode.value
        payload["capture_mode"] = self.capture_mode.value
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _number(observation: OrderflowObservation, name: str) -> float | None:
    value = observation.features.get(name)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _decision_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()[:24]


class OrderflowPolicy:
    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.config = config
        self._session_id: str | None = None
        self._processed_observation_ids: set[str] = set()
        self._processed_location_observation_ids: set[str] = set()
        self._location_detector = InteractionDetector("5m")
        self._admission_suspended = False
        self._gap_end: datetime | None = None
        self._opportunities: dict[str, SetupOpportunity] = {}
        self._execution_groups = ExecutionGroupRegistry()

    @property
    def execution_mode(self) -> ExecutionMode:
        return self.config.execution_mode

    @property
    def required_features(self) -> tuple[FeatureRequirement, ...]:
        return tuple(
            FeatureRequirement(name, timeframe=self.config.response_timeframe)
            for name in self.config.feature_set
        )

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext) -> tuple[PolicyDecision, ...]:
        orderflow = bundle.orderflow_context
        base = {
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
                                    "decision_id": _decision_id("unavailable", "context", bundle.bundle_id)}),)
        session_transitions: list[PolicyDecision] = []
        if self._session_id != orderflow.session_id:
            session_transitions = [
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
                for item in self._location_detector.close_all(
                    InteractionCloseReason.SESSION_END, context.as_of,
                )
            ]
            self._processed_observation_ids.clear()
            self._processed_location_observation_ids.clear()
            self._location_detector = InteractionDetector("5m")
            self._admission_suspended = False
            self._gap_end = None
            self._opportunities.clear()
            self._session_id = orderflow.session_id

        observations = orderflow.observations_for(self.config.response_timeframe)
        if not observations:
            return (*session_transitions, PolicyDecision({**base, "outcome": "unavailable", "reason": "required_features_unavailable",
                                    "decision_id": _decision_id("unavailable", "features", bundle.bundle_id)}))

        results: list[PolicyDecision] = list(session_transitions)
        futures = bundle.bars.get(MarketRole.FUTURES)
        expected_instrument = futures.instrument.symbol if futures is not None else None
        for observation in observations:
            if observation.observation_id in self._processed_observation_ids:
                continue
            if observation.timeframe != self.config.response_timeframe:
                continue
            if expected_instrument is not None and observation.instrument != expected_instrument:
                continue
            if observation.available_at > min(context.as_of, orderflow.cutoff):
                continue
            if observation.context is None or observation.context.session_id != orderflow.session_id:
                continue
            gap_status = observation.context.replay_provenance.get("gap_status")
            if gap_status == "source_gap":
                self._admission_suspended = True
                self._gap_end = observation.interval_end or observation.available_at
                for opportunity_id, opportunity in tuple(self._opportunities.items()):
                    if opportunity.status is not OpportunityStatus.ACTIVE:
                        continue
                    invalidated = opportunity.invalidate(
                        at=observation.available_at,
                        reason="source_gap",
                        evidence_ids=(observation.observation_id,),
                    )
                    self._opportunities[opportunity_id] = invalidated
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
                        "reason": "opportunity_invalidated_by_source_gap",
                        "decision_id": _decision_id(
                            "lifecycle", opportunity.lifecycle_id,
                            observation.observation_id, "source_gap",
                        ),
                        "opportunity_id": opportunity.lifecycle_id,
                        "setup_id": opportunity.setup.setup_id,
                        "source_observation_ids": [observation.observation_id],
                    }))
                for interaction in self._location_detector.observe_gap(observation.available_at):
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
                        "reason": InteractionCloseReason.DATA_GAP.value,
                        "decision_id": _decision_id(
                            "lifecycle", interaction.interaction_id,
                            observation.observation_id, "gap",
                        ),
                        "location": interaction.location_alias,
                        "interaction_id": interaction.interaction_id,
                        "reference_generation": interaction.reference_generation,
                        "interaction_state": InteractionStatus.ENDED.value,
                        "source_observation_ids": [observation.observation_id],
                    }))
            elif (
                self._admission_suspended
                and gap_status == "none"
                and observation.timeframe == "5m"
                and self._gap_end is not None
                and observation.available_at >= self._gap_end
            ):
                self._admission_suspended = False
                self._gap_end = None
            if not observation.location_observations and not orderflow.validate_complete_parent(observation):
                continue
            self._processed_observation_ids.add(observation.observation_id)
            for location_observation in observation.location_observations:
                if not isinstance(location_observation, LocationObservation):
                    raise ValueError("invalid causal location observation")
                location = LocationRecord(
                    location_observation.location_id,
                    location_observation.location_alias,
                    location_observation.reference,
                    LocationBand(location_observation.band_lower, location_observation.band_upper),
                    location_observation.reference_generation,
                    location_observation.available_at,
                    location_observation.source_aggregate_id,
                    timeframe=observation.timeframe,
                    source_identity=observation.instrument,
                )
                closed_before = len(self._location_detector.closed_interactions)
                interaction = self._location_detector.observe(
                    location,
                    observation,
                    band_contact=location_observation.band_contact,
                    observation_id=location_observation.observation_id,
                )
                location_key = location_observation.observation_id
                if location_key in self._processed_location_observation_ids:
                    continue
                self._processed_location_observation_ids.add(location_key)
                for ended in self._location_detector.closed_interactions[closed_before:]:
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
                        "reason": ended.close_reason.value if ended.close_reason else "interaction_ended",
                        "decision_id": _decision_id(
                            "lifecycle", ended.interaction_id, location_key, "ended",
                        ),
                        "location": ended.location_alias,
                        "interaction_id": ended.interaction_id,
                        "reference_generation": ended.reference_generation,
                        "response_state": "ENDED",
                        "source_observation_ids": [location_key],
                    }))
                if interaction is not None and interaction.status is InteractionStatus.ACTIVE:
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "opportunity",
                        "outcome": "recorded",
                        "reason": "location_interaction_observed",
                        "decision_id": _decision_id("opportunity", interaction.interaction_id, location_key),
                        "location": interaction.location_alias,
                        "interaction_id": interaction.interaction_id,
                        "reference_generation": interaction.reference_generation,
                        "source_observation_ids": [location_key],
                        "observed_at": location_observation.observed_at.isoformat(),
                        "available_at": location_observation.available_at.isoformat(),
                        "feature_values": dict(observation.features),
                    }))
            for interaction in self._location_detector.active_interactions:
                response_observations = tuple(
                    item for item in observations
                    if item.observed_at > interaction.created_at
                    and item.available_at <= min(context.as_of, orderflow.cutoff)
                )
                if not response_observations:
                    continue
                evaluation = evaluate_response_window(
                    interaction,
                    response_observations,
                    max_candles=self.config.response_window_candles,
                    evaluation_cutoff=min(context.as_of, orderflow.cutoff),
                )
                if evaluation.status is ResponseOutcomeStatus.UNAVAILABLE:
                    continue
                next_status = evaluation.status.value
                if next_status == "PENDING":
                    next_status = "PENDING_RESPONSE"
                if next_status == interaction.response_status:
                    continue
                updated = self._location_detector.update_response(
                    interaction.interaction_id, next_status,
                )
                if updated is None:
                    continue
                results.append(PolicyDecision({
                    **base,
                    "decision_stage": "response",
                    "outcome": "recorded" if next_status == "CONFIRMED" else "rejected",
                    "reason": evaluation.reason or next_status.lower(),
                    "decision_id": _decision_id(
                        "response", interaction.interaction_id,
                        evaluation.confirmation_observation_id or observation.observation_id,
                        next_status,
                    ),
                    "location": interaction.location_alias,
                    "interaction_id": interaction.interaction_id,
                    "reference_generation": interaction.reference_generation,
                    "response_state": next_status,
                    "response_evidence_ids": list(evaluation.evidence_ids),
                    "source_observation_ids": list(evaluation.evidence_ids),
                }))
                if next_status == "CONFIRMED":
                    formed_at = evaluation.confirmation_available_at or observation.available_at
                    direction = evaluation.direction or "long"
                    opportunity = SetupOpportunity.create(
                        interaction_id=interaction.interaction_id,
                        direction=direction,
                        formed_at=formed_at,
                        expires_at=formed_at + timedelta(minutes=self.config.response_window_candles),
                        entry_lower=interaction.band_lower or interaction.reference_level,
                        entry_upper=interaction.band_upper or interaction.reference_level,
                        failure_boundary=(
                            interaction.band_lower if direction == "long" else interaction.band_upper
                        ) or interaction.reference_level,
                        evidence_ids=evaluation.evidence_ids,
                        evidence_timeframe=interaction.timeframe,
                        regime_version=observation.context.generation if observation.context else None,
                    )
                    if opportunity.lifecycle_id not in self._opportunities:
                        admission = evaluate_admission(
                            opportunity,
                            context_available=True,
                            regime_allowed=not self._admission_suspended,
                            opening_gap_allowed=not self._admission_suspended,
                            active_position_allowed=context.exposure.open_positions == 0,
                            exposure_allowed=context.exposure.net_directional_lots == 0,
                            allocation_allowed=context.margin.available > 0,
                            sizing_allowed=True,
                            risk_allowed=context.capital.daily_loss >= 0,
                            setup_valid=True,
                            direction_matches=True,
                            confirmations_available=True,
                            current_at=formed_at,
                        )
                        self._opportunities[opportunity.lifecycle_id] = opportunity
                        claim = self._execution_groups.claim(
                            opportunity.execution_group_id,
                            opportunity.lifecycle_id,
                            evidence_ids=evaluation.evidence_ids,
                        )
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "admission",
                            "outcome": "accepted" if admission.status.value == "PASSED" and claim.accepted else "rejected",
                            "reason": "orderflow_admission",
                            "decision_id": admission.decision_id,
                            "opportunity_id": opportunity.lifecycle_id,
                            "setup_id": opportunity.setup.setup_id,
                            "interaction_id": interaction.interaction_id,
                            "location": interaction.location_alias,
                            "direction": direction,
                            "admission_status": admission.status.value,
                            "admission_reason": admission.reason,
                            "admission_predicates": [asdict(item) for item in admission.predicates],
                            "source_observation_ids": list(evaluation.evidence_ids),
                        }))
            previous_opportunities = dict(self._opportunities)
            self._opportunities = {
                key: advance_timeouts_before_formation(
                    (item,), timeframe=observation.timeframe,
                    at=observation.available_at,
                    evidence_id=observation.observation_id,
                )[0]
                for key, item in self._opportunities.items()
            }
            for opportunity_id, opportunity in self._opportunities.items():
                previous_opportunity = previous_opportunities.get(opportunity_id)
                if previous_opportunity is None or len(opportunity.transitions) <= len(previous_opportunity.transitions):
                    continue
                for transition in opportunity.transitions[len(previous_opportunity.transitions):]:
                    if transition.status is not OpportunityStatus.EXPIRED:
                        continue
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
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
            if self._admission_suspended:
                results.append(PolicyDecision({
                    **base,
                    "outcome": "unavailable",
                    "reason": "admission_suspended_after_source_gap",
                    "decision_id": _decision_id("unavailable", "gap", observation.observation_id),
                    "source_observation_ids": [observation.observation_id],
                    "expected_available_at": self._gap_end.isoformat() if self._gap_end else None,
                }))
                continue
        if not results and all(observation.observation_id in self._processed_observation_ids for observation in observations):
            return ()
        return tuple(results)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        # Diagnostic mode cannot own execution state.
        return None

    def snapshot(self) -> Mapping[str, object]:
        return {
            "schema_version": 2,
            "config": asdict(self.config) | {"execution_mode": self.config.execution_mode.value},
            "session_id": self._session_id,
            "processed_observation_ids": sorted(self._processed_observation_ids),
            "processed_location_observation_ids": sorted(self._processed_location_observation_ids),
            "location_detector": self._location_detector.snapshot(),
            "admission_suspended": self._admission_suspended,
            "gap_end": self._gap_end.isoformat() if self._gap_end else None,
            "opportunities": [self._opportunities[key].snapshot() for key in sorted(self._opportunities)],
            "execution_groups": self._execution_groups.snapshot(),
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "OrderflowPolicy":
        if snapshot.get("schema_version") != 2:
            raise ValueError("unsupported orderflow snapshot schema")
        raw_config = snapshot.get("config")
        if not isinstance(raw_config, Mapping):
            raise ValueError("orderflow snapshot config must be an object")
        values = dict(raw_config)
        allowed_config = set(OrderflowConfig.__dataclass_fields__)
        values.setdefault("timezone", "Asia/Kolkata")
        values.setdefault("market_session", ("09:15", "15:30"))
        values.setdefault("vehicle", "futures")
        values.setdefault("replay_mode", "causal")
        values.setdefault("capture_mode", CaptureMode.FULL)
        if set(values) != allowed_config:
            raise ValueError("orderflow snapshot config fields are invalid")
        values["execution_mode"] = ExecutionMode(values.get("execution_mode", "diagnostic"))
        values["capture_mode"] = CaptureMode(values.get("capture_mode", CaptureMode.FULL))
        try:
            policy = cls(OrderflowConfig(**values))
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid orderflow snapshot config") from exc
        session_id = snapshot.get("session_id")
        if session_id is not None and not isinstance(session_id, str):
            raise ValueError("invalid orderflow snapshot session_id")
        policy._session_id = session_id
        raw_processed = snapshot.get("processed_observation_ids", ())
        if not isinstance(raw_processed, (list, tuple)) or any(not isinstance(item, str) for item in raw_processed):
            raise ValueError("invalid processed orderflow observation IDs")
        policy._processed_observation_ids = set(raw_processed)
        raw_location_processed = snapshot.get("processed_location_observation_ids")
        if not isinstance(raw_location_processed, (list, tuple)) or any(
            not isinstance(item, str) for item in raw_location_processed
        ):
            raise ValueError("invalid processed location observation IDs")
        policy._processed_location_observation_ids = set(raw_location_processed)
        raw_location_detector = snapshot.get("location_detector")
        if not isinstance(raw_location_detector, Mapping):
            raise ValueError("invalid orderflow location detector snapshot")
        policy._location_detector = InteractionDetector.from_snapshot(raw_location_detector)
        admission_suspended = snapshot.get("admission_suspended")
        gap_end = snapshot.get("gap_end")
        if "gap_end" not in snapshot or not isinstance(admission_suspended, bool):
            raise ValueError("invalid orderflow admission suspension state")
        if gap_end is not None and not isinstance(gap_end, str):
            raise ValueError("invalid orderflow gap end")
        policy._admission_suspended = admission_suspended
        policy._gap_end = datetime.fromisoformat(gap_end) if gap_end is not None else None
        raw_opportunities = snapshot.get("opportunities")
        if not isinstance(raw_opportunities, (list, tuple)):
            raise ValueError("invalid orderflow opportunities")
        for item in raw_opportunities:
            if not isinstance(item, Mapping):
                raise ValueError("invalid orderflow opportunity")
            opportunity = SetupOpportunity.from_snapshot(item)
            if opportunity.lifecycle_id in policy._opportunities:
                raise ValueError("duplicate orderflow opportunity ID")
            policy._opportunities[opportunity.lifecycle_id] = opportunity
        raw_execution_groups = snapshot.get("execution_groups")
        if not isinstance(raw_execution_groups, Mapping):
            raise ValueError("invalid orderflow execution group snapshot")
        policy._execution_groups = ExecutionGroupRegistry.from_snapshot(raw_execution_groups)
        return policy


class OrderflowStrategy:
    """Strategy facade preserving the public Strategy protocol."""

    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.policy = OrderflowPolicy(config)
        self.coordinator = SharedDecisionCoordinator(self.policy)

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
        return self.coordinator.evaluate(bundle, context)

    @staticmethod
    def _fallback_cutoff(bundle: DecisionBundle):
        from ftx_paper.config import IST

        raw = bundle.minute if "T" in bundle.minute else f"{bundle.trading_date}T{bundle.minute}"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
        return parsed

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext):
        return self.coordinator.evaluate(bundle, context)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.coordinator.on_execution_event(event)

    def snapshot(self) -> Mapping[str, object]:
        return wrap_snapshot(
            strategy_id=ORDERFLOW_NAME,
            metadata={"name": self.metadata.name, "version": self.metadata.version,
                      "config_hash": self.metadata.config_hash},
            payload=self.policy.snapshot(),
        )

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object], *, capital_profile=None) -> "OrderflowStrategy":
        _, payload = unwrap_snapshot(snapshot)
        strategy = cls.__new__(cls)
        strategy.policy = OrderflowPolicy.from_snapshot(payload)
        strategy.coordinator = SharedDecisionCoordinator(strategy.policy)
        return strategy


__all__ = [
    "OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy",
    "ORDERFLOW_NAME", "ORDERFLOW_VERSION",
]
