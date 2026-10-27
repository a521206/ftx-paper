"""Diagnostic orderflow strategy for causal VWAP interaction observations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import hashlib
import json
import math
from dataclasses import replace
from typing import cast

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.contracts import MarketRole
from ftx_paper.market.bundles import DecisionBundle
from ftx_paper.market.location import Location, LocationDetector

from .coordinator import SharedDecisionCoordinator
from .orderflow_contract import OrderflowObservation, stable_orderflow_id
from .orderflow_trigger import observe_trigger
from .orderflow_opportunity import (
    ExecutionGroupRegistry, OpportunityStatus, SetupOpportunity,
    advance_timeouts_before_formation,
)
from .orderflow_admission import evaluate_admission
from .orderflow_capture import CaptureMode
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
    vwap_calculation_version: str = "typical_price_volume_v1"
    price_input: str = "typical_price"
    volume_input: str = "volume"
    session_reset: str = "trading_date"
    interaction_band: float = 5.0
    response_type: str = "confirmed_rejection"
    response_window_candles: int = 3
    confirmation_rule: str = "completed_response_candle"
    invalidation_rule: str = "close_outside_frozen_band"
    trigger_type: str = "diagnostic_touch"
    admission_policy_version: str = "diagnostic_v1"
    stop_policy: str = "shared_adaptive_stop"
    exit_policy: str = "shared_exit_state_machine"
    feature_set: tuple[str, ...] = ("close", "vwap", "volume")
    enabled_vehicles: tuple[str, ...] = ("futures",)
    execution_mode: ExecutionMode = ExecutionMode.DIAGNOSTIC
    timezone: str = "Asia/Kolkata"
    market_session: tuple[str, str] = ("09:15", "15:30")
    vehicle: str = "futures"
    replay_mode: str = "causal"
    capture_mode: CaptureMode = CaptureMode.FULL

    def __post_init__(self) -> None:
        if self.interaction_band <= 0:
            raise ValueError("interaction band must be positive")
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


@dataclass(frozen=True, slots=True)
class InteractionState:
    interaction_id: str
    timeframe: str
    instrument: str
    reference_level: float
    lower_band: float
    upper_band: float
    source_observation_id: str
    state: str
    response_state: str
    generation: int | None
    location: str | None = None
    response_direction: str | None = None
    response_armed: bool = False
    response_evidence_ids: tuple[str, ...] = ()
    response_age: int = 0

    def __post_init__(self) -> None:
        if self.state not in {"CREATED", "ACTIVE", "ENDED"}:
            raise ValueError("invalid orderflow interaction state")
        if self.response_state not in {"PENDING_RESPONSE", "CONFIRMED", "INVALIDATED", "EXPIRED"}:
            raise ValueError("invalid orderflow response state")
        if not self.source_observation_id:
            raise ValueError("orderflow interaction source observation is required")
        if not self.instrument.strip():
            raise ValueError("orderflow interaction instrument is required")
        if not all(math.isfinite(value) for value in (self.reference_level, self.lower_band, self.upper_band)):
            raise ValueError("orderflow interaction geometry must be finite")
        if self.lower_band > self.reference_level or self.reference_level > self.upper_band:
            raise ValueError("orderflow interaction band is inconsistent")
        if self.response_direction not in {None, "long", "short"}:
            raise ValueError("invalid orderflow response direction")


def _number(observation: OrderflowObservation, name: str) -> float | None:
    value = observation.features.get(name)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _decision_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()[:24]


def _response_candle(observation: OrderflowObservation) -> tuple[float, float, float, float] | None:
    close = _number(observation, "close")
    if close is None:
        return None
    previous = _number(observation, "previous_close")
    open_price = _number(observation, "open")
    high = _number(observation, "high")
    low = _number(observation, "low")
    open_price = previous if open_price is None else open_price
    if open_price is None:
        open_price = close
    high = max(open_price, close) if high is None else high
    low = min(open_price, close) if low is None else low
    return open_price, high, low, close


def _advance_response(
    interaction: InteractionState, observation: OrderflowObservation, *, max_candles: int,
) -> InteractionState:
    candle = _response_candle(observation)
    if candle is None:
        return interaction
    open_price, high, low, close = candle
    direction = interaction.response_direction
    if direction is None:
        previous = _number(observation, "previous_close")
        direction = "long" if previous is None or previous <= interaction.reference_level else "short"
    armed = interaction.response_armed or low <= interaction.reference_level <= high
    evidence_items = observation.evidence_ids or (observation.observation_id,)
    evidence = (*interaction.response_evidence_ids, *evidence_items)
    status = interaction.response_state
    age = interaction.response_age + 1
    if status == "PENDING_RESPONSE":
        if direction == "long" and close < interaction.lower_band:
            status = "INVALIDATED"
        elif direction == "short" and close > interaction.upper_band:
            status = "INVALIDATED"
        elif armed and direction == "long" and close >= interaction.reference_level and close > open_price:
            status = "CONFIRMED"
        elif armed and direction == "short" and close <= interaction.reference_level and close < open_price:
            status = "CONFIRMED"
        elif age >= max_candles:
            status = "EXPIRED"
    return replace(
        interaction,
        response_state=status,
        response_direction=direction,
        response_armed=armed,
        response_evidence_ids=evidence,
        response_age=age,
    )


class OrderflowPolicy:
    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.config = config
        self._interactions: dict[str, InteractionState] = {}
        self._session_id: str | None = None
        self._processed_observation_ids: set[str] = set()
        self._processed_location_observation_ids: set[str] = set()
        self._location_interactions: dict[str, dict[str, object]] = {}
        self._location_detector = LocationDetector()
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
            "feature_version": self.config.vwap_calculation_version,
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
            session_transitions = [PolicyDecision({
                **base, "outcome": "recorded", "reason": "session_boundary",
                "decision_id": _decision_id("lifecycle", item.interaction_id, bundle.trading_date, "session_end"),
                "interaction_id": item.interaction_id, "interaction_state": "ENDED",
                "decision_stage": "lifecycle",
            }) for item in self._interactions.values() if item.state in {"CREATED", "ACTIVE"}]
            self._interactions.clear()
            self._processed_observation_ids.clear()
            self._processed_location_observation_ids.clear()
            self._location_interactions.clear()
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
        location_features = (
            bundle.feature_snapshot.location_features
            if bundle.feature_snapshot is not None else None
        )
        if self._session_id == orderflow.session_id and not self._processed_observation_ids:
            self._location_detector.reset(
                prior_day_high=(
                    float(location_features.prior_day_high)
                    if location_features is not None
                    and isinstance(location_features.prior_day_high, (int, float))
                    and not isinstance(location_features.prior_day_high, bool)
                    else None
                ),
                prior_day_low=(
                    float(location_features.prior_day_low)
                    if location_features is not None
                    and isinstance(location_features.prior_day_low, (int, float))
                    and not isinstance(location_features.prior_day_low, bool)
                    else None
                ),
            )
        if futures is not None:
            location_snapshot = self._location_detector.observe(
                futures, features=location_features,
            )
        else:
            location_snapshot = None
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
            elif (
                self._admission_suspended
                and gap_status == "none"
                and observation.timeframe == "5m"
                and self._gap_end is not None
                and observation.available_at >= self._gap_end
            ):
                self._admission_suspended = False
                self._gap_end = None
            if observation.feature_version != self.config.vwap_calculation_version:
                continue
            if not orderflow.validate_complete_parent(observation):
                continue
            self._processed_observation_ids.add(observation.observation_id)
            close = _number(observation, "close")
            high = _number(observation, "high")
            low = _number(observation, "low")
            if observation.timeframe == "5m" and location_features is not None:
                width = max(float(location_features.atr) * 0.06, 1e-9)
                levels = {
                    Location.OR_HIGH: location_features.opening_range_high,
                    Location.OR_LOW: location_features.opening_range_low,
                    Location.SESSION_HIGH: location_features.session_high,
                    Location.SESSION_LOW: location_features.session_low,
                    Location.PRIOR_DAY_HIGH: location_features.prior_day_high,
                    Location.PRIOR_DAY_LOW: location_features.prior_day_low,
                }
                detected = {
                    location for location, level in levels.items()
                    if isinstance(level, (int, float))
                    and not isinstance(level, bool)
                    and high is not None
                    and low is not None
                    and low <= float(level) + width
                    and high >= float(level) - width
                }
                if location_snapshot is not None:
                    detected.update(
                        location for location in location_snapshot.locations
                        if location in levels
                    )
                for alias, interaction in tuple(self._location_interactions.items()):
                    lower = cast(float, interaction["lower"])
                    upper = cast(float, interaction["upper"])
                    if low is not None and high is not None and low <= upper and high >= lower:
                        continue
                    self._location_interactions.pop(alias, None)
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
                        "reason": "interaction_ended",
                        "decision_id": _decision_id(
                            "lifecycle", interaction["interaction_id"],
                            observation.observation_id, "ended",
                        ),
                        "location": alias,
                        "interaction_id": interaction["interaction_id"],
                        "response_state": "ENDED",
                        "source_observation_ids": [observation.observation_id],
                    }))
                for location in sorted(detected, key=lambda item: item.value):
                    alias = location.value
                    interaction = self._location_interactions.get(alias)
                    level = levels[location]
                    if not isinstance(level, (int, float)) or isinstance(level, bool):
                        continue
                    frozen_level = float(level)
                    if interaction is not None and interaction["level"] != frozen_level:
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "lifecycle",
                            "outcome": "rejected",
                            "reason": "reference_replaced",
                            "decision_id": _decision_id(
                                "lifecycle", interaction["interaction_id"],
                                observation.observation_id, "reference_replaced",
                            ),
                            "location": alias,
                            "interaction_id": interaction["interaction_id"],
                            "response_state": "ENDED",
                            "source_observation_ids": [observation.observation_id],
                        }))
                        interaction = None
                        self._location_interactions.pop(alias, None)
                    if interaction is None:
                        interaction = {
                            "interaction_id": stable_orderflow_id(
                                "interaction", observation.observation_id, alias,
                            ),
                            "level": frozen_level,
                            "lower": frozen_level - width,
                            "upper": frozen_level + width,
                        }
                        self._location_interactions[alias] = interaction
                    location_key = f"{observation.observation_id}:{alias}"
                    if location_key in self._processed_location_observation_ids:
                        continue
                    self._processed_location_observation_ids.add(location_key)
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "opportunity",
                        "outcome": "recorded",
                        "reason": "location_interaction_observed",
                        "decision_id": _decision_id(
                            "opportunity", interaction["interaction_id"],
                            observation.observation_id,
                        ),
                        "location": alias,
                        "interaction_id": interaction["interaction_id"],
                        "source_observation_ids": [observation.observation_id],
                        "observed_at": observation.observed_at.isoformat(),
                        "available_at": observation.available_at.isoformat(),
                        "feature_values": dict(observation.features),
                    }))
            vwap = _number(observation, "vwap")
            if vwap is None or close is None:
                results.append(PolicyDecision({
                    **base,
                    "outcome": "unavailable",
                    "reason": "required_feature_missing",
                    "decision_id": _decision_id("unavailable", "feature", observation.observation_id),
                    "source_observation_ids": [observation.observation_id],
                }))
                continue
            trigger = observe_trigger(
                observation,
                boundary=vwap,
                execution_mode=self.execution_mode.value,
                trigger_type=self.config.trigger_type,
            )
            provenance = {
                "timestamp": observation.observed_at.isoformat(),
                "observed_at": observation.observed_at.isoformat(),
                "available_at": observation.available_at.isoformat(),
                "interval_start": observation.interval_start.isoformat() if observation.interval_start else None,
                "interval_end": observation.interval_end.isoformat() if observation.interval_end else None,
                "source_observation_id": observation.source_observation_id,
                "source_observation_ids": [observation.observation_id],
                "response_evidence_ids": list(observation.evidence_ids),
                "timeframe": observation.timeframe,
                "instrument": observation.instrument,
                "generation": observation.context.generation if observation.context else None,
                "replay_provenance": dict(observation.context.replay_provenance) if observation.context else {},
                "trigger": asdict(trigger),
                "location": observation.location,
                "reference_level": vwap,
                "lower_band": vwap - self.config.interaction_band,
                "upper_band": vwap + self.config.interaction_band,
            }
            if self._admission_suspended:
                results.append(PolicyDecision({
                    **base, **provenance,
                    "outcome": "unavailable",
                    "reason": "admission_suspended_after_source_gap",
                    "decision_id": _decision_id("unavailable", "gap", observation.observation_id),
                    "source_observation_ids": [observation.observation_id],
                    "expected_available_at": self._gap_end.isoformat() if self._gap_end else None,
                }))
                continue
            generation = observation.context.generation if observation.context else None
            previous_opportunities = dict(self._opportunities)
            self._opportunities = {
                key: value for key, value in (
                    (key, advance_timeouts_before_formation((item,), timeframe=observation.timeframe,
                                                            at=observation.available_at,
                                                            evidence_id=observation.observation_id)[0])
                    for key, item in self._opportunities.items()
                )
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
            previous = next(
                (
                    item for item in self._interactions.values()
                    if item.timeframe == observation.timeframe
                    and item.instrument == observation.instrument
                    and item.generation == generation
                    and item.location == observation.location
                    and math.isclose(item.reference_level, vwap, rel_tol=0.0, abs_tol=1e-9)
                    and math.isclose(item.lower_band, vwap - self.config.interaction_band, rel_tol=0.0, abs_tol=1e-9)
                    and math.isclose(item.upper_band, vwap + self.config.interaction_band, rel_tol=0.0, abs_tol=1e-9)
                    and (observation.interaction_id is None or item.interaction_id == observation.interaction_id)
                    and item.state == "ACTIVE"
                ),
                None,
            )
            inside_band = (
                previous.lower_band <= close <= previous.upper_band
                if previous is not None
                else abs(close - vwap) <= self.config.interaction_band
            )
            if previous is None and not inside_band:
                decision_id = hashlib.sha256(
                    f"{self.config.name}:{observation.observation_id}:trigger".encode()
                ).hexdigest()[:24]
                results.append(PolicyDecision({
                    **base,
                    **provenance,
                    "decision_stage": "trigger",
                    "cell": observation.location or "orderflow",
                    "subject": {"vehicle": "futures", "location": observation.location or "orderflow"},
                    "outcome": "rejected",
                    "reason": "no_orderflow_interaction",
                    "decision_id": decision_id,
                    "source_observation_ids": [observation.observation_id],
                }))
                continue
            interaction_id = previous.interaction_id if previous is not None else (
                observation.interaction_id or hashlib.sha256(
                    f"{observation.timeframe}:{observation.instrument}:{observation.location}:"
                    f"{generation}:{observation.observation_id}:{self.config.config_hash}".encode()
                ).hexdigest()[:24]
            )
            if previous is None:
                previous = InteractionState(
                    interaction_id=interaction_id,
                    timeframe=observation.timeframe,
                    instrument=observation.instrument,
                    reference_level=vwap,
                    lower_band=vwap - self.config.interaction_band,
                    upper_band=vwap + self.config.interaction_band,
                    source_observation_id=observation.observation_id,
                    state="CREATED",
                    response_state="PENDING_RESPONSE",
                    generation=observation.context.generation if observation.context else None,
                    location=observation.location,
                )
                self._interactions[interaction_id] = previous
            previous = _advance_response(
                previous, observation, max_candles=self.config.response_window_candles,
            )
            provenance["cell"] = observation.location or "orderflow"
            provenance["direction"] = previous.response_direction
            if not inside_band:
                terminal_response = previous.response_state
                if terminal_response == "PENDING_RESPONSE":
                    terminal_response = (
                        "INVALIDATED"
                        if (
                            (previous.response_direction == "long" and close < previous.lower_band)
                            or (previous.response_direction == "short" and close > previous.upper_band)
                        )
                        else "EXPIRED"
                    )
                ended = replace(previous, state="ENDED", response_state=(
                    terminal_response
                ))
                self._interactions[interaction_id] = ended
                results.append(PolicyDecision({
                    **base,
                    **provenance,
                    "decision_stage": "lifecycle",
                    "cell": observation.location or "orderflow",
                    "subject": {"vehicle": "futures", "location": observation.location or "orderflow",
                                 "direction": previous.response_direction},
                    "outcome": "rejected",
                    "reason": "interaction_ended",
                    "decision_id": _decision_id("lifecycle", interaction_id, observation.observation_id, "ended"),
                    "interaction_id": interaction_id,
                    "interaction_state": ended.state,
                    "response_state": ended.response_state,
                    "source_observation_ids": [observation.observation_id],
                }))
                continue
            previous = replace(previous, state="ACTIVE")
            if previous.response_state == "INVALIDATED":
                self._interactions[interaction_id] = previous
                self._opportunities = {
                    key: value.invalidate(
                        at=observation.available_at, reason="response_invalidated",
                        evidence_ids=(observation.observation_id,),
                    ) if value.response_episode_id == f"response-{interaction_id}" else value
                    for key, value in self._opportunities.items()
                }
                results.append(PolicyDecision({
                    **base, **provenance, "decision_stage": "lifecycle",
                    "cell": observation.location or "orderflow",
                    "subject": {"vehicle": "futures", "location": observation.location or "orderflow",
                                 "direction": previous.response_direction},
                    "outcome": "rejected",
                    "reason": "response_invalidated", "decision_id": _decision_id("response", interaction_id, observation.observation_id, "invalidated"),
                    "interaction_id": interaction_id, "response_state": previous.response_state,
                    "response_evidence_ids": list(previous.response_evidence_ids),
                }))
                continue
            if previous.response_state != "CONFIRMED":
                self._interactions[interaction_id] = previous
                if previous.response_state == "EXPIRED":
                    self._opportunities = {
                        key: value.advance_timeout(
                            timeframe=observation.timeframe, at=observation.available_at,
                            evidence_id=observation.observation_id,
                        ) if value.response_episode_id == f"response-{interaction_id}" else value
                        for key, value in self._opportunities.items()
                    }
                results.append(PolicyDecision({
                    **base,
                    **provenance,
                    "decision_stage": "opportunity",
                    "cell": observation.location or "orderflow",
                    "subject": {"vehicle": "futures", "location": observation.location or "orderflow",
                                 "direction": previous.response_direction},
                    "outcome": "rejected",
                    "reason": "response_expired" if previous.response_state == "EXPIRED" else "response_pending",
                    "decision_id": _decision_id("response", interaction_id, observation.observation_id, previous.response_state),
                    "interaction_id": interaction_id,
                    "response_state": previous.response_state,
                    "response_evidence_ids": list(previous.response_evidence_ids),
                }))
                continue
            previous = replace(previous, response_state="CONFIRMED")
            self._interactions[interaction_id] = previous
            formed_at = observation.available_at
            opportunity = SetupOpportunity.create(
                interaction_id=interaction_id,
                direction=previous.response_direction or "long",
                formed_at=formed_at,
                expires_at=formed_at + timedelta(minutes=self.config.response_window_candles),
                entry_lower=previous.lower_band,
                entry_upper=previous.upper_band,
                failure_boundary=previous.lower_band if previous.response_direction == "long" else previous.upper_band,
                evidence_ids=previous.response_evidence_ids,
                evidence_timeframe=observation.timeframe,
                regime_version=generation,
            )
            admission = evaluate_admission(
                opportunity,
                context_available=orderflow is not None,
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
                current_at=observation.available_at,
            )
            self._opportunities[opportunity.lifecycle_id] = opportunity
            claim = self._execution_groups.claim(
                opportunity.execution_group_id, opportunity.lifecycle_id,
                evidence_ids=opportunity.setup.evidence_ids,
            )
            if not claim.accepted:
                admission = evaluate_admission(
                    opportunity, context_available=orderflow is not None,
                    regime_allowed=not self._admission_suspended,
                    opening_gap_allowed=not self._admission_suspended,
                    active_position_allowed=context.exposure.open_positions == 0,
                    exposure_allowed=context.exposure.net_directional_lots == 0,
                    allocation_allowed=context.margin.available > 0,
                    sizing_allowed=True, risk_allowed=context.capital.daily_loss >= 0,
                    setup_valid=False, current_at=observation.available_at,
                )
            payload = {
                **base,
                **provenance,
                "reason": "orderflow_admission",
                "decision_id": admission.decision_id,
                "opportunity_id": opportunity.lifecycle_id,
                "setup_id": stable_orderflow_id(
                    "setup", interaction_id, observation.observation_id,
                    previous.response_direction, generation, self.config.config_hash,
                ),
                "execution_group_id": stable_orderflow_id(
                    "execution_group", interaction_id, generation, self.config.config_hash,
                ),
                "outcome": "accepted" if admission.status.value == "PASSED" and claim.accepted else "rejected",
                "admission_status": admission.status.value,
                "admission_reason": admission.reason,
                "admission_predicates": [asdict(item) for item in admission.predicates],
                "interaction_id": interaction_id,
                "source_observation_ids": [observation.observation_id],
                "response_evidence_ids": list(observation.evidence_ids),
                "template_id": "orderflow_lifecycle",
                "template_version": "1",
                "feature_values": dict(observation.features),
                "trigger": asdict(trigger),
                "interaction_state": previous.state,
                "response_state": previous.response_state,
                "decision_stage": "admission",
                "cell": observation.location or "orderflow",
                "direction": previous.response_direction,
                "diagnostic_intent": {
                    "intent_id": stable_orderflow_id(
                        "diagnostic_intent", interaction_id, observation.observation_id,
                    ),
                    "opportunity_id": interaction_id,
                    "executable": False,
                    "evidence_ids": list(previous.response_evidence_ids),
                },
                "subject": {"vehicle": "futures", "location": observation.location or "orderflow",
                             "direction": previous.response_direction},
            }
            results.append(PolicyDecision(payload))
            results.append(PolicyDecision({
                **base, **provenance, "decision_stage": "opportunity",
                "outcome": "recorded", "reason": "response_confirmed",
                "decision_id": _decision_id(
                    "opportunity", opportunity.lifecycle_id,
                    observation.observation_id, "resolved",
                ),
                "opportunity_id": opportunity.lifecycle_id,
                "interaction_id": interaction_id,
                "response_state": previous.response_state,
                "source_observation_ids": [observation.observation_id],
            }))
        if not results and all(observation.observation_id in self._processed_observation_ids for observation in observations):
            return ()
        return tuple(results)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        # Diagnostic mode cannot own execution state.
        return None

    def snapshot(self) -> Mapping[str, object]:
        return {
            "schema_version": 1,
            "config": asdict(self.config) | {"execution_mode": self.config.execution_mode.value},
            "interactions": [asdict(self._interactions[key]) for key in sorted(self._interactions)],
            "session_id": self._session_id,
            "processed_observation_ids": sorted(self._processed_observation_ids),
            "processed_location_observation_ids": sorted(self._processed_location_observation_ids),
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "OrderflowPolicy":
        if snapshot.get("schema_version") != 1:
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
        raw_location_processed = snapshot.get("processed_location_observation_ids", ())
        if not isinstance(raw_location_processed, (list, tuple)) or any(
            not isinstance(item, str) for item in raw_location_processed
        ):
            raise ValueError("invalid processed location observation IDs")
        policy._processed_location_observation_ids = set(raw_location_processed)
        raw_interactions = snapshot.get("interactions", ())
        if not isinstance(raw_interactions, (list, tuple)):
            raise ValueError("orderflow snapshot interactions must be a sequence")
        seen_ids: set[str] = set()
        allowed_interaction = set(InteractionState.__dataclass_fields__)
        for item in raw_interactions:
            if not isinstance(item, Mapping):
                raise ValueError("orderflow snapshot interaction must be an object")
            if set(item) != allowed_interaction:
                raise ValueError("orderflow snapshot interaction fields are invalid")
            try:
                interaction = InteractionState(**dict(item))
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid orderflow snapshot interaction") from exc
            if not interaction.interaction_id or interaction.interaction_id in seen_ids:
                raise ValueError("duplicate orderflow snapshot interaction ID")
            seen_ids.add(interaction.interaction_id)
            policy._interactions[interaction.interaction_id] = interaction
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
    "InteractionState", "OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy",
    "ORDERFLOW_NAME", "ORDERFLOW_VERSION",
]
